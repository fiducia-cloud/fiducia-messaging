//! Hardened NATS connection policy (local replica of the canonical helper in
//! `fiducia-messaging.rs/src/connect.rs`).
//!
//! [`connect`] decides — before dialing — whether the connection must present
//! TLS: a non-loopback endpoint gets `require_tls(true)`; loopback
//! (`localhost` / `127.0.0.0/8` / `::1`) may stay plaintext because the traffic
//! never leaves the host. Two environment switches adjust the policy:
//!
//! * `FIDUCIA_NATS_REQUIRE_TLS=1` — force TLS even for loopback.
//! * `FIDUCIA_NATS_ALLOW_PLAINTEXT=1` — explicit opt-out for a non-loopback
//!   endpoint; the helper connects but logs a loud warning.
//!
//! Credentials ride the environment, not the URL: `NATS_CREDS_FILE` names an
//! nkey/JWT `.creds` file loaded via `ConnectOptions::with_credentials_file`.
//! Nothing here logs the URL or credentials.

use std::net::IpAddr;

/// `FIDUCIA_NATS_REQUIRE_TLS=1` — enforce TLS even for loopback endpoints.
pub const REQUIRE_TLS_ENV: &str = "FIDUCIA_NATS_REQUIRE_TLS";

/// `FIDUCIA_NATS_ALLOW_PLAINTEXT=1` — explicit opt-out: allow plaintext to a
/// non-loopback endpoint (logged loudly). [`REQUIRE_TLS_ENV`] wins if both set.
pub const ALLOW_PLAINTEXT_ENV: &str = "FIDUCIA_NATS_ALLOW_PLAINTEXT";

/// `NATS_CREDS_FILE` — path to an nkey/JWT `.creds` file, so credentials never
/// ride the `NATS_URL`.
pub const CREDS_FILE_ENV: &str = "NATS_CREDS_FILE";

/// The TLS decision for one connection attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsPolicy {
    /// `require_tls(true)`: every server in the URL list must offer TLS.
    Required,
    /// Every host is loopback; plaintext is acceptable (traffic stays on-host).
    LoopbackPlaintext,
    /// Non-loopback plaintext, explicitly opted into via
    /// [`ALLOW_PLAINTEXT_ENV`]. The caller must log a loud warning.
    PlaintextOptedOut,
}

/// Whether an env-switch value means "on". Accepts `1` and `true` (any case);
/// everything else — including empty — is off.
pub fn env_flag(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim),
        Some(v) if v == "1" || v.eq_ignore_ascii_case("true")
    )
}

/// Extract the host from one NATS server URL: strip the scheme, any userinfo,
/// the port, and IPv6 brackets. Returns an empty string for something
/// host-less; callers must treat that as **not** loopback so an unparseable URL
/// fails toward requiring TLS.
pub fn host_of(url: &str) -> &str {
    let rest = match url.find("://") {
        Some(at) => &url[at + 3..],
        None => url,
    };
    let rest = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    let rest = rest.split(['/', '?']).next().unwrap_or(rest);
    if let Some(inner) = rest.strip_prefix('[') {
        return inner.split(']').next().unwrap_or("");
    }
    if rest.parse::<IpAddr>().is_ok() {
        return rest;
    }
    rest.rsplit_once(':')
        .filter(|(_, port)| !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()))
        .map_or(rest, |(host, _)| host)
}

/// Whether a single host is loopback: `localhost` (case-insensitive) or an IP
/// literal whose `IpAddr::is_loopback` holds.
pub fn is_loopback_host(host: &str) -> bool {
    !host.is_empty()
        && (host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .map(|ip| ip.is_loopback())
                .unwrap_or(false))
}

/// Whether *every* server in a (possibly comma-separated) NATS URL list is
/// loopback. One non-loopback (or undeterminable) entry makes the whole list
/// non-loopback: mixed lists take the stricter policy.
pub fn all_hosts_loopback(nats_url: &str) -> bool {
    let mut any = false;
    for server in nats_url.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if !is_loopback_host(host_of(server)) {
            return false;
        }
        any = true;
    }
    any
}

/// The pure policy decision: `require_tls` dominates; the plaintext opt-out
/// only applies where TLS would otherwise be enforced.
pub fn decide_tls_policy(nats_url: &str, require_tls: bool, allow_plaintext: bool) -> TlsPolicy {
    if require_tls {
        return TlsPolicy::Required;
    }
    if all_hosts_loopback(nats_url) {
        return TlsPolicy::LoopbackPlaintext;
    }
    if allow_plaintext {
        return TlsPolicy::PlaintextOptedOut;
    }
    TlsPolicy::Required
}

/// Connect to NATS under the policy above. Neither the URL nor the credentials
/// are ever logged.
pub async fn connect(nats_url: &str) -> Result<async_nats::Client, Box<dyn std::error::Error>> {
    let policy = decide_tls_policy(
        nats_url,
        env_flag(std::env::var(REQUIRE_TLS_ENV).ok().as_deref()),
        env_flag(std::env::var(ALLOW_PLAINTEXT_ENV).ok().as_deref()),
    );

    let mut options = match std::env::var(CREDS_FILE_ENV) {
        Ok(path) if !path.trim().is_empty() => {
            async_nats::ConnectOptions::with_credentials_file(path.trim())
                .await
                // The error names the file path (not its contents) — safe to surface.
                .map_err(|error| format!("read {CREDS_FILE_ENV}: {error}"))?
        }
        _ => async_nats::ConnectOptions::new(),
    };

    match policy {
        TlsPolicy::Required => {
            options = options.require_tls(true);
        }
        TlsPolicy::LoopbackPlaintext => {
            tracing::debug!("NATS endpoint is loopback; TLS not enforced");
        }
        TlsPolicy::PlaintextOptedOut => {
            tracing::warn!(
                "{ALLOW_PLAINTEXT_ENV}=1: connecting to a NON-LOOPBACK NATS endpoint \
                 WITHOUT enforced TLS — messages (and any credentials) can cross the \
                 network in the clear. Remove the override and terminate TLS instead."
            );
        }
    }

    // The connect error is surfaced as-is; this crate never interpolates the
    // URL into messages it constructs.
    Ok(options.connect(nats_url).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_flag_accepts_only_explicit_truths() {
        assert!(env_flag(Some("1")));
        assert!(env_flag(Some("true")));
        for off in [None, Some(""), Some("0"), Some("false"), Some("yes")] {
            assert!(!env_flag(off), "treated {off:?} as on");
        }
    }

    #[test]
    fn host_extraction_handles_schemes_userinfo_ports_and_ipv6() {
        assert_eq!(host_of("nats://localhost:4222"), "localhost");
        assert_eq!(host_of("nats://user:pass@10.0.0.5:4222"), "10.0.0.5");
        assert_eq!(host_of("nats://[::1]:4222"), "::1");
        assert_eq!(host_of("nats.example.com"), "nats.example.com");
        assert_eq!(host_of(""), "");
    }

    #[test]
    fn policy_matrix() {
        // Non-loopback requires TLS by default; the opt-out is explicit.
        assert_eq!(
            decide_tls_policy("nats://nats.example.com:4222", false, false),
            TlsPolicy::Required
        );
        assert_eq!(
            decide_tls_policy("nats://nats.example.com:4222", false, true),
            TlsPolicy::PlaintextOptedOut
        );
        // Loopback may stay plaintext unless TLS is forced.
        assert_eq!(
            decide_tls_policy("nats://localhost:4222", false, false),
            TlsPolicy::LoopbackPlaintext
        );
        assert_eq!(
            decide_tls_policy("nats://localhost:4222", true, false),
            TlsPolicy::Required
        );
        // Requiring wins over the opt-out; mixed lists take the stricter policy.
        assert_eq!(
            decide_tls_policy("nats://nats.example.com:4222", true, true),
            TlsPolicy::Required
        );
        assert_eq!(
            decide_tls_policy("nats://localhost:4222,nats://10.0.0.5:4222", false, false),
            TlsPolicy::Required
        );
        assert_eq!(decide_tls_policy("", false, false), TlsPolicy::Required);
    }
}
