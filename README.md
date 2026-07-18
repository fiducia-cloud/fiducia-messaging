# fiducia-messaging

Shared Rust messaging primitives for Fiducia services:

- A versioned, tenant-aware NATS envelope with message, causation, correlation, source, time, and trace metadata.
- A PostgreSQL transactional outbox. Services enqueue the envelope in the same database transaction as their domain mutation.
- A PostgreSQL inbox. Consumers claim each message once per consumer inside the same transaction as their side effects.
- A publisher worker using FOR UPDATE SKIP LOCKED, bounded batches, durable retry metadata, exponential backoff, and a NATS flush before marking delivery.

Run the worker with `DATABASE_URL` and `NATS_URL` configured. The worker applies
the idempotent bundled schema through SeaORM. Delivery is at least once;
consumers obtain effective exactly-once side effects by calling `Inbox::begin`
and committing their side effect with `Inbox::mark_processed` in the same
SeaORM `DatabaseTransaction`.

The binary keeps `main.rs` as a thin bootstrap and delegates runtime wiring to
`service`. `fiducia-telemetry` emits JSON logs for the collector/Loki pipeline
and OTLP traces and metrics for the collector/Prometheus pipeline.

## Security maintenance

This repository is deprecated in favor of `fiducia-messaging.rs`, but retained
builds still receive dependency fixes. Database access is owned by SeaORM; the
crate has no direct SQLx dependency or query API. `async-nats` remains on the
patched TLS stack and `cargo audit` is enforced.
