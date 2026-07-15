# src

DEPRECATED — superseded by `fiducia-messaging.rs`. Original outbox/publisher
kept for history; its known flaw (DB transaction held across NATS I/O) is what
the replacement's claim-lease design fixed.
