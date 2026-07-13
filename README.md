# fiducia-messaging

Shared Rust messaging primitives for Fiducia services:

- A versioned, tenant-aware NATS envelope with message, causation, correlation, source, time, and trace metadata.
- A PostgreSQL transactional outbox. Services enqueue the envelope in the same database transaction as their domain mutation.
- A PostgreSQL inbox. Consumers claim each message once per consumer inside the same transaction as their side effects.
- A publisher worker using FOR UPDATE SKIP LOCKED, bounded batches, durable retry metadata, exponential backoff, and a NATS flush before marking delivery.

Run the worker with DATABASE_URL and NATS_URL configured. Apply the bundled migration in every service database that uses the outbox/inbox. Delivery is at least once; consumers obtain effective exactly-once side effects by using Inbox::begin and committing their side effect with Inbox::mark_processed in one transaction.
