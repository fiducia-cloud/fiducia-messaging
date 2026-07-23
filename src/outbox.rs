use crate::Envelope;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, DbErr, Statement,
    TransactionTrait,
};
use serde::Serialize;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone)]
pub struct Outbox {
    _database: DatabaseConnection,
}

impl Outbox {
    pub fn new(database: DatabaseConnection) -> Self {
        Self {
            _database: database,
        }
    }

    pub async fn enqueue<T: Serialize>(
        &self,
        tx: &DatabaseTransaction,
        subject: &str,
        envelope: &Envelope<T>,
    ) -> Result<(), OutboxError> {
        if subject.trim().is_empty() {
            return Err(OutboxError::InvalidSubject);
        }
        let body = envelope.encode()?;
        tx.execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO message_outbox (message_id, tenant_id, subject, envelope) VALUES ($1,$2,$3,$4)",
            [
                envelope.message_id.into(),
                envelope.tenant_id.into(),
                subject.into(),
                body.into(),
            ],
        ))
        .await?;
        Ok(())
    }
}

pub struct OutboxPublisher {
    database: DatabaseConnection,
    js: async_nats::jetstream::Context,
    batch_size: i64,
}

impl OutboxPublisher {
    /// Wrap a NATS client. The client is wrapped in a JetStream context so
    /// every publish is confirmed by a **server ack** before the row is marked
    /// published — `flush()` only proved the bytes left the client buffer,
    /// which let the DB report success for a message the broker never stored.
    pub fn new(database: DatabaseConnection, nats: async_nats::Client) -> Self {
        Self {
            database,
            js: async_nats::jetstream::new(nats),
            batch_size: 100,
        }
    }

    pub async fn publish_batch(&self) -> Result<u64, OutboxError> {
        let tx = self.database.begin().await?;
        let rows = tx
            .query_all(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT message_id, subject, envelope FROM message_outbox WHERE published_at IS NULL AND available_at <= now() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $1",
                [self.batch_size.into()],
            ))
            .await?;
        let mut published = 0;
        for row in rows {
            let message_id: Uuid = row.try_get("", "message_id")?;
            let subject: String = row.try_get("", "subject")?;
            let body: Vec<u8> = row.try_get("", "envelope")?;
            // Tag the publish with the envelope's message_id as `Nats-Msg-Id`
            // so the JetStream stream over this subject collapses a
            // crash-window re-publish (published-but-not-marked) into one
            // stored message, and await the server ack so "published" means
            // *durably stored* — a subject no stream covers, or a broker that
            // drops the message, surfaces as a per-row error with backoff
            // metadata instead of a fire-and-forget flush() the DB records as
            // success. (Mirrors the ack-awaiting publisher in
            // fiducia-messaging.rs.)
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", message_id.to_string().as_str());
            let publish = match self
                .js
                .publish_with_headers(subject, headers, body.into())
                .await
            {
                Ok(ack) => ack.await.map(|_ack| ()),
                Err(error) => Err(error),
            };
            match publish {
                Ok(()) => {
                    tx.execute(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "UPDATE message_outbox SET published_at=now(), attempts=attempts+1, last_error=NULL WHERE message_id=$1",
                        [message_id.into()],
                    ))
                    .await?;
                    published += 1;
                }
                Err(error) => {
                    tx.execute(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "UPDATE message_outbox SET attempts=attempts+1,last_error=$2,available_at=now()+least(interval '5 minutes', interval '1 second' * power(2, least(attempts, 8))) WHERE message_id=$1",
                        [message_id.into(), error.to_string().into()],
                    ))
                    .await?;
                    break;
                }
            }
        }
        tx.commit().await?;
        Ok(published)
    }
    pub async fn run(self, interval: Duration) -> Result<(), OutboxError> {
        let mut timer = tokio::time::interval(interval);
        loop {
            timer.tick().await;
            if let Err(error) = self.publish_batch().await {
                tracing::error!(%error, "outbox publish batch failed");
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error("subject must be non-empty")]
    InvalidSubject,
    #[error(transparent)]
    Envelope(#[from] crate::EnvelopeError),
    #[error(transparent)]
    Database(#[from] DbErr),
    #[error("NATS operation failed: {0}")]
    Nats(String),
}
