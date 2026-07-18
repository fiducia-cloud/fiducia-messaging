use crate::Envelope;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, DbErr, Statement,
};
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone)]
pub struct Inbox {
    _database: DatabaseConnection,
}

#[derive(Debug, PartialEq, Eq)]
pub enum InboxDecision {
    Process,
    Duplicate,
}

impl Inbox {
    pub fn new(database: DatabaseConnection) -> Self {
        Self {
            _database: database,
        }
    }

    pub async fn begin<T: Serialize>(
        &self,
        tx: &DatabaseTransaction,
        consumer: &str,
        envelope: &Envelope<T>,
    ) -> Result<InboxDecision, InboxError> {
        if consumer.trim().is_empty() {
            return Err(InboxError::InvalidConsumer);
        }
        let inserted = tx
            .execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO message_inbox (consumer, message_id, tenant_id) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                [
                    consumer.into(),
                    envelope.message_id.into(),
                    envelope.tenant_id.into(),
                ],
            ))
            .await?;
        Ok(if inserted.rows_affected() == 1 {
            InboxDecision::Process
        } else {
            InboxDecision::Duplicate
        })
    }

    pub async fn mark_processed(
        &self,
        tx: &DatabaseTransaction,
        consumer: &str,
        message_id: Uuid,
    ) -> Result<(), InboxError> {
        tx.execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE message_inbox SET processed_at=now() WHERE consumer=$1 AND message_id=$2",
            [consumer.into(), message_id.into()],
        ))
        .await?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    #[error("consumer must be non-empty")]
    InvalidConsumer,
    #[error(transparent)]
    Database(#[from] DbErr),
}
