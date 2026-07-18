//! SeaORM-owned database bootstrap for the legacy messaging service.

use sea_orm::{ConnectionTrait, DatabaseConnection, DbErr};

/// Apply the idempotent outbox/inbox schema with SeaORM's unprepared path so
/// the tracked multi-statement migration remains the single source of truth.
pub async fn apply_schema(database: &DatabaseConnection) -> Result<(), DbErr> {
    database
        .execute_unprepared(include_str!("../migrations/0001_outbox_inbox.sql"))
        .await?;
    Ok(())
}
