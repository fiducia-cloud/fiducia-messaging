//! Runtime wiring for the legacy outbox publisher binary.

use std::time::Duration;

use sea_orm::{ConnectOptions, Database};

use crate::{database, OutboxPublisher};

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be configured")?;
    let nats_url = std::env::var("NATS_URL").map_err(|_| "NATS_URL must be configured")?;

    let mut options = ConnectOptions::new(database_url);
    options
        .max_connections(10)
        .connect_timeout(Duration::from_secs(5));
    let database = Database::connect(options).await?;
    database::apply_schema(&database).await?;

    let nats = async_nats::connect(nats_url).await?;
    tracing::info!(database.orm = "sea-orm", "fiducia outbox publisher started");
    OutboxPublisher::new(database, nats)
        .run(Duration::from_millis(250))
        .await?;
    Ok(())
}
