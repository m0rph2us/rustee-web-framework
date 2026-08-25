use std::{fmt, sync::Arc, time::Duration};

use rustee_events_kafka::KafkaFailurePublisher;
use sqlx::{PgPool, Row};

use super::{
    config::{
        KafkaDelayedRetryReadinessConfig, KafkaDelayedRetryReadinessError,
        KafkaDelayedRetryRelayConfig,
    },
    observation::{KafkaDelayedRetryRelayObserver, NoopKafkaDelayedRetryRelayObserver},
};

mod executor;
mod runner;
#[cfg(test)]
mod tests;

/// Aggregate-only snapshot of unpublished delayed-retry rows from the database clock.
///
/// Applications obtain this with [`PostgresKafkaDelayedRetryRelay::backlog`] at their chosen
/// polling cadence. The snapshot deliberately excludes row IDs, topics, keys, and payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaDelayedRetryBacklog {
    /// Rows that have not yet been confirmed after Kafka acknowledgement.
    pub unpublished: u64,
    /// Unpublished rows currently due according to the `PostgreSQL` database clock.
    pub due: u64,
    /// Unpublished rows with a currently active relay lease.
    pub leased: u64,
    /// Age of the oldest due unpublished row, if one exists.
    pub oldest_due_age: Option<Duration>,
}

/// Sanitized delayed-retry backlog query failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KafkaDelayedRetryBacklogError {
    /// `PostgreSQL` could not read aggregate delayed-retry backlog data.
    #[error("Kafka delayed retry backlog query failed")]
    Database,
}

/// Explicit bounded relay for due `PostgreSQL` delayed-retry rows.
///
/// Its `Debug` output keeps application pool and deployment-routing values redacted.
#[derive(Clone)]
pub struct PostgresKafkaDelayedRetryRelay {
    pool: PgPool,
    publisher: KafkaFailurePublisher,
    config: KafkaDelayedRetryRelayConfig,
    observer: Arc<dyn KafkaDelayedRetryRelayObserver>,
}

impl fmt::Debug for PostgresKafkaDelayedRetryRelay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresKafkaDelayedRetryRelay")
            .field("pool", &"[REDACTED]")
            .field("publisher", &"[REDACTED]")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl PostgresKafkaDelayedRetryRelay {
    /// Creates a relay with an application-owned pool, publisher, and lease configuration.
    #[must_use]
    pub fn new(
        pool: PgPool,
        publisher: KafkaFailurePublisher,
        config: KafkaDelayedRetryRelayConfig,
    ) -> Self {
        Self {
            pool,
            publisher,
            config,
            observer: Arc::new(NoopKafkaDelayedRetryRelayObserver),
        }
    }

    /// Attaches one exporter-neutral delayed-retry relay pass observer.
    #[must_use]
    pub fn with_relay_observer(
        mut self,
        observer: Arc<dyn KafkaDelayedRetryRelayObserver>,
    ) -> Self {
        self.observer = observer;
        self
    }

    /// Checks access to the delayed-retry table and both configured Kafka failure topics.
    ///
    /// This method does not create topics, run migrations, drain due rows, or choose a health
    /// endpoint. `PostgreSQL` and Kafka checks run concurrently, each with its own bounded
    /// timeout from [`KafkaDelayedRetryReadinessConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`KafkaDelayedRetryReadinessError::Database`] when the retry table cannot be
    /// read, or [`KafkaDelayedRetryReadinessError::Kafka`] when retry or dead-letter metadata
    /// cannot be read.
    pub async fn readiness(
        &self,
        config: KafkaDelayedRetryReadinessConfig,
    ) -> Result<(), KafkaDelayedRetryReadinessError> {
        let database_pool = self.pool.clone();
        let database = async move {
            tokio::time::timeout(
                config.database_timeout(),
                sqlx::query("SELECT 1 FROM rustee_kafka_delayed_retries LIMIT 1")
                    .execute(&database_pool),
            )
            .await
            .map_err(|_| KafkaDelayedRetryReadinessError::Database)?
            .map_err(|_| KafkaDelayedRetryReadinessError::Database)?;
            Ok(())
        };
        let publisher = self.publisher.clone();
        let kafka = async move {
            tokio::task::spawn_blocking(move || publisher.readiness(config.kafka_timeout()))
                .await
                .map_err(|_| KafkaDelayedRetryReadinessError::Kafka)?
                .map_err(|_| KafkaDelayedRetryReadinessError::Kafka)
        };
        let _ = tokio::try_join!(database, kafka)?;
        Ok(())
    }

    /// Returns aggregate unpublished, due, leased, and oldest-due-row backlog data.
    ///
    /// The query uses the `PostgreSQL` database clock and does not claim rows, create topics,
    /// or inspect payloads. Callers own polling cadence, timeout, alert thresholds, and metric
    /// export policy.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaDelayedRetryBacklogError::Database`] when the aggregate query fails.
    pub async fn backlog(&self) -> Result<KafkaDelayedRetryBacklog, KafkaDelayedRetryBacklogError> {
        let row = sqlx::query("SELECT COUNT(*) FILTER (WHERE published_at IS NULL) AS unpublished, COUNT(*) FILTER (WHERE published_at IS NULL AND available_at <= clock_timestamp()) AS due, COUNT(*) FILTER (WHERE published_at IS NULL AND leased_until > clock_timestamp()) AS leased, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - MIN(available_at) FILTER (WHERE published_at IS NULL AND available_at <= clock_timestamp()))) * 1000)::bigint AS oldest_due_age_ms FROM rustee_kafka_delayed_retries")
            .fetch_one(&self.pool)
            .await
            .map_err(|_| KafkaDelayedRetryBacklogError::Database)?;
        let oldest_due_age = match row
            .try_get::<Option<i64>, _>("oldest_due_age_ms")
            .map_err(|_| KafkaDelayedRetryBacklogError::Database)?
        {
            Some(milliseconds) => Some(Duration::from_millis(
                u64::try_from(milliseconds).map_err(|_| KafkaDelayedRetryBacklogError::Database)?,
            )),
            None => None,
        };
        Ok(KafkaDelayedRetryBacklog {
            unpublished: u64::try_from(
                row.try_get::<i64, _>("unpublished")
                    .map_err(|_| KafkaDelayedRetryBacklogError::Database)?,
            )
            .map_err(|_| KafkaDelayedRetryBacklogError::Database)?,
            due: u64::try_from(
                row.try_get::<i64, _>("due")
                    .map_err(|_| KafkaDelayedRetryBacklogError::Database)?,
            )
            .map_err(|_| KafkaDelayedRetryBacklogError::Database)?,
            leased: u64::try_from(
                row.try_get::<i64, _>("leased")
                    .map_err(|_| KafkaDelayedRetryBacklogError::Database)?,
            )
            .map_err(|_| KafkaDelayedRetryBacklogError::Database)?,
            oldest_due_age,
        })
    }
}
