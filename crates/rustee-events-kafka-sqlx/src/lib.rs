//! PostgreSQL-backed delayed retry router for Kafka event failures.
//!
//! Enable the `rdkafka` feature to use the Kafka adapter.

#[cfg(feature = "rdkafka")]
mod adapter {

    use std::{
        fmt,
        future::Future,
        num::NonZeroU16,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Arc,
        time::{Duration, Instant},
    };

    use futures_util::future::BoxFuture;
    use rustee_events_kafka::{
        KafkaDelayedRetryRecord, KafkaError, KafkaFailureKind, KafkaFailurePublisher,
        KafkaFailureRecord, KafkaFailureRouter, KafkaRetryAction,
    };
    use sqlx::{PgPool, Row};
    use uuid::Uuid;

    /// Deployment-owned migration for durable Kafka delayed retries.
    pub const KAFKA_DELAYED_RETRY_MIGRATION_SQL: &str =
        include_str!("../migrations/0001_rustee_kafka_delayed_retries.sql");

    /// Bounded fixed delay applied before a failed Kafka event is released to its retry topic.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct KafkaDelayedRetryDelay(Duration);

    impl KafkaDelayedRetryDelay {
        /// Creates a delay of at most 366 days.
        ///
        /// # Errors
        ///
        /// Returns [`KafkaDelayedRetryDelayError::InvalidDelay`] for zero or oversized values.
        pub fn new(delay: Duration) -> Result<Self, KafkaDelayedRetryDelayError> {
            if delay.is_zero() || delay > Duration::from_secs(366 * 24 * 60 * 60) {
                return Err(KafkaDelayedRetryDelayError::InvalidDelay);
            }
            Ok(Self(delay))
        }

        fn milliseconds(self) -> i64 {
            i64::try_from(self.0.as_millis()).expect("validated delay fits i64")
        }
    }

    /// Invalid delayed retry delay.
    #[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
    pub enum KafkaDelayedRetryDelayError {
        #[error("Kafka delayed retry delay must be greater than zero and at most 366 days")]
        InvalidDelay,
    }

    /// Explicit lease and retry timing for a `PostgreSQL` delayed-retry relay.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct KafkaDelayedRetryRelayConfig {
        lease: KafkaDelayedRetryDelay,
        retry_after_failure: KafkaDelayedRetryDelay,
    }

    impl KafkaDelayedRetryRelayConfig {
        /// Creates relay timing from validated positive, bounded durations.
        #[must_use]
        pub const fn new(
            lease: KafkaDelayedRetryDelay,
            retry_after_failure: KafkaDelayedRetryDelay,
        ) -> Self {
            Self {
                lease,
                retry_after_failure,
            }
        }
    }

    const MAX_READINESS_TIMEOUT: Duration = Duration::from_secs(60);
    const MAX_RELAY_IDLE_DELAY: Duration = Duration::from_hours(1);

    /// Bounded per-dependency timeout for [`PostgresKafkaDelayedRetryRelay::readiness`].
    ///
    /// The framework does not register a health route or decide whether a Kafka delayed-retry
    /// relay is required for a deployment. The application calls this check from its chosen
    /// readiness policy.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct KafkaDelayedRetryReadinessConfig {
        database_timeout: Duration,
        kafka_timeout: Duration,
    }

    impl KafkaDelayedRetryReadinessConfig {
        /// Creates explicit bounded timeout settings for the `PostgreSQL` and Kafka checks.
        ///
        /// # Errors
        ///
        /// Returns [`KafkaDelayedRetryReadinessConfigError`] when either timeout is zero or
        /// longer than one minute.
        pub fn new(
            database_timeout: Duration,
            kafka_timeout: Duration,
        ) -> Result<Self, KafkaDelayedRetryReadinessConfigError> {
            if database_timeout.is_zero() || kafka_timeout.is_zero() {
                return Err(KafkaDelayedRetryReadinessConfigError::ZeroTimeout);
            }
            if database_timeout > MAX_READINESS_TIMEOUT || kafka_timeout > MAX_READINESS_TIMEOUT {
                return Err(KafkaDelayedRetryReadinessConfigError::TimeoutTooLong);
            }
            Ok(Self {
                database_timeout,
                kafka_timeout,
            })
        }

        /// Returns the timeout used for `PostgreSQL` retry-table access.
        #[must_use]
        pub const fn database_timeout(self) -> Duration {
            self.database_timeout
        }

        /// Returns the timeout used for retry and dead-letter Kafka topic metadata.
        #[must_use]
        pub const fn kafka_timeout(self) -> Duration {
            self.kafka_timeout
        }
    }

    impl Default for KafkaDelayedRetryReadinessConfig {
        fn default() -> Self {
            Self::new(Duration::from_secs(5), Duration::from_secs(5))
                .expect("default Kafka delayed-retry readiness configuration is valid")
        }
    }

    /// Invalid explicit delayed-retry readiness timeout settings.
    #[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
    pub enum KafkaDelayedRetryReadinessConfigError {
        /// A zero timeout cannot bound a dependency check.
        #[error("Kafka delayed retry readiness timeouts must be greater than zero")]
        ZeroTimeout,
        /// The timeout exceeded the supported operational interval.
        #[error("Kafka delayed retry readiness timeouts must be at most one minute")]
        TimeoutTooLong,
    }

    /// Sanitized delayed-retry readiness failure.
    #[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
    pub enum KafkaDelayedRetryReadinessError {
        /// `PostgreSQL` could not query the delayed-retry table before its configured timeout.
        #[error("Kafka delayed retry PostgreSQL readiness check failed")]
        Database,
        /// Kafka could not return retry and dead-letter topic metadata before its configured timeout.
        #[error("Kafka delayed retry Kafka readiness check failed")]
        Kafka,
    }

    /// Explicit polling settings for [`PostgresKafkaDelayedRetryRelay::run_until`].
    ///
    /// This configuration does not start a background task. The application chooses where to
    /// await the relay, supplies its shutdown future, and owns readiness, supervision, and metric
    /// export.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct KafkaDelayedRetryRelayLoopConfig {
        batch_size: NonZeroU16,
        idle_delay: Duration,
    }

    impl KafkaDelayedRetryRelayLoopConfig {
        /// Creates bounded relay-loop settings with a delay after an empty pass.
        ///
        /// # Errors
        ///
        /// Returns [`KafkaDelayedRetryRelayLoopConfigError`] when `idle_delay` is zero or longer
        /// than one hour.
        pub fn new(
            batch_size: NonZeroU16,
            idle_delay: Duration,
        ) -> Result<Self, KafkaDelayedRetryRelayLoopConfigError> {
            if idle_delay.is_zero() {
                return Err(KafkaDelayedRetryRelayLoopConfigError::ZeroIdleDelay);
            }
            if idle_delay > MAX_RELAY_IDLE_DELAY {
                return Err(KafkaDelayedRetryRelayLoopConfigError::IdleDelayTooLong);
            }
            Ok(Self {
                batch_size,
                idle_delay,
            })
        }

        /// Returns the maximum number of due rows claimed by each pass.
        #[must_use]
        pub const fn batch_size(self) -> NonZeroU16 {
            self.batch_size
        }

        /// Returns the delay inserted only after a pass publishes no rows.
        #[must_use]
        pub const fn idle_delay(self) -> Duration {
            self.idle_delay
        }
    }

    impl Default for KafkaDelayedRetryRelayLoopConfig {
        fn default() -> Self {
            Self::new(
                NonZeroU16::new(100).expect("default Kafka relay batch size is non-zero"),
                Duration::from_secs(1),
            )
            .expect("default Kafka relay loop configuration is valid")
        }
    }

    /// Invalid explicit Kafka delayed-retry relay-loop settings.
    #[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
    pub enum KafkaDelayedRetryRelayLoopConfigError {
        /// An empty-pass delay of zero would create a database polling loop.
        #[error("Kafka delayed retry relay idle delay must be greater than zero")]
        ZeroIdleDelay,
        /// The bounded polling delay exceeded the supported operational interval.
        #[error("Kafka delayed retry relay idle delay must be at most one hour")]
        IdleDelayTooLong,
    }

    /// Aggregate counts collected while an explicit Kafka delayed-retry relay loop was running.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct KafkaDelayedRetryRelayLoopReport {
        /// Number of bounded relay passes completed before shutdown.
        pub passes: usize,
        /// Total records confirmed after Kafka acknowledgement across completed passes.
        pub published: usize,
    }

    impl KafkaDelayedRetryRelayLoopReport {
        fn record(&mut self, published: u16) {
            self.passes = self.passes.saturating_add(1);
            self.published = self.published.saturating_add(usize::from(published));
        }
    }

    /// Terminal outcome of one Kafka delayed-retry relay pass.
    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub enum KafkaDelayedRetryRelayOutcome {
        /// The pass claimed, published, and confirmed all selected rows.
        Succeeded,
        /// A database or Kafka error ended the pass.
        Failed,
        /// The relay future was cancelled before it returned a terminal result.
        Abandoned,
    }

    impl KafkaDelayedRetryRelayOutcome {
        /// Returns the fixed exporter-safe outcome label.
        #[must_use]
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Succeeded => "succeeded",
                Self::Failed => "failed",
                Self::Abandoned => "abandoned",
            }
        }
    }

    /// Metadata emitted when a bounded Kafka delayed-retry relay pass starts.
    ///
    /// Topic names, event identifiers, payloads, endpoints, and configuration are intentionally
    /// absent so an observer can retain only aggregate operational telemetry.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct KafkaDelayedRetryRelayPassStarted;

    /// Metadata emitted when a Kafka delayed-retry relay pass finishes or is abandoned.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct KafkaDelayedRetryRelayPassFinished {
        outcome: KafkaDelayedRetryRelayOutcome,
        published: Option<u16>,
        duration: Duration,
    }

    impl KafkaDelayedRetryRelayPassFinished {
        /// Returns the terminal status without exposing a database or Kafka error.
        #[must_use]
        pub const fn outcome(self) -> KafkaDelayedRetryRelayOutcome {
            self.outcome
        }

        /// Returns records confirmed in a fully successful pass.
        ///
        /// Failed and externally abandoned passes return `None`: they may have reached Kafka
        /// before a later database transition failed, so callers must not treat a missing count as
        /// proof that no duplicate was published.
        #[must_use]
        pub const fn published(self) -> Option<u16> {
            self.published
        }

        /// Returns elapsed time for the bounded database/Kafka pass.
        #[must_use]
        pub const fn duration(self) -> Duration {
            self.duration
        }
    }

    /// Synchronous, non-blocking observer for Kafka delayed-retry relay passes.
    ///
    /// Implementations should aggregate locally or enqueue bounded export work. Observer panics
    /// are caught, so observability cannot change source-offset or retry delivery semantics.
    pub trait KafkaDelayedRetryRelayObserver: Send + Sync + 'static {
        /// Records the beginning of one bounded relay pass.
        fn on_relay_pass_started(&self, pass: KafkaDelayedRetryRelayPassStarted);

        /// Records one completed, failed, or externally abandoned pass.
        fn on_relay_pass_finished(&self, pass: KafkaDelayedRetryRelayPassFinished);
    }

    /// No-op observer used unless a relay explicitly opts into observability.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct NoopKafkaDelayedRetryRelayObserver;

    impl KafkaDelayedRetryRelayObserver for NoopKafkaDelayedRetryRelayObserver {
        fn on_relay_pass_started(&self, _pass: KafkaDelayedRetryRelayPassStarted) {}

        fn on_relay_pass_finished(&self, _pass: KafkaDelayedRetryRelayPassFinished) {}
    }

    /// In-progress observability value owned by one delayed-retry relay pass future.
    ///
    /// Dropping this value without [`Self::finish`] records an `abandoned` pass, including task
    /// cancellation while the relay is about to acquire or holds database leases.
    pub struct KafkaDelayedRetryRelayPassObservation {
        observer: Arc<dyn KafkaDelayedRetryRelayObserver>,
        started_at: Instant,
        finished: bool,
    }

    impl KafkaDelayedRetryRelayPassObservation {
        /// Starts observing one bounded delayed-retry relay pass.
        #[must_use]
        pub fn start(observer: Arc<dyn KafkaDelayedRetryRelayObserver>) -> Self {
            notify_relay_started(&observer, KafkaDelayedRetryRelayPassStarted);
            Self {
                observer,
                started_at: Instant::now(),
                finished: false,
            }
        }

        /// Emits a terminal outcome after the relay future returns.
        pub fn finish(mut self, outcome: KafkaDelayedRetryRelayOutcome, published: Option<u16>) {
            self.finished = true;
            notify_relay_finished(
                &self.observer,
                KafkaDelayedRetryRelayPassFinished {
                    outcome,
                    published,
                    duration: self.started_at.elapsed(),
                },
            );
        }
    }

    impl Drop for KafkaDelayedRetryRelayPassObservation {
        fn drop(&mut self) {
            if !self.finished {
                notify_relay_finished(
                    &self.observer,
                    KafkaDelayedRetryRelayPassFinished {
                        outcome: KafkaDelayedRetryRelayOutcome::Abandoned,
                        published: None,
                        duration: self.started_at.elapsed(),
                    },
                );
            }
        }
    }

    impl fmt::Debug for KafkaDelayedRetryRelayPassObservation {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("KafkaDelayedRetryRelayPassObservation")
                .field("finished", &self.finished)
                .finish_non_exhaustive()
        }
    }

    fn notify_relay_started(
        observer: &Arc<dyn KafkaDelayedRetryRelayObserver>,
        pass: KafkaDelayedRetryRelayPassStarted,
    ) {
        let _ = catch_unwind(AssertUnwindSafe(|| observer.on_relay_pass_started(pass)));
    }

    fn notify_relay_finished(
        observer: &Arc<dyn KafkaDelayedRetryRelayObserver>,
        pass: KafkaDelayedRetryRelayPassFinished,
    ) {
        let _ = catch_unwind(AssertUnwindSafe(|| observer.on_relay_pass_finished(pass)));
    }

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

    /// Stages retry attempts durably before the Kafka consumer commits its source offset.
    #[derive(Clone, Debug)]
    pub struct PostgresKafkaDelayedRetryRouter {
        pool: PgPool,
        fallback: KafkaFailurePublisher,
        delay: KafkaDelayedRetryDelay,
    }

    impl PostgresKafkaDelayedRetryRouter {
        #[must_use]
        pub fn new(
            pool: PgPool,
            fallback: KafkaFailurePublisher,
            delay: KafkaDelayedRetryDelay,
        ) -> Self {
            Self {
                pool,
                fallback,
                delay,
            }
        }
    }

    impl KafkaFailureRouter for PostgresKafkaDelayedRetryRouter {
        fn retry_topic(&self) -> &str {
            self.fallback.retry_config().retry_topic()
        }

        fn route<'a>(
            &'a self,
            record: KafkaFailureRecord<'a>,
            failure: KafkaFailureKind,
            attempt: u16,
        ) -> BoxFuture<'a, Result<KafkaRetryAction, KafkaError>> {
            Box::pin(async move {
                let action = self.fallback.retry_config().after_failure(attempt);
                let KafkaRetryAction::Retry { next_attempt } = action else {
                    return KafkaFailureRouter::route(&self.fallback, record, failure, attempt)
                        .await;
                };
                let payload = record.payload().ok_or(KafkaError::MissingPayload)?;
                if payload.len() > 1_048_576 {
                    return Err(KafkaError::FailureRoute);
                }
                let origin_topic = record.origin_topic();
                let origin_partition = record.origin_partition();
                let origin_offset = record.origin_offset();
                let failure_kind = match failure {
                    KafkaFailureKind::Decode => "decode",
                    KafkaFailureKind::Handler => "handler",
                };
                sqlx::query("INSERT INTO rustee_kafka_delayed_retries (id, origin_topic, origin_partition, origin_offset, retry_topic, retry_attempt, failure_kind, event_key, payload, available_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,clock_timestamp()+($10::bigint * INTERVAL '1 millisecond')) ON CONFLICT (origin_topic, origin_partition, origin_offset, retry_attempt) DO NOTHING")
                .bind(Uuid::new_v4()).bind(origin_topic).bind(origin_partition).bind(origin_offset)
                .bind(self.retry_topic()).bind(i32::from(next_attempt)).bind(failure_kind)
                .bind(record.key()).bind(payload).bind(self.delay.milliseconds()).execute(&self.pool).await
                .map_err(|_| KafkaError::FailureRoute)?;
                Ok(action)
            })
        }
    }

    /// Explicit bounded relay for due `PostgreSQL` delayed-retry rows.
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
                .field("pool", &self.pool)
                .field("publisher", &self.publisher)
                .field("config", &self.config)
                .finish_non_exhaustive()
        }
    }

    impl PostgresKafkaDelayedRetryRelay {
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
        pub async fn backlog(
            &self,
        ) -> Result<KafkaDelayedRetryBacklog, KafkaDelayedRetryBacklogError> {
            let row = sqlx::query("SELECT COUNT(*) FILTER (WHERE published_at IS NULL) AS unpublished, COUNT(*) FILTER (WHERE published_at IS NULL AND available_at <= clock_timestamp()) AS due, COUNT(*) FILTER (WHERE published_at IS NULL AND leased_until > clock_timestamp()) AS leased, FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - MIN(available_at) FILTER (WHERE published_at IS NULL AND available_at <= clock_timestamp()))) * 1000)::bigint AS oldest_due_age_ms FROM rustee_kafka_delayed_retries")
                .fetch_one(&self.pool)
                .await
                .map_err(|_| KafkaDelayedRetryBacklogError::Database)?;
            let oldest_due_age = match row
                .try_get::<Option<i64>, _>("oldest_due_age_ms")
                .map_err(|_| KafkaDelayedRetryBacklogError::Database)?
            {
                Some(milliseconds) => Some(Duration::from_millis(
                    u64::try_from(milliseconds)
                        .map_err(|_| KafkaDelayedRetryBacklogError::Database)?,
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

        /// Publishes at most `limit` due rows and confirms each only after Kafka acknowledgement.
        ///
        /// A successful Kafka delivery followed by a failed `PostgreSQL` acknowledgement can publish a
        /// duplicate. Consumers must retain their normal event-id or domain-key idempotency boundary.
        ///
        /// # Errors
        ///
        /// Returns [`KafkaError::FailureRoute`] when a durable retry row cannot be claimed, decoded,
        /// released, or acknowledged. Returns the Kafka publisher error after releasing every
        /// unpublished row claimed by this pass after the configured delay.
        pub async fn relay_once(&self, limit: u16) -> Result<u16, KafkaError> {
            let observation =
                KafkaDelayedRetryRelayPassObservation::start(Arc::clone(&self.observer));
            match self.relay_once_inner(limit).await {
                Ok(published) => {
                    observation.finish(KafkaDelayedRetryRelayOutcome::Succeeded, Some(published));
                    Ok(published)
                }
                Err(error) => {
                    observation.finish(KafkaDelayedRetryRelayOutcome::Failed, None);
                    Err(error)
                }
            }
        }

        async fn relay_once_inner(&self, limit: u16) -> Result<u16, KafkaError> {
            let token = Uuid::new_v4();
            let rows = sqlx::query("WITH candidates AS (SELECT id FROM rustee_kafka_delayed_retries WHERE published_at IS NULL AND available_at <= clock_timestamp() AND (leased_until IS NULL OR leased_until <= clock_timestamp()) ORDER BY available_at, created_at, id FOR UPDATE SKIP LOCKED LIMIT $1), claimed AS (UPDATE rustee_kafka_delayed_retries r SET lease_token=$2, leased_until=clock_timestamp()+($3::bigint * INTERVAL '1 millisecond'), relay_attempt=r.relay_attempt+1 FROM candidates WHERE r.id=candidates.id RETURNING r.*) SELECT * FROM claimed")
            .bind(i64::from(limit))
            .bind(token)
            .bind(self.config.lease.milliseconds())
            .fetch_all(&self.pool)
            .await
            .map_err(|_| KafkaError::FailureRoute)?;
            let mut published = 0;
            for row in rows {
                let retry = match Self::retry_from_row(&row) {
                    Ok(retry) => retry,
                    Err(error) => {
                        self.release_claims(token).await?;
                        return Err(error);
                    }
                };
                if let Err(error) = self
                    .publisher
                    .publish_delayed_retry(KafkaDelayedRetryRecord {
                        retry_topic: &retry.retry_topic,
                        origin_topic: &retry.origin_topic,
                        origin_partition: retry.origin_partition,
                        origin_offset: retry.origin_offset,
                        failure: retry.failure,
                        attempt: retry.attempt,
                        key: retry.event_key.as_deref(),
                        payload: &retry.payload,
                    })
                    .await
                {
                    self.release_claims(token).await?;
                    return Err(error);
                }
                let changed = sqlx::query("UPDATE rustee_kafka_delayed_retries SET published_at=clock_timestamp(), leased_until=NULL, lease_token=NULL WHERE id=$1 AND lease_token=$2")
                .bind(retry.id)
                .bind(token)
                .execute(&self.pool)
                .await;
                let Ok(changed) = changed else {
                    self.release_claims(token).await?;
                    return Err(KafkaError::FailureRoute);
                };
                if changed.rows_affected() == 1 {
                    published += 1;
                }
            }
            Ok(published)
        }

        /// Repeatedly executes bounded passes until the supplied shutdown future resolves.
        ///
        /// A shutdown signal is observed before each new pass and while waiting after an empty
        /// pass. A pass already holding leases finishes before shutdown is returned, so the loop
        /// never drops an in-progress pass merely to stop quickly. Kafka and `PostgreSQL` errors end
        /// the loop for the application supervisor to handle.
        ///
        /// # Errors
        ///
        /// Returns the first [`KafkaError`] produced by one bounded pass.
        pub async fn run_until<Shutdown>(
            &self,
            loop_config: KafkaDelayedRetryRelayLoopConfig,
            shutdown: Shutdown,
        ) -> Result<KafkaDelayedRetryRelayLoopReport, KafkaError>
        where
            Shutdown: Future<Output = ()> + Send,
        {
            tokio::pin!(shutdown);
            let mut total = KafkaDelayedRetryRelayLoopReport::default();
            loop {
                tokio::select! {
                    biased;
                    () = &mut shutdown => return Ok(total),
                    () = tokio::task::yield_now() => {}
                }
                let published = self.relay_once(loop_config.batch_size().get()).await?;
                total.record(published);
                if published == 0 {
                    tokio::select! {
                        biased;
                        () = &mut shutdown => return Ok(total),
                        () = tokio::time::sleep(loop_config.idle_delay()) => {}
                    }
                }
            }
        }

        fn retry_from_row(row: &sqlx::postgres::PgRow) -> Result<DelayedRetryRow, KafkaError> {
            let failure = match row
                .try_get::<String, _>("failure_kind")
                .map_err(|_| KafkaError::FailureRoute)?
                .as_str()
            {
                "decode" => KafkaFailureKind::Decode,
                "handler" => KafkaFailureKind::Handler,
                _ => return Err(KafkaError::FailureRoute),
            };
            Ok(DelayedRetryRow {
                id: row.try_get("id").map_err(|_| KafkaError::FailureRoute)?,
                retry_topic: row
                    .try_get("retry_topic")
                    .map_err(|_| KafkaError::FailureRoute)?,
                origin_topic: row
                    .try_get("origin_topic")
                    .map_err(|_| KafkaError::FailureRoute)?,
                origin_partition: row
                    .try_get("origin_partition")
                    .map_err(|_| KafkaError::FailureRoute)?,
                origin_offset: row
                    .try_get("origin_offset")
                    .map_err(|_| KafkaError::FailureRoute)?,
                failure,
                attempt: u16::try_from(
                    row.try_get::<i32, _>("retry_attempt")
                        .map_err(|_| KafkaError::FailureRoute)?,
                )
                .map_err(|_| KafkaError::FailureRoute)?,
                event_key: row
                    .try_get("event_key")
                    .map_err(|_| KafkaError::FailureRoute)?,
                payload: row
                    .try_get("payload")
                    .map_err(|_| KafkaError::FailureRoute)?,
            })
        }

        async fn release_claims(&self, token: Uuid) -> Result<(), KafkaError> {
            sqlx::query("UPDATE rustee_kafka_delayed_retries SET available_at=clock_timestamp()+($1::bigint * INTERVAL '1 millisecond'), leased_until=NULL, lease_token=NULL WHERE published_at IS NULL AND lease_token=$2")
            .bind(self.config.retry_after_failure.milliseconds())
            .bind(token)
            .execute(&self.pool)
            .await
            .map_err(|_| KafkaError::FailureRoute)?;
            Ok(())
        }
    }

    #[derive(Debug)]
    struct DelayedRetryRow {
        id: Uuid,
        retry_topic: String,
        origin_topic: String,
        origin_partition: i32,
        origin_offset: i64,
        failure: KafkaFailureKind,
        attempt: u16,
        event_key: Option<Vec<u8>>,
        payload: Vec<u8>,
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use rustee_events_kafka::{KafkaConfig, KafkaRetryConfig};
        use sqlx::postgres::PgPoolOptions;

        #[test]
        fn delayed_retry_timing_is_positive_and_bounded() {
            assert!(KafkaDelayedRetryDelay::new(Duration::ZERO).is_err());
            assert!(
                KafkaDelayedRetryDelay::new(Duration::from_secs(366 * 24 * 60 * 60 + 1)).is_err()
            );
            let delay = KafkaDelayedRetryDelay::new(Duration::from_millis(1)).unwrap();
            assert_eq!(delay.milliseconds(), 1);
        }

        #[test]
        fn relay_timing_keeps_both_durations_explicit() {
            let lease = KafkaDelayedRetryDelay::new(Duration::from_secs(30)).unwrap();
            let retry = KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap();
            let config = KafkaDelayedRetryRelayConfig::new(lease, retry);
            assert_eq!(config.lease.milliseconds(), 30_000);
            assert_eq!(config.retry_after_failure.milliseconds(), 1_000);
        }

        #[test]
        fn relay_loop_configuration_is_bounded_and_explicit() {
            let batch_size = NonZeroU16::new(8).unwrap();
            assert!(matches!(
                KafkaDelayedRetryRelayLoopConfig::new(batch_size, Duration::ZERO),
                Err(KafkaDelayedRetryRelayLoopConfigError::ZeroIdleDelay)
            ));
            assert!(matches!(
                KafkaDelayedRetryRelayLoopConfig::new(batch_size, Duration::from_secs(60 * 60 + 1)),
                Err(KafkaDelayedRetryRelayLoopConfigError::IdleDelayTooLong)
            ));
            let config =
                KafkaDelayedRetryRelayLoopConfig::new(batch_size, Duration::from_millis(1))
                    .unwrap();
            assert_eq!(config.batch_size().get(), 8);
            assert_eq!(config.idle_delay(), Duration::from_millis(1));
        }

        #[test]
        fn readiness_configuration_is_bounded_and_explicit() {
            assert!(matches!(
                KafkaDelayedRetryReadinessConfig::new(Duration::ZERO, Duration::from_secs(1)),
                Err(KafkaDelayedRetryReadinessConfigError::ZeroTimeout)
            ));
            assert!(matches!(
                KafkaDelayedRetryReadinessConfig::new(
                    Duration::from_secs(1),
                    Duration::from_secs(61)
                ),
                Err(KafkaDelayedRetryReadinessConfigError::TimeoutTooLong)
            ));
            let config = KafkaDelayedRetryReadinessConfig::new(
                Duration::from_millis(5),
                Duration::from_millis(7),
            )
            .unwrap();
            assert_eq!(config.database_timeout(), Duration::from_millis(5));
            assert_eq!(config.kafka_timeout(), Duration::from_millis(7));
        }

        #[tokio::test]
        async fn relay_loop_observes_an_immediate_shutdown_before_touching_postgres() {
            let producer_config = KafkaConfig::new("127.0.0.1:1", "events.source").unwrap();
            let retry =
                KafkaRetryConfig::new("events.retry", "events.dlq", NonZeroU16::new(2).unwrap())
                    .unwrap();
            let publisher = KafkaFailurePublisher::connect(&producer_config, retry).unwrap();
            let pool = PgPoolOptions::new()
                .connect_lazy("postgres://rustee:rustee@127.0.0.1:1/rustee")
                .unwrap();
            let relay = PostgresKafkaDelayedRetryRelay::new(
                pool,
                publisher,
                KafkaDelayedRetryRelayConfig::new(
                    KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap(),
                    KafkaDelayedRetryDelay::new(Duration::from_secs(1)).unwrap(),
                ),
            );
            let report = relay
                .run_until(KafkaDelayedRetryRelayLoopConfig::default(), async {})
                .await
                .unwrap();
            assert_eq!(report, KafkaDelayedRetryRelayLoopReport::default());
        }
    }
}

#[cfg(feature = "rdkafka")]
pub use adapter::*;
