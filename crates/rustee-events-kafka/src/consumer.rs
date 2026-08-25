//! Manual-commit `Kafka` event consumption, lag inspection, and failure routing.

use std::{fmt, future::Future, num::NonZeroU16, sync::Arc};

use rdkafka::{
    ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
};
use rustee_events::{
    Event, EventDeliveryObservation, EventDeliveryObserver, EventDeliveryOutcome, EventEnvelope,
    EventHandler, NoopEventDeliveryObserver, dispatch,
};

use super::config::{validate_group_id, validate_topic_name};
use super::failure::retry_attempt;
use super::{
    KafkaConsumerConfig, KafkaError, KafkaFailureKind, KafkaFailureRecord, KafkaFailureRouter,
    KafkaRetryAction,
};

mod inspection;

pub use inspection::{KafkaLagSnapshotLimit, KafkaPartitionLag};

/// A manual-commit `Kafka` consumer for one event topic and consumer group.
///
/// It processes records serially. This preserves per-partition offset correctness: a record is
/// committed only after its handler succeeds, and a handler failure leaves the offset uncommitted
/// for retry after process recovery or a rebalance. Its `Debug` output keeps deployment-routing
/// values redacted.
#[cfg(feature = "rdkafka")]
pub struct KafkaEventConsumer {
    consumer: StreamConsumer,
    topic: String,
    group_id: String,
    topics: Vec<String>,
    topic_scoped_readiness: bool,
    observer: Arc<dyn EventDeliveryObserver>,
}

#[cfg(feature = "rdkafka")]
impl KafkaEventConsumer {
    /// Creates and subscribes a consumer with automatic commit, offset storage, and topic
    /// creation disabled.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::CreateConsumer`] when librdkafka rejects the configuration, or
    /// [`KafkaError::Subscribe`] when the topic subscription cannot be established.
    pub fn connect(config: &KafkaConsumerConfig) -> Result<Self, KafkaError> {
        let mut client = ClientConfig::new();
        for (key, value) in config.options() {
            client.set(key, value);
        }
        // Typed configuration owns the group, commit, and topic-provisioning invariants.
        client
            .set("bootstrap.servers", config.bootstrap_servers())
            .set("group.id", config.group_id())
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("allow.auto.create.topics", "false");
        let consumer = client
            .create::<StreamConsumer>()
            .map_err(|_| KafkaError::CreateConsumer)?;
        let subscription_topics = config.subscription_topics();
        consumer
            .subscribe(&subscription_topics)
            .map_err(|_| KafkaError::Subscribe)?;
        Ok(Self {
            consumer,
            topic: config.topic().to_owned(),
            group_id: config.group_id().to_owned(),
            topics: subscription_topics
                .into_iter()
                .map(ToOwned::to_owned)
                .collect(),
            topic_scoped_readiness: true,
            observer: Arc::new(NoopEventDeliveryObserver),
        })
    }

    /// Wraps and subscribes an already-configured native consumer for dependency injection.
    ///
    /// The caller retains all native client settings, including topic-creation policy. Readiness
    /// therefore uses a full metadata request before it checks the subscribed topics.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Subscribe`] when the topic subscription cannot be established.
    pub fn new(
        consumer: StreamConsumer,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Result<Self, KafkaError> {
        let topic = topic.into();
        let group_id = group_id.into();
        validate_topic_name(&topic).map_err(|_| KafkaError::Subscribe)?;
        validate_group_id(&group_id).map_err(|_| KafkaError::Subscribe)?;
        consumer
            .subscribe(&[topic.as_str()])
            .map_err(|_| KafkaError::Subscribe)?;
        Ok(Self {
            consumer,
            topics: vec![topic.clone()],
            topic,
            group_id,
            topic_scoped_readiness: false,
            observer: Arc::new(NoopEventDeliveryObserver),
        })
    }

    /// Attaches a non-blocking observer for bounded event-delivery lifecycle telemetry.
    ///
    /// Observer panics are isolated from Kafka commit and failure-routing behavior. Use
    /// `rustee-events-observability::EventMetrics` for the built-in exporter-neutral collector.
    #[must_use]
    pub fn with_delivery_observer(mut self, observer: Arc<dyn EventDeliveryObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Receives, dispatches, and synchronously commits records until `shutdown` resolves.
    ///
    /// Shutdown wins over a ready next record. A handler already in progress is allowed to finish,
    /// then its successful record is committed before the consumer returns.
    ///
    /// The consumer is intentionally serial: committing offset <em>n</em> also commits all lower
    /// offsets in that partition. A handler must complete inside its configured Kafka poll interval.
    ///
    /// # Errors
    ///
    /// Returns an error without committing the current record when receive, decoding, handler, or
    /// commit fails.
    pub async fn run_until<E, H, F>(&self, handler: H, shutdown: F) -> Result<(), KafkaError>
    where
        E: Event,
        H: EventHandler<E>,
        F: Future<Output = ()> + Send,
    {
        let mut shutdown = Box::pin(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                message = self.consumer.recv() => {
                    let message = message.map_err(|_| KafkaError::Receive)?;
                    let observation = EventDeliveryObservation::start(Arc::clone(&self.observer), "apache_kafka");
                    let payload = message.payload().ok_or(KafkaError::MissingPayload)?;
                    let envelope = EventEnvelope::<E>::decode(payload).map_err(|_| KafkaError::Decode)?;
                    dispatch(envelope, &handler).await.map_err(|_| KafkaError::Handler)?;
                    self.consumer
                        .commit_message(&message, CommitMode::Sync)
                        .map_err(|_| KafkaError::Commit)?;
                    observation.finish(None, EventDeliveryOutcome::Acknowledged);
                }
            }
        }
    }

    /// Receives, routes failures to immediate retry or dead-letter Kafka topics, and commits only
    /// after the configured failure publisher reports broker acknowledgement.
    ///
    /// The consumer configuration must subscribe to `failure_publisher`'s retry topic via
    /// [`KafkaConsumerConfig::with_retry_topic`]. Retry records carry a one-based attempt header;
    /// malformed or duplicate attempt headers stop the worker without committing their source
    /// offset. Plain Kafka retry is immediate. Durable delayed delivery belongs in a
    /// deployment-owned failure router and relay, such as the optional
    /// `rustee-events-kafka-sqlx` integration, rather than a hidden sleep in this worker.
    ///
    /// # Errors
    ///
    /// Returns an error without committing the current record when receive, retry metadata,
    /// failure routing, or source-offset commit fails.
    pub async fn run_with_failure_routing<E, H, F>(
        &self,
        handler: H,
        failure_router: &impl KafkaFailureRouter,
        shutdown: F,
    ) -> Result<(), KafkaError>
    where
        E: Event,
        H: EventHandler<E>,
        F: Future<Output = ()> + Send,
    {
        validate_failure_route_topics(
            &self.topic,
            &self.topics,
            failure_router.retry_topic(),
            failure_router.dead_letter_topic(),
        )?;

        let mut shutdown = Box::pin(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                message = self.consumer.recv() => {
                    let message = message.map_err(|_| KafkaError::Receive)?;
                    let observation = EventDeliveryObservation::start(Arc::clone(&self.observer), "apache_kafka");
                    let attempt = retry_attempt(&message)?;
                    let payload = message.payload().ok_or(KafkaError::MissingPayload)?;
                    let outcome = match EventEnvelope::<E>::decode(payload) {
                        Ok(envelope) => match dispatch(envelope, &handler).await {
                            Ok(()) => EventDeliveryOutcome::Acknowledged,
                            Err(_) => {
                                match failure_router
                            .route(KafkaFailureRecord::new(&message)?, KafkaFailureKind::Handler, attempt)
                                    .await?
                                {
                                    KafkaRetryAction::Retry { .. } => EventDeliveryOutcome::Retried,
                                    KafkaRetryAction::DeadLetter => EventDeliveryOutcome::DeadLettered,
                                }
                            }
                        },
                        Err(_) => {
                            match failure_router
                            .route(KafkaFailureRecord::new(&message)?, KafkaFailureKind::Decode, attempt)
                                .await?
                            {
                                KafkaRetryAction::Retry { .. } => EventDeliveryOutcome::Retried,
                                KafkaRetryAction::DeadLetter => EventDeliveryOutcome::DeadLettered,
                            }
                        }
                    };
                    self.consumer
                        .commit_message(&message, CommitMode::Sync)
                        .map_err(|_| KafkaError::Commit)?;
                    observation.finish(NonZeroU16::new(attempt), outcome);
                }
            }
        }
    }
}

fn validate_failure_route_topics(
    source_topic: &str,
    subscribed_topics: &[String],
    retry_topic: &str,
    dead_letter_topic: &str,
) -> Result<(), KafkaError> {
    if validate_topic_name(retry_topic).is_err()
        || validate_topic_name(dead_letter_topic).is_err()
        || retry_topic == source_topic
        || dead_letter_topic == source_topic
        || retry_topic == dead_letter_topic
    {
        return Err(KafkaError::FailureRouteConfiguration);
    }
    if !subscribed_topics.iter().any(|topic| topic == retry_topic) {
        return Err(KafkaError::RetryTopicNotSubscribed);
    }
    Ok(())
}

#[cfg(feature = "rdkafka")]
impl fmt::Debug for KafkaEventConsumer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaEventConsumer")
            .field("topic", &"[REDACTED]")
            .field("topic_length", &self.topic.len())
            .field("group_id", &"[REDACTED]")
            .field("group_id_length", &self.group_id.len())
            .field("subscribed_topic_count", &self.topics.len())
            .field(
                "subscribed_topic_lengths",
                &self.topics.iter().map(String::len).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "rdkafka"))]
mod tests {
    use super::validate_failure_route_topics;
    use crate::KafkaError;

    #[test]
    fn failure_routes_must_be_distinct_from_the_source_and_each_other() {
        let subscribed = vec!["orders.paid".to_owned(), "orders.paid.retry".to_owned()];

        for (retry, dead_letter) in [
            ("orders.paid", "orders.paid.dlq"),
            ("orders.paid.retry", "orders.paid"),
            ("orders.paid.retry", "orders.paid.retry"),
            (" ", "orders.paid.dlq"),
        ] {
            assert_eq!(
                validate_failure_route_topics("orders.paid", &subscribed, retry, dead_letter),
                Err(KafkaError::FailureRouteConfiguration)
            );
        }
    }

    #[test]
    fn failure_routes_require_the_retry_topic_to_be_subscribed() {
        let subscribed = vec!["orders.paid".to_owned()];

        assert_eq!(
            validate_failure_route_topics(
                "orders.paid",
                &subscribed,
                "orders.paid.retry",
                "orders.paid.dlq",
            ),
            Err(KafkaError::RetryTopicNotSubscribed)
        );
    }
}
