//! Read-only Kafka consumer readiness, membership, and lag inspection.

use std::{fmt, num::NonZeroU16, time::Duration};

use rdkafka::consumer::Consumer;

use super::{KafkaError, KafkaEventConsumer};
use crate::topic_metadata_is_healthy;

const DEFAULT_LAG_SNAPSHOT_PARTITION_LIMIT: u16 = 128;

/// A non-zero maximum number of assigned partitions inspected in one lag snapshot.
///
/// Each partition requires a separate broker-watermark request, so callers with large
/// assignments must make the operational cost explicit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaLagSnapshotLimit(NonZeroU16);

impl KafkaLagSnapshotLimit {
    /// Creates an explicit maximum number of partitions inspected in one snapshot.
    #[must_use]
    pub const fn new(partitions: NonZeroU16) -> Self {
        Self(partitions)
    }

    /// Returns the maximum number of partitions inspected in one snapshot.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

impl Default for KafkaLagSnapshotLimit {
    fn default() -> Self {
        Self(
            NonZeroU16::new(DEFAULT_LAG_SNAPSHOT_PARTITION_LIMIT)
                .expect("default Kafka lag snapshot partition limit is non-zero"),
        )
    }
}

/// A bounded point-in-time lag observation for one assigned Kafka partition.
///
/// The observation is for diagnostics and metrics collection. Rebalances and retention can change
/// assignment, position, or watermarks immediately after it is returned. Its `Debug` output keeps
/// the deployment topic redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct KafkaPartitionLag {
    topic: String,
    partition: i32,
    position: Option<i64>,
    low_watermark: i64,
    high_watermark: i64,
    lag: Option<u64>,
}

impl KafkaPartitionLag {
    /// Returns the assigned topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the assigned partition number.
    #[must_use]
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the consumer's next position when librdkafka reports a concrete offset.
    #[must_use]
    pub const fn position(&self) -> Option<i64> {
        self.position
    }

    /// Returns the broker's current low watermark.
    #[must_use]
    pub const fn low_watermark(&self) -> i64 {
        self.low_watermark
    }

    /// Returns the broker's current high watermark.
    #[must_use]
    pub const fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// Returns the non-negative distance from the effective position to the high watermark.
    ///
    /// `None` means librdkafka has not established a concrete position for this assigned partition.
    #[must_use]
    pub const fn lag(&self) -> Option<u64> {
        self.lag
    }
}

impl fmt::Debug for KafkaPartitionLag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaPartitionLag")
            .field("topic", &"[REDACTED]")
            .field("topic_length", &self.topic.len())
            .field("partition", &self.partition)
            .field("position", &self.position)
            .field("low_watermark", &self.low_watermark)
            .field("high_watermark", &self.high_watermark)
            .field("lag", &self.lag)
            .finish()
    }
}

impl KafkaEventConsumer {
    /// Queries metadata for every subscribed source or retry topic before a worker starts.
    ///
    /// Framework-created consumers disable automatic topic creation and query each configured
    /// topic directly. An injected native consumer retains its caller-owned configuration, so it
    /// uses full metadata before checking subscribed topics. Each metadata request is bounded by
    /// `timeout`; readiness does not wait for group assignment, move offsets, confirm a handler
    /// can process a record, or start a background poll loop.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Readiness`] when broker metadata cannot be read before `timeout` or
    /// a subscribed topic is absent or has a broker-reported error.
    pub fn readiness(&self, timeout: Duration) -> Result<(), KafkaError> {
        if self.topic_scoped_readiness {
            for topic in &self.topics {
                let metadata = self
                    .consumer
                    .fetch_metadata(Some(topic), timeout)
                    .map_err(|_| KafkaError::Readiness)?;
                if !topic_metadata_is_healthy(&metadata, topic) {
                    return Err(KafkaError::Readiness);
                }
            }
            return Ok(());
        }

        let metadata = self
            .consumer
            .fetch_metadata(None, timeout)
            .map_err(|_| KafkaError::Readiness)?;
        for topic in &self.topics {
            if !topic_metadata_is_healthy(&metadata, topic) {
                return Err(KafkaError::Readiness);
            }
        }
        Ok(())
    }

    /// Returns the broker-reported number of members in this consumer group.
    ///
    /// This is an operational snapshot, not a readiness or assignment guarantee. Callers should
    /// use it for diagnostics and bounded health decisions rather than coordinating work.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::GroupMembership`] when the broker cannot report group state before
    /// `timeout`.
    pub fn group_member_count(&self, timeout: Duration) -> Result<usize, KafkaError> {
        let groups = self
            .consumer
            .fetch_group_list(Some(&self.group_id), timeout)
            .map_err(|_| KafkaError::GroupMembership)?;
        Ok(groups
            .groups()
            .iter()
            .find(|group| group.name() == self.group_id)
            .map_or(0, |group| group.members().len()))
    }

    /// Returns one lag observation for each partition currently assigned to this consumer, up to
    /// the default [`KafkaLagSnapshotLimit`] of 128 partitions.
    ///
    /// Each broker watermark request is bounded by `timeout`; the whole snapshot can therefore
    /// take longer than one timeout when more than one partition is assigned. A returned snapshot
    /// is not a coordination primitive and may already be stale after a rebalance.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::LagSnapshot`] when assignment, position, or broker watermarks cannot
    /// be read, or [`KafkaError::LagSnapshotLimitExceeded`] when the assignment exceeds the
    /// default limit.
    pub fn lag_snapshot(&self, timeout: Duration) -> Result<Vec<KafkaPartitionLag>, KafkaError> {
        self.lag_snapshot_with_limit(KafkaLagSnapshotLimit::default(), timeout)
    }

    /// Returns one lag observation for every currently assigned partition within `limit`.
    ///
    /// Each broker watermark request is bounded by `timeout`; the whole snapshot can therefore
    /// take longer than one timeout when more than one partition is assigned. A returned snapshot
    /// is not a coordination primitive and may already be stale after a rebalance.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::LagSnapshot`] when assignment, position, or broker watermarks cannot
    /// be read, or [`KafkaError::LagSnapshotLimitExceeded`] before making watermark requests when
    /// the assignment exceeds `limit`.
    pub fn lag_snapshot_with_limit(
        &self,
        limit: KafkaLagSnapshotLimit,
        timeout: Duration,
    ) -> Result<Vec<KafkaPartitionLag>, KafkaError> {
        let assignment = self
            .consumer
            .assignment()
            .map_err(|_| KafkaError::LagSnapshot)?;
        if assignment.count() > usize::from(limit.get()) {
            return Err(KafkaError::LagSnapshotLimitExceeded);
        }
        let positions = self
            .consumer
            .position()
            .map_err(|_| KafkaError::LagSnapshot)?;
        let mut snapshots = Vec::with_capacity(assignment.count());
        for assigned in assignment.elements() {
            let topic = assigned.topic().to_owned();
            let partition = assigned.partition();
            let position = positions
                .find_partition(&topic, partition)
                .and_then(|position| match position.offset() {
                    rdkafka::Offset::Offset(offset) if offset >= 0 => Some(offset),
                    _ => None,
                });
            let (low_watermark, high_watermark) = self
                .consumer
                .fetch_watermarks(&topic, partition, timeout)
                .map_err(|_| KafkaError::LagSnapshot)?;
            let lag = position.map(|position| {
                u64::try_from(
                    high_watermark
                        .saturating_sub(position.max(low_watermark))
                        .max(0),
                )
                .unwrap_or(0)
            });
            snapshots.push(KafkaPartitionLag {
                topic,
                partition,
                position,
                low_watermark,
                high_watermark,
                lag,
            });
        }
        Ok(snapshots)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use super::{KafkaLagSnapshotLimit, KafkaPartitionLag};

    #[test]
    fn lag_snapshot_limits_are_non_zero_and_explicit() {
        let limit = KafkaLagSnapshotLimit::new(NonZeroU16::new(8).unwrap());
        assert_eq!(limit.get(), 8);
        assert_eq!(KafkaLagSnapshotLimit::default().get(), 128);
    }

    #[test]
    fn lag_debug_output_redacts_the_deployment_topic() {
        let lag = KafkaPartitionLag {
            topic: "tenant.acme.orders.paid.v1".to_owned(),
            partition: 3,
            position: Some(17),
            low_watermark: 0,
            high_watermark: 24,
            lag: Some(7),
        };

        let debug = format!("{lag:?}");
        assert!(!debug.contains("tenant.acme.orders.paid.v1"));
        assert!(debug.contains("topic_length"));
        assert!(debug.contains("partition: 3"));
    }
}
