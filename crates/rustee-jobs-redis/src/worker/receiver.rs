//! Redis Streams consumer-group reads, PEL reclaim, and delivery reconstruction.

use rustee_redis::redis::{
    self, AsyncCommands,
    streams::{
        StreamAutoClaimOptions, StreamId, StreamPendingCountReply, StreamReadOptions,
        StreamReadReply,
    },
};

use crate::{
    ATTEMPT_FIELD, PAYLOAD_FIELD, RedisStreamsError, delivery::RedisStreamsDelivery,
    operation::bounded,
};

use super::RedisStreamsWorker;

impl RedisStreamsWorker {
    pub(super) async fn read_new(
        &self,
        capacity: usize,
    ) -> Result<Vec<RedisStreamsDelivery>, RedisStreamsError> {
        let count = capacity.min(self.config.batch_size());
        let options = StreamReadOptions::default()
            .group(self.config.group(), self.config.consumer())
            .count(count)
            .block(self.config.block_timeout_ms());
        let mut connection = self.connection.clone();
        let reply: StreamReadReply = bounded(
            self.config.operation_timeout(),
            connection.xread_options(&[self.config.stream()], &[">"], &options),
        )
        .await
        .map_err(|()| RedisStreamsError::Receive)?;
        Ok(reply
            .keys
            .into_iter()
            .flat_map(|key| key.ids)
            .map(|entry| self.delivery_from_entry(entry, None))
            .collect())
    }

    pub(super) async fn reclaim_pending(
        &self,
        count: usize,
    ) -> Result<Vec<RedisStreamsDelivery>, RedisStreamsError> {
        let mut connection = self.connection.clone();
        let response: redis::streams::StreamAutoClaimReply = bounded(
            self.config.operation_timeout(),
            connection.xautoclaim_options(
                self.config.stream(),
                self.config.group(),
                self.config.consumer(),
                self.config.reclaim_idle_ms(),
                "0-0",
                StreamAutoClaimOptions::default().count(count),
            ),
        )
        .await
        .map_err(|()| RedisStreamsError::Reclaim)?;
        if !response.deleted_ids.is_empty() || response.invalid_entries {
            return Err(RedisStreamsError::ClaimedEntryMissing);
        }
        let mut deliveries = Vec::with_capacity(response.claimed.len());
        for entry in response.claimed {
            let delivery_count = self.pending_delivery_count(&entry.id).await?;
            deliveries.push(self.delivery_from_entry(entry, Some(delivery_count)));
        }
        Ok(deliveries)
    }

    async fn pending_delivery_count(&self, entry_id: &str) -> Result<usize, RedisStreamsError> {
        let mut connection = self.connection.clone();
        let pending: StreamPendingCountReply = bounded(
            self.config.operation_timeout(),
            connection.xpending_count(
                self.config.stream(),
                self.config.group(),
                entry_id,
                entry_id,
                1,
            ),
        )
        .await
        .map_err(|()| RedisStreamsError::Reclaim)?;
        pending
            .ids
            .first()
            .map(|entry| entry.times_delivered)
            .ok_or(RedisStreamsError::DeliveryMetadata)
    }

    fn delivery_from_entry(
        &self,
        entry: StreamId,
        pending_deliveries: Option<usize>,
    ) -> RedisStreamsDelivery {
        let payload = entry.get::<Vec<u8>>(PAYLOAD_FIELD).unwrap_or_default();
        let attempt = entry
            .get::<u16>(ATTEMPT_FIELD)
            .and_then(|base_attempt| match pending_deliveries {
                Some(deliveries) => deliveries
                    .checked_sub(1)
                    .and_then(|redeliveries| u16::try_from(redeliveries).ok())
                    .and_then(|redeliveries| base_attempt.checked_add(redeliveries)),
                None => Some(base_attempt),
            })
            .filter(|attempt| *attempt > 0);
        RedisStreamsDelivery::new(self.clone(), entry.id, payload, attempt)
    }
}
