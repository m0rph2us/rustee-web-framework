//! Ownership-checked `Redis` Lua settlement and server-clock retry promotion.

use std::time::Duration;

use rustee_redis::redis::Script;

use crate::{RedisStreamsError, config::nonzero_duration_to_millis, operation::bounded};

use super::RedisStreamsWorker;

const ACK_IF_OWNED_SCRIPT: &str = r"
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, ARGV[3])
if #pending == 0 then
  return 0
end
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
";

const SCHEDULE_RETRY_SCRIPT: &str = r"
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, ARGV[3])
if #pending == 0 then
  return 0
end
local now = redis.call('TIME')
local due = now[1] * 1000 + math.floor(now[2] / 1000) + tonumber(ARGV[5])
redis.call('HSET', KEYS[3], ARGV[4], ARGV[6])
redis.call('HSET', KEYS[4], ARGV[4], ARGV[7])
redis.call('ZADD', KEYS[2], due, ARGV[4])
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
";

const DEAD_LETTER_IF_OWNED_SCRIPT: &str = r"
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, ARGV[3])
if #pending == 0 then
  return 0
end
redis.call('XADD', KEYS[2], '*', 'payload', ARGV[4], 'attempt', ARGV[5], 'source_entry_id', ARGV[2])
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
";

const PROMOTE_DUE_RETRIES_SCRIPT: &str = r"
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms, 'LIMIT', 0, tonumber(ARGV[1]))
for _, id in ipairs(ids) do
  if not redis.call('HEXISTS', KEYS[3], id) or not redis.call('HEXISTS', KEYS[4], id) then
    return redis.error_reply('rustee retry record is incomplete')
  end
end
for _, id in ipairs(ids) do
  local payload = redis.call('HGET', KEYS[3], id)
  local attempt = redis.call('HGET', KEYS[4], id)
  redis.call('XADD', KEYS[1], '*', 'payload', payload, 'attempt', attempt)
  redis.call('HDEL', KEYS[3], id)
  redis.call('HDEL', KEYS[4], id)
  redis.call('ZREM', KEYS[2], id)
end
return #ids
";

impl RedisStreamsWorker {
    pub(crate) async fn acknowledge(&self, entry_id: &str) -> Result<(), RedisStreamsError> {
        let mut connection = self.connection.clone();
        let script = Script::new(ACK_IF_OWNED_SCRIPT);
        let mut invoke = script.prepare_invoke();
        invoke
            .key(self.config.stream())
            .arg(self.config.group())
            .arg(entry_id)
            .arg(self.config.consumer());
        let settled: usize = bounded(
            self.config.operation_timeout(),
            invoke.invoke_async(&mut connection),
        )
        .await
        .map_err(|()| RedisStreamsError::Acknowledge)?;
        (settled == 1)
            .then_some(())
            .ok_or(RedisStreamsError::DeliveryOwnershipLost)
    }

    pub(crate) async fn schedule_retry(
        &self,
        entry_id: &str,
        payload: &[u8],
        next_attempt: u16,
        delay: Duration,
    ) -> Result<(), RedisStreamsError> {
        let delay_ms =
            nonzero_duration_to_millis(delay).map_err(|_| RedisStreamsError::RetrySchedule)?;
        let mut connection = self.connection.clone();
        let script = Script::new(SCHEDULE_RETRY_SCRIPT);
        let mut invoke = script.prepare_invoke();
        invoke
            .key(self.config.stream())
            .key(self.config.retry_schedule_key())
            .key(self.config.retry_payload_key())
            .key(self.config.retry_attempt_key())
            .arg(self.config.group())
            .arg(entry_id)
            .arg(self.config.consumer())
            .arg(entry_id)
            .arg(delay_ms)
            .arg(payload)
            .arg(next_attempt);
        let settled: usize = bounded(
            self.config.operation_timeout(),
            invoke.invoke_async(&mut connection),
        )
        .await
        .map_err(|()| RedisStreamsError::RetrySchedule)?;
        (settled == 1)
            .then_some(())
            .ok_or(RedisStreamsError::DeliveryOwnershipLost)
    }

    pub(crate) async fn dead_letter(
        &self,
        entry_id: &str,
        payload: &[u8],
        attempt: u16,
    ) -> Result<(), RedisStreamsError> {
        let mut connection = self.connection.clone();
        let script = Script::new(DEAD_LETTER_IF_OWNED_SCRIPT);
        let mut invoke = script.prepare_invoke();
        invoke
            .key(self.config.stream())
            .key(self.config.dead_letter_stream())
            .arg(self.config.group())
            .arg(entry_id)
            .arg(self.config.consumer())
            .arg(payload)
            .arg(attempt);
        let settled: usize = bounded(
            self.config.operation_timeout(),
            invoke.invoke_async(&mut connection),
        )
        .await
        .map_err(|()| RedisStreamsError::DeadLetter)?;
        (settled == 1)
            .then_some(())
            .ok_or(RedisStreamsError::DeliveryOwnershipLost)
    }

    pub(super) async fn promote_due_retries(&self, count: usize) -> Result<(), RedisStreamsError> {
        let mut connection = self.connection.clone();
        let script = Script::new(PROMOTE_DUE_RETRIES_SCRIPT);
        let mut invoke = script.prepare_invoke();
        invoke
            .key(self.config.stream())
            .key(self.config.retry_schedule_key())
            .key(self.config.retry_payload_key())
            .key(self.config.retry_attempt_key())
            .arg(count);
        bounded(
            self.config.operation_timeout(),
            invoke.invoke_async::<usize>(&mut connection),
        )
        .await
        .map(|_| ())
        .map_err(|()| RedisStreamsError::RetryPromotion)
    }
}
