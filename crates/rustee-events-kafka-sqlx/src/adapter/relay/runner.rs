//! Shutdown-aware supervision of bounded delayed-retry relay passes.

use std::future::Future;

use rustee_events_kafka::KafkaError;

use super::super::config::{KafkaDelayedRetryRelayLoopConfig, KafkaDelayedRetryRelayLoopReport};
use super::PostgresKafkaDelayedRetryRelay;

impl PostgresKafkaDelayedRetryRelay {
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
            let published = self.relay_once(loop_config.batch_size()).await?;
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
}
