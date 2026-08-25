use std::time::Duration;

use crate::WorkerConfig;

#[test]
fn default_worker_settings_allow_bounded_parallelism_and_shutdown_drain() {
    let config = WorkerConfig::default();

    assert_eq!(config.concurrency.get(), 8);
    assert_eq!(config.drain_timeout, Duration::from_secs(30));
}
