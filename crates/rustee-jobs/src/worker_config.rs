//! Shared provider-neutral job-worker runtime settings.

use std::{num::NonZeroUsize, time::Duration};

/// Worker settings shared by provider runtimes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerConfig {
    /// Maximum number of handler invocations running in this worker process.
    pub concurrency: NonZeroUsize,
    /// Time allowed to drain active handler invocations during shutdown.
    pub drain_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
            drain_timeout: Duration::from_secs(30),
        }
    }
}
