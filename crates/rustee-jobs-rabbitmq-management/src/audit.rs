use std::{fmt, time::Duration};

use reqwest::Client;
use rustee_jobs::RetryPolicy;
use rustee_jobs_rabbitmq::RabbitMqWorkerConfig;

use crate::{
    QueueSnapshot, RabbitMqManagementConfig, RabbitMqManagementError,
    transport::fetch_queue_snapshot,
};

/// Read-only topology auditor for one worker source queue.
///
/// Its `Debug` output does not delegate to HTTP client diagnostics.
#[derive(Clone)]
pub struct RabbitMqTopologyAuditor {
    client: Client,
    config: RabbitMqManagementConfig,
    worker: RabbitMqWorkerConfig,
    retry_policy: RetryPolicy,
}

impl fmt::Debug for RabbitMqTopologyAuditor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqTopologyAuditor")
            .field("client", &"[REDACTED]")
            .field("config", &self.config)
            .field("worker", &self.worker)
            .field("retry_policy", &self.retry_policy)
            .finish_non_exhaustive()
    }
}

impl RabbitMqTopologyAuditor {
    /// Builds a read-only auditor for one worker and retry policy.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqManagementError::Client`] when the HTTP client cannot be initialized.
    pub fn new(
        config: RabbitMqManagementConfig,
        worker: RabbitMqWorkerConfig,
        retry_policy: RetryPolicy,
    ) -> Result<Self, RabbitMqManagementError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| RabbitMqManagementError::Client)?;
        Ok(Self {
            client,
            config,
            worker,
            retry_policy,
        })
    }

    /// Fetches and validates the configured source queue without changing broker state.
    ///
    /// # Errors
    ///
    /// Returns a sanitized management transport, response, or topology error.
    pub async fn audit(&self) -> Result<RabbitMqTopologyReport, RabbitMqManagementError> {
        let snapshot =
            fetch_queue_snapshot(&self.client, &self.config, self.worker.queue()).await?;
        self.audit_snapshot(&snapshot)
    }

    /// Validates a management queue snapshot, useful for deployment tests and recorded audits.
    ///
    /// # Errors
    ///
    /// Returns [`RabbitMqManagementError::TopologyMismatch`] when the snapshot does not prove
    /// the quorum queue and effective delayed-retry/DLQ policy expected by the worker.
    pub fn audit_snapshot(
        &self,
        snapshot: &QueueSnapshot,
    ) -> Result<RabbitMqTopologyReport, RabbitMqManagementError> {
        if snapshot.queue_type != "quorum" || !snapshot.durable || snapshot.auto_delete {
            return Err(RabbitMqManagementError::TopologyMismatch);
        }

        let values = snapshot.effective_values();
        let native = self.worker.native_retry();
        let min = duration_millis(native.minimum_delay())
            .ok_or(RabbitMqManagementError::TopologyMismatch)?;
        let max = duration_millis(native.maximum_delay())
            .ok_or(RabbitMqManagementError::TopologyMismatch)?;
        let delivery_limit = values
            .integer("delivery-limit")
            .and_then(|value| u16::try_from(value).ok())
            .ok_or(RabbitMqManagementError::TopologyMismatch)?;

        if values.text("delayed-retry-type") != Some("failed")
            || values.integer("delayed-retry-min") != Some(min)
            || values.integer("delayed-retry-max") != Some(max)
            || delivery_limit < self.retry_policy.max_deliveries
            || values.text("dead-letter-exchange") != Some(self.worker.dead_letter_exchange())
            || values.text("dead-letter-routing-key") != Some(self.worker.dead_letter_routing_key())
        {
            return Err(RabbitMqManagementError::TopologyMismatch);
        }

        Ok(RabbitMqTopologyReport { delivery_limit })
    }
}

/// Safe result of one successful topology audit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RabbitMqTopologyReport {
    delivery_limit: u16,
}

impl RabbitMqTopologyReport {
    /// Returns the effective quorum queue delivery limit reported by `RabbitMQ`.
    #[must_use]
    pub const fn delivery_limit(self) -> u16 {
        self.delivery_limit
    }
}

fn duration_millis(value: Duration) -> Option<i64> {
    i64::try_from(value.as_millis()).ok()
}
