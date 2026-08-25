use std::{fmt, future::Future, sync::Arc};

use aws_sdk_sqs::{Client, types::Message};
use futures_util::future::BoxFuture;
use rustee_jobs::{
    DeliveryAction, Job, JobDeliveryObserver, JobDeliveryOutcome, JobEnvelope, JobHandler,
    JobRegistry, JobRegistryError, NoopJobDeliveryObserver, RetryPolicy, WorkerConfig, dispatch,
};

use crate::{
    SqsError, SqsWorkerConfig,
    delivery::{SqsDelivery, outcome_for_action, run_with_lease, settle_delivery},
};

mod readiness;
mod runner;

pub(crate) use runner::validate_retry_policy;
#[cfg(test)]
pub(crate) use runner::{SaturatedWorkerWait, wait_for_task_or_shutdown};

/// An SQS worker that preserves visibility/delete/redrive semantics for one Rustee job queue.
#[derive(Clone)]
pub struct SqsWorker {
    client: Client,
    config: SqsWorkerConfig,
    observer: Arc<dyn JobDeliveryObserver>,
}

impl SqsWorker {
    /// Wraps an explicitly configured AWS SDK client and a deployment-owned SQS worker route.
    #[must_use]
    pub fn new(client: Client, config: SqsWorkerConfig) -> Self {
        Self {
            client,
            config,
            observer: Arc::new(NoopJobDeliveryObserver),
        }
    }

    /// Attaches a non-blocking observer for bounded delivery lifecycle telemetry.
    ///
    /// Observer panics are isolated from SQS visibility, direct-DLQ, and delete behavior.
    #[must_use]
    pub fn with_delivery_observer(mut self, observer: Arc<dyn JobDeliveryObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Verifies source/DLQ access, queue kinds, and the source redrive policy without mutation.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError::Readiness`] for a service or IAM failure, [`SqsError::QueueType`] when
    /// a configured Standard/FIFO mode does not match, or [`SqsError::RedrivePolicy`] when the
    /// deployment's DLQ ARN or max receive count differs from this worker configuration.
    pub async fn readiness(&self) -> Result<(), SqsError> {
        readiness::verify(&self.client, &self.config).await
    }

    /// Runs one typed handler until `shutdown` resolves, preserving the SQS visibility lease.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`SqsError`] when a receive, heartbeat, visibility, direct-DLQ, or
    /// delete operation fails. The source receipt is left unacknowledged on a settlement failure.
    pub async fn run_until<J, H, Shutdown>(
        &self,
        handler: H,
        retry_policy: RetryPolicy,
        worker_config: WorkerConfig,
        shutdown: Shutdown,
    ) -> Result<(), SqsError>
    where
        J: Job,
        H: JobHandler<J>,
        Shutdown: Future<Output = ()> + Send,
    {
        validate_retry_policy(
            retry_policy,
            self.config.expected_redrive_max_receive_count(),
        )?;
        let client = self.client.clone();
        let config = self.config.clone();
        let processor = move |message: Message| {
            let client = client.clone();
            let config = config.clone();
            let handler = handler.clone();
            Box::pin(async move {
                let delivery = SqsDelivery::from_message(&message)?;
                let payload = delivery.payload().to_vec();
                let attempt = delivery.attempt();
                let timeout_action = retry_policy.after_failure(attempt);
                let action = run_with_lease(
                    &client,
                    &config,
                    &delivery,
                    async move {
                        match JobEnvelope::<J>::decode(&payload) {
                            Ok(envelope) => match envelope.with_attempt(attempt) {
                                Ok(envelope) => match dispatch(envelope, &handler).await {
                                    Ok(action) => Ok(action),
                                    Err(_) => Ok(retry_policy.after_failure(attempt)),
                                },
                                Err(_) => Ok(DeliveryAction::DeadLetter),
                            },
                            Err(_) => Ok(DeliveryAction::DeadLetter),
                        }
                    },
                    timeout_action,
                )
                .await?;
                settle_delivery(&client, &config, &delivery, action).await?;
                Ok((attempt, outcome_for_action(action)))
            }) as BoxFuture<'static, Result<(u16, JobDeliveryOutcome), SqsError>>
        };
        runner::run(
            &self.client,
            &self.config,
            Arc::clone(&self.observer),
            worker_config,
            shutdown,
            processor,
        )
        .await
    }

    /// Runs an immutable typed registry until `shutdown` resolves.
    ///
    /// Unknown and malformed envelopes are sent straight to the configured direct DLQ; only a
    /// registered handler failure follows `retry_policy`.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`SqsError`] when delivery, lease, or settlement fails.
    pub async fn run_registry_until<Shutdown>(
        &self,
        registry: JobRegistry,
        retry_policy: RetryPolicy,
        worker_config: WorkerConfig,
        shutdown: Shutdown,
    ) -> Result<(), SqsError>
    where
        Shutdown: Future<Output = ()> + Send,
    {
        validate_retry_policy(
            retry_policy,
            self.config.expected_redrive_max_receive_count(),
        )?;
        let client = self.client.clone();
        let config = self.config.clone();
        let processor = move |message: Message| {
            let client = client.clone();
            let config = config.clone();
            let registry = registry.clone();
            Box::pin(async move {
                let delivery = SqsDelivery::from_message(&message)?;
                let payload = delivery.payload().to_vec();
                let attempt = delivery.attempt();
                let timeout_action = retry_policy.after_failure(attempt);
                let action = run_with_lease(
                    &client,
                    &config,
                    &delivery,
                    async move {
                        match registry.dispatch(&payload, attempt).await {
                            Ok(action) => Ok(action),
                            Err(JobRegistryError::Handler { .. }) => {
                                Ok(retry_policy.after_failure(attempt))
                            }
                            Err(_) => Ok(DeliveryAction::DeadLetter),
                        }
                    },
                    timeout_action,
                )
                .await?;
                settle_delivery(&client, &config, &delivery, action).await?;
                Ok((attempt, outcome_for_action(action)))
            }) as BoxFuture<'static, Result<(u16, JobDeliveryOutcome), SqsError>>
        };
        runner::run(
            &self.client,
            &self.config,
            Arc::clone(&self.observer),
            worker_config,
            shutdown,
            processor,
        )
        .await
    }

    /// Returns the worker's deployment-provisioned configuration.
    #[must_use]
    pub fn config(&self) -> &SqsWorkerConfig {
        &self.config
    }
}

impl fmt::Debug for SqsWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqsWorker")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
