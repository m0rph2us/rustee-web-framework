use std::{collections::HashMap, time::Duration};

use aws_sdk_sqs::{Client, types::QueueAttributeName};
use tokio::time::timeout;

use crate::{SqsError, SqsQueueTarget};

pub(crate) async fn queue_attributes(
    client: &Client,
    target: &SqsQueueTarget,
    request_timeout: Duration,
) -> Result<HashMap<QueueAttributeName, String>, ()> {
    timeout(
        request_timeout,
        client
            .get_queue_attributes()
            .queue_url(target.queue_url())
            .attribute_names(QueueAttributeName::FifoQueue)
            .attribute_names(QueueAttributeName::QueueArn)
            .attribute_names(QueueAttributeName::RedrivePolicy)
            .send(),
    )
    .await
    .map_err(|_| ())?
    .map_err(|_| ())?
    .attributes
    .ok_or(())
}

pub(crate) async fn verify_queue_kind(
    client: &Client,
    target: &SqsQueueTarget,
    request_timeout: Duration,
) -> Result<(), SqsError> {
    let attributes = queue_attributes(client, target, request_timeout)
        .await
        .map_err(|()| SqsError::Readiness)?;
    verify_queue_kind_attributes(&attributes, target)
}

pub(crate) fn verify_queue_kind_attributes(
    attributes: &HashMap<QueueAttributeName, String>,
    target: &SqsQueueTarget,
) -> Result<(), SqsError> {
    let actual_fifo = attributes
        .get(&QueueAttributeName::FifoQueue)
        .is_some_and(|value| value == "true");
    if actual_fifo == target.kind().is_fifo() {
        Ok(())
    } else {
        Err(SqsError::QueueType)
    }
}
