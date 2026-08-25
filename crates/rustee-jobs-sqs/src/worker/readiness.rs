//! Read-only source and dead-letter queue deployment verification.

use aws_sdk_sqs::{Client, types::QueueAttributeName};
use serde::Deserialize;

use crate::{
    SqsError, SqsWorkerConfig,
    readiness::{queue_attributes, verify_queue_kind_attributes},
};

#[derive(Deserialize)]
struct RedrivePolicy {
    #[serde(rename = "deadLetterTargetArn")]
    dead_letter_target_arn: String,
    #[serde(rename = "maxReceiveCount")]
    max_receive_count: String,
}

pub(super) async fn verify(client: &Client, config: &SqsWorkerConfig) -> Result<(), SqsError> {
    let source = queue_attributes(client, config.source(), config.request_timeout())
        .await
        .map_err(|()| SqsError::Readiness)?;
    let dead_letter = queue_attributes(client, config.dead_letter(), config.request_timeout())
        .await
        .map_err(|()| SqsError::Readiness)?;
    verify_queue_kind_attributes(&source, config.source())?;
    verify_queue_kind_attributes(&dead_letter, config.dead_letter())?;

    let dead_letter_arn = dead_letter
        .get(&QueueAttributeName::QueueArn)
        .ok_or(SqsError::RedrivePolicy)?;
    let redrive = source
        .get(&QueueAttributeName::RedrivePolicy)
        .ok_or(SqsError::RedrivePolicy)?;
    let redrive: RedrivePolicy =
        serde_json::from_str(redrive).map_err(|_| SqsError::RedrivePolicy)?;
    let max_receive_count = redrive
        .max_receive_count
        .parse::<u16>()
        .map_err(|_| SqsError::RedrivePolicy)?;
    if redrive.dead_letter_target_arn != *dead_letter_arn
        || max_receive_count != config.expected_redrive_max_receive_count()
    {
        return Err(SqsError::RedrivePolicy);
    }
    Ok(())
}
