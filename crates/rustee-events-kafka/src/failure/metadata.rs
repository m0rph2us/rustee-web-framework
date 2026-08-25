//! Bounded retry-header parsing and preserved-origin reconstruction.

use rdkafka::message::{BorrowedMessage, Headers, Message};

use crate::{KafkaError, config::validate_topic_name};

pub(super) const RETRY_ATTEMPT_HEADER: &str = "rustee-event-retry-attempt";
pub(super) const FAILURE_KIND_HEADER: &str = "rustee-event-failure-kind";
pub(super) const ORIGIN_TOPIC_HEADER: &str = "rustee-event-origin-topic";
pub(super) const ORIGIN_PARTITION_HEADER: &str = "rustee-event-origin-partition";
pub(super) const ORIGIN_OFFSET_HEADER: &str = "rustee-event-origin-offset";

#[derive(Clone)]
pub(super) struct FailureOrigin {
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) offset: i64,
}

impl FailureOrigin {
    pub(super) fn from_message(message: &BorrowedMessage<'_>) -> Result<Self, KafkaError> {
        Self::from_headers(
            message.topic(),
            message.partition(),
            message.offset(),
            message.headers(),
        )
    }

    fn from_headers<H>(
        source_topic: &str,
        source_partition: i32,
        source_offset: i64,
        headers: Option<&H>,
    ) -> Result<Self, KafkaError>
    where
        H: Headers,
    {
        let topic = unique_header_text(headers, ORIGIN_TOPIC_HEADER)?;
        let partition = unique_header_text(headers, ORIGIN_PARTITION_HEADER)?;
        let offset = unique_header_text(headers, ORIGIN_OFFSET_HEADER)?;
        match (topic, partition, offset) {
            (None, None, None) => Self::source(source_topic, source_partition, source_offset),
            (Some(topic), Some(partition), Some(offset)) => {
                validate_topic_name(topic).map_err(|_| KafkaError::RetryMetadata)?;
                let partition = parse_non_negative_i32(partition)?;
                let offset = parse_non_negative_i64(offset)?;
                Ok(Self {
                    topic: topic.to_owned(),
                    partition,
                    offset,
                })
            }
            _ => Err(KafkaError::RetryMetadata),
        }
    }

    fn source(topic: &str, partition: i32, offset: i64) -> Result<Self, KafkaError> {
        validate_topic_name(topic).map_err(|_| KafkaError::RetryMetadata)?;
        if partition < 0 || offset < 0 {
            return Err(KafkaError::RetryMetadata);
        }
        Ok(Self {
            topic: topic.to_owned(),
            partition,
            offset,
        })
    }
}

pub(crate) fn retry_attempt(message: &BorrowedMessage<'_>) -> Result<u16, KafkaError> {
    let Some(headers) = message.headers() else {
        return Ok(1);
    };
    let mut attempts = headers
        .iter()
        .filter(|header| header.key == RETRY_ATTEMPT_HEADER);
    let Some(header) = attempts.next() else {
        return Ok(1);
    };
    if attempts.next().is_some() {
        return Err(KafkaError::RetryMetadata);
    }
    let value = header
        .value
        .and_then(|value| std::str::from_utf8(value).ok())
        .ok_or(KafkaError::RetryMetadata)?;
    value
        .parse::<u16>()
        .ok()
        .filter(|attempt| *attempt > 0)
        .ok_or(KafkaError::RetryMetadata)
}

fn unique_header_text<'a, H>(
    headers: Option<&'a H>,
    name: &str,
) -> Result<Option<&'a str>, KafkaError>
where
    H: Headers,
{
    let Some(headers) = headers else {
        return Ok(None);
    };
    let mut matching = headers.iter().filter(|header| header.key == name);
    let Some(header) = matching.next() else {
        return Ok(None);
    };
    if matching.next().is_some() {
        return Err(KafkaError::RetryMetadata);
    }
    let value = header
        .value
        .and_then(|value| std::str::from_utf8(value).ok())
        .filter(|value| !value.trim().is_empty() && value.len() <= 255)
        .ok_or(KafkaError::RetryMetadata)?;
    Ok(Some(value))
}

fn parse_non_negative_i32(value: &str) -> Result<i32, KafkaError> {
    value
        .parse()
        .ok()
        .filter(|value: &i32| *value >= 0)
        .ok_or(KafkaError::RetryMetadata)
}

fn parse_non_negative_i64(value: &str) -> Result<i64, KafkaError> {
    value
        .parse()
        .ok()
        .filter(|value: &i64| *value >= 0)
        .ok_or(KafkaError::RetryMetadata)
}

#[cfg(test)]
mod tests {
    use rdkafka::message::{Header, OwnedHeaders};

    use super::{FailureOrigin, KafkaError};

    fn headers(values: &[(&str, &str)]) -> OwnedHeaders {
        values
            .iter()
            .fold(OwnedHeaders::new(), |headers, (key, value)| {
                headers.insert(Header {
                    key,
                    value: Some(*value),
                })
            })
    }

    #[test]
    fn origin_metadata_is_preserved_only_when_the_complete_unique_tuple_is_valid() {
        let headers = headers(&[
            ("rustee-event-origin-topic", "orders.paid.v1"),
            ("rustee-event-origin-partition", "2"),
            ("rustee-event-origin-offset", "41"),
        ]);
        let origin = FailureOrigin::from_headers("orders.retry.v1", 4, 99, Some(&headers))
            .expect("complete valid origin headers must be preserved");

        assert_eq!(origin.topic, "orders.paid.v1");
        assert_eq!(origin.partition, 2);
        assert_eq!(origin.offset, 41);

        let source = FailureOrigin::from_headers::<OwnedHeaders>("orders.retry.v1", 4, 99, None)
            .expect("missing origin tuple must use the consumed record metadata");
        assert_eq!(source.topic, "orders.retry.v1");
        assert_eq!(source.partition, 4);
        assert_eq!(source.offset, 99);
    }

    #[test]
    fn origin_metadata_rejects_partial_duplicate_and_malformed_values() {
        for headers in [
            headers(&[("rustee-event-origin-topic", "orders.paid.v1")]),
            headers(&[
                ("rustee-event-origin-topic", "orders.paid.v1"),
                ("rustee-event-origin-topic", "orders.other.v1"),
                ("rustee-event-origin-partition", "2"),
                ("rustee-event-origin-offset", "41"),
            ]),
            headers(&[
                ("rustee-event-origin-topic", "orders paid"),
                ("rustee-event-origin-partition", "2"),
                ("rustee-event-origin-offset", "41"),
            ]),
            headers(&[
                ("rustee-event-origin-topic", "orders.paid.v1"),
                ("rustee-event-origin-partition", "-1"),
                ("rustee-event-origin-offset", "41"),
            ]),
        ] {
            assert!(matches!(
                FailureOrigin::from_headers("orders.retry.v1", 4, 99, Some(&headers)),
                Err(KafkaError::RetryMetadata)
            ));
        }
    }
}
