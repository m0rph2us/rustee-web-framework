use std::{num::NonZeroU16, time::Duration};

use crate::{
    ConfigError, KafkaConfig, KafkaConsumerConfig, KafkaRetryAction, KafkaRetryConfig,
    MAX_NATIVE_OPTION_COUNT, MAX_NATIVE_OPTION_KEY_BYTES, MAX_NATIVE_OPTION_VALUE_BYTES,
    MAX_TOPIC_BYTES,
};

#[test]
fn configuration_debug_redacts_connection_and_routing_values() {
    let config = KafkaConfig::new("broker:9092", "tenant.acme.orders.paid.v1")
        .unwrap()
        .with_option("sasl.password", "secret")
        .unwrap();
    let consumer = KafkaConsumerConfig::new(
        "broker:9092",
        "tenant.acme.orders.paid.v1",
        "tenant.acme.billing",
    )
    .unwrap()
    .with_retry_topic("tenant.acme.orders.paid.retry.v1")
    .unwrap()
    .with_option("sasl.password", "secret")
    .unwrap();
    let retry = KafkaRetryConfig::new(
        "tenant.acme.orders.paid.retry.v1",
        "tenant.acme.orders.paid.dlq.v1",
        NonZeroU16::new(3).unwrap(),
    )
    .unwrap();

    let producer_debug = format!("{config:?}");
    let consumer_debug = format!("{consumer:?}");
    let retry_debug = format!("{retry:?}");
    for exposed in [
        "secret",
        "sasl.password",
        "broker:9092",
        "tenant.acme.orders.paid.v1",
        "tenant.acme.billing",
        "tenant.acme.orders.paid.retry.v1",
        "tenant.acme.orders.paid.dlq.v1",
    ] {
        assert!(!producer_debug.contains(exposed));
        assert!(!consumer_debug.contains(exposed));
        assert!(!retry_debug.contains(exposed));
    }
    assert!(producer_debug.contains("topic_length"));
    assert!(producer_debug.contains("native_option_count"));
    assert!(consumer_debug.contains("group_id_length"));
    assert!(consumer_debug.contains("retry_topic_length"));
    assert!(retry_debug.contains("dead_letter_topic_length"));
}

#[test]
fn topic_admission_rejects_whitespace_nul_and_values_beyond_the_broker_bound() {
    for topic in [
        "orders paid".to_owned(),
        "orders\0paid".to_owned(),
        "o".repeat(MAX_TOPIC_BYTES + 1),
    ] {
        assert_eq!(
            KafkaConfig::new("broker:9092", topic.as_str()).unwrap_err(),
            ConfigError::InvalidTopic
        );
        assert_eq!(
            KafkaConsumerConfig::new("broker:9092", topic.as_str(), "billing").unwrap_err(),
            ConfigError::InvalidTopic
        );
        assert_eq!(
            KafkaConsumerConfig::new("broker:9092", "orders.paid.v1", "billing")
                .unwrap()
                .with_retry_topic(topic.as_str())
                .unwrap_err(),
            ConfigError::InvalidTopic
        );
        assert_eq!(
            KafkaRetryConfig::new(
                topic.as_str(),
                "orders.paid.dlq.v1",
                NonZeroU16::new(2).unwrap(),
            )
            .unwrap_err(),
            ConfigError::InvalidTopic
        );
    }

    assert!(KafkaConfig::new("broker:9092", "o".repeat(MAX_TOPIC_BYTES)).is_ok());
}

#[test]
fn bootstrap_servers_are_bounded_and_revalidated_for_each_client_role() {
    for bootstrap_servers in ["", " \t", "broker\0:9092"] {
        assert_eq!(
            KafkaConfig::new(bootstrap_servers, "orders.paid.v1").unwrap_err(),
            ConfigError::InvalidBootstrapServers
        );
        assert_eq!(
            KafkaConsumerConfig::new(bootstrap_servers, "orders.paid.v1", "billing").unwrap_err(),
            ConfigError::InvalidBootstrapServers
        );
    }
    let oversized = "b".repeat(16 * 1024 + 1);
    assert_eq!(
        KafkaConfig::new(oversized, "orders.paid.v1").unwrap_err(),
        ConfigError::InvalidBootstrapServers
    );
}

#[test]
fn consumer_group_with_whitespace_is_rejected() {
    let error =
        KafkaConsumerConfig::new("broker:9092", "orders.paid.v1", "billing group").unwrap_err();
    assert_eq!(error, ConfigError::InvalidGroupId);
}

#[test]
fn retry_topic_must_differ_from_the_source_topic() {
    let error = KafkaConsumerConfig::new("broker:9092", "orders.paid.v1", "billing")
        .unwrap()
        .with_retry_topic("orders.paid.v1")
        .unwrap_err();

    assert_eq!(error, ConfigError::RetryTopicMatchesSource);
}

#[test]
fn retry_policy_advances_then_dead_letters_at_its_delivery_budget() {
    let retry = KafkaRetryConfig::new(
        "orders.paid.retry.v1",
        "orders.paid.dlq.v1",
        NonZeroU16::new(3).unwrap(),
    )
    .unwrap();

    assert_eq!(
        retry.after_failure(1),
        KafkaRetryAction::Retry { next_attempt: 2 }
    );
    assert_eq!(
        retry.after_failure(2),
        KafkaRetryAction::Retry { next_attempt: 3 }
    );
    assert_eq!(retry.after_failure(3), KafkaRetryAction::DeadLetter);
}

#[test]
fn retry_and_dead_letter_topics_must_differ() {
    let error = KafkaRetryConfig::new(
        "orders.paid.retry.v1",
        "orders.paid.retry.v1",
        NonZeroU16::new(2).unwrap(),
    )
    .unwrap_err();

    assert_eq!(error, ConfigError::RetryTopicMatchesDeadLetter);
}

#[test]
fn delivery_deadline_must_be_representable_and_non_zero() {
    let zero = KafkaConfig::new("broker:9092", "orders.paid.v1")
        .unwrap()
        .with_delivery_timeout(Duration::ZERO)
        .unwrap_err();
    assert_eq!(zero, ConfigError::InvalidDeliveryTimeout);

    let sub_millisecond = KafkaConfig::new("broker:9092", "orders.paid.v1")
        .unwrap()
        .with_delivery_timeout(Duration::from_nanos(1))
        .unwrap_err();
    assert_eq!(sub_millisecond, ConfigError::InvalidDeliveryTimeout);

    let fractional_millisecond = KafkaConfig::new("broker:9092", "orders.paid.v1")
        .unwrap()
        .with_delivery_timeout(Duration::from_millis(1) + Duration::from_nanos(1))
        .unwrap_err();
    assert_eq!(fractional_millisecond, ConfigError::InvalidDeliveryTimeout);

    let too_large = KafkaConfig::new("broker:9092", "orders.paid.v1")
        .unwrap()
        .with_delivery_timeout(Duration::from_millis(i32::MAX as u64 + 1))
        .unwrap_err();
    assert_eq!(too_large, ConfigError::InvalidDeliveryTimeout);
}

#[test]
fn native_option_admission_is_bounded_and_allows_replacement() {
    let base = KafkaConfig::new("broker:9092", "orders.paid.v1").unwrap();
    assert_eq!(
        base.clone().with_option("", "value").unwrap_err(),
        ConfigError::InvalidNativeOption
    );
    assert_eq!(
        base.clone()
            .with_option("x".repeat(MAX_NATIVE_OPTION_KEY_BYTES + 1), "value")
            .unwrap_err(),
        ConfigError::InvalidNativeOption
    );
    assert_eq!(
        base.clone()
            .with_option(
                "security.protocol",
                "x".repeat(MAX_NATIVE_OPTION_VALUE_BYTES + 1)
            )
            .unwrap_err(),
        ConfigError::InvalidNativeOption
    );

    let mut config = base;
    for index in 0..MAX_NATIVE_OPTION_COUNT {
        config = config
            .with_option(format!("option.{index}"), "value")
            .unwrap();
    }
    assert_eq!(
        config.clone().with_option("one-more", "value").unwrap_err(),
        ConfigError::NativeOptionLimit
    );
    assert!(config.with_option("option.0", "replacement").is_ok());
}
