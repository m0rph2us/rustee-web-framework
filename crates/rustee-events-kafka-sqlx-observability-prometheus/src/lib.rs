//! Prometheus text exposition for Rustee Kafka `PostgreSQL` delayed-retry relay metrics.
//!
//! Enable the `rdkafka` feature to use this adapter. It has no registry, global state, listener,
//! query loop, or automatic route. An application owns a [`KafkaDelayedRetryRelayMetrics`]
//! collector and explicitly mounts [`metrics_response`] where its scrape policy permits.

#[cfg(feature = "rdkafka")]
mod adapter {
    use std::fmt::Write;

    use http::{
        HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE},
    };
    use rustee_core::{Response, full_body, response};
    use rustee_events_kafka_sqlx_observability::{
        KafkaDelayedRetryRelayMetrics, KafkaDelayedRetryRelayMetricsSnapshot, metric_names,
    };

    /// Prometheus text exposition content type for version 0.0.4.
    pub const CONTENT_TYPE_PROMETHEUS: &str = "text/plain; version=0.0.4; charset=utf-8";

    /// Encodes a point-in-time delayed-retry relay metrics snapshot in Prometheus text format.
    #[must_use]
    pub fn encode_snapshot(snapshot: &KafkaDelayedRetryRelayMetricsSnapshot) -> String {
        let mut output = String::new();
        append_line(
            &mut output,
            "# HELP rustee_kafka_delayed_retry_relay_passes_total Delayed-retry relay passes whose future started.",
        );
        append_line(
            &mut output,
            "# TYPE rustee_kafka_delayed_retry_relay_passes_total counter",
        );
        append_line(
            &mut output,
            &format!(
                "{} {}",
                metric_names::RELAY_PASSES_TOTAL,
                snapshot.started()
            ),
        );

        append_line(
            &mut output,
            "# HELP rustee_kafka_delayed_retry_relay_passes_in_flight Delayed-retry relay pass futures currently executing.",
        );
        append_line(
            &mut output,
            "# TYPE rustee_kafka_delayed_retry_relay_passes_in_flight gauge",
        );
        append_line(
            &mut output,
            &format!(
                "{} {}",
                metric_names::RELAY_PASSES_IN_FLIGHT,
                snapshot.in_flight()
            ),
        );

        append_line(
            &mut output,
            "# HELP rustee_kafka_delayed_retry_relay_pass_outcomes_total Completed delayed-retry relay passes by fixed outcome.",
        );
        append_line(
            &mut output,
            "# TYPE rustee_kafka_delayed_retry_relay_pass_outcomes_total counter",
        );
        for (outcome, count) in snapshot.outcome_counts() {
            append_line(
                &mut output,
                &format!(
                    "{}{{outcome=\"{}\"}} {count}",
                    metric_names::RELAY_PASS_OUTCOMES_TOTAL,
                    outcome.as_str(),
                ),
            );
        }

        append_line(
            &mut output,
            "# HELP rustee_kafka_delayed_retry_relay_published_total Records confirmed after Kafka acknowledgement in fully successful relay passes.",
        );
        append_line(
            &mut output,
            "# TYPE rustee_kafka_delayed_retry_relay_published_total counter",
        );
        append_line(
            &mut output,
            &format!(
                "{} {}",
                metric_names::RELAY_PUBLISHED_TOTAL,
                snapshot.published()
            ),
        );

        append_backlog_metrics(&mut output, snapshot);
        append_duration_metrics(&mut output, snapshot);
        output
    }

    fn append_backlog_metrics(
        output: &mut String,
        snapshot: &KafkaDelayedRetryRelayMetricsSnapshot,
    ) {
        append_line(
            output,
            "# HELP rustee_kafka_delayed_retry_backlog_rows Latest database-derived delayed-retry backlog rows by fixed state.",
        );
        append_line(
            output,
            "# TYPE rustee_kafka_delayed_retry_backlog_rows gauge",
        );
        append_line(
            output,
            "# HELP rustee_kafka_delayed_retry_oldest_due_seconds Latest database-derived age of the oldest due delayed-retry row.",
        );
        append_line(
            output,
            "# TYPE rustee_kafka_delayed_retry_oldest_due_seconds gauge",
        );
        if let Some(backlog) = snapshot.backlog() {
            for (state, count) in [
                ("unpublished", backlog.unpublished),
                ("due", backlog.due),
                ("leased", backlog.leased),
            ] {
                append_line(
                    output,
                    &format!(
                        "{}{{state=\"{state}\"}} {count}",
                        metric_names::BACKLOG_ROWS
                    ),
                );
            }
            if let Some(oldest_due_age) = backlog.oldest_due_age {
                append_line(
                    output,
                    &format!(
                        "{} {}",
                        metric_names::OLDEST_DUE_SECONDS,
                        oldest_due_age.as_secs_f64(),
                    ),
                );
            }
        }
    }

    fn append_duration_metrics(
        output: &mut String,
        snapshot: &KafkaDelayedRetryRelayMetricsSnapshot,
    ) {
        append_line(
            output,
            "# HELP rustee_kafka_delayed_retry_relay_pass_duration_seconds Delayed-retry relay pass duration histogram.",
        );
        append_line(
            output,
            "# TYPE rustee_kafka_delayed_retry_relay_pass_duration_seconds histogram",
        );
        for (upper_bound, count) in snapshot.duration_bucket_counts() {
            append_line(
                output,
                &format!(
                    "{}_bucket{{le=\"{}\"}} {count}",
                    metric_names::RELAY_PASS_DURATION_SECONDS,
                    upper_bound.as_secs_f64(),
                ),
            );
        }
        append_line(
            output,
            &format!(
                "{}_bucket{{le=\"+Inf\"}} {}",
                metric_names::RELAY_PASS_DURATION_SECONDS,
                snapshot.completed(),
            ),
        );
        append_line(
            output,
            &format!(
                "{}_sum {}",
                metric_names::RELAY_PASS_DURATION_SECONDS,
                snapshot.total_duration().as_secs_f64(),
            ),
        );
        append_line(
            output,
            &format!(
                "{}_count {}",
                metric_names::RELAY_PASS_DURATION_SECONDS,
                snapshot.completed(),
            ),
        );
    }

    /// Encodes the current value of a delayed-retry relay metric collector.
    #[must_use]
    pub fn encode(metrics: &KafkaDelayedRetryRelayMetrics) -> String {
        encode_snapshot(&metrics.snapshot())
    }

    /// Creates a scrape response for a delayed-retry relay metric collector.
    ///
    /// Applications explicitly register this response at a protected or network-isolated route.
    #[must_use]
    pub fn metrics_response(metrics: &KafkaDelayedRetryRelayMetrics) -> Response {
        let mut response = response(StatusCode::OK, full_body(encode(metrics)));
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static(CONTENT_TYPE_PROMETHEUS),
        );
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    }

    fn append_line(output: &mut String, line: &str) {
        writeln!(output, "{line}").expect("writing to an owned String must not fail");
    }

    #[cfg(test)]
    mod tests {
        use std::{sync::Arc, time::Duration};

        use http_body_util::BodyExt;
        use rustee_events_kafka_sqlx_observability::KafkaDelayedRetryRelayMetrics;
        use rustee_events_kafka_sqlx_observability::{
            KafkaDelayedRetryBacklog, KafkaDelayedRetryRelayObserver,
            KafkaDelayedRetryRelayOutcome, KafkaDelayedRetryRelayPassObservation,
        };

        use super::{CONTENT_TYPE_PROMETHEUS, encode, metrics_response};

        #[tokio::test]
        async fn encodes_only_fixed_delayed_retry_labels() {
            let metrics = KafkaDelayedRetryRelayMetrics::new();
            let observer: Arc<dyn KafkaDelayedRetryRelayObserver> = Arc::new(metrics.clone());
            KafkaDelayedRetryRelayPassObservation::start(observer)
                .finish(KafkaDelayedRetryRelayOutcome::Succeeded, Some(2));
            metrics.record_backlog(KafkaDelayedRetryBacklog {
                unpublished: 4,
                due: 3,
                leased: 1,
                oldest_due_age: Some(Duration::from_secs(9)),
            });

            let encoded = encode(&metrics);
            assert!(encoded.contains("rustee_kafka_delayed_retry_relay_passes_total 1"));
            assert!(encoded.contains(
                "rustee_kafka_delayed_retry_relay_pass_outcomes_total{outcome=\"succeeded\"} 1"
            ));
            assert!(encoded.contains("rustee_kafka_delayed_retry_relay_published_total 2"));
            assert!(encoded.contains("rustee_kafka_delayed_retry_backlog_rows{state=\"due\"} 3"));
            assert!(encoded.contains("rustee_kafka_delayed_retry_oldest_due_seconds 9"));
            assert!(encoded.contains(
                "rustee_kafka_delayed_retry_relay_pass_duration_seconds_bucket{le=\"+Inf\"} 1"
            ));
            assert!(!encoded.contains("topic"));
            assert!(!encoded.contains("payload"));
            assert!(!encoded.contains("endpoint"));

            let response = metrics_response(&metrics);
            assert_eq!(response.status(), 200);
            assert_eq!(response.headers()["content-type"], CONTENT_TYPE_PROMETHEUS);
            assert_eq!(response.headers()["cache-control"], "no-store");
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                encoded.as_bytes()
            );
        }
    }
}

#[cfg(feature = "rdkafka")]
pub use adapter::*;
