//! Prometheus text exposition for Rustee event-delivery metrics.
//!
//! This crate deliberately has no registry, global state, listener, or automatic route. An
//! application owns an [`EventMetrics`] collector and explicitly mounts [`metrics_response`]
//! wherever its deployment policy permits scraping.

use std::fmt::Write;

use http::{
    HeaderValue, StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use rustee_core::{Response, full_body, response};
use rustee_events_observability::{EventMetrics, EventMetricsSnapshot, metric_names};

/// Prometheus text exposition content type for version 0.0.4.
pub const CONTENT_TYPE_PROMETHEUS: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Encodes a point-in-time event-delivery metrics snapshot in Prometheus text format.
#[must_use]
pub fn encode_snapshot(snapshot: &EventMetricsSnapshot) -> String {
    let mut output = String::new();
    append_line(
        &mut output,
        "# HELP rustee_event_deliveries_total Provider deliveries whose consumer task started.",
    );
    append_line(&mut output, "# TYPE rustee_event_deliveries_total counter");
    append_line(
        &mut output,
        &format!(
            "{} {}",
            metric_names::EVENT_DELIVERIES_TOTAL,
            snapshot.started()
        ),
    );

    append_line(
        &mut output,
        "# HELP rustee_event_deliveries_in_flight Provider deliveries currently executing in this process.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_event_deliveries_in_flight gauge",
    );
    append_line(
        &mut output,
        &format!(
            "{} {}",
            metric_names::EVENT_DELIVERIES_IN_FLIGHT,
            snapshot.in_flight()
        ),
    );

    append_line(
        &mut output,
        "# HELP rustee_event_delivery_outcomes_total Completed provider deliveries by bounded provider and outcome.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_event_delivery_outcomes_total counter",
    );
    for (provider, outcome, count) in snapshot.outcome_counts() {
        append_line(
            &mut output,
            &format!(
                "{}{{provider=\"{}\",outcome=\"{}\"}} {count}",
                metric_names::EVENT_DELIVERY_OUTCOMES_TOTAL,
                escape_label_value(provider),
                outcome.as_str(),
            ),
        );
    }

    append_duration_metrics(&mut output, snapshot);
    output
}

fn append_duration_metrics(output: &mut String, snapshot: &EventMetricsSnapshot) {
    append_line(
        output,
        "# HELP rustee_event_delivery_duration_seconds Provider event delivery duration histogram.",
    );
    append_line(
        output,
        "# TYPE rustee_event_delivery_duration_seconds histogram",
    );
    for (upper_bound, count) in snapshot.duration_bucket_counts() {
        append_line(
            output,
            &format!(
                "{}_bucket{{le=\"{}\"}} {count}",
                metric_names::EVENT_DELIVERY_DURATION_SECONDS,
                upper_bound.as_secs_f64(),
            ),
        );
    }
    append_line(
        output,
        &format!(
            "{}_bucket{{le=\"+Inf\"}} {}",
            metric_names::EVENT_DELIVERY_DURATION_SECONDS,
            snapshot.completed(),
        ),
    );
    append_line(
        output,
        &format!(
            "{}_sum {}",
            metric_names::EVENT_DELIVERY_DURATION_SECONDS,
            snapshot.total_duration().as_secs_f64(),
        ),
    );
    append_line(
        output,
        &format!(
            "{}_count {}",
            metric_names::EVENT_DELIVERY_DURATION_SECONDS,
            snapshot.completed(),
        ),
    );
}

/// Encodes the current value of an event-delivery metric collector.
#[must_use]
pub fn encode(metrics: &EventMetrics) -> String {
    encode_snapshot(&metrics.snapshot())
}

/// Creates a scrape response for an event-delivery metric collector.
///
/// Applications explicitly register this response at a protected or network-isolated route.
#[must_use]
pub fn metrics_response(metrics: &EventMetrics) -> Response {
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

fn escape_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU16, sync::Arc};

    use http_body_util::BodyExt;
    use rustee_events::{EventDeliveryObservation, EventDeliveryOutcome};
    use rustee_events_observability::EventMetrics;

    use super::{CONTENT_TYPE_PROMETHEUS, encode, escape_label_value, metrics_response};

    #[tokio::test]
    async fn encodes_only_bounded_event_delivery_labels() {
        let metrics = EventMetrics::new();
        let observer = Arc::new(metrics.clone());
        EventDeliveryObservation::start(observer, "apache_kafka")
            .finish(NonZeroU16::new(1), EventDeliveryOutcome::Acknowledged);

        let encoded = encode(&metrics);
        assert!(encoded.contains("rustee_event_deliveries_total 1"));
        assert!(encoded.contains(
            "rustee_event_delivery_outcomes_total{provider=\"apache_kafka\",outcome=\"acknowledged\"} 1"
        ));
        assert!(encoded.contains("# TYPE rustee_event_delivery_duration_seconds histogram"));
        assert!(encoded.contains("rustee_event_delivery_duration_seconds_bucket{le=\"+Inf\"} 1"));
        assert!(!encoded.contains("event_id"));
        assert!(!encoded.contains("topic"));
        assert!(!encoded.contains("partition"));
        assert!(!encoded.contains("payload"));

        let response = metrics_response(&metrics);
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-type"], CONTENT_TYPE_PROMETHEUS);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            encoded.as_bytes()
        );
    }

    #[test]
    fn encodes_collector_provider_labels_without_prometheus_text_injection() {
        let metrics = EventMetrics::new();
        let observer = Arc::new(metrics.clone());
        EventDeliveryObservation::start(observer, "quote=\" slash=\\ newline=\n")
            .finish(NonZeroU16::new(1), EventDeliveryOutcome::Acknowledged);

        let encoded = encode(&metrics);
        assert_eq!(
            escape_label_value("quote=\" slash=\\ newline=\n"),
            "quote=\\\" slash=\\\\ newline=\\n"
        );
        assert!(
            encoded.contains(
                "provider=\"quote=\\\" slash=\\\\ newline=\\n\",outcome=\"acknowledged\""
            )
        );
        assert!(!encoded.contains("provider=\"quote=\" slash=\\ newline=\n\""));
    }
}
