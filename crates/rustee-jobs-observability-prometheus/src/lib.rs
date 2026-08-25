//! Prometheus text exposition for Rustee durable job-delivery metrics.
//!
//! This crate deliberately has no registry, global state, listener, or automatic route. An
//! application owns a [`JobMetrics`] collector and explicitly mounts [`metrics_response`]
//! wherever its deployment policy permits scraping.

use http::{
    HeaderValue, StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use rustee_core::{Response, full_body, response};
use rustee_jobs_observability::{JobMetrics, JobMetricsSnapshot, metric_names};
use rustee_observability_core::prometheus::{append_line, escape_label_value};

pub use rustee_observability_core::prometheus::PROMETHEUS_TEXT_CONTENT_TYPE as CONTENT_TYPE_PROMETHEUS;

/// Encodes a point-in-time job-delivery metrics snapshot in Prometheus text format.
#[must_use]
pub fn encode_snapshot(snapshot: &JobMetricsSnapshot) -> String {
    let mut output = String::new();
    append_line(
        &mut output,
        "# HELP rustee_job_deliveries_total Provider deliveries whose worker task started.",
    );
    append_line(&mut output, "# TYPE rustee_job_deliveries_total counter");
    append_line(
        &mut output,
        &format!(
            "{} {}",
            metric_names::JOB_DELIVERIES_TOTAL,
            snapshot.started()
        ),
    );

    append_line(
        &mut output,
        "# HELP rustee_job_deliveries_in_flight Provider deliveries currently executing in this process.",
    );
    append_line(&mut output, "# TYPE rustee_job_deliveries_in_flight gauge");
    append_line(
        &mut output,
        &format!(
            "{} {}",
            metric_names::JOB_DELIVERIES_IN_FLIGHT,
            snapshot.in_flight()
        ),
    );

    append_line(
        &mut output,
        "# HELP rustee_job_delivery_outcomes_total Completed provider deliveries by bounded provider and outcome.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_job_delivery_outcomes_total counter",
    );
    for (provider, outcome, count) in snapshot.outcome_counts() {
        append_line(
            &mut output,
            &format!(
                "{}{{provider=\"{}\",outcome=\"{}\"}} {count}",
                metric_names::JOB_DELIVERY_OUTCOMES_TOTAL,
                escape_label_value(provider),
                outcome.as_str(),
            ),
        );
    }

    append_duration_metrics(&mut output, snapshot);
    output
}

fn append_duration_metrics(output: &mut String, snapshot: &JobMetricsSnapshot) {
    append_line(
        output,
        "# HELP rustee_job_delivery_duration_seconds Provider delivery duration histogram.",
    );
    append_line(
        output,
        "# TYPE rustee_job_delivery_duration_seconds histogram",
    );
    for (upper_bound, count) in snapshot.duration_bucket_counts() {
        append_line(
            output,
            &format!(
                "{}_bucket{{le=\"{}\"}} {count}",
                metric_names::JOB_DELIVERY_DURATION_SECONDS,
                upper_bound.as_secs_f64(),
            ),
        );
    }
    append_line(
        output,
        &format!(
            "{}_bucket{{le=\"+Inf\"}} {}",
            metric_names::JOB_DELIVERY_DURATION_SECONDS,
            snapshot.completed(),
        ),
    );
    append_line(
        output,
        &format!(
            "{}_sum {}",
            metric_names::JOB_DELIVERY_DURATION_SECONDS,
            snapshot.total_duration().as_secs_f64(),
        ),
    );
    append_line(
        output,
        &format!(
            "{}_count {}",
            metric_names::JOB_DELIVERY_DURATION_SECONDS,
            snapshot.completed(),
        ),
    );
}

/// Encodes the current value of a job-delivery metric collector.
#[must_use]
pub fn encode(metrics: &JobMetrics) -> String {
    encode_snapshot(&metrics.snapshot())
}

/// Creates a scrape response for a job-delivery metric collector.
///
/// Applications explicitly register this response at a protected or network-isolated route.
#[must_use]
pub fn metrics_response(metrics: &JobMetrics) -> Response {
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

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU16, sync::Arc};

    use http_body_util::BodyExt;
    use rustee_jobs::{JobDeliveryObservation, JobDeliveryOutcome};
    use rustee_jobs_observability::JobMetrics;

    use super::{CONTENT_TYPE_PROMETHEUS, encode, escape_label_value, metrics_response};

    #[tokio::test]
    async fn encodes_only_bounded_job_delivery_labels() {
        let metrics = JobMetrics::new();
        let observer = Arc::new(metrics.clone());
        JobDeliveryObservation::start(observer, "nats_jetstream")
            .finish(NonZeroU16::new(1), JobDeliveryOutcome::Acknowledged);

        let encoded = encode(&metrics);
        assert!(encoded.contains("rustee_job_deliveries_total 1"));
        assert!(encoded.contains(
            "rustee_job_delivery_outcomes_total{provider=\"nats_jetstream\",outcome=\"acknowledged\"} 1"
        ));
        assert!(encoded.contains("# TYPE rustee_job_delivery_duration_seconds histogram"));
        assert!(encoded.contains("rustee_job_delivery_duration_seconds_bucket{le=\"+Inf\"} 1"));
        assert!(!encoded.contains("payload"));
        assert!(!encoded.contains("queue_route"));
        assert!(!encoded.contains("receipt_handle"));

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
        let metrics = JobMetrics::new();
        let observer = Arc::new(metrics.clone());
        JobDeliveryObservation::start(observer, "quote=\" slash=\\ newline=\n")
            .finish(NonZeroU16::new(1), JobDeliveryOutcome::Acknowledged);

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
