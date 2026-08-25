//! Prometheus text exposition for Rustee `PostgreSQL` recurring-scheduler metrics.
//!
//! This crate deliberately has no registry, global state, listener, or automatic route. An
//! application owns a [`RecurringJobFireMetrics`] collector and explicitly mounts
//! [`metrics_response`] wherever its deployment policy permits scraping.

use http::{
    HeaderValue, StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use rustee_core::{Response, full_body, response};
use rustee_jobs_cron_observability::{
    RecurringJobFireMetrics, RecurringJobFireMetricsSnapshot, metric_names,
};
use rustee_observability_core::prometheus::append_line;

pub use rustee_observability_core::prometheus::PROMETHEUS_TEXT_CONTENT_TYPE as CONTENT_TYPE_PROMETHEUS;

/// Encodes a point-in-time recurring-scheduler metrics snapshot in Prometheus text format.
#[must_use]
pub fn encode_snapshot(snapshot: &RecurringJobFireMetricsSnapshot) -> String {
    let mut output = String::new();
    append_line(
        &mut output,
        "# HELP rustee_scheduler_fire_passes_total Recurring scheduler passes whose future started.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_scheduler_fire_passes_total counter",
    );
    append_line(
        &mut output,
        &format!("{} {}", metric_names::FIRE_PASSES_TOTAL, snapshot.started()),
    );

    append_line(
        &mut output,
        "# HELP rustee_scheduler_fire_passes_in_flight Recurring scheduler pass futures currently executing in this process.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_scheduler_fire_passes_in_flight gauge",
    );
    append_line(
        &mut output,
        &format!(
            "{} {}",
            metric_names::FIRE_PASSES_IN_FLIGHT,
            snapshot.in_flight()
        ),
    );

    append_line(
        &mut output,
        "# HELP rustee_scheduler_fire_pass_outcomes_total Completed recurring scheduler passes by fixed outcome.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_scheduler_fire_pass_outcomes_total counter",
    );
    for (outcome, count) in snapshot.outcome_counts() {
        append_line(
            &mut output,
            &format!(
                "{}{{outcome=\"{}\"}} {count}",
                metric_names::FIRE_PASS_OUTCOMES_TOTAL,
                outcome.as_str(),
            ),
        );
    }

    append_line(
        &mut output,
        "# HELP rustee_scheduler_fire_rows_total Aggregate recurring scheduler counts by fixed count name.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_scheduler_fire_rows_total counter",
    );
    for (count_name, count) in snapshot.row_counts() {
        append_line(
            &mut output,
            &format!(
                "{}{{count=\"{}\"}} {count}",
                metric_names::FIRE_ROWS_TOTAL,
                count_name.as_str(),
            ),
        );
    }

    append_duration_metrics(&mut output, snapshot);
    output
}

fn append_duration_metrics(output: &mut String, snapshot: &RecurringJobFireMetricsSnapshot) {
    append_line(
        output,
        "# HELP rustee_scheduler_fire_pass_duration_seconds Recurring scheduler pass duration histogram.",
    );
    append_line(
        output,
        "# TYPE rustee_scheduler_fire_pass_duration_seconds histogram",
    );
    for (upper_bound, count) in snapshot.duration_bucket_counts() {
        append_line(
            output,
            &format!(
                "{}_bucket{{le=\"{}\"}} {count}",
                metric_names::FIRE_PASS_DURATION_SECONDS,
                upper_bound.as_secs_f64(),
            ),
        );
    }
    append_line(
        output,
        &format!(
            "{}_bucket{{le=\"+Inf\"}} {}",
            metric_names::FIRE_PASS_DURATION_SECONDS,
            snapshot.completed(),
        ),
    );
    append_line(
        output,
        &format!(
            "{}_sum {}",
            metric_names::FIRE_PASS_DURATION_SECONDS,
            snapshot.total_duration().as_secs_f64(),
        ),
    );
    append_line(
        output,
        &format!(
            "{}_count {}",
            metric_names::FIRE_PASS_DURATION_SECONDS,
            snapshot.completed(),
        ),
    );
}

/// Encodes the current value of a recurring-scheduler metrics collector.
#[must_use]
pub fn encode(metrics: &RecurringJobFireMetrics) -> String {
    encode_snapshot(&metrics.snapshot())
}

/// Creates a scrape response for a recurring-scheduler metrics collector.
///
/// Applications explicitly register this response at a protected or network-isolated route.
#[must_use]
pub fn metrics_response(metrics: &RecurringJobFireMetrics) -> Response {
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
    use std::sync::Arc;

    use http_body_util::BodyExt;
    use rustee_jobs_cron_observability::RecurringJobFireMetrics;
    use rustee_jobs_cron_sqlx::{
        RecurringJobFireLimit, RecurringJobFireObservation, RecurringJobFireOutcome,
        RecurringJobFireReport,
    };

    use super::{CONTENT_TYPE_PROMETHEUS, encode, metrics_response};

    #[tokio::test]
    async fn encodes_only_bounded_scheduler_labels() {
        let metrics = RecurringJobFireMetrics::new();
        let observer = Arc::new(metrics.clone());
        RecurringJobFireObservation::start(observer, RecurringJobFireLimit::default()).finish(
            RecurringJobFireOutcome::Succeeded,
            Some(RecurringJobFireReport::default()),
        );

        let encoded = encode(&metrics);
        assert!(encoded.contains("rustee_scheduler_fire_passes_total 1"));
        assert!(
            encoded.contains("rustee_scheduler_fire_pass_outcomes_total{outcome=\"succeeded\"} 1")
        );
        assert!(encoded.contains("# TYPE rustee_scheduler_fire_pass_duration_seconds histogram"));
        assert!(
            encoded.contains("rustee_scheduler_fire_pass_duration_seconds_bucket{le=\"+Inf\"} 1")
        );
        assert!(!encoded.contains("schedule_key"));
        assert!(!encoded.contains("destination"));
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
}
