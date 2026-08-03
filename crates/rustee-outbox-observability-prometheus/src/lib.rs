//! Prometheus text exposition for Rustee transactional-outbox relay metrics.
//!
//! This crate deliberately has no registry, global state, listener, or automatic route. An
//! application owns an [`OutboxRelayMetrics`] collector and explicitly mounts [`metrics_response`]
//! wherever its deployment policy permits scraping.

use std::fmt::Write;

use http::{
    HeaderValue, StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use rustee_core::{Response, full_body, response};
use rustee_outbox_observability::{OutboxRelayMetrics, OutboxRelayMetricsSnapshot, metric_names};

/// Prometheus text exposition content type for version 0.0.4.
pub const CONTENT_TYPE_PROMETHEUS: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Encodes a point-in-time outbox relay metrics snapshot in Prometheus text exposition format.
#[must_use]
pub fn encode_snapshot(snapshot: &OutboxRelayMetricsSnapshot) -> String {
    let mut output = String::new();
    append_line(
        &mut output,
        "# HELP rustee_outbox_relay_passes_total Outbox relay passes whose future started.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_outbox_relay_passes_total counter",
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
        "# HELP rustee_outbox_relay_passes_in_flight Outbox relay pass futures currently executing.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_outbox_relay_passes_in_flight gauge",
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
        "# HELP rustee_outbox_relay_pass_outcomes_total Completed relay passes by fixed kind and outcome.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_outbox_relay_pass_outcomes_total counter",
    );
    for (kind, outcome, count) in snapshot.outcome_counts() {
        append_line(
            &mut output,
            &format!(
                "{}{{kind=\"{}\",outcome=\"{}\"}} {count}",
                metric_names::RELAY_PASS_OUTCOMES_TOTAL,
                kind.as_str(),
                outcome.as_str(),
            ),
        );
    }

    append_line(
        &mut output,
        "# HELP rustee_outbox_relay_rows_total Aggregate relay rows by fixed kind and count name.",
    );
    append_line(&mut output, "# TYPE rustee_outbox_relay_rows_total counter");
    for (kind, count_name, count) in snapshot.row_counts() {
        append_line(
            &mut output,
            &format!(
                "{}{{kind=\"{}\",count=\"{}\"}} {count}",
                metric_names::RELAY_ROWS_TOTAL,
                kind.as_str(),
                count_name.as_str(),
            ),
        );
    }

    append_duration_metrics(&mut output, snapshot);
    output
}

fn append_duration_metrics(output: &mut String, snapshot: &OutboxRelayMetricsSnapshot) {
    append_line(
        output,
        "# HELP rustee_outbox_relay_pass_duration_seconds Outbox relay pass duration histogram.",
    );
    append_line(
        output,
        "# TYPE rustee_outbox_relay_pass_duration_seconds histogram",
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

/// Encodes the current value of an outbox relay metric collector.
#[must_use]
pub fn encode(metrics: &OutboxRelayMetrics) -> String {
    encode_snapshot(&metrics.snapshot())
}

/// Creates a scrape response for the current value of an outbox relay metric collector.
///
/// Applications explicitly register this response at a protected or network-isolated route.
#[must_use]
pub fn metrics_response(metrics: &OutboxRelayMetrics) -> Response {
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
    use std::sync::Arc;

    use http_body_util::BodyExt;
    use rustee_outbox_observability::OutboxRelayMetrics;
    use rustee_outbox_sqlx::{RelayPassKind, RelayPassObservation, RelayPassOutcome, RelayReport};

    use super::{CONTENT_TYPE_PROMETHEUS, encode, metrics_response};

    #[tokio::test]
    async fn encodes_only_bounded_outbox_relay_labels() {
        let metrics = OutboxRelayMetrics::new();
        let observer = Arc::new(metrics.clone());
        RelayPassObservation::start(observer, RelayPassKind::Job).finish(
            RelayPassOutcome::Succeeded,
            Some(RelayReport {
                claimed: 3,
                published: 2,
                retry_scheduled: 1,
                lease_lost: 0,
            }),
        );

        let encoded = encode(&metrics);
        assert!(encoded.contains("rustee_outbox_relay_passes_total 1"));
        assert!(encoded.contains(
            "rustee_outbox_relay_pass_outcomes_total{kind=\"job\",outcome=\"succeeded\"} 1"
        ));
        assert!(
            encoded.contains("rustee_outbox_relay_rows_total{kind=\"job\",count=\"claimed\"} 3")
        );
        assert!(encoded.contains("# TYPE rustee_outbox_relay_pass_duration_seconds histogram"));
        assert!(
            encoded.contains("rustee_outbox_relay_pass_duration_seconds_bucket{le=\"+Inf\"} 1")
        );
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
