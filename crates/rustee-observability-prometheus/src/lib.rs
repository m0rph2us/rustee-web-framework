//! Prometheus text exposition for Rustee's bounded request metric collector.
//!
//! This crate deliberately has no registry, global state, listener, or automatic route. An
//! application owns a [`rustee_observability::RequestMetrics`] collector and explicitly mounts
//! [`metrics_response`] wherever its deployment policy permits scraping.

use http::{
    HeaderValue, StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use rustee_core::{Response, full_body, response};
use rustee_observability::{RequestMetrics, RequestMetricsSnapshot, metric_names};
use rustee_observability_core::prometheus::{append_line, escape_label_value};

pub use rustee_observability_core::prometheus::PROMETHEUS_TEXT_CONTENT_TYPE as CONTENT_TYPE_PROMETHEUS;

/// Encodes a point-in-time request metrics snapshot in Prometheus text exposition format.
#[must_use]
pub fn encode_snapshot(snapshot: &RequestMetricsSnapshot) -> String {
    let mut output = String::new();
    append_line(
        &mut output,
        "# HELP rustee_http_requests_total Completed HTTP requests by status class.",
    );
    append_line(&mut output, "# TYPE rustee_http_requests_total counter");
    for (class, count) in snapshot.status_class_counts() {
        append_line(
            &mut output,
            &format!(
                "{}{{status_class=\"{class}\"}} {count}",
                metric_names::HTTP_REQUESTS_TOTAL
            ),
        );
    }

    append_line(
        &mut output,
        "# HELP rustee_http_route_requests_total Completed HTTP requests by router classification and status class.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_http_route_requests_total counter",
    );
    for (route, class, count) in snapshot.route_classification_status_class_counts() {
        append_line(
            &mut output,
            &format!(
                "{}{{route=\"{}\",status_class=\"{class}\"}} {count}",
                metric_names::HTTP_ROUTE_REQUESTS_TOTAL,
                escape_label_value(route),
            ),
        );
    }

    append_line(
        &mut output,
        "# HELP rustee_http_requests_in_flight HTTP requests currently executing.",
    );
    append_line(&mut output, "# TYPE rustee_http_requests_in_flight gauge");
    append_line(
        &mut output,
        &format!(
            "{} {}",
            metric_names::HTTP_REQUESTS_IN_FLIGHT,
            snapshot.in_flight()
        ),
    );

    append_line(
        &mut output,
        "# HELP rustee_http_request_duration_seconds Request duration histogram for completed HTTP requests.",
    );
    append_line(
        &mut output,
        "# TYPE rustee_http_request_duration_seconds histogram",
    );
    for (upper_bound, count) in snapshot.duration_bucket_counts() {
        append_line(
            &mut output,
            &format!(
                "{}_bucket{{le=\"{}\"}} {count}",
                metric_names::HTTP_REQUEST_DURATION_SECONDS,
                upper_bound.as_secs_f64(),
            ),
        );
    }
    append_line(
        &mut output,
        &format!(
            "{}_bucket{{le=\"+Inf\"}} {}",
            metric_names::HTTP_REQUEST_DURATION_SECONDS,
            snapshot.completed()
        ),
    );
    append_line(
        &mut output,
        &format!(
            "{}_sum {}",
            metric_names::HTTP_REQUEST_DURATION_SECONDS,
            snapshot.total_duration().as_secs_f64()
        ),
    );
    append_line(
        &mut output,
        &format!(
            "{}_count {}",
            metric_names::HTTP_REQUEST_DURATION_SECONDS,
            snapshot.completed()
        ),
    );
    output
}

/// Encodes the current value of a request metric collector.
#[must_use]
pub fn encode(metrics: &RequestMetrics) -> String {
    encode_snapshot(&metrics.snapshot())
}

/// Creates a scrape response for the current value of a request metric collector.
///
/// Applications explicitly register this response at a protected or network-isolated route.
#[must_use]
pub fn metrics_response(metrics: &RequestMetrics) -> Response {
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
    use http::Request;
    use http_body_util::BodyExt;
    use rustee_core::empty_body;
    use rustee_observability::{MetricsLayer, RequestMetrics};
    use rustee_router::App;
    use tower::{Layer, ServiceExt};

    use super::{CONTENT_TYPE_PROMETHEUS, encode, escape_label_value, metrics_response};

    #[tokio::test]
    async fn encodes_stable_route_outcomes_without_the_raw_unmatched_path() {
        let metrics = RequestMetrics::new();
        let service = MetricsLayer::new(metrics.clone())
            .layer(App::new().get("/users/:id", || async { "user" }));
        let matched = Request::builder()
            .uri("/users/42")
            .body(empty_body())
            .unwrap();
        let unmatched = Request::builder()
            .uri("/attacker-controlled-path")
            .body(empty_body())
            .unwrap();

        assert_eq!(
            service.clone().oneshot(matched).await.unwrap().status(),
            200
        );
        assert_eq!(service.oneshot(unmatched).await.unwrap().status(), 404);

        let encoded = encode(&metrics);
        assert!(encoded.contains("rustee_http_requests_total{status_class=\"2\"} 1"));
        assert!(encoded.contains("rustee_http_requests_total{status_class=\"4\"} 1"));
        assert!(encoded.contains(
            "rustee_http_route_requests_total{route=\"/users/:id\",status_class=\"2\"} 1"
        ));
        assert!(encoded.contains(
            "rustee_http_route_requests_total{route=\"<not-found>\",status_class=\"4\"} 1"
        ));
        assert!(encoded.contains("# TYPE rustee_http_request_duration_seconds histogram"));
        assert!(encoded.contains("rustee_http_request_duration_seconds_bucket{le=\"+Inf\"} 2"));
        assert!(!encoded.contains("attacker-controlled-path"));

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
    fn label_escaping_prevents_prometheus_text_injection() {
        assert_eq!(
            escape_label_value("quote=\" slash=\\ newline=\n"),
            "quote=\\\" slash=\\\\ newline=\\n"
        );
    }
}
