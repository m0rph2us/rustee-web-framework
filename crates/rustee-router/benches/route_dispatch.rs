use std::{fs, path::Path, time::Instant};

use http::{Method, StatusCode};
use rustee_core::empty_body;
use rustee_router::App;

const DEFAULT_ITERATIONS: u64 = 100_000;
const MAX_ITERATIONS: u64 = 10_000_000;
const WARMUP_ITERATIONS: u64 = 10_000;

fn iteration_count() -> u64 {
    std::env::var("RUSTEE_BENCH_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &u64| (1..=MAX_ITERATIONS).contains(value))
        .unwrap_or(DEFAULT_ITERATIONS)
}

fn write_report(
    iterations: u64,
    elapsed_nanos: u128,
    operations_per_second: u128,
) -> std::io::Result<()> {
    let Ok(output_path) = std::env::var("RUSTEE_BENCH_OUTPUT") else {
        return Ok(());
    };
    let output_path = Path::new(&output_path);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let report = serde_json::json!({
        "schema": 1,
        "benchmark": "route_dispatch",
        "iterations": iterations,
        "warmup_iterations": WARMUP_ITERATIONS,
        "elapsed_nanos": elapsed_nanos,
        "operations_per_second": operations_per_second,
        "source_ref": std::env::var("RUSTEE_BENCH_SOURCE_REF").ok(),
        "runner": std::env::var("RUSTEE_BENCH_RUNNER").ok(),
    });
    fs::write(output_path, report.to_string())
}

async fn dispatch(app: &App) {
    let request = http::Request::builder()
        .method(Method::GET)
        .uri("/accounts/42/orders/2026-08-08")
        .body(empty_body())
        .expect("benchmark request is valid");
    let response = app.call(request).await;
    assert_eq!(response.status(), StatusCode::OK);
}

fn main() {
    let app = App::new()
        .get("/health", || async { "ok" })
        .get("/accounts/:account_id", || async { "account" })
        .get("/accounts/:account_id/orders/:order_id", || async {
            "order"
        });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime is available");
    let iterations = iteration_count();

    runtime.block_on(async {
        for _ in 0..WARMUP_ITERATIONS {
            dispatch(&app).await;
        }
    });

    let started = Instant::now();
    runtime.block_on(async {
        for _ in 0..iterations {
            dispatch(&app).await;
        }
    });
    let elapsed = started.elapsed();
    let elapsed_nanos = elapsed.as_nanos();
    let operations_per_second = u128::from(iterations)
        .saturating_mul(1_000_000_000)
        .checked_div(elapsed_nanos)
        .unwrap_or_default();
    write_report(iterations, elapsed_nanos, operations_per_second)
        .expect("benchmark report is written");

    println!(
        "route_dispatch iterations={iterations} elapsed_ms={} ops_per_second={operations_per_second:.0}",
        elapsed.as_millis(),
    );
}
