use opentelemetry::trace::Tracer;
use tracing::Subscriber;
use tracing_subscriber::{
    EnvFilter, layer::SubscriberExt, registry::LookupSpan, util::SubscriberInitExt,
};

/// Creates a composable `tracing` layer backed by one application-owned tracer.
///
/// Use this when the application already owns its subscriber assembly. The layer exports Rustee's
/// existing request span without adding request paths, hosts, credentials, or payloads.
#[must_use]
pub fn layer<S, T>(tracer: T) -> tracing_opentelemetry::OpenTelemetryLayer<S, T>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    T: Tracer + Send + Sync + 'static,
    T::Span: Send + Sync,
{
    tracing_opentelemetry::layer().with_tracer(tracer)
}

/// Installs formatted `tracing` output and OpenTelemetry export for one application tracer.
///
/// The function reads `RUST_LOG` using the same fallback as `rustee-observability::init`. It is
/// process-global: call it once, retain the tracer provider in application state, and shut that
/// provider down during the application's graceful shutdown sequence.
/// Returns `true` when this call installed the global subscriber. A `false` result means another
/// subscriber was already installed; no exporter or span policy is changed.
#[must_use]
pub fn init<T>(tracer: T) -> bool
where
    T: Tracer + Send + Sync + 'static,
    T::Span: Send + Sync,
{
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .with(layer(tracer))
        .try_init()
        .is_ok()
}
