//! Optional OpenTelemetry export and W3C Trace Context propagation for Rustee `tracing` spans.
//!
//! This crate does not construct an exporter, read endpoint credentials, or own SDK shutdown.
//! Applications build their tracer provider, choose an exporter and sampling policy, then install
//! the returned tracer with [`init`]. Rustee request spans already contain only the generated
//! request ID, method, status, duration, and configured route classification. W3C propagation is
//! opt-in through [`TraceContextLayer`] and never writes trace headers to HTTP responses.

mod propagation;
mod subscriber;

pub use opentelemetry;
pub use propagation::{
    TraceContextLayer, TraceContextService, W3cTraceContext, capture_w3c_context,
    extract_w3c_context, inject_context, inject_current_context,
};
pub use subscriber::{init, layer};
pub use tracing_opentelemetry;
