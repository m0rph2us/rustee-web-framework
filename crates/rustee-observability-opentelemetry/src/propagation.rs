//! Stable public facade for W3C Trace Context propagation.

mod carrier;
mod layer;
mod w3c;

pub use carrier::{inject_context, inject_current_context};
pub use layer::{TraceContextLayer, TraceContextService};
pub use w3c::{W3cTraceContext, capture_w3c_context, extract_w3c_context};

#[cfg(test)]
mod tests;
