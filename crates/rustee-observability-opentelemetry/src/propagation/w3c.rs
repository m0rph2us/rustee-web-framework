//! Bounded transport-neutral W3C carrier capture and extraction.

use std::fmt;

use opentelemetry::{
    Context,
    propagation::{Extractor, Injector, TextMapPropagator},
    trace::TraceContextExt,
};
use opentelemetry_sdk::propagation::TraceContextPropagator;

pub(super) const TRACEPARENT: &str = "traceparent";
pub(super) const TRACESTATE: &str = "tracestate";
pub(super) const MAX_TRACEPARENT_LEN: usize = 512;
pub(super) const MAX_TRACESTATE_LEN: usize = 512;

/// A bounded W3C trace carrier detached from HTTP transport.
#[derive(Clone, Eq, PartialEq)]
pub struct W3cTraceContext {
    traceparent: String,
    tracestate: Option<String>,
}

impl fmt::Debug for W3cTraceContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("W3cTraceContext")
            .field("has_traceparent", &true)
            .field("has_tracestate", &self.tracestate.is_some())
            .finish_non_exhaustive()
    }
}

impl W3cTraceContext {
    /// Returns the bounded W3C `traceparent` field.
    #[must_use]
    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }

    /// Returns the optional bounded W3C `tracestate` field.
    #[must_use]
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }
}

/// Captures a valid OpenTelemetry context into a bounded transport-neutral W3C carrier.
///
/// The returned carrier contains only ASCII fields no longer than 512 bytes. Invalid contexts or
/// oversized `tracestate` values are not propagated.
#[must_use]
pub fn capture_w3c_context(context: &Context) -> Option<W3cTraceContext> {
    if !context.span().span_context().is_valid() {
        return None;
    }

    let mut carrier = OutgoingW3cTraceCarrier::default();
    TraceContextPropagator::new().inject_context(context, &mut carrier);
    let traceparent = carrier
        .traceparent
        .filter(|value| is_bounded_w3c_value(value, MAX_TRACEPARENT_LEN))?;
    let tracestate = carrier
        .tracestate
        .filter(|value| is_bounded_w3c_value(value, MAX_TRACESTATE_LEN));
    Some(W3cTraceContext {
        traceparent,
        tracestate,
    })
}

/// Extracts a valid OpenTelemetry parent from bounded transport-neutral W3C fields.
///
/// Invalid, non-ASCII, or oversized fields return `None` so callers can start a new root span.
#[must_use]
pub fn extract_w3c_context(traceparent: &str, tracestate: Option<&str>) -> Option<Context> {
    if !is_bounded_w3c_value(traceparent, MAX_TRACEPARENT_LEN)
        || tracestate
            .is_some_and(|tracestate| !is_bounded_w3c_value(tracestate, MAX_TRACESTATE_LEN))
    {
        return None;
    }

    let carrier = BorrowedW3cTraceCarrier {
        traceparent,
        tracestate,
    };
    let context = TraceContextPropagator::new().extract(&carrier);
    context.span().span_context().is_valid().then_some(context)
}

#[derive(Default)]
struct OutgoingW3cTraceCarrier {
    traceparent: Option<String>,
    tracestate: Option<String>,
}

impl Injector for OutgoingW3cTraceCarrier {
    fn set(&mut self, key: &str, value: String) {
        if key.eq_ignore_ascii_case(TRACEPARENT) {
            self.traceparent = Some(value);
        } else if key.eq_ignore_ascii_case(TRACESTATE) {
            self.tracestate = (!value.is_empty()).then_some(value);
        }
    }
}

struct BorrowedW3cTraceCarrier<'a> {
    traceparent: &'a str,
    tracestate: Option<&'a str>,
}

impl Extractor for BorrowedW3cTraceCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case(TRACEPARENT) {
            Some(self.traceparent)
        } else if key.eq_ignore_ascii_case(TRACESTATE) {
            self.tracestate
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = vec![TRACEPARENT];
        if self.tracestate.is_some() {
            keys.push(TRACESTATE);
        }
        keys
    }
}

fn is_bounded_w3c_value(value: &str, maximum_len: usize) -> bool {
    value.len() <= maximum_len && value.is_ascii()
}

#[cfg(test)]
mod tests {
    use opentelemetry::{
        Context,
        trace::{TraceContextExt, Tracer, TracerProvider},
    };
    use opentelemetry_sdk::trace::SdkTracerProvider;

    use super::{MAX_TRACESTATE_LEN, capture_w3c_context, extract_w3c_context};

    #[test]
    fn transport_neutral_carrier_round_trips_only_bounded_valid_context() {
        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("rustee-w3c-carrier-contract");
        let context = Context::current_with_span(tracer.start("source"));

        let carrier = capture_w3c_context(&context).expect("valid span must yield a carrier");
        let debug = format!("{carrier:?}");
        assert!(debug.contains("has_traceparent: true"));
        assert!(!debug.contains(carrier.traceparent()));

        let parent = extract_w3c_context(carrier.traceparent(), carrier.tracestate())
            .expect("captured carrier must extract a valid parent");
        assert_eq!(
            parent.span().span_context().trace_id(),
            context.span().span_context().trace_id()
        );
        assert!(extract_w3c_context(&"x".repeat(MAX_TRACESTATE_LEN + 1), None).is_none());
        provider.shutdown().unwrap();
    }
}
