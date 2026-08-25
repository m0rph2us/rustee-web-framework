use std::fmt;

use http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::{
    Context,
    propagation::{Extractor, Injector, TextMapPropagator},
    trace::TraceContextExt,
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use rustee_observability::RequestSpanParentHook;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::w3c::{MAX_TRACEPARENT_LEN, MAX_TRACESTATE_LEN, TRACEPARENT, TRACESTATE};

/// Injects the current Rustee `tracing` span context into an outbound HTTP header map.
///
/// Existing `traceparent` and `tracestate` values are removed first. With no active OpenTelemetry
/// span, the headers remain absent rather than forwarding a caller-supplied value. This helper is
/// explicit: Rustee does not inject propagation headers into response headers or arbitrary clients.
pub fn inject_current_context(headers: &mut HeaderMap) {
    inject_context(&tracing::Span::current().context(), headers);
}

/// Injects one OpenTelemetry context into an outbound HTTP header map.
///
/// Existing W3C trace headers are removed before injection. Applications commonly use this from a
/// client request created inside a Rustee request handler.
pub fn inject_context(context: &Context, headers: &mut HeaderMap) {
    headers.remove(TRACEPARENT);
    headers.remove(TRACESTATE);
    let mut injector = HeaderInjector(headers);
    TraceContextPropagator::new().inject_context(context, &mut injector);
}

#[derive(Clone)]
pub(super) struct RemoteTraceParent(Context);

impl fmt::Debug for RemoteTraceParent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteTraceParent")
            .field(
                "has_valid_context",
                &self.0.span().span_context().is_valid(),
            )
            .finish()
    }
}

impl RequestSpanParentHook for RemoteTraceParent {
    fn apply(&self, span: &tracing::Span) {
        let _ = span.set_parent(self.0.clone());
    }
}

pub(super) fn remote_parent(
    propagator: &TraceContextPropagator,
    headers: &HeaderMap,
) -> Option<RemoteTraceParent> {
    let carrier = IncomingHeaders::from_headers(headers)?;
    let context = propagator.extract(&carrier);
    context
        .span()
        .span_context()
        .is_valid()
        .then_some(RemoteTraceParent(context))
}

struct IncomingHeaders {
    traceparent: String,
    tracestate: Option<String>,
}

impl fmt::Debug for IncomingHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IncomingHeaders")
            .field("has_traceparent", &true)
            .field("has_tracestate", &self.tracestate.is_some())
            .finish_non_exhaustive()
    }
}

impl IncomingHeaders {
    fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let traceparent = one_bounded_header(headers, TRACEPARENT, MAX_TRACEPARENT_LEN)?;
        let tracestate = joined_bounded_headers(headers, TRACESTATE, MAX_TRACESTATE_LEN).ok()?;
        Some(Self {
            traceparent,
            tracestate,
        })
    }
}

impl Extractor for IncomingHeaders {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case(TRACEPARENT) {
            Some(&self.traceparent)
        } else if key.eq_ignore_ascii_case(TRACESTATE) {
            self.tracestate.as_deref()
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

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if key.eq_ignore_ascii_case(TRACESTATE) && value.is_empty() {
            self.0.remove(TRACESTATE);
            return;
        }
        let Ok(name) = HeaderName::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(value) = HeaderValue::from_str(&value) else {
            return;
        };
        self.0.insert(name, value);
    }
}

fn one_bounded_header(headers: &HeaderMap, name: &str, maximum_len: usize) -> Option<String> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() || value.len() > maximum_len || !value.is_ascii() {
        return None;
    }
    Some(value.to_owned())
}

fn joined_bounded_headers(
    headers: &HeaderMap,
    name: &str,
    maximum_len: usize,
) -> Result<Option<String>, ()> {
    let mut joined = String::new();
    for value in &headers.get_all(name) {
        let value = value.to_str().map_err(|_| ())?;
        let separator_len = usize::from(!joined.is_empty());
        if !value.is_ascii()
            || joined
                .len()
                .saturating_add(separator_len)
                .saturating_add(value.len())
                > maximum_len
        {
            return Err(());
        }
        if !joined.is_empty() {
            joined.push(',');
        }
        joined.push_str(value);
    }
    Ok((!joined.is_empty()).then_some(joined))
}

#[cfg(test)]
mod tests {
    use http::HeaderMap;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    use super::{IncomingHeaders, remote_parent};
    use crate::propagation::w3c::{MAX_TRACESTATE_LEN, TRACEPARENT, TRACESTATE};

    #[test]
    fn propagation_debug_redacts_raw_header_and_trace_values() {
        let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let tracestate = "vendor=private-state";
        let mut headers = HeaderMap::new();
        headers.insert(TRACEPARENT, traceparent.parse().unwrap());
        headers.insert(TRACESTATE, tracestate.parse().unwrap());

        let carrier = IncomingHeaders::from_headers(&headers).unwrap();
        let carrier_debug = format!("{carrier:?}");
        assert!(carrier_debug.contains("has_traceparent: true"));
        assert!(carrier_debug.contains("has_tracestate: true"));
        assert!(!carrier_debug.contains(traceparent));
        assert!(!carrier_debug.contains(tracestate));

        let parent = remote_parent(&TraceContextPropagator::new(), &headers).unwrap();
        let parent_debug = format!("{parent:?}");
        assert!(parent_debug.contains("has_valid_context: true"));
        assert!(!parent_debug.contains("0af7651916cd43dd8448eb211c80319c"));
        assert!(!parent_debug.contains("b7ad6b7169203331"));
        assert!(!parent_debug.contains(tracestate));
    }

    #[test]
    fn oversized_tracestate_rejects_the_entire_inbound_context() {
        let mut headers = HeaderMap::new();
        headers.insert(
            TRACEPARENT,
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
                .parse()
                .unwrap(),
        );
        headers.insert(
            TRACESTATE,
            format!("vendor={}", "a".repeat(MAX_TRACESTATE_LEN))
                .parse()
                .unwrap(),
        );

        assert!(IncomingHeaders::from_headers(&headers).is_none());
        assert!(remote_parent(&TraceContextPropagator::new(), &headers).is_none());
    }
}
