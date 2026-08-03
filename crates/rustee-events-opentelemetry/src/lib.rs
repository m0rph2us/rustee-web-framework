//! Optional OpenTelemetry W3C trace propagation for Rustee event envelopes.
//!
//! This crate captures the current `tracing` span into an event envelope only when it has a valid
//! OpenTelemetry context. On consumption, [`TraceContextHandler`] makes a valid envelope context
//! the parent of one bounded event-handling span. Invalid or absent carriers start a new root span.

use futures_util::future::BoxFuture;
use opentelemetry::{
    Context,
    propagation::{Extractor, Injector, TextMapPropagator},
    trace::TraceContextExt,
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use rustee_events::{Event, EventContext, EventEnvelope, EventHandler, EventTraceContext};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub use opentelemetry;
pub use tracing_opentelemetry;

const TRACEPARENT: &str = "traceparent";
const TRACESTATE: &str = "tracestate";

/// Captures a valid current OpenTelemetry span as a bounded event trace carrier.
#[must_use]
pub fn capture_current_trace_context() -> Option<EventTraceContext> {
    capture_trace_context(&tracing::Span::current().context())
}

/// Captures a valid OpenTelemetry context as a bounded event trace carrier.
#[must_use]
pub fn capture_trace_context(context: &Context) -> Option<EventTraceContext> {
    context.span().span_context().is_valid().then(|| {
        let mut carrier = TraceCarrier::default();
        TraceContextPropagator::new().inject_context(context, &mut carrier);
        EventTraceContext::new(carrier.traceparent?, carrier.tracestate).ok()
    })?
}

/// Adds the current valid OpenTelemetry context to an event envelope when one is active.
#[must_use]
pub fn with_current_trace_context<E>(envelope: EventEnvelope<E>) -> EventEnvelope<E>
where
    E: Event,
{
    match capture_current_trace_context() {
        Some(context) => envelope.with_trace_context(context),
        None => envelope,
    }
}

/// Wraps a typed handler so an event's valid W3C carrier becomes its event span parent.
#[derive(Clone, Debug)]
pub struct TraceContextHandler<H> {
    inner: H,
}

impl<H> TraceContextHandler<H> {
    /// Creates a trace-propagating wrapper around one typed event handler.
    #[must_use]
    pub fn new(inner: H) -> Self {
        Self { inner }
    }

    /// Returns the wrapped handler.
    #[must_use]
    pub fn into_inner(self) -> H {
        self.inner
    }
}

impl<E, H> EventHandler<E> for TraceContextHandler<H>
where
    E: Event,
    H: EventHandler<E>,
{
    type Error = H::Error;

    fn handle(
        &self,
        payload: E,
        context: EventContext,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let span = tracing::info_span!(
                "Rustee event",
                event.type = context.event_type(),
                event.version = context.version(),
            );
            if let Some(parent) = context.trace_context().and_then(extract_parent) {
                let _ = span.set_parent(parent);
            }
            inner.handle(payload, context).instrument(span).await
        })
    }
}

fn extract_parent(trace_context: &EventTraceContext) -> Option<Context> {
    let carrier = TraceCarrier {
        traceparent: Some(trace_context.traceparent().to_owned()),
        tracestate: trace_context.tracestate().map(ToOwned::to_owned),
    };
    let context = TraceContextPropagator::new().extract(&carrier);
    context.span().span_context().is_valid().then_some(context)
}

#[derive(Default)]
struct TraceCarrier {
    traceparent: Option<String>,
    tracestate: Option<String>,
}

impl Injector for TraceCarrier {
    fn set(&mut self, key: &str, value: String) {
        if key.eq_ignore_ascii_case(TRACEPARENT) {
            self.traceparent = Some(value);
        } else if key.eq_ignore_ascii_case(TRACESTATE) {
            self.tracestate = (!value.is_empty()).then_some(value);
        }
    }
}

impl Extractor for TraceCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case(TRACEPARENT) {
            self.traceparent.as_deref()
        } else if key.eq_ignore_ascii_case(TRACESTATE) {
            self.tracestate.as_deref()
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(2);
        if self.traceparent.is_some() {
            keys.push(TRACEPARENT);
        }
        if self.tracestate.is_some() {
            keys.push(TRACESTATE);
        }
        keys
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use rustee_events::{Event, EventEnvelope};
    use serde::{Deserialize, Serialize};
    use tracing_subscriber::layer::SubscriberExt;

    use super::{TraceContextHandler, capture_current_trace_context};

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct ContractEvent;

    impl Event for ContractEvent {
        const TYPE: &'static str = "rustee.events.otel.contract.v1";
        const VERSION: u16 = 1;
    }

    #[tokio::test]
    async fn handler_span_uses_the_envelope_w3c_parent() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer().with_tracer(provider.tracer("rustee-events-contract")),
        );
        let subscriber_dispatch = tracing::Dispatch::new(subscriber);
        let guard = tracing::dispatcher::set_default(&subscriber_dispatch);
        let observed = Arc::new(Mutex::new(None));
        let observed_handler = Arc::clone(&observed);
        let handler = TraceContextHandler::new(move |_event: ContractEvent, _| {
            let observed = Arc::clone(&observed_handler);
            async move {
                *observed.lock().unwrap() = capture_current_trace_context();
                Ok::<(), std::convert::Infallible>(())
            }
        });
        let envelope = EventEnvelope::new(ContractEvent, "contract-key")
            .unwrap()
            .with_trace_context(
                rustee_events::EventTraceContext::new(
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                    Some("vendor=one".to_owned()),
                )
                .unwrap(),
            );
        rustee_events::dispatch(envelope, &handler).await.unwrap();
        drop(guard);

        let traceparent = observed
            .lock()
            .unwrap()
            .as_ref()
            .map(|context| context.traceparent().to_owned())
            .expect("event handler should capture a valid child trace context");
        assert!(traceparent.starts_with("00-0af7651916cd43dd8448eb211c80319c-"));
        assert_ne!(
            traceparent,
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        );

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "Rustee event")
            .expect("event handler span must be exported");
        assert_eq!(span.parent_span_id.to_string(), "b7ad6b7169203331");
        provider.shutdown().unwrap();
    }
}
