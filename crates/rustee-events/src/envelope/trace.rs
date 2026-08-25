//! Bounded W3C trace-context carrier values for event envelopes.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::EnvelopeError;

const MAX_TRACEPARENT_LEN: usize = 512;
const MAX_TRACESTATE_LEN: usize = 512;

/// A bounded W3C trace-context carrier attached to an event envelope.
///
/// `rustee-events` deliberately stores only the transport-neutral carrier. An optional telemetry
/// integration validates it against its propagator and decides whether it becomes a parent span.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct EventTraceContext {
    traceparent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tracestate: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct SerializedEventTraceContext {
    traceparent: String,
    tracestate: Option<String>,
}

impl<'de> Deserialize<'de> for EventTraceContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedEventTraceContext::deserialize(deserializer)?;
        Self::from_serialized(serialized).map_err(serde::de::Error::custom)
    }
}

impl EventTraceContext {
    pub(super) fn from_serialized(
        serialized: SerializedEventTraceContext,
    ) -> Result<Self, EnvelopeError> {
        Self::new(serialized.traceparent, serialized.tracestate)
    }

    /// Creates a bounded ASCII W3C trace-context carrier.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::InvalidTraceContext`] when a value is blank, non-ASCII, or larger
    /// than the documented W3C carrier bounds. Syntax and sampling are validated by the optional
    /// telemetry propagator that consumes this carrier.
    pub fn new(
        traceparent: impl Into<String>,
        tracestate: Option<String>,
    ) -> Result<Self, EnvelopeError> {
        let traceparent = traceparent.into();
        let context = Self {
            traceparent,
            tracestate,
        };
        context.validate()?;
        Ok(context)
    }

    /// Returns the W3C traceparent carrier value.
    #[must_use]
    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }

    /// Returns the optional W3C tracestate carrier value.
    #[must_use]
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }

    pub(super) fn validate(&self) -> Result<(), EnvelopeError> {
        if self.traceparent.trim().is_empty()
            || !self.traceparent.is_ascii()
            || self.traceparent.len() > MAX_TRACEPARENT_LEN
            || self.tracestate.as_deref().is_some_and(|value| {
                value.trim().is_empty() || !value.is_ascii() || value.len() > MAX_TRACESTATE_LEN
            })
        {
            return Err(EnvelopeError::InvalidTraceContext);
        }
        Ok(())
    }
}

impl fmt::Debug for EventTraceContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventTraceContext")
            .field("traceparent", &"[REDACTED]")
            .field("has_tracestate", &self.tracestate.is_some())
            .finish()
    }
}
