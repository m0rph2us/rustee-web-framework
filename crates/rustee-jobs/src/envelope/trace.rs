//! Bounded W3C trace-context carrier values for durable job envelopes.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::EnvelopeError;

const MAX_TRACEPARENT_LEN: usize = 512;
const MAX_TRACESTATE_LEN: usize = 512;

/// A bounded W3C trace-context carrier attached to a durable job envelope.
///
/// `rustee-jobs` persists only transport-neutral carrier values. An optional telemetry adapter
/// decides whether a valid carrier becomes the parent of a job-handling span.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct JobTraceContext {
    traceparent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tracestate: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct SerializedJobTraceContext {
    traceparent: String,
    tracestate: Option<String>,
}

impl<'de> Deserialize<'de> for JobTraceContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedJobTraceContext::deserialize(deserializer)?;
        Self::from_serialized(serialized).map_err(serde::de::Error::custom)
    }
}

impl JobTraceContext {
    pub(super) fn from_serialized(
        serialized: SerializedJobTraceContext,
    ) -> Result<Self, EnvelopeError> {
        Self::new(serialized.traceparent, serialized.tracestate)
    }

    /// Creates a bounded ASCII W3C trace-context carrier.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::InvalidTraceContext`] when a value is blank, non-ASCII, or larger
    /// than the documented W3C carrier bounds. The optional telemetry propagator validates syntax
    /// and sampling when it consumes this carrier.
    pub fn new(
        traceparent: impl Into<String>,
        tracestate: Option<String>,
    ) -> Result<Self, EnvelopeError> {
        let context = Self {
            traceparent: traceparent.into(),
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

impl fmt::Debug for JobTraceContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobTraceContext")
            .field("traceparent", &"[REDACTED]")
            .field("has_tracestate", &self.tracestate.is_some())
            .finish()
    }
}
