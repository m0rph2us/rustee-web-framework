//! Durable `MongoDB` change-stream checkpoint and shutdown-aware read contracts.

use std::{error::Error as StdError, fmt, future::Future};

use futures_util::future::BoxFuture;
use mongodb::change_stream::{ChangeStream, event::ResumeToken};
use serde::de::DeserializeOwned;

const MAX_CHANGE_STREAM_CONSUMER_BYTES: usize = 255;

/// A bounded stable identity for one durable change-stream consumer checkpoint.
///
/// A consumer identifies the exact watched scope and pipeline contract, not merely a process. Do
/// not reuse it after changing the watched collection, filter, event decoding, or durable handler
/// semantics. Run at most one active worker for an identity unless a deployment-owned leader
/// writer coordination prevents stale workers from saving an older token.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ChangeStreamConsumer(String);

impl ChangeStreamConsumer {
    /// Creates one non-blank, NUL-free, bounded consumer identity.
    ///
    /// # Errors
    ///
    /// Returns [`ChangeStreamConsumerError::InvalidConsumer`] when `consumer` is not safe for a
    /// durable checkpoint key.
    pub fn new(consumer: impl Into<String>) -> Result<Self, ChangeStreamConsumerError> {
        let consumer = consumer.into();
        if consumer.trim().is_empty()
            || consumer.contains('\0')
            || consumer.len() > MAX_CHANGE_STREAM_CONSUMER_BYTES
        {
            return Err(ChangeStreamConsumerError::InvalidConsumer);
        }
        Ok(Self(consumer))
    }

    /// Returns the stable consumer identity for a storage adapter key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ChangeStreamConsumer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ChangeStreamConsumer")
            .field(&"[REDACTED]")
            .finish()
    }
}

/// Invalid durable change-stream consumer identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ChangeStreamConsumerError {
    /// The identity was blank, contained a NUL byte, or exceeded the storage bound.
    #[error("change-stream consumer must be non-blank, NUL-free, and bounded")]
    InvalidConsumer,
}

/// Durable storage boundary for opaque `MongoDB` change-stream resume tokens.
///
/// Load the token before creating a new driver stream with `Watch::resume_after`. Save it only
/// after a received event's durable, idempotent handler succeeds. This contract deliberately does
/// not start workers, retry handlers, resolve invalidated tokens, or coordinate active workers;
/// those failure and exclusive-writer policies remain application deployment concerns.
pub trait ChangeStreamCheckpointStore: Clone + Send + Sync + 'static {
    /// Storage-specific failure type.
    type Error: StdError + Send + Sync + 'static;

    /// Loads the last durable resume token for a consumer identity.
    fn load(
        &self,
        consumer: ChangeStreamConsumer,
    ) -> BoxFuture<'static, Result<Option<ResumeToken>, Self::Error>>;

    /// Replaces the last durable resume token after successful event handling.
    fn save(
        &self,
        consumer: ChangeStreamConsumer,
        resume_token: ResumeToken,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// The outcome of waiting for one `MongoDB` change stream item with a shutdown boundary.
pub enum ChangeStreamNext<T> {
    /// One event was read. Persist `resume_token` only after durable event handling succeeds.
    Event {
        /// The driver-decoded change event.
        event: T,
        /// The opaque token that resumes from this observation point when the stream restarts.
        resume_token: Option<ResumeToken>,
    },
    /// The stream ended without an event. Its last observed token remains available for recovery.
    Ended {
        /// The most recent opaque resume token known by the driver.
        resume_token: Option<ResumeToken>,
    },
    /// Shutdown resolved before the next event was read. No event was handed to the application;
    /// stop using that stream and let the supervisor create a new one on its next start.
    Shutdown {
        /// The most recent opaque resume token known by the driver.
        resume_token: Option<ResumeToken>,
    },
}

impl<T> fmt::Debug for ChangeStreamNext<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Event { resume_token, .. } => formatter
                .debug_struct("ChangeStreamNext::Event")
                .field("has_resume_token", &resume_token.is_some())
                .finish(),
            Self::Ended { resume_token } => formatter
                .debug_struct("ChangeStreamNext::Ended")
                .field("has_resume_token", &resume_token.is_some())
                .finish(),
            Self::Shutdown { resume_token } => formatter
                .debug_struct("ChangeStreamNext::Shutdown")
                .field("has_resume_token", &resume_token.is_some())
                .finish(),
        }
    }
}

/// Waits for one change-stream event or a shutdown signal without starting an unbounded worker.
///
/// Shutdown has priority when it is already ready. A shutdown result ends the stream's ownership
/// in the caller's worker; drop it and let the supervisor create a new stream on restart. When an
/// event wins, the returned token is only a checkpoint candidate: persist it after the event's
/// durable, idempotent handling succeeds. The official driver owns resume attempts for a live
/// stream; this helper does not invent queue delivery guarantees, retry failed handlers, or
/// persist tokens.
///
/// # Errors
///
/// Returns a driver error from the next change-stream operation. The caller owns supervisor
/// restart/backoff and decides whether a stored token remains valid for recovery.
pub async fn next_change_until<T, Shutdown>(
    stream: &mut ChangeStream<T>,
    shutdown: Shutdown,
) -> mongodb::error::Result<ChangeStreamNext<T>>
where
    T: DeserializeOwned,
    Shutdown: Future<Output = ()>,
{
    tokio::select! {
        biased;
        () = shutdown => Ok(ChangeStreamNext::Shutdown {
            resume_token: stream.resume_token(),
        }),
        next = stream.next_if_any() => match next? {
            Some(event) => Ok(ChangeStreamNext::Event {
                event,
                resume_token: stream.resume_token(),
            }),
            None => Ok(ChangeStreamNext::Ended {
                resume_token: stream.resume_token(),
            }),
        },
    }
}
