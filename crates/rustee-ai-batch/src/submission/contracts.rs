//! Content-free batch submission models and application-owned integration contracts.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};

/// Maximum UTF-8 byte length accepted for a durable AI batch identifier.
pub const MAX_BATCH_IDENTIFIER_BYTES: usize = 128;

/// Content-free reference to an application-owned batch catalog entry.
///
/// `scope` must be an opaque tenant and policy boundary. `catalog_id` locates the application
/// record to load after authorization. `run_key` is the stable idempotency key shared with the
/// provider and durable worker delivery; none of these values may contain request content.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AiBatchReference {
    scope: String,
    catalog_id: String,
    run_key: String,
}

impl<'de> Deserialize<'de> for AiBatchReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawAiBatchReference {
            scope: String,
            catalog_id: String,
            run_key: String,
        }

        let raw = RawAiBatchReference::deserialize(deserializer)?;
        Self::new(raw.scope, raw.catalog_id, raw.run_key).map_err(serde::de::Error::custom)
    }
}

impl fmt::Debug for AiBatchReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiBatchReference")
            .field("scope", &"[REDACTED]")
            .field("catalog_id", &"[REDACTED]")
            .field("run_key", &"[REDACTED]")
            .finish()
    }
}

impl AiBatchReference {
    /// Creates a bounded, content-free batch reference.
    ///
    /// # Errors
    ///
    /// Returns [`AiBatchConfigError::InvalidIdentifier`] for unsafe identifiers.
    pub fn new(
        scope: impl Into<String>,
        catalog_id: impl Into<String>,
        run_key: impl Into<String>,
    ) -> Result<Self, AiBatchConfigError> {
        let reference = Self {
            scope: scope.into(),
            catalog_id: catalog_id.into(),
            run_key: run_key.into(),
        };
        if !valid_identifier(&reference.scope)
            || !valid_identifier(&reference.catalog_id)
            || !valid_identifier(&reference.run_key)
        {
            return Err(AiBatchConfigError::InvalidIdentifier);
        }
        Ok(reference)
    }

    /// Returns the application-owned tenant/policy isolation scope.
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Returns the application catalog identifier without loading its contents.
    #[must_use]
    pub fn catalog_id(&self) -> &str {
        &self.catalog_id
    }

    /// Returns the stable provider and delivery idempotency key.
    #[must_use]
    pub fn run_key(&self) -> &str {
        &self.run_key
    }
}

/// Safe provider receipt retained after one accepted asynchronous batch submission.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct AiBatchReceipt {
    provider_batch_id: String,
}

impl fmt::Debug for AiBatchReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiBatchReceipt")
            .field("provider_batch_id", &"[REDACTED]")
            .finish()
    }
}

impl<'de> Deserialize<'de> for AiBatchReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawAiBatchReceipt {
            provider_batch_id: String,
        }

        let raw = RawAiBatchReceipt::deserialize(deserializer)?;
        Self::new(raw.provider_batch_id).map_err(serde::de::Error::custom)
    }
}

impl AiBatchReceipt {
    /// Creates a safe, bounded provider batch identifier.
    ///
    /// # Errors
    ///
    /// Returns [`AiBatchConfigError::InvalidIdentifier`] when the adapter emits unsafe metadata.
    pub fn new(provider_batch_id: impl Into<String>) -> Result<Self, AiBatchConfigError> {
        let provider_batch_id = provider_batch_id.into();
        if !valid_identifier(&provider_batch_id) {
            return Err(AiBatchConfigError::InvalidIdentifier);
        }
        Ok(Self { provider_batch_id })
    }

    /// Returns the provider's safe batch identifier for application polling/reconciliation.
    #[must_use]
    pub fn provider_batch_id(&self) -> &str {
        &self.provider_batch_id
    }
}

/// Invalid batch reference, receipt, or in-memory ledger configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AiBatchConfigError {
    /// Public identifiers may not contain raw content, whitespace, URL syntax, or backend keys.
    #[error(
        "AI batch identifiers must use bounded ASCII letters, digits, underscore, hyphen, or dot"
    )]
    InvalidIdentifier,
    /// The in-memory ledger intentionally has a bounded development/test capacity.
    #[error("AI in-memory batch ledger capacity must be between one and 10,000 records")]
    InvalidInMemoryCapacity,
}

/// State returned when one run key is reserved for provider submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AiBatchReservation {
    /// This caller owns the first submission attempt for the reference.
    Reserved,
    /// A prior submission is durably recorded; the caller must reuse its receipt.
    Submitted(AiBatchReceipt),
    /// A prior caller may have reached the provider but lacks a durable receipt.
    Pending,
}

/// Application-owned durable idempotency boundary for batch submission.
///
/// A production ledger must survive worker restarts. It must make `reserve` atomic per exact
/// [`AiBatchReference`] and retain `Pending` after a provider error until application
/// reconciliation decides what happened. This trait has no release-and-retry operation.
pub trait AiBatchSubmissionLedger: Clone + Send + Sync + 'static {
    /// Ledger-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Atomically reserves one submission, returns its prior receipt, or exposes ambiguity.
    fn reserve(
        &self,
        reference: AiBatchReference,
    ) -> BoxFuture<'static, Result<AiBatchReservation, Self::Error>>;

    /// Records an accepted provider receipt for an existing reservation.
    fn record_submission(
        &self,
        reference: AiBatchReference,
        receipt: AiBatchReceipt,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Application-owned catalog loader for raw provider batch work.
///
/// The work type can contain prompts, expected answers, files, or provider request bodies, but it
/// remains in the trusted application process and is never serialized by this crate.
pub trait AiBatchCatalog: Clone + Send + Sync + 'static {
    /// Application-owned provider work loaded after authorization.
    type Work: Send + 'static;
    /// Catalog lookup failure.
    type Error: StdError + Send + Sync + 'static;

    /// Loads work for the exact tenant-scoped reference.
    fn load(
        &self,
        reference: AiBatchReference,
    ) -> BoxFuture<'static, Result<Self::Work, Self::Error>>;
}

/// Provider-specific asynchronous batch submission adapter.
///
/// The adapter owns provider request codec, limits, auth, cancellation, polling, partial-result
/// retrieval, billing interpretation, and any native idempotency header. It must bind `run_key`
/// when the provider supports one and return only a safe receipt identifier here.
pub trait AiBatchProvider<Work>: Clone + Send + Sync + 'static
where
    Work: Send + 'static,
{
    /// Provider submission failure.
    type Error: StdError + Send + Sync + 'static;

    /// Submits exactly one loaded batch without automatic retry.
    fn submit(
        &self,
        reference: AiBatchReference,
        work: Work,
    ) -> BoxFuture<'static, Result<AiBatchReceipt, Self::Error>>;
}

pub(crate) fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_BATCH_IDENTIFIER_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}
