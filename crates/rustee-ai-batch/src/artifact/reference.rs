//! Content-free artifact identity and durable reservation contracts.

use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::{AiBatchConfigError, AiBatchReference, valid_identifier};

/// Provider-owned artifact category selected for application reconciliation.
///
/// This describes only the provider file's role. It does not imply that the framework downloads,
/// parses, retains, or deletes the file.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AiBatchArtifactKind {
    /// The provider's completed or partial successful-result artifact.
    Output,
    /// The provider's per-row error artifact.
    Error,
}

/// Content-free reference to one provider batch artifact that an application may reconcile.
///
/// The durable `reconciliation_key` must bind one exact worker delivery attempt. `provider_file_id`
/// is a validated opaque provider identifier, never an artifact body. Applications authorize the
/// tenant-scoped batch reference before downloading the selected file and must persist any
/// row-level side effects idempotently, normally by the provider `custom_id` plus a domain key.
/// After an ambiguous attempt, a reviewed explicit retry uses a new reconciliation key rather than
/// releasing or replaying this reference.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AiBatchArtifactReference {
    batch: AiBatchReference,
    kind: AiBatchArtifactKind,
    provider_file_id: String,
    reconciliation_key: String,
}

impl<'de> Deserialize<'de> for AiBatchArtifactReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawAiBatchArtifactReference {
            batch: AiBatchReference,
            kind: AiBatchArtifactKind,
            provider_file_id: String,
            reconciliation_key: String,
        }

        let raw = RawAiBatchArtifactReference::deserialize(deserializer)?;
        Self::new(
            raw.batch,
            raw.kind,
            raw.provider_file_id,
            raw.reconciliation_key,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl fmt::Debug for AiBatchArtifactReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiBatchArtifactReference")
            .field("batch", &self.batch)
            .field("kind", &self.kind)
            .field("provider_file_id", &"[REDACTED]")
            .field("reconciliation_key", &"[REDACTED]")
            .finish()
    }
}

impl AiBatchArtifactReference {
    /// Creates a bounded, content-free reference to an output or error artifact.
    ///
    /// # Errors
    ///
    /// Returns [`AiBatchConfigError::InvalidIdentifier`] for unsafe file or reconciliation keys.
    pub fn new(
        batch: AiBatchReference,
        kind: AiBatchArtifactKind,
        provider_file_id: impl Into<String>,
        reconciliation_key: impl Into<String>,
    ) -> Result<Self, AiBatchConfigError> {
        let reference = Self {
            batch,
            kind,
            provider_file_id: provider_file_id.into(),
            reconciliation_key: reconciliation_key.into(),
        };
        if !valid_identifier(&reference.provider_file_id)
            || !valid_identifier(&reference.reconciliation_key)
        {
            return Err(AiBatchConfigError::InvalidIdentifier);
        }
        Ok(reference)
    }

    /// Returns the tenant-scoped batch reference that must be authorized before access.
    #[must_use]
    pub const fn batch(&self) -> &AiBatchReference {
        &self.batch
    }

    /// Returns the provider artifact role selected by the application.
    #[must_use]
    pub const fn kind(&self) -> AiBatchArtifactKind {
        self.kind
    }

    /// Returns the opaque provider file identifier, never its contents.
    #[must_use]
    pub fn provider_file_id(&self) -> &str {
        &self.provider_file_id
    }

    /// Returns the stable durable-delivery idempotency key for this reconciliation attempt.
    #[must_use]
    pub fn reconciliation_key(&self) -> &str {
        &self.reconciliation_key
    }
}

/// State returned when one artifact reference is reserved for reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiBatchArtifactReservation {
    /// This caller owns the first reconciliation attempt for the exact artifact.
    Reserved,
    /// A prior reconciliation was durably recorded; no artifact reload is allowed.
    Reconciled,
    /// A prior attempt may have downloaded or applied some rows but lacks a final record.
    Pending,
}

/// Application-owned durable idempotency boundary for whole-artifact reconciliation.
///
/// A production ledger must survive worker restarts and reserve an exact
/// [`AiBatchArtifactReference`] atomically. It intentionally has no release-and-retry operation:
/// after a loader, processor, or record failure, applications reconcile partial row effects using
/// their own `custom_id`-bound domain ledger before explicitly scheduling another attempt with a
/// new reconciliation key.
pub trait AiBatchArtifactLedger: Clone + Send + Sync + 'static {
    /// Ledger-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Atomically reserves the artifact, returns a prior completion, or exposes ambiguity.
    fn reserve(
        &self,
        reference: AiBatchArtifactReference,
    ) -> BoxFuture<'static, Result<AiBatchArtifactReservation, Self::Error>>;

    /// Records that the application reconciled every intended side effect for the artifact.
    fn record_reconciled(
        &self,
        reference: AiBatchArtifactReference,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}
