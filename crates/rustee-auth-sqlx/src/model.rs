//! Credential-lifecycle models for the durable API-key store.

use std::{fmt, time::SystemTime};

use rustee_auth::{ApiKeyFingerprint, Principal};
use uuid::Uuid;

/// A new active API-key credential represented only by its keyed fingerprint.
pub struct ApiKeyRegistration {
    fingerprint: ApiKeyFingerprint,
    principal: Principal,
    expires_at: Option<SystemTime>,
}

impl ApiKeyRegistration {
    /// Creates a registration for a principal without an expiry.
    #[must_use]
    pub fn new(fingerprint: ApiKeyFingerprint, principal: Principal) -> Self {
        Self {
            fingerprint,
            principal,
            expires_at: None,
        }
    }

    /// Sets a whole-second explicit expiry evaluated against the `PostgreSQL` clock.
    #[must_use]
    pub fn expires_at(mut self, expires_at: SystemTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    pub(super) fn into_parts(self) -> (ApiKeyFingerprint, Principal, Option<SystemTime>) {
        (self.fingerprint, self.principal, self.expires_at)
    }
}

impl fmt::Debug for ApiKeyRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiKeyRegistration")
            .field("fingerprint", &"[REDACTED]")
            .field("principal", &"[REDACTED]")
            .field("expires_at", &self.expires_at.is_some())
            .finish()
    }
}

/// Opaque record identity used for revocation and client-key rotation.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ApiKeyRecordId(Uuid);

impl ApiKeyRecordId {
    /// Returns the deployment-owned record identity for an authorized admin workflow.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }

    pub(super) const fn new(value: Uuid) -> Self {
        Self(value)
    }
}

impl fmt::Debug for ApiKeyRecordId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKeyRecordId([redacted])")
    }
}
