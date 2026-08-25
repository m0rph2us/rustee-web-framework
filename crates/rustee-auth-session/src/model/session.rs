//! Opaque session record construction, durable restoration, and redacted diagnostics.

use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use rustee_auth::Principal;
use uuid::{Uuid, Variant};

/// Opaque, randomly generated server-side session identifier.
///
/// [`Debug`] redacts the bearer value; [`fmt::Display`] remains available for the cookie and
/// persistence paths that require the identifier.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize)]
pub struct SessionId(Uuid);

impl SessionId {
    /// Generates a random version-4 session identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        let id = Uuid::parse_str(value).ok()?;
        (id.to_string() == value)
            .then_some(id)
            .and_then(Self::from_uuid)
    }

    fn from_uuid(id: Uuid) -> Option<Self> {
        (id.get_variant() == Variant::RFC4122 && id.get_version_num() == 4).then_some(Self(id))
    }
}

impl<'de> serde::Deserialize<'de> for SessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let id = <Uuid as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_uuid(id).ok_or_else(|| {
            serde::de::Error::custom("stored session ID must be an RFC 4122 UUID v4")
        })
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionId([REDACTED])")
    }
}

/// A server-side session record with an expiry and a CSRF token.
#[derive(Clone, Eq, PartialEq, serde::Serialize)]
pub struct Session {
    pub(super) id: SessionId,
    pub(super) principal: Principal,
    pub(super) csrf_token: String,
    pub(super) expires_at_unix_seconds: u64,
}

#[derive(serde::Deserialize)]
struct SerializedSession {
    id: SessionId,
    principal: Principal,
    csrf_token: String,
    expires_at_unix_seconds: u64,
}

impl<'de> serde::Deserialize<'de> for Session {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedSession::deserialize(deserializer)?;
        Self::from_serialized(serialized).map_err(serde::de::Error::custom)
    }
}

impl Session {
    pub(super) fn new(principal: Principal, ttl_seconds: u64) -> Self {
        Self {
            id: SessionId::new(),
            principal,
            csrf_token: Uuid::new_v4().to_string(),
            expires_at_unix_seconds: unix_seconds().saturating_add(ttl_seconds),
        }
    }

    fn from_serialized(serialized: SerializedSession) -> Result<Self, &'static str> {
        if !valid_csrf_token(&serialized.csrf_token) {
            return Err("stored session CSRF token is invalid");
        }
        Ok(Self {
            id: serialized.id,
            principal: serialized.principal,
            csrf_token: serialized.csrf_token,
            expires_at_unix_seconds: serialized.expires_at_unix_seconds,
        })
    }

    /// Returns the authenticated principal held by this session.
    #[must_use]
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the opaque identifier persisted as the cookie value.
    #[must_use]
    pub const fn id(&self) -> SessionId {
        self.id
    }

    /// Returns whether the session is expired at the current system time.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.expires_at_unix_seconds <= unix_seconds()
    }

    /// Returns the remaining persistence TTL, or `None` when the session is expired.
    #[must_use]
    pub fn remaining_ttl_seconds(&self) -> Option<u64> {
        self.expires_at_unix_seconds
            .checked_sub(unix_seconds())
            .filter(|ttl| *ttl > 0)
    }

    pub(crate) fn into_authenticated_context(self) -> (Principal, String) {
        (self.principal, self.csrf_token)
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("id", &"[REDACTED]")
            .field("principal", &"[REDACTED]")
            .field("csrf_token", &"[REDACTED]")
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn valid_csrf_token(value: &str) -> bool {
    value.len() == 36 && Uuid::parse_str(value).is_ok_and(|token| token.to_string() == value)
}

#[cfg(test)]
mod tests {
    use super::SessionId;

    #[test]
    fn session_ids_accept_only_canonical_random_uuids() {
        let id = SessionId::new();
        let serialized = serde_json::to_string(&id).expect("session ID serialization must work");

        assert_eq!(
            serde_json::from_str::<SessionId>(&serialized).ok(),
            Some(id)
        );
        assert!(SessionId::parse(&id.to_string()).is_some());
        assert!(SessionId::parse(&id.to_string().to_uppercase()).is_none());
        assert!(
            serde_json::from_str::<SessionId>("\"550e8400-e29b-11d4-a716-446655440000\"").is_err()
        );
    }
}
