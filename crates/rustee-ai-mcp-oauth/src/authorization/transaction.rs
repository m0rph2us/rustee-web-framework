//! One-time state and PKCE transaction storage contracts.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use rustee_core::is_valid_oauth_authorization_value;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{
    InMemoryMcpOAuthStoreError,
    config::{MAX_TRANSACTION_TTL, valid_resource_url},
};

pub(crate) const MAX_IN_MEMORY_TRANSACTIONS: usize = 1_024;

/// State retained only by the application between an OAuth redirect and its callback.
///
/// The state and verifier are capability values. A durable implementation must encrypt this
/// record at rest, apply [`Self::remaining_ttl_seconds`] as its storage TTL, and make `take`
/// atomic across application instances.
#[derive(Clone, Serialize)]
pub struct McpOAuthPendingAuthorization {
    pub(crate) state: String,
    pub(crate) code_verifier: String,
    pub(crate) token_endpoint: Url,
    pub(crate) resource: Url,
    pub(crate) expires_at_unix_seconds: u64,
}

#[derive(Deserialize)]
struct SerializedMcpOAuthPendingAuthorization {
    state: String,
    code_verifier: String,
    token_endpoint: Url,
    resource: Url,
    expires_at_unix_seconds: u64,
}

impl<'de> Deserialize<'de> for McpOAuthPendingAuthorization {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedMcpOAuthPendingAuthorization::deserialize(deserializer)?;
        if !Self::has_valid_fields(
            &serialized.state,
            &serialized.code_verifier,
            &serialized.token_endpoint,
            &serialized.resource,
            serialized.expires_at_unix_seconds,
        ) {
            return Err(serde::de::Error::custom(
                "invalid MCP OAuth authorization transaction",
            ));
        }
        Ok(Self {
            state: serialized.state,
            code_verifier: serialized.code_verifier,
            token_endpoint: serialized.token_endpoint,
            resource: serialized.resource,
            expires_at_unix_seconds: serialized.expires_at_unix_seconds,
        })
    }
}

impl McpOAuthPendingAuthorization {
    /// Returns the opaque state used exclusively as a transaction-store key.
    #[must_use]
    pub fn state(&self) -> &str {
        &self.state
    }

    pub(super) fn is_expired(&self) -> bool {
        self.expires_at_unix_seconds <= unix_seconds()
    }

    pub(super) fn is_valid_for(&self, state: &str, resource: &Url, token_endpoint: &Url) -> bool {
        self.state == state
            && self.resource == *resource
            && self.token_endpoint == *token_endpoint
            && Self::has_valid_fields(
                &self.state,
                &self.code_verifier,
                &self.token_endpoint,
                &self.resource,
                self.expires_at_unix_seconds,
            )
    }

    /// Returns the remaining storage TTL, or `None` for an expired transaction.
    #[must_use]
    pub fn remaining_ttl_seconds(&self) -> Option<u64> {
        self.expires_at_unix_seconds
            .checked_sub(unix_seconds())
            .filter(|ttl| *ttl > 0)
    }

    fn has_valid_fields(
        state: &str,
        code_verifier: &str,
        token_endpoint: &Url,
        resource: &Url,
        expires_at_unix_seconds: u64,
    ) -> bool {
        is_valid_oauth_authorization_value(state)
            && is_valid_oauth_authorization_value(code_verifier)
            && valid_resource_url(token_endpoint)
            && valid_resource_url(resource)
            && valid_expiry(expires_at_unix_seconds)
    }
}

fn valid_expiry(expires_at_unix_seconds: u64) -> bool {
    expires_at_unix_seconds
        .checked_sub(unix_seconds())
        .is_none_or(|remaining| remaining <= MAX_TRANSACTION_TTL.as_secs())
}

impl fmt::Debug for McpOAuthPendingAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthPendingAuthorization")
            .field("state", &"[REDACTED]")
            .field("code_verifier", &"[REDACTED]")
            .field("token_endpoint", &"[REDACTED]")
            .field("resource", &"[REDACTED]")
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

/// Atomic application-owned storage for one OAuth authorization transaction.
pub trait McpOAuthTransactionStore: Clone + Send + Sync + 'static {
    /// Store-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Atomically saves a new short-lived state and PKCE verifier transaction only when its state
    /// is absent.
    ///
    /// Stores must never replace an unexpired state because it is a one-time capability bound to
    /// the original verifier, resource, and token endpoint.
    fn save(
        &self,
        transaction: McpOAuthPendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;

    /// Atomically retrieves and consumes the transaction matching `state`.
    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<McpOAuthPendingAuthorization>, Self::Error>>;
}

/// Process-local transaction storage for tests and single-instance local development only.
///
/// The store retains at most 1,024 unexpired transactions. Saving a new transaction removes
/// expired entries first and fails closed when a colliding state or the fixed capacity prevents a
/// new entry.
#[derive(Clone, Default)]
pub struct InMemoryMcpOAuthTransactionStore {
    pub(crate) transactions: Arc<Mutex<BTreeMap<String, McpOAuthPendingAuthorization>>>,
}

impl fmt::Debug for InMemoryMcpOAuthTransactionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryMcpOAuthTransactionStore")
            .finish_non_exhaustive()
    }
}

impl McpOAuthTransactionStore for InMemoryMcpOAuthTransactionStore {
    type Error = InMemoryMcpOAuthStoreError;

    fn save(
        &self,
        transaction: McpOAuthPendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let transactions = Arc::clone(&self.transactions);
        Box::pin(async move {
            let mut transactions = transactions
                .lock()
                .map_err(|_| InMemoryMcpOAuthStoreError::StateUnavailable)?;
            transactions.retain(|_, pending| !pending.is_expired());
            if transactions.contains_key(&transaction.state) {
                return Err(InMemoryMcpOAuthStoreError::DuplicateTransactionState);
            }
            if transactions.len() >= MAX_IN_MEMORY_TRANSACTIONS {
                return Err(InMemoryMcpOAuthStoreError::TransactionCapacityExhausted);
            }
            transactions.insert(transaction.state.clone(), transaction);
            Ok(())
        })
    }

    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<McpOAuthPendingAuthorization>, Self::Error>> {
        let transactions = Arc::clone(&self.transactions);
        Box::pin(async move {
            Ok(transactions
                .lock()
                .map_err(|_| InMemoryMcpOAuthStoreError::StateUnavailable)?
                .remove(&state))
        })
    }
}

/// Supplies URL-safe, high-entropy state and PKCE verifier values.
pub trait McpOAuthValueGenerator: Clone + Send + Sync + 'static {
    /// Returns one independently generated authorization value.
    fn generate(&self) -> String;
}

/// UUID v4-based value generator with 244 random bits for every value.
#[derive(Clone, Copy, Debug, Default)]
pub struct UuidMcpOAuthValueGenerator;

impl McpOAuthValueGenerator for UuidMcpOAuthValueGenerator {
    fn generate(&self) -> String {
        format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
    }
}

pub(crate) fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
