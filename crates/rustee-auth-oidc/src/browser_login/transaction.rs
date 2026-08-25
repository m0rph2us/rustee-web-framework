//! One-time browser OIDC authorization transactions and durable store contracts.

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

use super::{
    config::{MAX_TRANSACTION_TTL, is_valid_https_url},
    unix_seconds,
};

const MAX_IN_MEMORY_TRANSACTIONS: usize = 1_024;

/// State retained server-side between authorization redirect and callback.
#[derive(Clone, Serialize)]
pub struct PendingAuthorization {
    pub(super) state: String,
    pub(super) nonce: String,
    pub(super) code_verifier: String,
    pub(super) token_endpoint: Url,
    pub(super) expires_at_unix_seconds: u64,
}

#[derive(Deserialize)]
struct SerializedPendingAuthorization {
    state: String,
    nonce: String,
    code_verifier: String,
    token_endpoint: Url,
    expires_at_unix_seconds: u64,
}

impl<'de> Deserialize<'de> for PendingAuthorization {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedPendingAuthorization::deserialize(deserializer)?;
        if !Self::has_valid_fields(
            &serialized.state,
            &serialized.nonce,
            &serialized.code_verifier,
            &serialized.token_endpoint,
            serialized.expires_at_unix_seconds,
        ) {
            return Err(serde::de::Error::custom(
                "invalid OIDC authorization transaction",
            ));
        }
        Ok(Self {
            state: serialized.state,
            nonce: serialized.nonce,
            code_verifier: serialized.code_verifier,
            token_endpoint: serialized.token_endpoint,
            expires_at_unix_seconds: serialized.expires_at_unix_seconds,
        })
    }
}

impl PendingAuthorization {
    /// Returns the opaque state used as the transaction-store key.
    ///
    /// Stores must treat this as a capability value and must not log it.
    #[must_use]
    pub fn state(&self) -> &str {
        &self.state
    }

    pub(super) fn is_expired(&self) -> bool {
        self.expires_at_unix_seconds <= unix_seconds()
    }

    pub(super) fn is_valid_for_state(&self, state: &str) -> bool {
        self.state == state
            && Self::has_valid_fields(
                &self.state,
                &self.nonce,
                &self.code_verifier,
                &self.token_endpoint,
                self.expires_at_unix_seconds,
            )
    }

    /// Returns the remaining storage TTL, or `None` when the transaction is expired.
    #[must_use]
    pub fn remaining_ttl_seconds(&self) -> Option<u64> {
        self.expires_at_unix_seconds
            .checked_sub(unix_seconds())
            .filter(|ttl| *ttl > 0)
    }

    fn has_valid_fields(
        state: &str,
        nonce: &str,
        code_verifier: &str,
        token_endpoint: &Url,
        expires_at_unix_seconds: u64,
    ) -> bool {
        is_valid_oauth_authorization_value(state)
            && is_valid_oauth_authorization_value(nonce)
            && is_valid_oauth_authorization_value(code_verifier)
            && is_valid_https_url(token_endpoint)
            && valid_expiry(expires_at_unix_seconds)
    }
}

fn valid_expiry(expires_at_unix_seconds: u64) -> bool {
    expires_at_unix_seconds
        .checked_sub(unix_seconds())
        .is_none_or(|remaining| remaining <= MAX_TRANSACTION_TTL.as_secs())
}

impl fmt::Debug for PendingAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingAuthorization")
            .field("state", &"[REDACTED]")
            .field("nonce", &"[REDACTED]")
            .field("code_verifier", &"[REDACTED]")
            .field("token_endpoint", &"[REDACTED]")
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

/// Durable, atomic state store for one browser authorization transaction.
pub trait AuthorizationTransactionStore: Clone + Send + Sync + 'static {
    /// Store-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Atomically saves a newly-created pending authorization transaction only when its state is
    /// absent.
    ///
    /// Stores must never replace an unexpired state because it is a one-time capability bound to
    /// the original nonce and PKCE verifier.
    fn save(
        &self,
        transaction: PendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;

    /// Atomically retrieves and consumes a transaction identified by state.
    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<PendingAuthorization>, Self::Error>>;
}

/// In-memory authorization transaction store for local development and tests only.
///
/// The store retains at most 1,024 unexpired transactions. Saving a new transaction removes
/// expired entries first and fails closed when a colliding state or the fixed capacity prevents a
/// new entry.
#[derive(Clone, Default)]
pub struct InMemoryAuthorizationTransactionStore {
    pub(super) transactions: Arc<Mutex<BTreeMap<String, PendingAuthorization>>>,
}

impl fmt::Debug for InMemoryAuthorizationTransactionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryAuthorizationTransactionStore")
            .finish_non_exhaustive()
    }
}

/// In-memory authorization transaction-store failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InMemoryAuthorizationTransactionStoreError {
    /// A poisoned lock prevents safely persisting or consuming local authorization state.
    #[error("in-memory authorization transaction store state is unavailable")]
    StateUnavailable,
    /// Unconsumed authorization transactions exhausted the fixed local-store capacity.
    #[error("in-memory authorization transaction store capacity is exhausted")]
    CapacityExhausted,
    /// A live one-time state was already present.
    #[error("in-memory authorization transaction store state already exists")]
    DuplicateState,
}

impl AuthorizationTransactionStore for InMemoryAuthorizationTransactionStore {
    type Error = InMemoryAuthorizationTransactionStoreError;

    fn save(
        &self,
        transaction: PendingAuthorization,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let transactions = Arc::clone(&self.transactions);
        Box::pin(async move {
            let mut transactions = transactions
                .lock()
                .map_err(|_| InMemoryAuthorizationTransactionStoreError::StateUnavailable)?;
            transactions.retain(|_, pending| !pending.is_expired());
            if transactions.contains_key(&transaction.state) {
                return Err(InMemoryAuthorizationTransactionStoreError::DuplicateState);
            }
            if transactions.len() >= MAX_IN_MEMORY_TRANSACTIONS {
                return Err(InMemoryAuthorizationTransactionStoreError::CapacityExhausted);
            }
            transactions.insert(transaction.state.clone(), transaction);
            Ok(())
        })
    }

    fn take(
        &self,
        state: String,
    ) -> BoxFuture<'static, Result<Option<PendingAuthorization>, Self::Error>> {
        let transactions = Arc::clone(&self.transactions);
        Box::pin(async move {
            Ok(transactions
                .lock()
                .map_err(|_| InMemoryAuthorizationTransactionStoreError::StateUnavailable)?
                .remove(&state))
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use url::Url;

    use super::{
        AuthorizationTransactionStore, InMemoryAuthorizationTransactionStore,
        InMemoryAuthorizationTransactionStoreError, MAX_IN_MEMORY_TRANSACTIONS,
        PendingAuthorization, unix_seconds,
    };

    #[tokio::test]
    async fn transaction_store_prunes_expired_state_and_bounds_unconsumed_transactions() {
        let store = InMemoryAuthorizationTransactionStore::default();
        let expired = transaction("expired", 0);
        store
            .transactions
            .lock()
            .expect("new authorization transaction lock must be available")
            .insert(expired.state.clone(), expired);

        store
            .save(transaction("fresh", valid_expiry()))
            .await
            .expect("expired transaction must be pruned before saving");
        {
            let transactions = store
                .transactions
                .lock()
                .expect("new authorization transaction lock must be available");
            assert_eq!(transactions.len(), 1);
            assert!(transactions.contains_key("fresh"));
        }

        let capacity_store = InMemoryAuthorizationTransactionStore::default();
        {
            let mut transactions = capacity_store
                .transactions
                .lock()
                .expect("new authorization transaction lock must be available");
            for index in 0..MAX_IN_MEMORY_TRANSACTIONS {
                let pending = transaction(&format!("state-{index}"), valid_expiry());
                transactions.insert(pending.state.clone(), pending);
            }
        }

        assert_eq!(
            capacity_store
                .save(transaction("overflow", valid_expiry()))
                .await,
            Err(InMemoryAuthorizationTransactionStoreError::CapacityExhausted)
        );
    }

    #[tokio::test]
    async fn transaction_store_rejects_a_duplicate_state_without_replacing_it() {
        let store = InMemoryAuthorizationTransactionStore::default();
        let original = transaction("state", valid_expiry());
        let mut replacement = transaction("state", valid_expiry());
        replacement.nonce = "r".repeat(43);

        store.save(original.clone()).await.unwrap();
        assert_eq!(
            store.save(replacement).await,
            Err(InMemoryAuthorizationTransactionStoreError::DuplicateState)
        );
        assert_eq!(
            store.take("state".to_owned()).await.unwrap().unwrap().nonce,
            original.nonce
        );
    }

    #[test]
    fn durable_transaction_deserialization_revalidates_capabilities_and_endpoint() {
        let valid = json!({
            "state":"s".repeat(43),
            "nonce":"n".repeat(43),
            "code_verifier":"v".repeat(43),
            "token_endpoint":"https://issuer.example.test/token",
            "expires_at_unix_seconds":valid_expiry(),
        });
        let mut invalid_state = valid.clone();
        invalid_state["state"] = json!("invalid state");
        let mut invalid_endpoint = valid.clone();
        invalid_endpoint["token_endpoint"] = json!("http://issuer.example.test/token");
        let mut invalid_expiry = valid.clone();
        invalid_expiry["expires_at_unix_seconds"] = json!(u64::MAX);

        assert!(serde_json::from_value::<PendingAuthorization>(invalid_state).is_err());
        assert!(serde_json::from_value::<PendingAuthorization>(invalid_endpoint).is_err());
        assert!(serde_json::from_value::<PendingAuthorization>(invalid_expiry).is_err());
        assert!(serde_json::from_value::<PendingAuthorization>(valid).is_ok());
    }

    fn valid_expiry() -> u64 {
        unix_seconds() + 60
    }

    fn transaction(state: &str, expires_at_unix_seconds: u64) -> PendingAuthorization {
        PendingAuthorization {
            state: state.to_owned(),
            nonce: "n".repeat(43),
            code_verifier: "v".repeat(43),
            token_endpoint: Url::parse("https://issuer.example.test/token")
                .expect("test token endpoint must be valid"),
            expires_at_unix_seconds,
        }
    }
}

/// Supplies cryptographically unguessable state, nonce, and PKCE verifier values.
pub trait AuthorizationValueGenerator: Clone + Send + Sync + 'static {
    /// Returns one URL-safe authorization value.
    fn generate(&self) -> String;
}

/// UUID v4-based generator with 244 random bits per generated protocol value.
#[derive(Clone, Copy, Debug, Default)]
pub struct UuidAuthorizationValueGenerator;

impl AuthorizationValueGenerator for UuidAuthorizationValueGenerator {
    fn generate(&self) -> String {
        format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
    }
}
