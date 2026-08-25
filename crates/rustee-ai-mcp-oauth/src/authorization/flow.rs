use std::{fmt, time::SystemTime};

use rustee_core::{
    is_valid_oauth_authorization_code, is_valid_oauth_authorization_value,
    is_valid_oauth_provider_error,
};
use url::Url;

use super::{
    MAX_PROVIDER_ERROR_BYTES, McpOAuthAuthorizationCallback, McpOAuthAuthorizationRedirect,
    McpOAuthPendingAuthorization, McpOAuthTokenExchangeRequest, McpOAuthTransactionStore,
    McpOAuthValueGenerator, pkce_challenge, refresh_gate::RefreshGateRegistry, unix_seconds,
};
use crate::{
    McpOAuthAuthorizationServerMetadata, McpOAuthClientConfig, McpOAuthError,
    McpOAuthTokenExchanger, McpOAuthTokenRevoker, McpOAuthTokenSet, McpOAuthTokenStore,
    McpOAuthTokenStoreKey, config::canonical_resource,
};

/// Explicit authorization completion and token lifecycle for one selected MCP resource.
///
/// This orchestrator creates no browser session and never replays an MCP action. Applications
/// call [`Self::begin`], route the callback to [`Self::complete`], then persist the result under
/// an application-owned tenant/user key. [`Self::refresh`] is explicit and coalesces calls that
/// observe the same token set within this flow instance; distributed stores must provide their
/// own cross-instance refresh serialization.
#[derive(Clone)]
pub struct McpOAuthAuthorizationFlow<S, E, G> {
    config: McpOAuthClientConfig,
    provider: McpOAuthAuthorizationServerMetadata,
    transactions: S,
    exchanger: E,
    generator: G,
    refresh_gates: RefreshGateRegistry,
}

impl<S, E, G> McpOAuthAuthorizationFlow<S, E, G>
where
    S: McpOAuthTransactionStore,
    E: McpOAuthTokenExchanger,
    G: McpOAuthValueGenerator,
{
    /// Creates a flow using explicitly selected, validated authorization-server metadata.
    #[must_use]
    pub fn new(
        config: McpOAuthClientConfig,
        provider: McpOAuthAuthorizationServerMetadata,
        transactions: S,
        exchanger: E,
        generator: G,
    ) -> Self {
        Self {
            config,
            provider,
            transactions,
            exchanger,
            generator,
            refresh_gates: RefreshGateRegistry::default(),
        }
    }

    /// Stores one state/PKCE transaction and returns the user-consent redirect URL.
    ///
    /// # Errors
    ///
    /// Returns a sanitized failure when the value generator is unsuitable, the fully-bound
    /// redirect exceeds its URL budget, or the transaction store is unavailable. The transaction
    /// is stored only after the redirect has been validated.
    pub async fn begin(&self) -> Result<McpOAuthAuthorizationRedirect, McpOAuthError> {
        let state = self.generator.generate();
        let code_verifier = self.generator.generate();
        if !is_valid_oauth_authorization_value(&state)
            || !is_valid_oauth_authorization_value(&code_verifier)
        {
            return Err(McpOAuthError::CallbackRejected);
        }
        let mut location = self.provider.authorization_endpoint().clone();
        let scope = self.config.scopes().collect::<Vec<_>>().join(" ");
        let mut query = location.query_pairs_mut();
        query.append_pair("response_type", "code");
        query.append_pair("client_id", self.config.client_id());
        query.append_pair("redirect_uri", self.config.redirect_uri().as_str());
        query.append_pair("resource", self.config.resource().as_str());
        if !scope.is_empty() {
            query.append_pair("scope", &scope);
        }
        query.append_pair("state", &state);
        query.append_pair("code_challenge", &pkce_challenge(&code_verifier));
        query.append_pair("code_challenge_method", "S256");
        drop(query);
        let redirect = McpOAuthAuthorizationRedirect::new(location)?;

        let transaction = McpOAuthPendingAuthorization {
            state: state.clone(),
            code_verifier,
            token_endpoint: self.provider.token_endpoint().clone(),
            resource: self.config.resource().clone(),
            expires_at_unix_seconds: unix_seconds()
                .saturating_add(self.config.transaction_ttl().as_secs()),
        };
        self.transactions
            .save(transaction)
            .await
            .map_err(|_| McpOAuthError::TransactionStoreUnavailable)?;
        Ok(redirect)
    }

    /// Atomically consumes one callback state and exchanges its code with PKCE proof.
    ///
    /// # Errors
    ///
    /// Returns a sanitized state, callback, provider, store, or token-exchange failure. A failed
    /// exchange consumes the transaction, so callers must start a new authorization flow.
    pub async fn complete(
        &self,
        callback: McpOAuthAuthorizationCallback,
    ) -> Result<McpOAuthTokenSet, McpOAuthError> {
        let state = callback.state.ok_or(McpOAuthError::CallbackRejected)?;
        if !is_valid_oauth_authorization_value(&state) {
            return Err(McpOAuthError::CallbackRejected);
        }
        let transaction = self
            .transactions
            .take(state.clone())
            .await
            .map_err(|_| McpOAuthError::TransactionStoreUnavailable)?
            .ok_or(McpOAuthError::StateRejected)?;
        if !transaction.is_valid_for(
            &state,
            self.config.resource(),
            self.provider.token_endpoint(),
        ) {
            return Err(McpOAuthError::StateRejected);
        }
        if transaction.is_expired() {
            return Err(McpOAuthError::TransactionExpired);
        }
        if callback
            .error_description
            .as_deref()
            .is_some_and(|description| description.len() > MAX_PROVIDER_ERROR_BYTES)
        {
            return Err(McpOAuthError::CallbackRejected);
        }
        if callback
            .error
            .as_deref()
            .is_some_and(|error| !is_valid_oauth_provider_error(error, MAX_PROVIDER_ERROR_BYTES))
        {
            return Err(McpOAuthError::CallbackRejected);
        }
        if callback.error.is_some() {
            return Err(McpOAuthError::ProviderRejected);
        }
        let code = callback.code.ok_or(McpOAuthError::CallbackRejected)?;
        if !is_valid_oauth_authorization_code(&code, super::MAX_AUTHORIZATION_CODE_BYTES) {
            return Err(McpOAuthError::CallbackRejected);
        }
        self.exchanger
            .exchange(
                transaction.token_endpoint,
                McpOAuthTokenExchangeRequest {
                    client_id: self.config.client_id().to_owned(),
                    code,
                    redirect_uri: self.config.redirect_uri().clone(),
                    code_verifier: transaction.code_verifier,
                    resource: transaction.resource,
                },
            )
            .await
            .map_err(|_| McpOAuthError::TokenExchangeUnavailable)
    }

    /// Persists an authorization result after checking it remains bound to this MCP resource.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::ResourceMismatch`] for another resource or
    /// [`McpOAuthError::TokenStoreUnavailable`] when the application store fails.
    pub async fn save<T>(
        &self,
        store: &T,
        key: McpOAuthTokenStoreKey,
        tokens: McpOAuthTokenSet,
    ) -> Result<(), McpOAuthError>
    where
        T: McpOAuthTokenStore,
    {
        self.ensure_resource(tokens.resource())?;
        store
            .save(key, tokens)
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)
    }

    /// Loads a current token without implicitly refreshing it.
    ///
    /// # Errors
    ///
    /// Returns [`McpOAuthError::TokenUnavailable`] when the key has no token,
    /// [`McpOAuthError::TokenExpired`] when its access token is expired,
    /// [`McpOAuthError::ResourceMismatch`] for another resource, or
    /// [`McpOAuthError::TokenStoreUnavailable`] when the application store fails.
    pub async fn load_current<T>(
        &self,
        store: &T,
        key: McpOAuthTokenStoreKey,
        now: SystemTime,
    ) -> Result<McpOAuthTokenSet, McpOAuthError>
    where
        T: McpOAuthTokenStore,
    {
        let tokens = self.load_bound_token(store, &key).await?;
        if tokens.access_token().is_expired_at(now) {
            return Err(McpOAuthError::TokenExpired);
        }
        Ok(tokens)
    }

    /// Explicitly refreshes and atomically replaces one stored token set.
    ///
    /// Calls that observe the same stored token set while a local refresh is in flight reuse its
    /// persisted replacement. A later call deliberately performs a new explicit refresh. This
    /// local coordination does not span flow instances. The method does not infer scopes, retry
    /// an MCP action, or decide whether a remote 401/403 should trigger refresh.
    ///
    /// # Errors
    ///
    /// Returns a sanitized unavailable, missing-token, missing-refresh-token, resource-mismatch,
    /// or token-exchange failure. A failed refresh preserves the stored token set.
    pub async fn refresh<T>(
        &self,
        store: &T,
        key: McpOAuthTokenStoreKey,
    ) -> Result<McpOAuthTokenSet, McpOAuthError>
    where
        T: McpOAuthTokenStore,
    {
        let observed = self.load_bound_token(store, &key).await?;
        let refresh_gate = self.refresh_gates.lease(&key)?;
        let _refresh_guard = refresh_gate.gate().lock().await;
        let current = self.load_bound_token(store, &key).await?;
        if current != observed {
            return Ok(current);
        }
        let request = current.refresh_request(self.config.client_id())?;
        let refreshed = self
            .exchanger
            .refresh(self.provider.token_endpoint().clone(), request)
            .await
            .map_err(|_| McpOAuthError::TokenExchangeUnavailable)?;
        self.ensure_resource(refreshed.resource())?;
        store
            .save(key, refreshed.clone())
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)?;
        Ok(refreshed)
    }

    /// Explicitly revokes a stored token at a published endpoint, then removes its local record.
    ///
    /// The token remains stored if remote revocation cannot be confirmed, so the application can
    /// surface a retryable disconnect outcome or deliberately remove it under its own incident
    /// policy. This method never retries an MCP action.
    ///
    /// # Errors
    ///
    /// Returns a sanitized missing-token, resource, local-store, unsupported-endpoint, or remote
    /// revocation failure. A successful remote revocation followed by a failed local delete returns
    /// [`McpOAuthError::TokenStoreUnavailable`] because the application must reconcile the record.
    pub async fn revoke_and_remove<T, R>(
        &self,
        store: &T,
        key: McpOAuthTokenStoreKey,
        revoker: &R,
    ) -> Result<(), McpOAuthError>
    where
        T: McpOAuthTokenStore,
        R: McpOAuthTokenRevoker,
    {
        let endpoint = self
            .provider
            .revocation_endpoint()
            .cloned()
            .ok_or(McpOAuthError::RevocationUnsupported)?;
        let tokens = store
            .load(key.clone())
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)?
            .ok_or(McpOAuthError::TokenUnavailable)?;
        self.ensure_resource(tokens.resource())?;
        let request = tokens.revocation_request(self.config.client_id());
        revoker
            .revoke(endpoint, request)
            .await
            .map_err(|_| McpOAuthError::RevocationUnavailable)?;
        store
            .remove(key)
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)
    }

    fn ensure_resource(&self, resource: &Url) -> Result<(), McpOAuthError> {
        (canonical_resource(resource) == canonical_resource(self.config.resource()))
            .then_some(())
            .ok_or(McpOAuthError::ResourceMismatch)
    }

    async fn load_bound_token<T>(
        &self,
        store: &T,
        key: &McpOAuthTokenStoreKey,
    ) -> Result<McpOAuthTokenSet, McpOAuthError>
    where
        T: McpOAuthTokenStore,
    {
        let tokens = store
            .load(key.clone())
            .await
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)?
            .ok_or(McpOAuthError::TokenUnavailable)?;
        self.ensure_resource(tokens.resource())?;
        Ok(tokens)
    }
}

impl<S, E, G> fmt::Debug for McpOAuthAuthorizationFlow<S, E, G> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthAuthorizationFlow")
            .field("config", &self.config)
            .field("provider", &self.provider)
            .field("transaction_store", &std::any::type_name::<S>())
            .field("exchanger", &std::any::type_name::<E>())
            .finish_non_exhaustive()
    }
}
