//! Browser-login orchestration.

use rustee_auth::AuthError;
use rustee_auth_session::{IssuedSession, SessionManager, SessionStore};
use rustee_core::{
    is_valid_oauth_authorization_code, is_valid_oauth_authorization_value,
    is_valid_oauth_provider_error,
};

use crate::IdTokenVerifier;

use super::{
    AuthorizationCallback, AuthorizationRedirect, AuthorizationTransactionStore,
    AuthorizationValueGenerator, MAX_AUTHORIZATION_CODE_BYTES, MAX_ID_TOKEN_BYTES,
    MAX_PROVIDER_ERROR_BYTES, OidcBrowserLogin, OidcDiscovery, OidcLoginError, OidcLoginResult,
    OidcProviderMetadata, OidcTokenExchangeRequest, OidcTokenExchanger, PendingAuthorization,
    pkce_challenge, unix_seconds,
};

impl<S, D, E, V, G> OidcBrowserLogin<S, D, E, V, G>
where
    S: AuthorizationTransactionStore,
    D: OidcDiscovery,
    E: OidcTokenExchanger,
    V: IdTokenVerifier,
    G: AuthorizationValueGenerator,
{
    /// Creates a browser-login orchestrator from explicit provider, store, exchange, and verifier
    /// capabilities.
    #[must_use]
    pub fn new(
        config: super::OidcBrowserConfig,
        transactions: S,
        discovery: D,
        exchanger: E,
        verifier: V,
        generator: G,
    ) -> Self {
        Self {
            config,
            transactions,
            discovery,
            exchanger,
            verifier,
            generator,
        }
    }

    /// Loads trusted discovery metadata, persists a one-time transaction, and builds the provider
    /// authorization redirect.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error when discovery metadata is unavailable or untrusted, when the
    /// transaction store is unavailable, or when the configured value generator is invalid.
    pub async fn begin(&self) -> Result<AuthorizationRedirect, OidcLoginError> {
        let provider = self.discover_provider().await?;
        let state = self.generator.generate();
        let nonce = self.generator.generate();
        let code_verifier = self.generator.generate();
        if !is_valid_oauth_authorization_value(&state)
            || !is_valid_oauth_authorization_value(&nonce)
            || !is_valid_oauth_authorization_value(&code_verifier)
        {
            return Err(OidcLoginError::CallbackRejected);
        }

        let mut location = provider.authorization_endpoint;
        let scope = self.config.scopes().collect::<Vec<_>>().join(" ");
        location.query_pairs_mut().extend_pairs([
            ("response_type", "code"),
            ("client_id", self.config.client_id()),
            ("redirect_uri", self.config.redirect_uri().as_str()),
            ("scope", &scope),
            ("state", &state),
            ("nonce", &nonce),
            ("code_challenge", &pkce_challenge(&code_verifier)),
            ("code_challenge_method", "S256"),
        ]);
        let redirect = AuthorizationRedirect::new(location)?;

        let transaction = PendingAuthorization {
            state: state.clone(),
            nonce: nonce.clone(),
            code_verifier: code_verifier.clone(),
            token_endpoint: provider.token_endpoint,
            expires_at_unix_seconds: unix_seconds()
                .saturating_add(self.config.transaction_ttl().as_secs()),
        };
        self.transactions
            .save(transaction)
            .await
            .map_err(|_| OidcLoginError::TransactionStoreUnavailable)?;

        Ok(redirect)
    }

    /// Atomically consumes one callback state, exchanges its code, and validates the ID-token
    /// nonce before returning a verified principal.
    ///
    /// # Errors
    ///
    /// Returns a sanitized error for an invalid, replayed, expired, or provider-rejected callback;
    /// token-exchange/provider availability failure; or invalid ID token and nonce binding.
    pub async fn complete(
        &self,
        callback: AuthorizationCallback,
    ) -> Result<OidcLoginResult, OidcLoginError> {
        let state = callback.state.ok_or(OidcLoginError::CallbackRejected)?;
        if !is_valid_oauth_authorization_value(&state) {
            return Err(OidcLoginError::CallbackRejected);
        }
        let transaction = self
            .transactions
            .take(state.clone())
            .await
            .map_err(|_| OidcLoginError::TransactionStoreUnavailable)?
            .ok_or(OidcLoginError::StateRejected)?;
        if !transaction.is_valid_for_state(&state) {
            return Err(OidcLoginError::StateRejected);
        }
        if transaction.is_expired() {
            return Err(OidcLoginError::TransactionExpired);
        }
        if callback
            .error_description
            .as_deref()
            .is_some_and(|description| description.len() > MAX_PROVIDER_ERROR_BYTES)
        {
            return Err(OidcLoginError::CallbackRejected);
        }
        if callback
            .error
            .as_deref()
            .is_some_and(|error| !is_valid_oauth_provider_error(error, MAX_PROVIDER_ERROR_BYTES))
        {
            return Err(OidcLoginError::CallbackRejected);
        }
        if callback.error.is_some() {
            return Err(OidcLoginError::ProviderRejected);
        }
        let code = callback.code.ok_or(OidcLoginError::CallbackRejected)?;
        if !is_valid_oauth_authorization_code(&code, MAX_AUTHORIZATION_CODE_BYTES) {
            return Err(OidcLoginError::CallbackRejected);
        }
        let token_response = self
            .exchanger
            .exchange(
                transaction.token_endpoint,
                OidcTokenExchangeRequest {
                    client_id: self.config.client_id().to_owned(),
                    authentication: self.config.authentication().clone(),
                    code,
                    redirect_uri: self.config.redirect_uri().clone(),
                    code_verifier: transaction.code_verifier,
                },
            )
            .await
            .map_err(|_| OidcLoginError::TokenExchangeUnavailable)?;
        let id_token = token_response
            .into_id_token()
            .ok_or(OidcLoginError::MissingIdToken)?;
        if id_token.len() > MAX_ID_TOKEN_BYTES {
            return Err(OidcLoginError::IdentityTokenRejected);
        }
        let principal = self
            .verifier
            .verify_id_token(&id_token, &transaction.nonce)
            .await
            .map_err(map_id_token_error)?;
        Ok(OidcLoginResult { principal })
    }

    /// Completes a verified callback and establishes a new opaque Rustee browser session.
    ///
    /// The caller applies the returned [`IssuedSession`] to its chosen same-origin success
    /// response; Rustee never accepts a callback-controlled post-login redirect target.
    ///
    /// # Errors
    ///
    /// Returns the same sanitized failures as [`Self::complete`] or
    /// [`OidcLoginError::SessionUnavailable`] when the session store cannot persist the verified
    /// principal.
    pub async fn complete_session<SS>(
        &self,
        callback: AuthorizationCallback,
        sessions: &SessionManager<SS>,
    ) -> Result<IssuedSession, OidcLoginError>
    where
        SS: SessionStore,
    {
        let principal = self.complete(callback).await?.into_principal();
        sessions
            .establish(principal)
            .await
            .map_err(|_| OidcLoginError::SessionUnavailable)
    }

    async fn discover_provider(&self) -> Result<OidcProviderMetadata, OidcLoginError> {
        let provider = self
            .discovery
            .discover(self.config.issuer().clone())
            .await
            .map_err(|_| OidcLoginError::DiscoveryUnavailable)?;
        provider.validate(&self.config)?;
        Ok(provider)
    }
}

fn map_id_token_error(error: AuthError) -> OidcLoginError {
    match error {
        AuthError::ProviderUnavailable => OidcLoginError::IdentityProviderUnavailable,
        AuthError::MissingBearerToken
        | AuthError::InvalidBearerToken
        | AuthError::RejectedBearerToken => OidcLoginError::IdentityTokenRejected,
    }
}
