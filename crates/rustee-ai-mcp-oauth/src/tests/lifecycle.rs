//! Authorization, refresh, and revocation lifecycle regression coverage.

use super::*;

#[derive(Clone, Default)]
struct BlockingRefreshExchanger {
    refresh_started: Arc<tokio::sync::Notify>,
    release_refresh: Arc<tokio::sync::Notify>,
    refresh_calls: Arc<AtomicUsize>,
}

impl McpOAuthTokenExchanger for BlockingRefreshExchanger {
    type Error = TestError;

    fn exchange(
        &self,
        _: Url,
        _: McpOAuthTokenExchangeRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
        Box::pin(async { Err(TestError) })
    }

    fn refresh(
        &self,
        endpoint: Url,
        request: McpOAuthRefreshRequest,
    ) -> BoxFuture<'static, Result<McpOAuthTokenSet, Self::Error>> {
        let refresh_started = Arc::clone(&self.refresh_started);
        let release_refresh = Arc::clone(&self.release_refresh);
        let refresh_calls = Arc::clone(&self.refresh_calls);
        Box::pin(async move {
            assert_eq!(endpoint.as_str(), TOKEN_ENDPOINT);
            let resource = request.resource().clone();
            let call = refresh_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                refresh_started.notify_one();
                release_refresh.notified().await;
            }
            token_set(
                resource,
                "refreshed-access-token",
                Some("rotated-refresh-token".to_owned()),
            )
        })
    }
}

#[tokio::test]
async fn pkce_redirect_binds_resource_and_callback_state_is_single_use() {
    let exchanger = RecordingExchanger::default();
    let flow = flow(exchanger.clone());

    let redirect = flow.begin().await.expect("authorization must begin");
    let pairs = redirect
        .location()
        .query_pairs()
        .into_owned()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        redirect.location().as_str().split('?').next(),
        Some(AUTHORIZATION_ENDPOINT)
    );
    assert_eq!(pairs.get("response_type"), Some(&"code".to_owned()));
    assert_eq!(pairs.get("client_id"), Some(&CLIENT_ID.to_owned()));
    assert_eq!(pairs.get("redirect_uri"), Some(&REDIRECT_URI.to_owned()));
    assert_eq!(pairs.get("resource"), Some(&RESOURCE.to_owned()));
    assert_eq!(pairs.get("scope"), Some(&"orders:read".to_owned()));
    assert_eq!(pairs.get("state"), Some(&"s".repeat(43)));
    assert_eq!(pairs.get("code_challenge_method"), Some(&"S256".to_owned()));
    assert_eq!(
        pairs.get("code_challenge"),
        Some(&pkce_challenge(&"v".repeat(43)))
    );

    let tokens = flow
        .complete(McpOAuthAuthorizationCallback {
            code: Some("one-time-code".to_owned()),
            state: Some("s".repeat(43)),
            error: None,
            error_description: None,
        })
        .await
        .expect("valid callback must exchange a code");
    assert!(tokens.has_refresh_token());
    assert!(!format!("{tokens:?}").contains("initial-access-token"));
    let requests = exchanger.exchange_requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].code(), "one-time-code");
    assert_eq!(requests[0].code_verifier(), "v".repeat(43));
    assert_eq!(requests[0].resource().as_str(), RESOURCE);
    drop(requests);

    let replay = flow
        .complete(McpOAuthAuthorizationCallback {
            code: Some("one-time-code".to_owned()),
            state: Some("s".repeat(43)),
            error: None,
            error_description: None,
        })
        .await;
    assert_eq!(replay.unwrap_err(), McpOAuthError::StateRejected);
    assert_eq!(exchanger.exchange_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn mismatched_durable_transaction_binding_is_rejected_before_token_exchange() {
    let callback_state = "s".repeat(43);
    for transaction in [
        McpOAuthPendingAuthorization {
            state: "x".repeat(43),
            code_verifier: "v".repeat(43),
            token_endpoint: Url::parse(TOKEN_ENDPOINT).expect("test token endpoint must parse"),
            resource: Url::parse(RESOURCE).expect("test resource must parse"),
            expires_at_unix_seconds: u64::MAX,
        },
        McpOAuthPendingAuthorization {
            state: callback_state.clone(),
            code_verifier: "v".repeat(43),
            token_endpoint: Url::parse("https://other-auth.example.test/token")
                .expect("test token endpoint must parse"),
            resource: Url::parse(RESOURCE).expect("test resource must parse"),
            expires_at_unix_seconds: u64::MAX,
        },
        McpOAuthPendingAuthorization {
            state: callback_state.clone(),
            code_verifier: "v".repeat(43),
            token_endpoint: Url::parse(TOKEN_ENDPOINT).expect("test token endpoint must parse"),
            resource: Url::parse("https://other-mcp.example.test/mcp")
                .expect("test resource must parse"),
            expires_at_unix_seconds: u64::MAX,
        },
    ] {
        let exchanger = RecordingExchanger::default();
        let flow =
            flow_with_transactions(FixedTransactionStore::new(transaction), exchanger.clone());

        let error = flow
            .complete(McpOAuthAuthorizationCallback {
                code: Some("one-time-code".to_owned()),
                state: Some(callback_state.clone()),
                error: None,
                error_description: None,
            })
            .await
            .unwrap_err();

        assert_eq!(error, McpOAuthError::StateRejected);
        assert_eq!(exchanger.exchange_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn provider_rejection_consumes_state_without_exchanging_a_code() {
    let exchanger = RecordingExchanger::default();
    let flow = flow(exchanger.clone());
    flow.begin().await.expect("authorization must begin");

    let result = flow
        .complete(McpOAuthAuthorizationCallback {
            code: None,
            state: Some("s".repeat(43)),
            error: Some("access_denied".to_owned()),
            error_description: Some("provider-only diagnostic".to_owned()),
        })
        .await;
    assert_eq!(result.unwrap_err(), McpOAuthError::ProviderRejected);
    assert_eq!(exchanger.exchange_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn token_refresh_is_explicit_resource_bound_and_replaces_the_stored_set() {
    let exchanger = RecordingExchanger::default();
    let flow = flow(exchanger.clone());
    let store = InMemoryMcpOAuthTokenStore::default();
    let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:connection-a")
        .expect("test token-store key must be valid");
    let initial = token_set(
        Url::parse(RESOURCE).unwrap(),
        "old-access-token",
        Some("old-refresh-token".to_owned()),
    )
    .unwrap();
    flow.save(&store, key.clone(), initial)
        .await
        .expect("initial token must be persisted");

    let refreshed = flow
        .refresh(&store, key.clone())
        .await
        .expect("explicit refresh must succeed");
    assert!(refreshed.has_refresh_token());
    let persisted = store
        .load(key)
        .await
        .expect("local store must load")
        .expect("token must remain stored");
    let secrets = persisted.into_secrets();
    assert_eq!(
        secrets.access_token_for_encryption(),
        "refreshed-access-token"
    );
    assert_eq!(
        secrets.refresh_token_for_encryption(),
        Some("rotated-refresh-token")
    );
    let refresh_requests = exchanger.refresh_requests.lock().await;
    assert_eq!(refresh_requests.len(), 1);
    assert_eq!(refresh_requests[0].refresh_token(), "old-refresh-token");
    assert_eq!(refresh_requests[0].resource().as_str(), RESOURCE);
}

#[tokio::test]
async fn concurrent_refreshes_that_observe_one_token_set_share_the_first_replacement() {
    let exchanger = BlockingRefreshExchanger::default();
    let flow = McpOAuthAuthorizationFlow::new(
        config(),
        provider(),
        InMemoryMcpOAuthTransactionStore::default(),
        exchanger.clone(),
        SequenceGenerator::new(["s".repeat(43), "v".repeat(43)]),
    );
    let store = InMemoryMcpOAuthTokenStore::default();
    let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:concurrent")
        .expect("test token-store key must be valid");
    flow.save(
        &store,
        key.clone(),
        token_set(
            Url::parse(RESOURCE).expect("test resource URL must parse"),
            "old-access-token",
            Some("old-refresh-token".to_owned()),
        )
        .expect("test token set must be valid"),
    )
    .await
    .expect("initial token must be persisted");

    let first_flow = flow.clone();
    let first_store = store.clone();
    let first_key = key.clone();
    let first = tokio::spawn(async move { first_flow.refresh(&first_store, first_key).await });
    exchanger.refresh_started.notified().await;

    let second_flow = flow.clone();
    let second_store = store.clone();
    let second_key = key.clone();
    let second_entered = Arc::new(tokio::sync::Notify::new());
    let second_entered_in_task = Arc::clone(&second_entered);
    let second = tokio::spawn(async move {
        second_entered_in_task.notify_one();
        second_flow.refresh(&second_store, second_key).await
    });
    second_entered.notified().await;
    exchanger.release_refresh.notify_one();

    let first = first
        .await
        .expect("first refresh task must complete")
        .expect("first refresh must succeed");
    let second = second
        .await
        .expect("second refresh task must complete")
        .expect("second refresh must succeed");
    assert_eq!(first, second);
    assert_eq!(exchanger.refresh_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.load(key).await.expect("local store must load"),
        Some(first)
    );
}

#[tokio::test]
async fn refreshes_for_independent_token_store_keys_do_not_share_a_gate() {
    let exchanger = BlockingRefreshExchanger::default();
    let flow = McpOAuthAuthorizationFlow::new(
        config(),
        provider(),
        InMemoryMcpOAuthTransactionStore::default(),
        exchanger.clone(),
        SequenceGenerator::new(["s".repeat(43), "v".repeat(43)]),
    );
    let store = InMemoryMcpOAuthTokenStore::default();
    let first_key = McpOAuthTokenStoreKey::new("tenant-a:user-a:one")
        .expect("first test token-store key must be valid");
    let second_key = McpOAuthTokenStoreKey::new("tenant-a:user-a:two")
        .expect("second test token-store key must be valid");
    for key in [&first_key, &second_key] {
        flow.save(
            &store,
            key.clone(),
            token_set(
                Url::parse(RESOURCE).expect("test resource URL must parse"),
                "old-access-token",
                Some("old-refresh-token".to_owned()),
            )
            .expect("test token set must be valid"),
        )
        .await
        .expect("initial token must be persisted");
    }

    let first_flow = flow.clone();
    let first_store = store.clone();
    let first = tokio::spawn(async move { first_flow.refresh(&first_store, first_key).await });
    exchanger.refresh_started.notified().await;

    let second_flow = flow.clone();
    let second_store = store.clone();
    let second = tokio::spawn(async move { second_flow.refresh(&second_store, second_key).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), second)
        .await
        .expect("an independent token-store key must not wait for the first refresh")
        .expect("second refresh task must complete")
        .expect("second refresh must succeed");

    assert_eq!(exchanger.refresh_calls.load(Ordering::SeqCst), 2);
    exchanger.release_refresh.notify_one();
    first
        .await
        .expect("first refresh task must complete")
        .expect("first refresh must succeed");
}

#[tokio::test]
async fn expired_tokens_do_not_trigger_an_implicit_refresh() {
    let exchanger = RecordingExchanger::default();
    let flow = flow(exchanger.clone());
    let store = InMemoryMcpOAuthTokenStore::default();
    let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:expired")
        .expect("test token-store key must be valid");
    let expired_access_token =
        McpOAuthAccessToken::new("expired-access-token", Some(SystemTime::UNIX_EPOCH)).unwrap();
    let expired = McpOAuthTokenSet::new(
        Url::parse(RESOURCE).unwrap(),
        expired_access_token,
        Some("refresh-token".to_owned()),
    )
    .unwrap();
    flow.save(&store, key.clone(), expired)
        .await
        .expect("expired token may be stored");

    assert_eq!(
        flow.load_current(&store, key.clone(), SystemTime::now())
            .await
            .unwrap_err(),
        McpOAuthError::TokenExpired
    );
    assert_eq!(exchanger.refresh_calls.load(Ordering::SeqCst), 0);

    flow.refresh(&store, key)
        .await
        .expect("application-selected refresh must succeed");
    assert_eq!(exchanger.refresh_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn explicit_revocation_prefers_refresh_token_and_removes_only_after_success() {
    let flow = McpOAuthAuthorizationFlow::new(
        config(),
        provider_with_revocation(),
        InMemoryMcpOAuthTransactionStore::default(),
        RecordingExchanger::default(),
        SequenceGenerator::new(Vec::new()),
    );
    let store = InMemoryMcpOAuthTokenStore::default();
    let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:disconnect")
        .expect("test token-store key must be valid");
    let tokens = token_set(
        Url::parse(RESOURCE).unwrap(),
        "disconnect-access-token",
        Some("disconnect-refresh-token".to_owned()),
    )
    .unwrap();
    flow.save(&store, key.clone(), tokens)
        .await
        .expect("token must be stored before revocation");
    let revoker = RecordingRevoker::default();

    flow.revoke_and_remove(&store, key.clone(), &revoker)
        .await
        .expect("successful revocation must remove the local record");
    assert!(store.load(key).await.unwrap().is_none());
    assert_eq!(revoker.calls.load(Ordering::SeqCst), 1);
    let requests = revoker.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].token(), "disconnect-refresh-token");
    assert_eq!(
        requests[0].token_type_hint(),
        McpOAuthRevocationTokenType::RefreshToken
    );
    assert_eq!(requests[0].resource().as_str(), RESOURCE);
}

#[tokio::test]
async fn absent_revocation_endpoint_leaves_the_stored_token_untouched() {
    let flow = flow(RecordingExchanger::default());
    let store = InMemoryMcpOAuthTokenStore::default();
    let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:no-revoke")
        .expect("test token-store key must be valid");
    flow.save(
        &store,
        key.clone(),
        token_set(
            Url::parse(RESOURCE).unwrap(),
            "access-token",
            Some("refresh-token".to_owned()),
        )
        .unwrap(),
    )
    .await
    .expect("token must be stored");

    assert_eq!(
        flow.revoke_and_remove(&store, key.clone(), &RecordingRevoker::default())
            .await
            .unwrap_err(),
        McpOAuthError::RevocationUnsupported
    );
    assert!(store.load(key).await.unwrap().is_some());
}

#[test]
fn uuid_generator_creates_pkce_safe_values() {
    let value = UuidMcpOAuthValueGenerator.generate();
    assert!((43..=128).contains(&value.len()));
    assert!(value.bytes().all(|byte| byte.is_ascii_alphanumeric()));
}

#[tokio::test]
async fn oversized_authorization_redirect_does_not_store_a_transaction() {
    let mut config = McpOAuthClientConfig::new(
        Url::parse(RESOURCE).unwrap(),
        CLIENT_ID,
        Url::parse(REDIRECT_URI).unwrap(),
    )
    .unwrap();
    for index in 0..crate::config::MAX_SCOPES {
        let prefix = format!("{index:02}-");
        config = config
            .with_scope(format!(
                "{prefix}{}",
                "s".repeat(crate::config::MAX_SCOPE_BYTES - prefix.len())
            ))
            .unwrap();
    }
    let transactions = InMemoryMcpOAuthTransactionStore::default();
    let flow = McpOAuthAuthorizationFlow::new(
        config,
        provider(),
        transactions.clone(),
        RecordingExchanger::default(),
        SequenceGenerator::new(["s".repeat(43), "v".repeat(43)]),
    );

    assert_eq!(
        flow.begin().await.unwrap_err(),
        McpOAuthError::AuthorizationRedirectTooLong
    );
    assert!(
        transactions
            .take("s".repeat(43))
            .await
            .expect("test transaction store must remain available")
            .is_none()
    );
}
