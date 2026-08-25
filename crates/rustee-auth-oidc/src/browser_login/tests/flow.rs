//! PKCE browser-login initiation, completion, and session regression coverage.

use super::*;

#[tokio::test]
async fn begins_pkce_login_and_consumes_state_before_token_exchange() {
    let exchanger = RecordingExchanger::default();
    let verifier = RecordingVerifier::default();
    let login = login(exchanger.clone(), verifier.clone());

    let redirect = login.begin().await.expect("login start must succeed");
    let pairs = redirect
        .location()
        .query_pairs()
        .into_owned()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(redirect.location().origin().ascii_serialization(), ISSUER);
    assert_eq!(pairs.get("response_type"), Some(&"code".to_owned()));
    assert_eq!(pairs.get("client_id"), Some(&CLIENT_ID.to_owned()));
    assert_eq!(pairs.get("redirect_uri"), Some(&REDIRECT_URI.to_owned()));
    assert_eq!(pairs.get("scope"), Some(&"openid profile".to_owned()));
    assert_eq!(pairs.get("state"), Some(&"s".repeat(43)));
    assert_eq!(pairs.get("nonce"), Some(&"n".repeat(43)));
    assert_eq!(pairs.get("code_challenge_method"), Some(&"S256".to_owned()));
    assert_eq!(
        pairs.get("code_challenge"),
        Some(&pkce_challenge(&"v".repeat(43)))
    );
    let redirect_response = redirect.clone().into_response();
    assert_eq!(redirect_response.status(), StatusCode::FOUND);
    assert_eq!(
        redirect_response
            .headers()
            .get(LOCATION)
            .expect("redirect must have Location")
            .to_str()
            .expect("location must be ASCII"),
        redirect.location().as_str()
    );

    let result = login
        .complete(AuthorizationCallback {
            code: Some("one-time-code".to_owned()),
            state: Some("s".repeat(43)),
            error: None,
            error_description: None,
        })
        .await
        .expect("valid callback must establish a principal");
    assert_eq!(result.principal().subject(), "alice");
    assert_eq!(exchanger.calls.load(Ordering::SeqCst), 1);
    assert_eq!(verifier.nonces.lock().await.as_slice(), &["n".repeat(43)]);

    let replay = login
        .complete(AuthorizationCallback {
            code: Some("one-time-code".to_owned()),
            state: Some("s".repeat(43)),
            error: None,
            error_description: None,
        })
        .await;
    assert_eq!(replay.unwrap_err(), OidcLoginError::StateRejected);
    assert_eq!(exchanger.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oversized_id_token_is_rejected_before_verifier_invocation() {
    let verifier = RecordingVerifier::default();
    let login = OidcBrowserLogin::new(
        config(),
        InMemoryAuthorizationTransactionStore::default(),
        StaticDiscovery(provider()),
        OversizedIdTokenExchanger,
        verifier.clone(),
        SequenceGenerator::new(["s".repeat(43), "n".repeat(43), "v".repeat(43)]),
    );
    login.begin().await.expect("login start must succeed");

    let error = login
        .complete(AuthorizationCallback {
            code: Some("one-time-code".to_owned()),
            state: Some("s".repeat(43)),
            error: None,
            error_description: None,
        })
        .await
        .unwrap_err();

    assert_eq!(error, OidcLoginError::IdentityTokenRejected);
    assert!(verifier.nonces.lock().await.is_empty());
}

#[tokio::test]
async fn oversized_redirect_is_rejected_before_transaction_persistence() {
    let mut config = OidcBrowserConfig::new(
        Url::parse(ISSUER).expect("test issuer URL must parse"),
        CLIENT_ID,
        Url::parse(REDIRECT_URI).expect("test redirect URL must parse"),
        Url::parse(JWKS_URL).expect("test JWKS URL must parse"),
        OidcClientAuthentication::None,
    )
    .expect("test configuration must be valid");
    for index in 0..(MAX_SCOPES - 1) {
        config = config
            .with_scope(format!("s{index:02}-{}", "x".repeat(MAX_SCOPE_BYTES - 4)))
            .expect("bounded scope must be accepted");
    }

    let state = "s".repeat(43);
    let transactions = InMemoryAuthorizationTransactionStore::default();
    let login = OidcBrowserLogin::new(
        config,
        transactions.clone(),
        StaticDiscovery(provider()),
        RecordingExchanger::default(),
        RecordingVerifier::default(),
        SequenceGenerator::new([state.clone(), "n".repeat(43), "v".repeat(43)]),
    );

    assert_eq!(
        login.begin().await,
        Err(OidcLoginError::InvalidProviderMetadata)
    );
    assert!(
        transactions
            .take(state)
            .await
            .expect("transaction lookup must succeed")
            .is_none()
    );
}

#[tokio::test]
async fn provider_error_consumes_a_valid_state_without_exchanging_a_code() {
    let exchanger = RecordingExchanger::default();
    let login = login(exchanger.clone(), RecordingVerifier::default());
    login.begin().await.expect("login start must succeed");

    let rejected = login
        .complete(AuthorizationCallback {
            code: None,
            state: Some("s".repeat(43)),
            error: Some("access_denied".to_owned()),
            error_description: Some("raw provider text".to_owned()),
        })
        .await;

    assert_eq!(rejected.unwrap_err(), OidcLoginError::ProviderRejected);
    assert_eq!(exchanger.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn complete_session_issues_only_the_opaque_session_cookie() {
    let login = login(RecordingExchanger::default(), RecordingVerifier::default());
    let store = InMemorySessionStore::default();
    let sessions = SessionManager::new(
        store,
        SessionCookieConfig::new("rustee_session", 60)
            .expect("test cookie configuration must be valid"),
    );
    login.begin().await.expect("login start must succeed");

    let issued = login
        .complete_session(
            AuthorizationCallback {
                code: Some("one-time-code".to_owned()),
                state: Some("s".repeat(43)),
                error: None,
                error_description: None,
            },
            &sessions,
        )
        .await
        .expect("verified OIDC callback must create a browser session");
    let mut response = StatusCode::NO_CONTENT.into_response();
    issued.apply_to(&mut response);
    let cookie = response
        .headers()
        .get(SET_COOKIE)
        .expect("session must set a cookie")
        .to_str()
        .expect("cookie header must be valid");

    assert!(cookie.contains("HttpOnly"));
    assert!(cookie.contains("Secure"));
    assert!(!cookie.contains("signed-id-token"));
}
