//! Browser-login transaction-store and callback-admission regression coverage.

use super::*;

#[tokio::test]
async fn in_memory_store_has_atomic_take_semantics() {
    let store = InMemoryAuthorizationTransactionStore::default();
    let transaction = super::PendingAuthorization {
        state: "s".repeat(43),
        nonce: "n".repeat(43),
        code_verifier: "v".repeat(43),
        token_endpoint: Url::parse(TOKEN_ENDPOINT).expect("URL must parse"),
        expires_at_unix_seconds: super::unix_seconds() + 60,
    };
    store.save(transaction).await.expect("save must work");
    let first = store.take("s".repeat(43)).await.expect("take must work");
    let second = store.take("s".repeat(43)).await.expect("take must work");

    assert!(first.is_some());
    assert!(second.is_none());
}

#[tokio::test]
async fn poisoned_transaction_store_fails_closed_before_token_exchange() {
    let exchanger = RecordingExchanger::default();
    let login = login(exchanger.clone(), RecordingVerifier::default());
    login.begin().await.expect("login start must succeed");
    let transactions = Arc::clone(&login.transactions.transactions);
    let poison = std::thread::spawn(move || {
        let _guard = transactions
            .lock()
            .expect("new authorization transaction lock must be available");
        panic!("test must poison the authorization transaction store lock");
    });
    assert!(poison.join().is_err());

    let error = login
        .complete(AuthorizationCallback {
            code: Some("one-time-code".to_owned()),
            state: Some("s".repeat(43)),
            error: None,
            error_description: None,
        })
        .await
        .unwrap_err();

    assert_eq!(error, OidcLoginError::TransactionStoreUnavailable);
    assert_eq!(exchanger.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn malformed_state_is_rejected_before_transaction_store_access() {
    let exchanger = RecordingExchanger::default();
    let login = login(exchanger.clone(), RecordingVerifier::default());
    login.begin().await.expect("login start must succeed");
    let transactions = Arc::clone(&login.transactions.transactions);
    let poison = std::thread::spawn(move || {
        let _guard = transactions
            .lock()
            .expect("new authorization transaction lock must be available");
        panic!("test must poison the authorization transaction store lock");
    });
    assert!(poison.join().is_err());

    let error = login
        .complete(AuthorizationCallback {
            code: Some("one-time-code".to_owned()),
            state: Some("not-a-valid-authorization-state".to_owned()),
            error: None,
            error_description: None,
        })
        .await
        .unwrap_err();

    assert_eq!(error, OidcLoginError::CallbackRejected);
    assert_eq!(exchanger.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mismatched_durable_transaction_state_is_rejected_before_token_exchange() {
    let exchanger = RecordingExchanger::default();
    let login = login(exchanger.clone(), RecordingVerifier::default());
    login.begin().await.expect("login start must succeed");
    let callback_state = "s".repeat(43);
    let stored_state = "x".repeat(43);
    {
        let mut transactions = login
            .transactions
            .transactions
            .lock()
            .expect("test transaction lock must be available");
        let mut transaction = transactions
            .remove(&callback_state)
            .expect("test transaction must be present");
        transaction.state = stored_state;
        transactions.insert(callback_state.clone(), transaction);
    }

    let error = login
        .complete(AuthorizationCallback {
            code: Some("one-time-code".to_owned()),
            state: Some(callback_state),
            error: None,
            error_description: None,
        })
        .await
        .unwrap_err();

    assert_eq!(error, OidcLoginError::StateRejected);
    assert_eq!(exchanger.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn malformed_callback_values_are_rejected_before_token_exchange() {
    let callbacks = [
        AuthorizationCallback {
            code: Some("x".repeat(MAX_AUTHORIZATION_CODE_BYTES + 1)),
            state: Some("s".repeat(43)),
            error: None,
            error_description: None,
        },
        AuthorizationCallback {
            code: Some("one-time-code".to_owned()),
            state: Some("s".repeat(43)),
            error: Some("access denied".to_owned()),
            error_description: None,
        },
        AuthorizationCallback {
            code: None,
            state: Some("s".repeat(43)),
            error: Some("access_denied".to_owned()),
            error_description: Some("x".repeat(MAX_PROVIDER_ERROR_BYTES + 1)),
        },
    ];

    for callback in callbacks {
        let exchanger = RecordingExchanger::default();
        let login = login(exchanger.clone(), RecordingVerifier::default());
        login.begin().await.expect("login start must succeed");

        let error = login.complete(callback).await.unwrap_err();

        assert_eq!(error, OidcLoginError::CallbackRejected);
        assert_eq!(exchanger.calls.load(Ordering::SeqCst), 0);
    }
}
