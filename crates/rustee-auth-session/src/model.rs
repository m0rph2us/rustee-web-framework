//! Stable session model facade, cookie policy, storage contract, and local store.

mod cookie;
mod in_memory;
mod manager;
mod session;

pub use cookie::{
    CookieConfigError, IssuedSession, MAX_COOKIE_NAME_BYTES, SameSite, SessionCookieConfig,
};
pub use in_memory::{InMemorySessionStore, InMemorySessionStoreError};
pub use manager::{SessionManager, SessionStore};
pub use session::{Session, SessionId};

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, fmt};

    use futures_util::future::BoxFuture;
    use http::{HeaderValue, Request as HttpRequest, StatusCode, header::SET_COOKIE};
    use rustee_auth::Principal;
    use rustee_core::{empty_body, response};
    use rustee_router::App;
    use tower::{Layer, ServiceExt};
    use uuid::Uuid;

    use super::{
        InMemorySessionStore, InMemorySessionStoreError, IssuedSession, Session,
        SessionCookieConfig, SessionId, SessionManager, SessionStore,
    };

    #[derive(Clone)]
    struct LeakyDiagnosticStore;

    impl fmt::Debug for LeakyDiagnosticStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("LeakyDiagnosticStore(private-store-credential)")
        }
    }

    impl SessionStore for LeakyDiagnosticStore {
        type Error = Infallible;

        fn save(&self, _session: super::Session) -> BoxFuture<'static, Result<(), Self::Error>> {
            Box::pin(async { Ok(()) })
        }

        fn load(
            &self,
            _id: super::SessionId,
        ) -> BoxFuture<'static, Result<Option<super::Session>, Self::Error>> {
            Box::pin(async { Ok(None) })
        }

        fn delete(&self, _id: super::SessionId) -> BoxFuture<'static, Result<(), Self::Error>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn cookie(issued: &IssuedSession) -> HeaderValue {
        let mut response = response(StatusCode::NO_CONTENT, empty_body());
        issued.apply_to(&mut response);
        response
            .headers()
            .get(SET_COOKIE)
            .expect("issued session must add a cookie")
            .clone()
    }

    #[tokio::test]
    async fn poisoned_in_memory_store_returns_the_fail_closed_service_response() {
        let store = InMemorySessionStore::default();
        let cookie_config = SessionCookieConfig::new("rustee_session", 60)
            .unwrap()
            .with_secure(false)
            .unwrap();
        let issued = SessionManager::new(store.clone(), cookie_config.clone())
            .establish(Principal::new("alice").unwrap())
            .await
            .unwrap();
        let session_cookie = cookie(&issued);
        let sessions = std::sync::Arc::clone(&store.sessions);
        let poison = std::thread::spawn(move || {
            let _guard = sessions.lock().expect("new session lock must be available");
            panic!("test must poison the in-memory session store lock");
        });
        assert!(poison.join().is_err());

        let service = crate::SessionLayer::new(store, cookie_config)
            .layer(App::new().get("/profile", || async { "not reached" }));
        let response = service
            .oneshot(
                HttpRequest::builder()
                    .uri("/profile")
                    .header("cookie", session_cookie)
                    .body(empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn in_memory_store_prunes_expired_sessions_and_bounds_live_capacity() {
        let store = InMemorySessionStore::default();
        let expired_id = SessionId::new();
        {
            let mut sessions = store
                .sessions
                .lock()
                .expect("new session lock must be available");
            sessions.insert(expired_id, session(expired_id, 0));
        }

        assert!(store.load(expired_id).await.unwrap().is_none());
        assert!(
            store
                .sessions
                .lock()
                .expect("new session lock must be available")
                .is_empty()
        );
        assert_eq!(
            store.save(session(SessionId::new(), 0)).await,
            Err(InMemorySessionStoreError::ExpiredSession)
        );

        let stale_id = SessionId::new();
        {
            let mut sessions = store
                .sessions
                .lock()
                .expect("new session lock must be available");
            sessions.insert(stale_id, session(stale_id, 0));
        }
        let active = session(SessionId::new(), u64::MAX);
        store
            .save(active.clone())
            .await
            .expect("saving an active session must prune stale state first");
        assert_eq!(store.load(active.id()).await.unwrap(), Some(active));

        let capacity_store = InMemorySessionStore::default();
        {
            let mut sessions = capacity_store
                .sessions
                .lock()
                .expect("new session lock must be available");
            for _ in 0..super::in_memory::MAX_IN_MEMORY_SESSIONS {
                let id = SessionId::new();
                sessions.insert(id, session(id, u64::MAX));
            }
        }
        assert_eq!(
            capacity_store
                .save(session(SessionId::new(), u64::MAX))
                .await,
            Err(InMemorySessionStoreError::CapacityExhausted)
        );
    }

    #[tokio::test]
    async fn session_serialization_and_debug_do_not_expose_credentials() {
        let issued = SessionManager::new(
            InMemorySessionStore::default(),
            SessionCookieConfig::new("rustee_session", 60).unwrap(),
        )
        .establish(Principal::new("alice").unwrap())
        .await
        .unwrap();
        let store = InMemorySessionStore::default();
        let manager = SessionManager::new(
            store.clone(),
            SessionCookieConfig::new("session", 60).unwrap(),
        );
        let second_issued = manager
            .establish(Principal::new("bob").unwrap())
            .await
            .unwrap();
        let id = cookie(&second_issued)
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .and_then(|cookie| cookie.split_once('='))
            .and_then(|(_, value)| super::SessionId::parse(value))
            .unwrap();
        let session = store.load(id).await.unwrap().unwrap();

        let encoded = serde_json::to_string(&session).unwrap();
        let decoded: super::Session = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded.id(), session.id());
        assert!(decoded.remaining_ttl_seconds().is_some());
        assert!(!format!("{:?}", session.id()).contains(&session.id().to_string()));
        assert!(!format!("{session:?}").contains(&session.csrf_token));
        assert!(!format!("{issued:?}").contains(issued.csrf_token()));
    }

    #[test]
    fn cookie_policy_debug_redacts_the_cookie_name_through_public_wrappers() {
        let cookie = SessionCookieConfig::new("private-session-cookie", 60)
            .expect("test cookie configuration is valid");
        let cookie_debug = format!("{cookie:?}");
        let manager_debug = format!(
            "{:?}",
            SessionManager::new(InMemorySessionStore::default(), cookie.clone())
        );
        let layer_debug = format!(
            "{:?}",
            crate::SessionLayer::new(InMemorySessionStore::default(), cookie)
        );

        assert!(cookie_debug.contains("name: \"[REDACTED]\""));
        assert!(cookie_debug.contains("ttl_seconds: 60"));
        assert!(cookie_debug.contains("secure: true"));
        assert!(cookie_debug.contains("same_site: Lax"));
        for debug in [cookie_debug, manager_debug, layer_debug] {
            assert!(!debug.contains("private-session-cookie"), "{debug}");
        }
    }

    #[test]
    fn manager_debug_does_not_delegate_to_store_diagnostics() {
        let manager = SessionManager::new(
            LeakyDiagnosticStore,
            SessionCookieConfig::new("session", 60).expect("test cookie configuration is valid"),
        );

        let debug = format!("{manager:?}");
        assert!(debug.contains("store_type"));
        assert!(!debug.contains("private-store-credential"));
    }

    #[test]
    fn session_layer_debug_does_not_delegate_to_store_diagnostics() {
        let layer = crate::SessionLayer::new(
            LeakyDiagnosticStore,
            SessionCookieConfig::new("session", 60).expect("test cookie configuration is valid"),
        );

        let debug = format!("{layer:?}");

        assert!(debug.contains("store_type"));
        assert!(!debug.contains("private-store-credential"));
    }

    fn session(id: SessionId, expires_at_unix_seconds: u64) -> Session {
        Session {
            id,
            principal: Principal::new("alice").expect("test principal must be valid"),
            csrf_token: Uuid::new_v4().to_string(),
            expires_at_unix_seconds,
        }
    }

    #[test]
    fn durable_session_deserialization_revalidates_the_csrf_capability() {
        let id = SessionId::new();
        let serialized = serde_json::json!({
            "id": id.to_string(),
            "principal": {"subject": "alice", "scopes": []},
            "csrf_token": "private-invalid-csrf-token",
            "expires_at_unix_seconds": u64::MAX,
        });

        assert!(serde_json::from_value::<Session>(serialized).is_err());
    }
}
