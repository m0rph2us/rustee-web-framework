use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::future::BoxFuture;
use tokio::sync::Mutex;
use url::Url;

use crate::{OidcClientAuthentication, OpaqueIntrospectionConfig, OpaqueTokenIntrospection};

use super::super::{OpaqueTokenIntrospectionRequest, OpaqueTokenIntrospector, unix_seconds};

pub(super) const ISSUER: &str = "https://issuer.example.test";
pub(super) const AUDIENCE: &str = "rustee-api";

#[derive(Clone, Debug, thiserror::Error)]
#[error("test introspection endpoint is unavailable")]
pub(super) struct IntrospectionError;

#[derive(Clone)]
pub(super) struct FakeIntrospector {
    replies: Arc<Mutex<VecDeque<Result<OpaqueTokenIntrospection, IntrospectionError>>>>,
    calls: Arc<AtomicUsize>,
}

impl FakeIntrospector {
    pub(super) fn new(
        replies: impl IntoIterator<Item = Result<OpaqueTokenIntrospection, IntrospectionError>>,
    ) -> Self {
        Self {
            replies: Arc::new(Mutex::new(replies.into_iter().collect())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl OpaqueTokenIntrospector for FakeIntrospector {
    type Error = IntrospectionError;

    fn introspect(
        &self,
        endpoint: Url,
        request: OpaqueTokenIntrospectionRequest,
    ) -> BoxFuture<'static, Result<OpaqueTokenIntrospection, Self::Error>> {
        let replies = Arc::clone(&self.replies);
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            assert_eq!(
                endpoint.as_str(),
                "https://issuer.example.test/oauth2/introspect"
            );
            assert_eq!(request.client_id(), "rustee-resource-server");
            assert!(
                matches!(request.token(), "opaque-token" | "another-opaque-token"),
                "test must use one known opaque credential"
            );
            calls.fetch_add(1, Ordering::SeqCst);
            replies
                .lock()
                .await
                .pop_front()
                .expect("test introspector needs a queued reply")
        })
    }
}

pub(super) fn config() -> OpaqueIntrospectionConfig {
    OpaqueIntrospectionConfig::new(
        ISSUER,
        AUDIENCE,
        Url::parse("https://issuer.example.test/oauth2/introspect")
            .expect("test URL must be valid"),
        "rustee-resource-server",
        OidcClientAuthentication::None,
    )
    .expect("test configuration must be valid")
}

pub(super) fn active_response() -> OpaqueTokenIntrospection {
    OpaqueTokenIntrospection::active("alice", ISSUER, AUDIENCE)
        .with_expiration(unix_seconds() + 300)
        .with_tenant("acme")
        .with_scope("profile:read profile:write")
        .with_role("project-viewer")
        .with_permission("project:read")
}
