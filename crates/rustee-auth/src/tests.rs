use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use http::{HeaderValue, Request as HttpRequest, StatusCode, header::WWW_AUTHENTICATE};
use rustee_core::empty_body;
use rustee_router::App;
use rustee_server::{ServerOptions, serve_service_listener_with_options};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};
use tower::{Layer, ServiceExt};

use super::{
    ApiKeyAuthenticator, ApiKeyError, ApiKeyFingerprint, ApiKeyFingerprintStore, ApiKeyLayer,
    ApiKeyLayerError, ApiKeyPepper, ApiKeyPepperError, ApiKeyPepperRing, ApiKeyPepperRingError,
    AuthError, AuthLayer, AuthUser, BearerAuthenticator, HostTenantResolver,
    HostTenantResolverError, KeyedApiKeyAuthenticator, MAX_BEARER_TOKEN_BYTES,
    MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES, MAX_PRINCIPAL_AUTHORIZATION_VALUES,
    MAX_RETIRED_API_KEY_PEPPERS, PermissionPolicyError, Principal, RequireAuth,
    RequirePermissionsLayer, RequireScopesLayer, RequireTenantMatchLayer, RolePolicy,
    RolePolicyError, RotatingKeyedApiKeyAuthenticator, ScopePolicyError, StaticApiKeyAuthenticator,
    StaticApiKeyError, StaticTokenAuthenticator, StaticTokenError, TenantContext,
    TenantPolicyError, TenantResolutionLayer, TenantResolver,
};
use futures_util::future;

mod api_key;
mod bearer;
mod policy;
mod tenant;

fn authenticator() -> StaticTokenAuthenticator {
    let principal = Principal::new("alice")
        .unwrap()
        .with_scope("profile:read")
        .unwrap();
    let mut authenticator = StaticTokenAuthenticator::new();
    authenticator.insert("local-token", principal).unwrap();
    authenticator
}

fn request(token: Option<&str>) -> rustee_core::Request {
    let mut builder = HttpRequest::builder().method("GET").uri("/me");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(empty_body()).unwrap()
}
