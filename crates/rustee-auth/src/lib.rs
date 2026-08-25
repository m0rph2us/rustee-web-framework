//! Provider-neutral authentication principal, middleware, and authorization policy.
//!
//! Token verification belongs in a provider crate. This crate accepts only the verified identity
//! result and never stores a raw bearer token in a request extension.

mod api_key;
mod bearer;
mod constant_time;
mod policy;
mod principal;
mod tenant;

pub use api_key::{
    ApiKeyAuthenticator, ApiKeyError, ApiKeyFingerprint, ApiKeyFingerprintStore, ApiKeyLayer,
    ApiKeyLayerError, ApiKeyPepper, ApiKeyPepperError, ApiKeyPepperRing, ApiKeyPepperRingError,
    ApiKeyService, KeyedApiKeyAuthenticator, MAX_RETIRED_API_KEY_PEPPERS,
    RotatingKeyedApiKeyAuthenticator, StaticApiKeyAuthenticator, StaticApiKeyError,
};
pub use bearer::{
    AuthError, AuthLayer, AuthService, AuthUser, BearerAuthenticator, MAX_BEARER_TOKEN_BYTES,
    OptionalAuthUser, RequireAuth, StaticTokenAuthenticator, StaticTokenError,
    extract_bearer_token,
};
pub use constant_time::constant_time_eq;
pub use policy::{
    PermissionPolicyError, RequirePermissions, RequirePermissionsLayer, RequireScopes,
    RequireScopesLayer, RolePolicy, RolePolicyError, ScopePolicyError,
};
pub use principal::{
    MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES, MAX_PRINCIPAL_AUTHORIZATION_VALUES,
    MAX_PRINCIPAL_IDENTIFIER_BYTES, Principal, PrincipalError,
};
pub use rustee_tenant::{
    MAX_TENANT_IDENTIFIER_BYTES, TenantContext, TenantContextError as TenantPolicyError,
};
pub use tenant::{
    HostTenantResolver, HostTenantResolverError, RequireTenantMatch, RequireTenantMatchLayer,
    TenantResolution, TenantResolutionLayer, TenantResolver,
};

#[cfg(test)]
mod tests;
