use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    fmt,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::StatusCode;
use rustee_core::{BoxCloneServiceExt, Error, IntoResponse, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

use super::exceeds_principal_authorization_value_limit;
use crate::{
    AuthError, MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES, Principal, bearer::authentication_response,
};

/// A server-side mapping from trusted role names to granted permissions.
///
/// This policy deliberately lives in application configuration rather than token parsing. Each
/// deployment stays in control of what an IdP-provided role is allowed to do.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct RolePolicy {
    grants: BTreeMap<String, BTreeSet<String>>,
}

impl fmt::Debug for RolePolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RolePolicy")
            .field("configured_role_count", &self.grants.len())
            .field("granted_permission_count", &self.granted_permission_count())
            .finish()
    }
}

impl RolePolicy {
    /// Creates an empty policy that grants no role-derived permissions.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds every supplied permission to one role.
    ///
    /// # Errors
    ///
    /// Returns [`RolePolicyError`] when a value is blank or oversized, or no permissions are
    /// supplied.
    pub fn grant(
        &mut self,
        role: impl Into<String>,
        permissions: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<(), RolePolicyError> {
        let role = role.into();
        if role.trim().is_empty() {
            return Err(RolePolicyError::BlankRole);
        }
        if exceeds_principal_authorization_value_limit(&role) {
            return Err(RolePolicyError::RoleTooLong {
                max_bytes: MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES,
            });
        }
        let permissions = permissions
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>();
        if permissions.is_empty() {
            return Err(RolePolicyError::EmptyPermissions);
        }
        if permissions
            .iter()
            .any(|permission| permission.trim().is_empty())
        {
            return Err(RolePolicyError::BlankPermission);
        }
        if permissions
            .iter()
            .any(|permission| exceeds_principal_authorization_value_limit(permission))
        {
            return Err(RolePolicyError::PermissionTooLong {
                max_bytes: MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES,
            });
        }
        self.grants.entry(role).or_default().extend(permissions);
        Ok(())
    }

    /// Returns whether a direct permission or any principal role grants `permission`.
    #[must_use]
    pub fn permits(&self, principal: &Principal, permission: &str) -> bool {
        principal.has_permission(permission)
            || principal.roles().any(|role| {
                self.grants
                    .get(role)
                    .is_some_and(|permissions| permissions.contains(permission))
            })
    }

    fn granted_permission_count(&self) -> usize {
        self.grants.values().fold(0_usize, |count, permissions| {
            count.saturating_add(permissions.len())
        })
    }
}

/// Invalid role-to-permission policy settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RolePolicyError {
    /// The configured role was blank.
    #[error("a role policy role must not be blank")]
    BlankRole,
    /// The configured role had no permissions.
    #[error("a role policy must grant at least one permission")]
    EmptyPermissions,
    /// A configured permission was blank.
    #[error("a role policy permission must not be blank")]
    BlankPermission,
    /// A configured role cannot be held by a principal.
    #[error("a role policy role exceeds the {max_bytes}-byte principal limit")]
    RoleTooLong {
        /// The shared principal authorization-value limit.
        max_bytes: usize,
    },
    /// A configured permission cannot be held by a principal.
    #[error("a role policy permission exceeds the {max_bytes}-byte principal limit")]
    PermissionTooLong {
        /// The shared principal authorization-value limit.
        max_bytes: usize,
    },
}

/// A layer that requires every configured permission from direct grants or [`RolePolicy`] roles.
#[derive(Clone, Eq, PartialEq)]
#[must_use = "a permission policy must be applied to a service to have an effect"]
pub struct RequirePermissionsLayer {
    required: BTreeSet<String>,
    policy: RolePolicy,
}

impl fmt::Debug for RequirePermissionsLayer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequirePermissionsLayer")
            .field("required_permission_count", &self.required.len())
            .field("configured_role_count", &self.policy.grants.len())
            .field(
                "granted_permission_count",
                &self.policy.granted_permission_count(),
            )
            .finish()
    }
}

impl RequirePermissionsLayer {
    /// Creates a policy that requires every supplied permission.
    ///
    /// # Errors
    ///
    /// Returns [`PermissionPolicyError`] for empty, blank, or oversized permission requirements.
    pub fn new(
        permissions: impl IntoIterator<Item = impl Into<String>>,
        policy: RolePolicy,
    ) -> Result<Self, PermissionPolicyError> {
        let required = permissions
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>();
        if required.is_empty() {
            return Err(PermissionPolicyError::EmptyRequirement);
        }
        if required
            .iter()
            .any(|permission| permission.trim().is_empty())
        {
            return Err(PermissionPolicyError::BlankPermission);
        }
        if required
            .iter()
            .any(|permission| exceeds_principal_authorization_value_limit(permission))
        {
            return Err(PermissionPolicyError::PermissionTooLong {
                max_bytes: MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES,
            });
        }
        Ok(Self { required, policy })
    }
}

/// Invalid permission policy configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PermissionPolicyError {
    /// No permissions were required.
    #[error("a permission policy must require at least one permission")]
    EmptyRequirement,
    /// A supplied permission was blank.
    #[error("a required permission must not be blank")]
    BlankPermission,
    /// A supplied permission cannot be held by a principal.
    #[error("a required permission exceeds the {max_bytes}-byte principal limit")]
    PermissionTooLong {
        /// The shared principal authorization-value limit.
        max_bytes: usize,
    },
}

/// Service produced by [`RequirePermissionsLayer`].
#[derive(Clone)]
pub struct RequirePermissions {
    inner: BoxCloneService<Request, Response, Infallible>,
    required: BTreeSet<String>,
    policy: RolePolicy,
}

impl fmt::Debug for RequirePermissions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequirePermissions")
            .field("required_permission_count", &self.required.len())
            .field("configured_role_count", &self.policy.grants.len())
            .field(
                "granted_permission_count",
                &self.policy.granted_permission_count(),
            )
            .finish_non_exhaustive()
    }
}

impl<S> Layer<S> for RequirePermissionsLayer
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Service = RequirePermissions;

    fn layer(&self, inner: S) -> Self::Service {
        RequirePermissions {
            inner: BoxCloneService::new(inner),
            required: self.required.clone(),
            policy: self.policy.clone(),
        }
    }
}

impl Service<Request> for RequirePermissions {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let required = self.required.clone();
        let policy = self.policy.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            let Some(principal) = request.extensions().get::<Principal>() else {
                return Ok(authentication_response(AuthError::MissingBearerToken));
            };
            if !required
                .iter()
                .all(|permission| policy.permits(principal, permission))
            {
                return Ok(Error::new(
                    StatusCode::FORBIDDEN,
                    "insufficient_permission",
                    "the authenticated principal lacks a required permission",
                )
                .into_response());
            }
            inner.call_ready(request).await
        })
    }
}
