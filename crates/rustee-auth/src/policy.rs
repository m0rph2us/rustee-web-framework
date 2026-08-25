//! Scope and permission authorization policies over a verified principal.

mod permission;
mod scope;

pub use permission::{
    PermissionPolicyError, RequirePermissions, RequirePermissionsLayer, RolePolicy, RolePolicyError,
};
pub use scope::{RequireScopes, RequireScopesLayer, ScopePolicyError};

use crate::MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES;

pub(super) fn exceeds_principal_authorization_value_limit(value: &str) -> bool {
    value.len() > MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES
}
