//! Principal identifier, authorization-set, and error admission rules.

use std::collections::BTreeSet;

/// Maximum UTF-8 byte length of a principal subject, issuer, or tenant identifier.
pub const MAX_PRINCIPAL_IDENTIFIER_BYTES: usize = 1024;

/// Maximum UTF-8 byte length of one scope, role, or direct permission value.
pub const MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES: usize = 256;

/// Maximum number of distinct scopes, roles, or direct permissions on one principal.
pub const MAX_PRINCIPAL_AUTHORIZATION_VALUES: usize = 64;

/// Invalid principal content rejected before it reaches request extensions.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PrincipalError {
    /// A required identity field was blank.
    #[error("{field} must not be blank")]
    BlankField {
        /// The invalid field name.
        field: &'static str,
    },
    /// An identity or authorization value exceeded its fixed byte limit.
    #[error("{field} exceeds the {max_bytes}-byte limit")]
    ValueTooLong {
        /// The invalid field or set name.
        field: &'static str,
        /// The accepted UTF-8 byte limit.
        max_bytes: usize,
    },
    /// An authorization set exceeded its fixed number of distinct values.
    #[error("{field} exceeds the {max_values}-value limit")]
    TooManyValues {
        /// The authorization set name.
        field: &'static str,
        /// The maximum number of distinct values.
        max_values: usize,
    },
}

pub(super) fn validate_identifier(value: &str, field: &'static str) -> Result<(), PrincipalError> {
    ensure_not_blank(value, field)?;
    ensure_within_byte_limit(value, field, MAX_PRINCIPAL_IDENTIFIER_BYTES)
}

pub(super) fn insert_authorization_value(
    values: &mut BTreeSet<String>,
    value: String,
    field: &'static str,
) -> Result<(), PrincipalError> {
    ensure_not_blank(&value, field)?;
    ensure_within_byte_limit(&value, field, MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES)?;
    if !values.contains(&value) && values.len() == MAX_PRINCIPAL_AUTHORIZATION_VALUES {
        return Err(PrincipalError::TooManyValues {
            field,
            max_values: MAX_PRINCIPAL_AUTHORIZATION_VALUES,
        });
    }
    values.insert(value);
    Ok(())
}

fn ensure_not_blank(value: &str, field: &'static str) -> Result<(), PrincipalError> {
    if value.trim().is_empty() {
        return Err(PrincipalError::BlankField { field });
    }
    Ok(())
}

fn ensure_within_byte_limit(
    value: &str,
    field: &'static str,
    max_bytes: usize,
) -> Result<(), PrincipalError> {
    if value.len() > max_bytes {
        return Err(PrincipalError::ValueTooLong { field, max_bytes });
    }
    Ok(())
}
