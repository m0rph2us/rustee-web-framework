//! Stable facade for validated provider-neutral identity values.

mod admission;
mod model;

pub use admission::{
    MAX_PRINCIPAL_AUTHORIZATION_VALUE_BYTES, MAX_PRINCIPAL_AUTHORIZATION_VALUES,
    MAX_PRINCIPAL_IDENTIFIER_BYTES, PrincipalError,
};
pub use model::Principal;

#[cfg(test)]
mod tests;
