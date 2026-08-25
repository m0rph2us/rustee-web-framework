//! Static-key JWT resource-server authentication for Rustee.
//!
//! The verifier has a deliberately narrow secure default: one configured algorithm, a required
//! signature, and required `sub`, `iss`, `aud`, `exp`, and `nbf` claims. For remote OIDC/JWKS key
//! discovery and rotation, use the OIDC adapter rather than weakening this verifier.

mod claims;
mod config;
mod verifier;

pub use config::{JwtConfig, JwtConfigurationError};
pub use verifier::JwtAuthenticator;

#[cfg(test)]
mod tests;
