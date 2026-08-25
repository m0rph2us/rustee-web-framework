//! Stable API-key authentication facade.

mod authenticator;
mod http;
mod pepper;

pub use authenticator::{
    ApiKeyAuthenticator, ApiKeyError, ApiKeyFingerprintStore, KeyedApiKeyAuthenticator,
    RotatingKeyedApiKeyAuthenticator, StaticApiKeyAuthenticator, StaticApiKeyError,
};
pub use http::{ApiKeyLayer, ApiKeyLayerError, ApiKeyService};
pub use pepper::{
    ApiKeyFingerprint, ApiKeyPepper, ApiKeyPepperError, ApiKeyPepperRing, ApiKeyPepperRingError,
    MAX_RETIRED_API_KEY_PEPPERS,
};
