//! Stable bearer-authentication facade.

mod authenticator;
mod extractor;
mod http;
mod token;

pub use authenticator::{
    AuthError, BearerAuthenticator, StaticTokenAuthenticator, StaticTokenError,
};
pub use extractor::{AuthUser, OptionalAuthUser, RequireAuth};
pub use http::{AuthLayer, AuthService};
pub use token::{MAX_BEARER_TOKEN_BYTES, extract_bearer_token};

pub(crate) use http::authentication_response;
