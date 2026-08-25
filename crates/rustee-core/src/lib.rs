//! Stable, transport-independent primitives used by every Rustee crate.

mod extract;
mod header;
mod media_type;
mod oauth;
mod response;
mod routing;
mod service;

pub use extract::{FromHeader, FromRequest, Header, Json, Path, Query, State, StateStore};
pub use header::is_valid_http_bearer_value;
pub use media_type::is_standard_json_media_type;
pub use oauth::{
    are_valid_oauth_scope_tokens, is_valid_oauth_authorization_code,
    is_valid_oauth_authorization_value, is_valid_oauth_provider_error, is_valid_oauth_scope_token,
};
pub use response::{
    Body, BoxError, Error, IntoResponse, Request, Response, Result, empty_body, full_body,
    json_response, json_response_bounded, response, stream_body,
};
pub use routing::{ConnectionInfo, RouteClassification, RouteParams, RouteTemplate};
pub use service::BoxCloneServiceExt;

#[cfg(test)]
mod tests;
