//! Rustee's application-facing API.

pub use http::{Method, StatusCode, Uri};
pub use rustee_core::{
    Body, ConnectionInfo, Error, FromHeader, FromRequest, Header, IntoResponse, Json, Path, Query,
    Request, Response, Result, RouteClassification, RouteParams, RouteTemplate, State, StateStore,
    empty_body, full_body, json_response, response,
};
pub use rustee_router::{App, Handler, RouteError};
pub use rustee_server::{
    ServerOptions, serve, serve_listener, serve_listener_with_options,
    serve_service_listener_with_options, serve_with_options,
};

/// Re-exports optional ergonomic macros.
///
/// Enable the facade's <code>macros</code> feature to use explicit route-builder helpers and
/// derive supported extractor contracts. Macro expansion only calls existing Rustee APIs and does
/// not infer HTTP semantics.
#[cfg(feature = "macros")]
pub use rustee_macros::{FromHeader, routes};

/// Internal re-export used by Rustee's opt-in macros.
#[doc(hidden)]
pub use http as __http;

/// Internal re-export used by Rustee's opt-in macros.
#[doc(hidden)]
pub use rustee_core as __private;

/// Opt-in `OpenAPI` description support.
///
/// Enable the facade's <code>openapi</code> feature to use this module. Route handlers and their
/// `OpenAPI` operations remain separately declared so schema generation never infers authorization,
/// extraction, or response semantics from a handler signature.
#[cfg(feature = "openapi")]
pub mod openapi {
    pub use rustee_openapi::*;
}
