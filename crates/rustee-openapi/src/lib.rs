//! Explicit `OpenAPI` 3.1 descriptions for Rustee applications.
//!
//! This crate does not inspect route handlers, extractors, state, authorization, or response
//! values. Applications declare an [`OpenApiRoute`] and [`OpenApiOperation`] beside the Rustee
//! route they mount. The document checks that path parameters agree with the Rustee-style route
//! template and can be returned directly from a normal handler.

mod document;
mod model;
mod operation;
mod schema;
mod security;
mod validation;

pub use document::OpenApiDocument;
pub use model::{OpenApiError, OpenApiMethod, OpenApiRoute};
pub use operation::{OpenApiOperation, OpenApiOperationBuilder, OpenApiParameterLocation};
pub use schema::OpenApiSchema;
pub use security::{
    OpenApiApiKeyLocation, OpenApiOAuthFlow, OpenApiSecurityRequirement, OpenApiSecurityScheme,
};

pub(crate) use validation::{
    MAX_METADATA_CHARS, MAX_SCHEMA_BYTES, validate_identifier, validate_metadata,
};

#[cfg(test)]
mod tests;
