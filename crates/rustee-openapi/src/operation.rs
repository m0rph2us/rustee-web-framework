//! Explicit `OpenAPI` operation declaration facade.

mod builder;
mod model;

pub use builder::OpenApiOperationBuilder;
pub use model::{OpenApiOperation, OpenApiParameterLocation};

impl OpenApiOperation {
    /// Starts a builder for one operation ID.
    pub fn builder(operation_id: impl Into<String>) -> OpenApiOperationBuilder {
        OpenApiOperationBuilder::new(operation_id.into())
    }
}
