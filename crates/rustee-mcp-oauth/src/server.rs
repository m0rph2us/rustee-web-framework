//! Public protected-resource metadata and Bearer authorization HTTP service facade.

mod metadata;
mod resource;

pub use metadata::McpOAuthProtectedResourceMetadata;
pub use resource::{McpOAuthResourceServer, McpOAuthResourceServerLayer};
