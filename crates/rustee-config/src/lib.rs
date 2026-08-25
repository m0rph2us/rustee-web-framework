//! Small, explicit configuration primitives.
//!
//! The public facade keeps application configuration compact while the internal
//! modules separate source merging, typed environment parsing, and redacted
//! values.

mod builder;
mod environment;
mod model;

pub use builder::{
    ConfigBuilder, MAX_ENVIRONMENT_KEY_BYTES, MAX_ENVIRONMENT_PATH_SEGMENTS,
    MAX_ENVIRONMENT_VALUE_BYTES, MAX_ENVIRONMENT_VARIABLES,
};
pub use environment::{optional_env, required_env};
pub use model::{ConfigError, Secret, Source};
