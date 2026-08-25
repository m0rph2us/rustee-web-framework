//! Prefix-bound static file delivery for Rustee Tower services.
//!
//! The layer only handles `GET` and `HEAD` requests under an explicit mount path. It rejects
//! malformed percent encoding, decoded traversal components, directories, oversized files, and
//! canonical paths outside the configured root without revealing filesystem details. GET and HEAD
//! responses include weak file validators for conditional requests. Byte ranges are supported
//! for the identity representation. Opt-in precompressed variants are selected only for
//! non-range requests, and optional bounded streaming avoids buffering large representations.
//! Automatic index files remain a separate contract.

mod config;
mod delivery;
mod encoding;
mod layer;
mod range;
mod response;

pub use config::{StaticFiles, StaticFilesError};
pub use layer::{StaticFilesLayer, StaticFilesService};

#[cfg(test)]
use range::MAX_RANGE_MEMBERS;

#[cfg(test)]
mod tests;
