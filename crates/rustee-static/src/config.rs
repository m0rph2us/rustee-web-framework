//! Static-file configuration and request-path validation.

use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use http::HeaderValue;

use super::layer::StaticFilesLayer;

const DEFAULT_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;

/// Static file configuration rejected before a layer is created.
#[derive(thiserror::Error)]
pub enum StaticFilesError {
    /// The configured root could not be canonicalized.
    #[error("static file root could not be canonicalized")]
    RootCanonicalization(#[source] io::Error),
    /// The configured root was not a directory.
    #[error("static file root must be a directory")]
    RootNotDirectory,
    /// The configured mount path was unsafe or not an absolute URI path.
    #[error("static file mount path must be an absolute, normalized URI path")]
    InvalidMountPath,
    /// The maximum response size was zero.
    #[error("static file maximum size must be greater than zero")]
    ZeroMaxFileBytes,
    /// The streaming threshold was zero.
    #[error("static file streaming threshold must be greater than zero")]
    ZeroStreamingThreshold,
}

impl fmt::Debug for StaticFilesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::RootCanonicalization(_) => "root_canonicalization_failed",
            Self::RootNotDirectory => "root_not_directory",
            Self::InvalidMountPath => "invalid_mount_path",
            Self::ZeroMaxFileBytes => "zero_max_file_bytes",
            Self::ZeroStreamingThreshold => "zero_streaming_threshold",
        };
        formatter
            .debug_struct("StaticFilesError")
            .field("kind", &kind)
            .finish()
    }
}

/// Static files served only below one configured URI mount path.
#[derive(Clone)]
pub struct StaticFiles {
    pub(super) root: Arc<PathBuf>,
    pub(super) mount_path: String,
    pub(super) max_file_bytes: u64,
    pub(super) streaming_threshold: Option<u64>,
    pub(super) cache_control: HeaderValue,
    pub(super) precompressed_variants: bool,
}

impl fmt::Debug for StaticFiles {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticFiles")
            .field("root_configured", &true)
            .field("mount_path_configured", &true)
            .field("max_file_bytes", &self.max_file_bytes)
            .field("streaming_threshold", &self.streaming_threshold)
            .field("cache_control_configured", &true)
            .field("precompressed_variants", &self.precompressed_variants)
            .finish_non_exhaustive()
    }
}

impl StaticFiles {
    /// Canonicalizes an existing static file root with conservative defaults.
    ///
    /// The default mount path is `/static`, the maximum response size is 8 MiB, and responses use
    /// `Cache-Control: no-store` until an application deliberately chooses a cache policy.
    ///
    /// # Errors
    ///
    /// Returns [`StaticFilesError`] when `root` cannot be read or is not a directory.
    pub fn new(root: impl AsRef<Path>) -> Result<Self, StaticFilesError> {
        let root = fs::canonicalize(root).map_err(StaticFilesError::RootCanonicalization)?;
        if !root.is_dir() {
            return Err(StaticFilesError::RootNotDirectory);
        }
        Ok(Self {
            root: Arc::new(root),
            mount_path: String::from("/static"),
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            streaming_threshold: None,
            cache_control: HeaderValue::from_static("no-store"),
            precompressed_variants: false,
        })
    }

    /// Sets the URI mount path handled by this static file layer.
    ///
    /// The mount must be an absolute normalized path such as `/assets`. A root mount is allowed;
    /// use it only when no application route should handle unmatched paths.
    ///
    /// # Errors
    ///
    /// Returns [`StaticFilesError::InvalidMountPath`] for a trailing slash, duplicate slash,
    /// query/fragment marker, percent encoding, or traversal-like component.
    pub fn at(mut self, mount_path: impl AsRef<str>) -> Result<Self, StaticFilesError> {
        let mount_path = mount_path.as_ref();
        if !valid_mount_path(mount_path) {
            return Err(StaticFilesError::InvalidMountPath);
        }
        mount_path.clone_into(&mut self.mount_path);
        Ok(self)
    }

    /// Sets the maximum body size for one static file response.
    ///
    /// # Errors
    ///
    /// Returns [`StaticFilesError::ZeroMaxFileBytes`] when `max_file_bytes` is zero.
    pub fn with_max_file_bytes(mut self, max_file_bytes: u64) -> Result<Self, StaticFilesError> {
        if max_file_bytes == 0 {
            return Err(StaticFilesError::ZeroMaxFileBytes);
        }
        self.max_file_bytes = max_file_bytes;
        Ok(self)
    }

    /// Streams successful file bodies at or above one representation-size threshold.
    ///
    /// Streaming reads the selected full or single-range representation in bounded chunks. The
    /// configured [`StaticFiles::with_max_file_bytes`] limit remains the admission boundary, so
    /// applications must raise that limit deliberately before serving larger files.
    ///
    /// # Errors
    ///
    /// Returns [`StaticFilesError::ZeroStreamingThreshold`] when `threshold` is zero.
    pub fn with_streaming_threshold(mut self, threshold: u64) -> Result<Self, StaticFilesError> {
        if threshold == 0 {
            return Err(StaticFilesError::ZeroStreamingThreshold);
        }
        self.streaming_threshold = Some(threshold);
        Ok(self)
    }

    /// Sets the exact `Cache-Control` header used for successful static file responses.
    #[must_use]
    pub fn with_cache_control(mut self, cache_control: HeaderValue) -> Self {
        self.cache_control = cache_control;
        self
    }

    /// Enables selection of sibling `.br` and `.gz` files through `Accept-Encoding`.
    ///
    /// Variants are served only for requests without `Range`; range requests keep the identity
    /// representation and its existing conditional cache contract.
    #[must_use]
    pub const fn with_precompressed_variants(mut self, enabled: bool) -> Self {
        self.precompressed_variants = enabled;
        self
    }

    /// Returns a Tower layer that serves this configuration before its inner service.
    pub fn layer(self) -> StaticFilesLayer {
        StaticFilesLayer::new(self)
    }

    pub(super) fn relative_path(&self, path: &str) -> Result<Option<PathBuf>, ()> {
        let relative = if self.mount_path == "/" {
            path.strip_prefix('/').ok_or(())?
        } else if path == self.mount_path {
            ""
        } else {
            let Some(relative) = path
                .strip_prefix(self.mount_path.as_str())
                .and_then(|suffix| suffix.strip_prefix('/'))
            else {
                return Ok(None);
            };
            relative
        };

        if relative.is_empty() {
            return Ok(Some(PathBuf::new()));
        }

        let mut local = PathBuf::new();
        for segment in relative.split('/') {
            let segment = percent_decode_segment(segment)?;
            local.push(segment);
        }
        Ok(Some(local))
    }
}

fn valid_mount_path(path: &str) -> bool {
    if !path.starts_with('/')
        || path.contains(['?', '#', '%'])
        || path.contains("//")
        || (path.len() > 1 && path.ends_with('/'))
    {
        return false;
    }
    path == "/" || path.split('/').skip(1).all(valid_mount_segment)
}

fn valid_mount_segment(segment: &str) -> bool {
    !segment.is_empty() && !matches!(segment, "." | "..") && !segment.contains(['\\', '\0'])
}

fn percent_decode_segment(segment: &str) -> Result<String, ()> {
    if segment.is_empty() {
        return Err(());
    }
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = *bytes.get(index + 1).ok_or(())?;
            let low = *bytes.get(index + 2).ok_or(())?;
            decoded.push((hex_value(high)? << 4) | hex_value(low)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    let decoded = String::from_utf8(decoded).map_err(|_| ())?;
    if !valid_mount_segment(&decoded) || decoded.contains('/') {
        return Err(());
    }
    Ok(decoded)
}

fn hex_value(value: u8) -> Result<u8, ()> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(()),
    }
}
