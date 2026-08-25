//! Explicit HTTP transport limits and validation.

use std::{io, time::Duration};

use tokio::sync::Semaphore;

pub(super) const MIN_HTTP1_BUFFER_BYTES: usize = 8 * 1024;
const DEFAULT_MAX_HTTP1_BUFFER_BYTES: usize = 64 * 1024;

/// Transport limits that are deliberately explicit rather than unbounded defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServerOptions {
    /// Maximum number of bytes accepted from one request body.
    pub max_body_bytes: usize,
    /// Maximum non-zero duration allowed to receive a complete HTTP/1 request header.
    pub header_read_timeout: Duration,
    /// Maximum HTTP/1 connection buffer for incomplete input and buffered response output.
    ///
    /// This must be at least 8 KiB, the protocol minimum required by Hyper.
    pub max_http1_buffer_bytes: usize,
    /// Maximum number of open TCP connections accepted by this listener.
    pub max_connections: usize,
    /// Maximum non-zero duration allowed for application handler execution.
    pub request_timeout: Duration,
    /// Maximum number of handlers that may execute at once for this listener.
    pub max_in_flight_requests: usize,
    /// Maximum non-zero time to let active connections drain after shutdown starts.
    pub graceful_shutdown_timeout: Duration,
}

impl ServerOptions {
    pub(super) fn validate(self) -> io::Result<()> {
        if self.max_body_bytes == 0
            || self.max_connections == 0
            || self.max_in_flight_requests == 0
            || self.max_http1_buffer_bytes == 0
            || self.header_read_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.graceful_shutdown_timeout.is_zero()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Rustee server limits and timeouts must be greater than zero",
            ));
        }
        if self.max_http1_buffer_bytes < MIN_HTTP1_BUFFER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Rustee HTTP/1 buffer limit must be at least 8192 bytes",
            ));
        }
        if self.max_connections > Semaphore::MAX_PERMITS
            || self.max_in_flight_requests > Semaphore::MAX_PERMITS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Rustee server concurrency limits exceed the supported maximum",
            ));
        }
        Ok(())
    }
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            max_body_bytes: 2 * 1024 * 1024,
            header_read_timeout: Duration::from_secs(30),
            max_http1_buffer_bytes: DEFAULT_MAX_HTTP1_BUFFER_BYTES,
            max_connections: 4_096,
            request_timeout: Duration::from_secs(30),
            max_in_flight_requests: 1_024,
            graceful_shutdown_timeout: Duration::from_secs(10),
        }
    }
}
