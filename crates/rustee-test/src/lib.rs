//! Small, bounded in-process helpers for Rustee application tests.
//!
//! [`TestApp`] dispatches directly to [`rustee_router::App`]. An opt-in [`TestCookieJar`] can
//! retain simple response cookies for session-style tests, but it does not model browser origin,
//! path, secure transport, same-site, redirect, HTTP-version, or SSE behavior. Keep those
//! behaviors in focused wire or browser integration tests.

mod cookie;
mod request;
mod response;

pub use cookie::{DEFAULT_MAX_COOKIE_BYTES, DEFAULT_MAX_COOKIE_COUNT, TestCookieJar};
pub use request::{
    DEFAULT_MAX_REQUEST_BYTES, DEFAULT_MAX_RESPONSE_BYTES, TestApp, TestAppError, TestRequest,
    TestRequestError, request,
};
pub use response::{TestAssertionError, TestResponse, TestResponseError};

#[cfg(test)]
mod tests;
