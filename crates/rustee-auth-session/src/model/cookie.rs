//! Browser-cookie policy, rendering, and issued-session response attachment.

use std::fmt;

use http::{HeaderValue, header::SET_COOKIE};
use rustee_core::Response;

use super::SessionId;

/// Maximum accepted byte length for a server-side session cookie name.
///
/// Cookie names are ASCII HTTP tokens, so bytes and characters have the same length. Keeping the
/// configuration value small leaves predictable space for the opaque session identifier and
/// browser-managed cookie attributes in the emitted `Set-Cookie` header.
pub const MAX_COOKIE_NAME_BYTES: usize = 128;

/// Cookie `SameSite` policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SameSite {
    /// Prevent cross-site cookie delivery whenever the browser supports it.
    Strict,
    /// Allow top-level safe navigation while protecting ordinary cross-site requests.
    Lax,
    /// Allow cross-site delivery; requires `Secure` in modern browsers.
    None,
}

impl SameSite {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "Strict",
            Self::Lax => "Lax",
            Self::None => "None",
        }
    }
}

/// Browser-cookie configuration for server-side sessions.
#[derive(Clone, Eq, PartialEq)]
pub struct SessionCookieConfig {
    name: String,
    ttl_seconds: u64,
    secure: bool,
    same_site: SameSite,
}

impl fmt::Debug for SessionCookieConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionCookieConfig")
            .field("name", &"[REDACTED]")
            .field("ttl_seconds", &self.ttl_seconds)
            .field("secure", &self.secure)
            .field("same_site", &self.same_site)
            .finish()
    }
}

impl SessionCookieConfig {
    /// Creates a secure, HTTP-only, `SameSite=Lax` cookie configuration.
    ///
    /// # Errors
    ///
    /// Returns [`CookieConfigError::InvalidName`] when `name` is not a bounded valid cookie
    /// token.
    pub fn new(name: impl Into<String>, ttl_seconds: u64) -> Result<Self, CookieConfigError> {
        let name = name.into();
        if !valid_cookie_name(&name) {
            return Err(CookieConfigError::InvalidName);
        }
        if ttl_seconds == 0 {
            return Err(CookieConfigError::ZeroTtl);
        }
        Ok(Self {
            name,
            ttl_seconds,
            secure: true,
            same_site: SameSite::Lax,
        })
    }

    /// Changes the `SameSite` policy.
    ///
    /// # Errors
    ///
    /// Returns [`CookieConfigError::SameSiteNoneRequiresSecure`] when a cross-site cookie would
    /// be sent without secure transport.
    pub fn with_same_site(mut self, same_site: SameSite) -> Result<Self, CookieConfigError> {
        if matches!(same_site, SameSite::None) && !self.secure {
            return Err(CookieConfigError::SameSiteNoneRequiresSecure);
        }
        self.same_site = same_site;
        Ok(self)
    }

    /// Allows an explicitly insecure cookie for local development only.
    ///
    /// # Errors
    ///
    /// Returns [`CookieConfigError::SameSiteNoneRequiresSecure`] when the current policy requires
    /// a secure cookie.
    pub fn with_secure(mut self, secure: bool) -> Result<Self, CookieConfigError> {
        if !secure && matches!(self.same_site, SameSite::None) {
            return Err(CookieConfigError::SameSiteNoneRequiresSecure);
        }
        self.secure = secure;
        Ok(self)
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(super) const fn ttl_seconds(&self) -> u64 {
        self.ttl_seconds
    }

    pub(super) fn set_cookie(&self, id: SessionId) -> HeaderValue {
        HeaderValue::from_str(&format!(
            "{}={id}; Path=/; Max-Age={}; HttpOnly{}; SameSite={}",
            self.name,
            self.ttl_seconds,
            if self.secure { "; Secure" } else { "" },
            self.same_site.as_str(),
        ))
        .expect("validated cookie configuration produces a valid header")
    }

    pub(super) fn clear_cookie(&self) -> HeaderValue {
        HeaderValue::from_str(&format!(
            "{}=; Path=/; Max-Age=0; HttpOnly{}; SameSite={}",
            self.name,
            if self.secure { "; Secure" } else { "" },
            self.same_site.as_str(),
        ))
        .expect("validated cookie configuration produces a valid header")
    }
}

/// Invalid browser session cookie configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CookieConfigError {
    /// The cookie name cannot be represented safely in a Cookie header.
    #[error(
        "session cookie name must be a non-empty HTTP token of at most {MAX_COOKIE_NAME_BYTES} bytes"
    )]
    InvalidName,
    /// A zero TTL would immediately invalidate every issued session.
    #[error("session cookie TTL must be non-zero")]
    ZeroTtl,
    /// Modern browsers require a secure transport for cross-site cookies.
    #[error("SameSite=None session cookies require Secure")]
    SameSiteNoneRequiresSecure,
}

/// Result of establishing or rotating a server-side session.
#[derive(Clone)]
pub struct IssuedSession {
    csrf_token: String,
    set_cookie: HeaderValue,
}

impl fmt::Debug for IssuedSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedSession")
            .field("csrf_token", &"[REDACTED]")
            .field("set_cookie", &"[REDACTED]")
            .finish()
    }
}

impl IssuedSession {
    pub(super) fn new(csrf_token: String, set_cookie: HeaderValue) -> Self {
        Self {
            csrf_token,
            set_cookie,
        }
    }

    /// Returns the CSRF token to render into same-origin forms or client code.
    #[must_use]
    pub fn csrf_token(&self) -> &str {
        &self.csrf_token
    }

    /// Adds the secure session cookie to an HTTP response.
    pub fn apply_to(&self, response: &mut Response) {
        response
            .headers_mut()
            .append(SET_COOKIE, self.set_cookie.clone());
    }
}

fn valid_cookie_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_COOKIE_NAME_BYTES
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}
