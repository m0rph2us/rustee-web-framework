//! Bounded, intentionally non-browser cookie handling for in-process tests.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use http::{HeaderMap, HeaderValue, header::SET_COOKIE};

use crate::TestResponseError;

/// Maximum number of cookies retained by one [`TestCookieJar`].
pub const DEFAULT_MAX_COOKIE_COUNT: usize = 64;

/// Maximum byte length of the `Cookie` header emitted by one [`TestCookieJar`].
pub const DEFAULT_MAX_COOKIE_BYTES: usize = 8 * 1024;

/// A bounded cookie jar for opt-in [`crate::TestApp`] session-style flows.
///
/// `TestCookieJar` is intentionally not a browser emulator. It retains only valid ASCII cookie
/// name/value pairs, applies response `Max-Age=0` deletion, and emits one request `Cookie`
/// header. Cookie values are never included in its `Debug` output or public inspection API.
#[derive(Clone, Default)]
pub struct TestCookieJar {
    entries: Arc<Mutex<BTreeMap<String, String>>>,
}

impl fmt::Debug for TestCookieJar {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestCookieJar")
            .field("entry_count", &self.len())
            .finish()
    }
}

impl TestCookieJar {
    /// Clears all retained cookies.
    pub fn clear(&self) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// Returns the number of retained cookies without exposing names or values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Returns whether no cookies are retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn request_header(&self) -> Option<HeaderValue> {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (!entries.is_empty()).then(|| {
            let value = entries
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join("; ");
            HeaderValue::try_from(value)
                .expect("validated test cookie name/value pairs produce a valid header")
        })
    }

    pub(super) fn absorb(&self, headers: &HeaderMap) -> std::result::Result<(), TestResponseError> {
        let parsed_updates = headers
            .get_all(SET_COOKIE)
            .iter()
            .map(parse_set_cookie)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if parsed_updates.is_empty() {
            return Ok(());
        }

        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut next_entries = entries.clone();
        for update in parsed_updates {
            match update {
                CookieUpdate::Set { name, value } => {
                    next_entries.insert(name, value);
                }
                CookieUpdate::Remove { name } => {
                    next_entries.remove(&name);
                }
            }
            if next_entries.len() > DEFAULT_MAX_COOKIE_COUNT
                || cookie_header_len(&next_entries) > DEFAULT_MAX_COOKIE_BYTES
            {
                return Err(TestResponseError::CookieJarLimitExceeded);
            }
        }
        *entries = next_entries;
        Ok(())
    }
}

fn cookie_header_len(entries: &BTreeMap<String, String>) -> usize {
    let pairs = entries
        .iter()
        .map(|(name, value)| name.len() + 1 + value.len())
        .sum::<usize>();
    pairs + entries.len().saturating_sub(1) * 2
}

enum CookieUpdate {
    Set { name: String, value: String },
    Remove { name: String },
}

fn parse_set_cookie(value: &HeaderValue) -> std::result::Result<CookieUpdate, TestResponseError> {
    let value = value
        .to_str()
        .map_err(|_| TestResponseError::InvalidSetCookie)?;
    let mut attributes = value.split(';');
    let Some(pair) = attributes.next() else {
        return Err(TestResponseError::InvalidSetCookie);
    };
    let Some((name, cookie_value)) = pair.trim().split_once('=') else {
        return Err(TestResponseError::InvalidSetCookie);
    };
    if !valid_cookie_name(name) || !valid_cookie_value(cookie_value) {
        return Err(TestResponseError::InvalidSetCookie);
    }
    let expires_now = attributes.any(|attribute| {
        attribute
            .trim()
            .split_once('=')
            .is_some_and(|(name, value)| {
                name.trim().eq_ignore_ascii_case("max-age") && value.trim() == "0"
            })
    });
    if expires_now {
        Ok(CookieUpdate::Remove {
            name: name.to_owned(),
        })
    } else {
        Ok(CookieUpdate::Set {
            name: name.to_owned(),
            value: cookie_value.to_owned(),
        })
    }
}

fn valid_cookie_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

fn valid_cookie_value(value: &str) -> bool {
    value.bytes().all(|byte| {
        byte == b'!'
            || (b'#'..=b'+').contains(&byte)
            || (b'-'..=b':').contains(&byte)
            || (b'<'..=b'[').contains(&byte)
            || (b']'..=b'~').contains(&byte)
    })
}
