//! Shared bounded admission for trusted OIDC configuration values.

use url::Url;

pub(crate) const MAX_AUDIENCE_BYTES: usize = 1024;
pub(crate) const MAX_ISSUER_BYTES: usize = 2 * 1024;
pub(crate) const MAX_LEEWAY_SECONDS: u64 = 300;
pub(crate) const MAX_HTTPS_URL_BYTES: usize = 2 * 1024;

pub(crate) fn valid_text(value: &str, maximum_bytes: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= maximum_bytes
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

pub(crate) fn valid_https_url(url: &Url) -> bool {
    url.as_str().len() <= MAX_HTTPS_URL_BYTES
        && url.scheme() == "https"
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
}
