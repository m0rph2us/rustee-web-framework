//! Exact-one request-header admission for MCP control inputs.

use http::{HeaderMap, header::AsHeaderName};

/// Result of admitting one MCP request-control header.
pub(super) enum HeaderAdmission<'a> {
    /// The header was absent.
    Missing,
    /// Exactly one textual header value was present.
    Valid(&'a str),
    /// Multiple values or a non-text value was present.
    Invalid,
}

/// Admits one textual header value while retaining the distinction between absence and invalidity.
pub(super) fn admit_single_header<Name>(headers: &HeaderMap, name: Name) -> HeaderAdmission<'_>
where
    Name: AsHeaderName,
{
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return HeaderAdmission::Missing;
    };
    let Ok(value) = value.to_str() else {
        return HeaderAdmission::Invalid;
    };
    if values.next().is_none() {
        HeaderAdmission::Valid(value)
    } else {
        HeaderAdmission::Invalid
    }
}

#[cfg(test)]
mod tests {
    use http::{
        HeaderMap, HeaderValue,
        header::{CONTENT_TYPE, ORIGIN},
    };

    use super::{HeaderAdmission, admit_single_header};

    #[test]
    fn exact_one_header_admission_preserves_missing_and_invalid_states() {
        let mut headers = HeaderMap::new();
        assert!(matches!(
            admit_single_header(&headers, ORIGIN),
            HeaderAdmission::Missing
        ));

        headers.insert(ORIGIN, HeaderValue::from_static("https://console.example"));
        assert!(matches!(
            admit_single_header(&headers, ORIGIN),
            HeaderAdmission::Valid("https://console.example")
        ));

        headers.append(ORIGIN, HeaderValue::from_static("https://other.example"));
        assert!(matches!(
            admit_single_header(&headers, ORIGIN),
            HeaderAdmission::Invalid
        ));

        headers.insert(CONTENT_TYPE, HeaderValue::from_bytes(b"\x80").unwrap());
        assert!(matches!(
            admit_single_header(&headers, CONTENT_TYPE),
            HeaderAdmission::Invalid
        ));
    }
}
