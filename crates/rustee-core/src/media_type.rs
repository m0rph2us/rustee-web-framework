//! Standard media-type predicates shared by Rustee HTTP boundaries.

/// Returns whether one `Content-Type` field value declares standard JSON.
///
/// The value may be `application/json` or `application/*+json`, with optional media-type
/// parameters. Callers remain responsible for requiring exactly one valid HTTP field value.
#[must_use]
pub fn is_standard_json_media_type(value: &str) -> bool {
    let media_type = value.split(';').next().unwrap_or_default().trim();
    if media_type.eq_ignore_ascii_case("application/json") {
        return true;
    }
    let Some((top_level, subtype)) = media_type.split_once('/') else {
        return false;
    };
    top_level.eq_ignore_ascii_case("application")
        && !subtype.contains('/')
        && subtype.len() > "+json".len()
        && subtype
            .get(subtype.len() - "+json".len()..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case("+json"))
}

#[cfg(test)]
mod tests {
    use super::is_standard_json_media_type;

    #[test]
    fn accepts_standard_json_and_structured_json_media_types() {
        for value in [
            "application/json",
            "APPLICATION/JSON; charset=utf-8",
            "application/problem+json",
            "application/jwk-set+JSON; charset=utf-8",
        ] {
            assert!(is_standard_json_media_type(value), "{value}");
        }
    }

    #[test]
    fn rejects_nonstandard_json_like_media_types() {
        for value in [
            "text/problem+json",
            "application/jsonp",
            "application/+json",
            "application/problem+json/extra",
        ] {
            assert!(!is_standard_json_media_type(value), "{value}");
        }
    }
}
