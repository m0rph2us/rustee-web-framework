#![cfg(feature = "macros")]

use rustee::{FromHeader, StatusCode};

#[derive(rustee::FromHeader, Debug, Eq, PartialEq)]
#[rustee(header = "x-request-count")]
struct RequestCount(u64);

#[test]
fn derive_parses_a_valid_header_and_preserves_bad_request_failures() {
    let parsed =
        <RequestCount as FromHeader>::from_header(&rustee::__http::HeaderValue::from_static("42"))
            .expect("a numeric header must parse");
    assert_eq!(parsed, RequestCount(42));

    let invalid = <RequestCount as FromHeader>::from_header(
        &rustee::__http::HeaderValue::from_static("not-a-number"),
    )
    .expect_err("an invalid newtype value must fail");
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(<RequestCount as FromHeader>::NAME, "x-request-count");
}
