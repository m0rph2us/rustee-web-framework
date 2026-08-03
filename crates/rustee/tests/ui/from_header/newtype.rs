use rustee::FromHeader;

#[derive(rustee_macros::FromHeader)]
#[rustee(header = "x-request-id")]
struct RequestId(u64);

fn main() {
    let value = <RequestId as FromHeader>::from_header(&rustee::__http::HeaderValue::from_static("7"));
    assert!(value.is_ok());
}
