use futures_util::stream;
use http_body_util::BodyExt;
use rustee::{Bytes, StatusCode, response, stream_body};

#[tokio::test]
async fn facade_exposes_owned_streaming_response_construction() {
    let body = stream_body(stream::iter([
        Ok::<_, std::io::Error>(Bytes::from_static(b"Rust")),
        Ok(Bytes::from_static(b"ee")),
    ]));
    let response = response(StatusCode::OK, body);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("infallible stream should collect")
        .to_bytes();

    assert_eq!(body.as_ref(), b"Rustee");
}
