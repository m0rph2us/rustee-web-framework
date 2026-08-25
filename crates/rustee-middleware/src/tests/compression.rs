use std::{convert::Infallible, io::Cursor};

use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder};
use bytes::Bytes;
use http::{
    HeaderMap, HeaderValue, Method, Request as HttpRequest, StatusCode,
    header::{
        ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, RANGE, VARY,
    },
};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use rustee_core::{BoxError, IntoResponse, empty_body, response};
use rustee_router::App;
use tokio::io::{AsyncReadExt, BufReader};
use tower::{Layer, ServiceExt};

use crate::CompressionLayer;

const DOCUMENT: &str = "A useful Rustee document that compresses cleanly.";

#[tokio::test]
async fn compression_negotiates_brotli_and_updates_vary() {
    let service = CompressionLayer::new().layer(App::new().get("/document", || async {
        let mut response = DOCUMENT.into_response();
        response
            .headers_mut()
            .insert(CONTENT_LENGTH, HeaderValue::from_static("47"));
        response
            .headers_mut()
            .append(VARY, HeaderValue::from_static("Origin"));
        response
    }));

    let response = service
        .oneshot(compression_request("gzip;q=0.4, br"))
        .await
        .unwrap();
    assert_eq!(response.headers()[CONTENT_ENCODING], "br");
    assert!(response.headers().get(CONTENT_LENGTH).is_none());
    assert_eq!(
        response
            .headers()
            .get_all(VARY)
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["Origin", "Accept-Encoding"]
    );

    assert_eq!(
        decode_brotli(response.into_body().collect().await.unwrap().to_bytes()).await,
        DOCUMENT.as_bytes()
    );
}

#[tokio::test]
async fn compression_honors_gzip_quality_and_existing_coding() {
    let service = CompressionLayer::new().layer(App::new().get("/document", || async { DOCUMENT }));
    let response = service
        .clone()
        .oneshot(compression_request("br;q=0.5, gzip;q=0.8"))
        .await
        .unwrap();
    assert_eq!(response.headers()[CONTENT_ENCODING], "gzip");
    assert_eq!(
        decode_gzip(response.into_body().collect().await.unwrap().to_bytes()).await,
        DOCUMENT.as_bytes()
    );

    let existing = CompressionLayer::new().layer(App::new().get("/document", || async {
        let mut response = DOCUMENT.into_response();
        response
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("identity"));
        response
    }));
    let response = existing.oneshot(compression_request("gzip")).await.unwrap();
    assert_eq!(response.headers()[CONTENT_ENCODING], "identity");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        DOCUMENT
    );
}

#[tokio::test]
async fn compression_marks_an_identity_representation_as_varying() {
    let service = CompressionLayer::new().layer(App::new().get("/document", || async { DOCUMENT }));
    let response = service
        .oneshot(compression_request("identity"))
        .await
        .unwrap();

    assert!(response.headers().get(CONTENT_ENCODING).is_none());
    assert_eq!(response.headers()[VARY], "Accept-Encoding");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        DOCUMENT
    );
}

#[tokio::test]
async fn compression_bypasses_range_head_and_no_transform_responses() {
    let service = CompressionLayer::new().layer(App::new().get("/document", || async { DOCUMENT }));
    let mut range = compression_request("gzip");
    range
        .headers_mut()
        .insert(RANGE, HeaderValue::from_static("bytes=0-9"));
    let response = service.clone().oneshot(range).await.unwrap();
    assert!(response.headers().get(CONTENT_ENCODING).is_none());
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        DOCUMENT
    );

    let head = CompressionLayer::new().layer(tower::service_fn(|_| async {
        Ok::<_, Infallible>(DOCUMENT.into_response())
    }));
    let mut request = compression_request("gzip");
    *request.method_mut() = Method::HEAD;
    let response = head.oneshot(request).await.unwrap();
    assert!(response.headers().get(CONTENT_ENCODING).is_none());
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        DOCUMENT
    );

    let no_transform = CompressionLayer::new().layer(App::new().get("/document", || async {
        let mut response = DOCUMENT.into_response();
        response.headers_mut().insert(
            CACHE_CONTROL,
            HeaderValue::from_static("public, no-transform"),
        );
        response
    }));
    let response = no_transform
        .oneshot(compression_request("gzip"))
        .await
        .unwrap();
    assert!(response.headers().get(CONTENT_ENCODING).is_none());
}

#[tokio::test]
async fn compression_preserves_response_trailers() {
    let service = CompressionLayer::new().layer(tower::service_fn(|_| async {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-checksum", HeaderValue::from_static("verified"));
        let source = async_stream::stream! {
            yield Ok::<_, BoxError>(Frame::data(Bytes::from_static(DOCUMENT.as_bytes())));
            yield Ok(Frame::trailers(trailers));
        };
        let body = BodyExt::boxed_unsync(StreamBody::new(source));
        let mut response = response(StatusCode::OK, body);
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        Ok::<_, Infallible>(response)
    }));

    let response = service.oneshot(compression_request("gzip")).await.unwrap();
    let collected = response.into_body().collect().await.unwrap();
    assert_eq!(
        collected.trailers().unwrap()["x-checksum"],
        HeaderValue::from_static("verified")
    );
    assert_eq!(decode_gzip(collected.to_bytes()).await, DOCUMENT.as_bytes());
}

fn compression_request(accept_encoding: &str) -> rustee_core::Request {
    HttpRequest::builder()
        .method(Method::GET)
        .uri("/document")
        .header(ACCEPT_ENCODING, accept_encoding)
        .body(empty_body())
        .unwrap()
}

async fn decode_gzip(body: Bytes) -> Vec<u8> {
    let mut decoder = GzipDecoder::new(BufReader::new(Cursor::new(body)));
    let mut output = Vec::new();
    decoder.read_to_end(&mut output).await.unwrap();
    output
}

async fn decode_brotli(body: Bytes) -> Vec<u8> {
    let mut decoder = BrotliDecoder::new(BufReader::new(Cursor::new(body)));
    let mut output = Vec::new();
    decoder.read_to_end(&mut output).await.unwrap();
    output
}
