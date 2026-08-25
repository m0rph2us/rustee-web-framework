mod extractors;
mod nesting;
mod routing;

use http::{Method, Request as HttpRequest};
use rustee_core::{Request, empty_body};

pub(super) fn request(method: Method, uri: &str) -> Request {
    HttpRequest::builder()
        .method(method)
        .uri(uri)
        .body(empty_body())
        .unwrap()
}
