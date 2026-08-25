//! Bounded JSON decoding and sanitized failures for trusted OIDC HTTP endpoints.

use rustee_core::is_standard_json_media_type;
use serde::de::DeserializeOwned;

pub(crate) const MAX_JSON_RESPONSE_BYTES: usize = 1024 * 1024;

/// Sanitized failure from a trusted OIDC HTTP endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OidcHttpError {
    /// The request, HTTP status validation, or response stream failed.
    #[error("OIDC provider request failed")]
    Request,
    /// A successful provider response exceeded the fixed memory bound.
    #[error("OIDC provider response exceeded the fixed size limit")]
    ResponseTooLarge,
    /// A successful provider response did not declare exactly one JSON media type.
    #[error("OIDC provider response did not declare a JSON content type")]
    UnexpectedContentType,
    /// A successful provider response was not valid for its expected JSON model.
    #[error("OIDC provider response was malformed")]
    MalformedResponse,
}

pub(crate) async fn decode_json_response<T>(
    mut response: reqwest::Response,
) -> Result<T, OidcHttpError>
where
    T: DeserializeOwned,
{
    if !has_json_content_type(response.headers()) {
        return Err(OidcHttpError::UnexpectedContentType);
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_JSON_RESPONSE_BYTES as u64)
    {
        return Err(OidcHttpError::ResponseTooLarge);
    }

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| OidcHttpError::Request)? {
        if chunk.len() > MAX_JSON_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(OidcHttpError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| OidcHttpError::MalformedResponse)
}

fn has_json_content_type(headers: &reqwest::header::HeaderMap) -> bool {
    let mut values = headers.get_all(reqwest::header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(value) = value.to_str() else {
        return false;
    };
    is_standard_json_media_type(value)
}

#[cfg(test)]
mod tests {
    use reqwest::{
        Client,
        header::{CONTENT_TYPE, HeaderMap, HeaderValue},
    };
    use serde_json::Value;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{OidcHttpError, decode_json_response, has_json_content_type};

    #[test]
    fn decoder_requires_one_json_or_structured_json_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/jwk-set+json; charset=utf-8"),
        );
        assert!(has_json_content_type(&headers));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        assert!(!has_json_content_type(&headers));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/+json"));
        assert!(!has_json_content_type(&headers));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        assert!(!has_json_content_type(&headers));
    }

    #[tokio::test]
    async fn decoder_rejects_a_response_above_its_fixed_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: keep-alive\r\n\r\n",
                super::MAX_JSON_RESPONSE_BYTES + 1
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        });
        let response = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();

        assert_eq!(
            decode_json_response::<Value>(response).await.unwrap_err(),
            OidcHttpError::ResponseTooLarge
        );
        server.await.unwrap();
    }
}
