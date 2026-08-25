use reqwest::{Client, StatusCode};
use url::Url;

use crate::{
    QueueSnapshot, RabbitMqManagementConfig, RabbitMqManagementError,
    config::is_safe_url_path_segment,
};

pub(crate) async fn fetch_queue_snapshot(
    client: &Client,
    config: &RabbitMqManagementConfig,
    queue: &str,
) -> Result<QueueSnapshot, RabbitMqManagementError> {
    let endpoint = queue_endpoint(&config.base_url, &config.vhost, queue)?;
    let response = client
        .get(endpoint)
        .basic_auth(&config.username, Some(&config.password))
        .send()
        .await
        .map_err(|_| RabbitMqManagementError::Request)?;

    if response.status() == StatusCode::NOT_FOUND {
        return Err(RabbitMqManagementError::QueueNotFound);
    }
    if !response.status().is_success() {
        return Err(RabbitMqManagementError::Request);
    }

    decode_queue_snapshot(response, config.max_response_bytes).await
}

async fn decode_queue_snapshot(
    mut response: reqwest::Response,
    max_response_bytes: usize,
) -> Result<QueueSnapshot, RabbitMqManagementError> {
    if !has_json_content_type(response.headers()) {
        return Err(RabbitMqManagementError::MalformedResponse);
    }
    if response
        .content_length()
        .is_some_and(|length| length > max_response_bytes as u64)
    {
        return Err(RabbitMqManagementError::ResponseTooLarge);
    }

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| RabbitMqManagementError::Request)?
    {
        if chunk.len() > max_response_bytes.saturating_sub(body.len()) {
            return Err(RabbitMqManagementError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }

    serde_json::from_slice(&body).map_err(|_| RabbitMqManagementError::MalformedResponse)
}

fn has_json_content_type(headers: &reqwest::header::HeaderMap) -> bool {
    single_content_type(headers)
        .is_some_and(|media_type| media_type.eq_ignore_ascii_case("application/json"))
}

fn single_content_type(headers: &reqwest::header::HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(reqwest::header::CONTENT_TYPE).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let value = value.to_str().ok()?;
    value.split(';').next().map(str::trim)
}

fn queue_endpoint(base: &Url, vhost: &str, queue: &str) -> Result<Url, RabbitMqManagementError> {
    if !is_safe_url_path_segment(vhost) || !is_safe_url_path_segment(queue) {
        return Err(RabbitMqManagementError::InvalidEndpoint);
    }
    base.join(&format!(
        "api/queues/{}/{}",
        urlencoding(vhost),
        urlencoding(queue)
    ))
    .map_err(|_| RabbitMqManagementError::InvalidEndpoint)
}

fn urlencoding(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};

    use super::has_json_content_type;

    #[test]
    fn management_json_response_requires_one_json_media_type() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
        assert!(has_json_content_type(&headers));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        assert!(!has_json_content_type(&headers));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        assert!(!has_json_content_type(&headers));

        headers.clear();
        assert!(!has_json_content_type(&headers));
    }
}
