//! Bounded `WWW-Authenticate` aggregation and Bearer metadata-url admission.

use reqwest::header::{HeaderMap, WWW_AUTHENTICATE};
use url::Url;

use crate::{McpOAuthError, config::valid_resource_url};

pub(super) const MAX_WWW_AUTHENTICATE_BYTES: usize = 8192;

pub(super) fn resource_metadata_url(header: &str) -> Result<Option<Url>, McpOAuthError> {
    if header.len() > MAX_WWW_AUTHENTICATE_BYTES {
        return Err(McpOAuthError::InvalidChallenge);
    }
    let Some(value) = bearer_parameter(header, "resource_metadata")? else {
        return Ok(None);
    };
    let url = Url::parse(value).map_err(|_| McpOAuthError::InvalidChallenge)?;
    if !valid_resource_url(&url) {
        return Err(McpOAuthError::InvalidChallenge);
    }
    Ok(Some(url))
}

pub(super) fn www_authenticate_value(headers: &HeaderMap) -> Result<Option<String>, McpOAuthError> {
    let mut challenge = None;
    for value in &headers.get_all(WWW_AUTHENTICATE) {
        let value = value
            .to_str()
            .map_err(|_| McpOAuthError::InvalidChallenge)?;
        let existing_bytes = challenge.as_ref().map_or(0, String::len);
        let separator_bytes = usize::from(challenge.is_some()) * 2;
        if value.len() > MAX_WWW_AUTHENTICATE_BYTES.saturating_sub(existing_bytes + separator_bytes)
        {
            return Err(McpOAuthError::InvalidChallenge);
        }
        if let Some(challenge) = &mut challenge {
            challenge.push_str(", ");
            challenge.push_str(value);
        } else {
            challenge = Some(value.to_owned());
        }
    }
    Ok(challenge)
}

fn bearer_parameter<'a>(header: &'a str, name: &str) -> Result<Option<&'a str>, McpOAuthError> {
    let mut offset = 0;
    let mut bearer = false;
    let mut found = None;
    while let Some((segment, next_offset)) = next_challenge_segment(header, offset) {
        offset = next_offset;
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        if let Some((scheme, parameters)) = challenge_segment(segment) {
            bearer = scheme.eq_ignore_ascii_case("bearer");
            if bearer
                && let Some(value) = challenge_parameter(parameters, name)
                && found.replace(value).is_some()
            {
                return Err(McpOAuthError::InvalidChallenge);
            }
        } else if bearer
            && let Some(value) = challenge_parameter(segment, name)
            && found.replace(value).is_some()
        {
            return Err(McpOAuthError::InvalidChallenge);
        }
    }
    Ok(found)
}

fn next_challenge_segment(header: &str, offset: usize) -> Option<(&str, usize)> {
    if offset >= header.len() {
        return None;
    }
    let bytes = header.as_bytes();
    let mut index = offset;
    let mut quoted = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' if quoted && index + 1 < bytes.len() => {
                index += 2;
                continue;
            }
            b'"' => quoted = !quoted,
            b',' if !quoted => return Some((&header[offset..index], index + 1)),
            _ => {}
        }
        index += 1;
    }
    Some((&header[offset..], header.len()))
}

fn challenge_segment(segment: &str) -> Option<(&str, &str)> {
    let segment = segment.trim();
    let first_whitespace = segment.find(|character: char| character.is_ascii_whitespace());
    match first_whitespace {
        Some(index) if !segment[..index].contains('=') => {
            Some((&segment[..index], segment[index..].trim_start()))
        }
        None if !segment.contains('=') => Some((segment, "")),
        _ => None,
    }
}

fn challenge_parameter<'a>(parameters: &'a str, name: &str) -> Option<&'a str> {
    let (key, value) = parameters.trim().split_once('=')?;
    key.trim()
        .eq_ignore_ascii_case(name)
        .then(|| value.trim().strip_prefix('"')?.strip_suffix('"'))
        .flatten()
}
