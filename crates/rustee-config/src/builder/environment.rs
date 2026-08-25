//! OS-safe environment snapshots, nested path admission, and JSON value parsing.

use std::ffi::OsString;

use serde_json::{Map, Value};

use crate::ConfigError;

/// Maximum number of matching environment variables admitted into one configuration source.
pub const MAX_ENVIRONMENT_VARIABLES: usize = 256;
/// Maximum byte length of one matching environment variable name, including its prefix.
pub const MAX_ENVIRONMENT_KEY_BYTES: usize = 512;
/// Maximum nested path depth admitted from one matching environment variable name.
pub const MAX_ENVIRONMENT_PATH_SEGMENTS: usize = 16;
/// Maximum byte length of one matching environment variable value before JSON parsing.
pub const MAX_ENVIRONMENT_VALUE_BYTES: usize = 64 * 1024;

pub(super) fn collect_environment_values<K, V>(
    prefix: &str,
    environment: impl IntoIterator<Item = (K, V)>,
) -> Result<Map<String, Value>, ConfigError>
where
    K: Into<OsString>,
    V: Into<OsString>,
{
    if prefix.is_empty() {
        return Err(environment_prefix_error());
    }
    let mut values = Map::new();
    let mut matched_variables = 0_usize;
    for (key, raw_value) in environment {
        let key = match key.into().into_string() {
            Ok(key) => key,
            Err(key) => {
                if key.to_string_lossy().starts_with(prefix) {
                    return Err(environment_unicode_error());
                }
                continue;
            }
        };
        let Some(path) = key.strip_prefix(prefix) else {
            continue;
        };
        if path.is_empty() {
            continue;
        }
        if key.len() > MAX_ENVIRONMENT_KEY_BYTES {
            return Err(environment_key_limit_error());
        }
        if matched_variables == MAX_ENVIRONMENT_VARIABLES {
            return Err(environment_variable_limit_error());
        }
        let mut normalized_path = Vec::with_capacity(MAX_ENVIRONMENT_PATH_SEGMENTS);
        for segment in path.split("__") {
            if segment.is_empty() {
                return Err(environment_path_error(
                    "environment variable paths must not contain empty segments",
                ));
            }
            if !valid_environment_segment(segment) {
                return Err(environment_path_error(
                    "environment variable paths must contain only ASCII letters, digits, underscores, or hyphens",
                ));
            }
            if normalized_path.len() == MAX_ENVIRONMENT_PATH_SEGMENTS {
                return Err(environment_path_limit_error());
            }
            normalized_path.push(segment.to_ascii_lowercase());
        }
        let raw_value = raw_value
            .into()
            .into_string()
            .map_err(|_| environment_unicode_error())?;
        if raw_value.len() > MAX_ENVIRONMENT_VALUE_BYTES {
            return Err(environment_value_limit_error());
        }
        insert_path(
            &mut values,
            &normalized_path,
            parse_environment_value(&raw_value),
        )?;
        matched_variables += 1;
    }
    Ok(values)
}

fn valid_environment_segment(segment: &str) -> bool {
    segment
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn parse_environment_value(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_owned()))
}

fn insert_path(
    values: &mut Map<String, Value>,
    path: &[String],
    value: Value,
) -> Result<(), ConfigError> {
    let Some((head, tail)) = path.split_first() else {
        return Err(environment_path_error(
            "environment variable paths must not be empty",
        ));
    };
    if tail.is_empty() {
        if values.contains_key(head) {
            return Err(environment_path_error(
                "environment variable paths must not be duplicated or conflict",
            ));
        }
        values.insert(head.clone(), value);
        return Ok(());
    }

    let child = values
        .entry(head.clone())
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(child) = child.as_object_mut() else {
        return Err(environment_path_error(
            "environment variable paths must not be duplicated or conflict",
        ));
    };
    insert_path(child, tail, value)
}

fn environment_path_error(message: &'static str) -> ConfigError {
    ConfigError::new("environment", message)
}

fn environment_prefix_error() -> ConfigError {
    ConfigError::new(
        "environment",
        "environment variable prefix must not be empty",
    )
}

fn environment_variable_limit_error() -> ConfigError {
    ConfigError::new(
        "environment",
        "matching environment variable count exceeds the bounded limit",
    )
}

fn environment_key_limit_error() -> ConfigError {
    ConfigError::new(
        "environment",
        "matching environment variable name exceeds the bounded limit",
    )
}

fn environment_path_limit_error() -> ConfigError {
    ConfigError::new(
        "environment",
        "environment variable path depth exceeds the bounded limit",
    )
}

fn environment_value_limit_error() -> ConfigError {
    ConfigError::new(
        "environment",
        "matching environment variable value exceeds the bounded limit",
    )
}

fn environment_unicode_error() -> ConfigError {
    ConfigError::new(
        "environment",
        "matching environment variable name or value is not valid Unicode",
    )
}
