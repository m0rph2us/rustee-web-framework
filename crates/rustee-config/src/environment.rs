use std::{env::VarError, str::FromStr};

use crate::ConfigError;

/// Reads and parses a required environment value without exposing it in errors.
///
/// # Errors
///
/// Returns an error when the value is absent, not Unicode, or cannot be parsed as `T`.
pub fn required_env<T>(key: &str) -> Result<T, ConfigError>
where
    T: FromStr,
{
    required_env_value(key, std::env::var(key))
}

/// Reads and parses an optional environment value.
///
/// # Errors
///
/// Returns an error when a present value is not Unicode or cannot be parsed as `T`.
pub fn optional_env<T>(key: &str) -> Result<Option<T>, ConfigError>
where
    T: FromStr,
{
    match std::env::var(key) {
        Ok(value) => parse_value(key, &value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(ConfigError::new(key, "value is not valid Unicode"))
        }
    }
}

fn parse_value<T>(key: &str, value: &str) -> Result<T, ConfigError>
where
    T: FromStr,
{
    value
        .parse()
        .map_err(|_: T::Err| ConfigError::new(key, "the value could not be parsed"))
}

fn required_env_value<T>(key: &str, value: Result<String, VarError>) -> Result<T, ConfigError>
where
    T: FromStr,
{
    match value {
        Ok(value) => parse_value(key, &value),
        Err(VarError::NotPresent) => Err(ConfigError::new(key, "the required value is not set")),
        Err(VarError::NotUnicode(_)) => Err(ConfigError::new(key, "value is not valid Unicode")),
    }
}

#[cfg(test)]
mod tests {
    use std::{env::VarError, ffi::OsString};

    use super::{parse_value, required_env_value};

    #[test]
    fn parse_errors_do_not_expose_the_rejected_value() {
        let rejected_value = "not-a-port-with-a-secret";
        let error = parse_value::<u16>("RUSTEE_HTTP__PORT", rejected_value).unwrap_err();
        assert!(!error.to_string().contains(rejected_value));
        assert!(!format!("{error:?}").contains(rejected_value));
    }

    #[test]
    fn required_non_unicode_values_are_distinguished_without_rendering_them() {
        let error = required_env_value::<u16>(
            "RUSTEE_HTTP__PORT",
            Err(VarError::NotUnicode(OsString::from("private-value"))),
        )
        .expect_err("a non-Unicode required value must be rejected");

        assert_eq!(error.key(), "RUSTEE_HTTP__PORT");
        assert!(error.to_string().contains("value is not valid Unicode"));
        assert!(!error.to_string().contains("private-value"));
        assert!(!format!("{error:?}").contains("private-value"));
    }
}
