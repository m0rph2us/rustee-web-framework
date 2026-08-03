//! Small, explicit configuration primitives.

use std::{collections::BTreeMap, fmt, str::FromStr};

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

/// A configuration error that identifies a key without exposing its value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError {
    key: String,
    message: String,
}

impl ConfigError {
    /// Creates a configuration error for a named key.
    pub fn new(key: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            message: message.into(),
        }
    }

    /// Returns the configuration key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid configuration for {}: {}",
            self.key, self.message
        )
    }
}

impl std::error::Error for ConfigError {}

/// The fixed configuration precedence order used by [`ConfigBuilder`].
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Source {
    /// Values compiled into application defaults.
    Defaults,
    /// An explicitly selected local configuration file, parsed by the application.
    Local,
    /// Environment-provided values.
    Environment,
    /// Values injected by the deployment platform or secret manager.
    Deployment,
}

/// Collects object-shaped configuration sources with deterministic precedence.
#[derive(Clone, Debug, Default)]
pub struct ConfigBuilder {
    sources: BTreeMap<Source, Value>,
}

impl ConfigBuilder {
    /// Creates an empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a JSON object for one explicit source.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not a JSON object.
    pub fn source(mut self, source: Source, value: Value) -> Result<Self, ConfigError> {
        if !value.is_object() {
            return Err(ConfigError::new(
                source_name(source),
                "a configuration source must be a JSON object",
            ));
        }
        self.sources.insert(source, value);
        Ok(self)
    }

    /// Loads `PREFIX_*` environment variables as a source.
    ///
    /// A double underscore represents a nested object path. For example,
    /// `RUSTEE_HTTP__PORT=3000` becomes `{ "http": { "port": 3000 } }`.
    ///
    /// # Errors
    ///
    /// Returns an error only if the collected environment source cannot be represented safely.
    pub fn environment(mut self, prefix: &str) -> Result<Self, ConfigError> {
        let mut values = Map::new();
        for (key, raw_value) in std::env::vars() {
            let Some(path) = key.strip_prefix(prefix) else {
                continue;
            };
            if path.is_empty() {
                continue;
            }
            let path = path
                .split("__")
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>();
            insert_path(&mut values, &path, parse_environment_value(&raw_value));
        }
        self.sources
            .insert(Source::Environment, Value::Object(values));
        Ok(self)
    }

    /// Deserializes the merged values into one application-defined type.
    ///
    /// # Errors
    ///
    /// Returns an error when the merged values do not match `T`.
    pub fn build<T>(&self) -> Result<T, ConfigError>
    where
        T: DeserializeOwned,
    {
        let mut merged = Value::Object(Map::new());
        for source in [
            Source::Defaults,
            Source::Local,
            Source::Environment,
            Source::Deployment,
        ] {
            if let Some(value) = self.sources.get(&source) {
                merge_values(&mut merged, value.clone());
            }
        }
        serde_json::from_value(merged).map_err(|_| {
            ConfigError::new(
                "configuration",
                "the value does not match the expected schema",
            )
        })
    }
}

/// A value whose debug and display forms never expose its contents.
#[derive(Clone, Eq, PartialEq)]
pub struct Secret(String);

impl Secret {
    /// Stores a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the secret only to code that explicitly asks for it.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Secret([REDACTED])")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// Reads and parses a required environment value without exposing it in errors.
///
/// # Errors
///
/// Returns an error when the value is absent or cannot be parsed as `T`.
pub fn required_env<T>(key: &str) -> Result<T, ConfigError>
where
    T: FromStr,
{
    let value =
        std::env::var(key).map_err(|_| ConfigError::new(key, "the required value is not set"))?;
    parse_value(key, &value)
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

fn source_name(source: Source) -> &'static str {
    match source {
        Source::Defaults => "defaults",
        Source::Local => "local",
        Source::Environment => "environment",
        Source::Deployment => "deployment",
    }
}

fn parse_environment_value(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_owned()))
}

fn insert_path(values: &mut Map<String, Value>, path: &[String], value: Value) {
    let Some((head, tail)) = path.split_first() else {
        return;
    };
    if tail.is_empty() {
        values.insert(head.clone(), value);
        return;
    }

    let child = values
        .entry(head.clone())
        .or_insert_with(|| Value::Object(Map::new()));
    if !child.is_object() {
        *child = Value::Object(Map::new());
    }
    insert_path(
        child.as_object_mut().expect("object was created above"),
        tail,
        value,
    );
}

fn merge_values(target: &mut Value, source: Value) {
    match (target, source) {
        (Value::Object(target), Value::Object(source)) => {
            for (key, value) in source {
                merge_values(target.entry(key).or_insert(Value::Null), value);
            }
        }
        (target, source) => *target = source,
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::{ConfigBuilder, Secret, Source, parse_value};

    #[test]
    fn secret_redacts_debug_and_display() {
        let secret = Secret::new("do-not-log-me");
        assert!(!format!("{secret:?}").contains("do-not-log-me"));
        assert!(!secret.to_string().contains("do-not-log-me"));
    }

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct Settings {
        http: Http,
        token: String,
    }

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct Http {
        port: u16,
    }

    #[test]
    fn sources_merge_in_documented_precedence_order() {
        let settings = ConfigBuilder::new()
            .source(
                Source::Defaults,
                json!({"http": {"port": 3000}, "token": "default"}),
            )
            .unwrap()
            .source(Source::Local, json!({"http": {"port": 4000}}))
            .unwrap()
            .source(Source::Environment, json!({"token": "environment"}))
            .unwrap()
            .source(Source::Deployment, json!({"http": {"port": 8443}}))
            .unwrap()
            .build::<Settings>()
            .unwrap();

        assert_eq!(settings.http.port, 8443);
        assert_eq!(settings.token, "environment");
    }

    #[test]
    fn schema_errors_do_not_expose_the_rejected_value() {
        let rejected_value = "postgres://user:password@database.internal/app";
        let error = ConfigBuilder::new()
            .source(
                Source::Deployment,
                json!({"http": {"port": rejected_value}}),
            )
            .unwrap()
            .build::<Settings>()
            .unwrap_err();

        assert_eq!(error.key(), "configuration");
        assert!(!error.to_string().contains(rejected_value));
        assert!(!format!("{error:?}").contains(rejected_value));
    }

    #[test]
    fn environment_parse_errors_do_not_expose_the_rejected_value() {
        let rejected_value = "not-a-port-with-a-secret";
        let error = parse_value::<u16>("RUSTEE_HTTP__PORT", rejected_value).unwrap_err();
        assert!(!error.to_string().contains(rejected_value));
        assert!(!format!("{error:?}").contains(rejected_value));
    }
}
