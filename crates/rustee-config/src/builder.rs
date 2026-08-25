use std::{collections::BTreeMap, fmt};

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::{ConfigError, Source};

mod environment;

use environment::collect_environment_values;
pub use environment::{
    MAX_ENVIRONMENT_KEY_BYTES, MAX_ENVIRONMENT_PATH_SEGMENTS, MAX_ENVIRONMENT_VALUE_BYTES,
    MAX_ENVIRONMENT_VARIABLES,
};

/// Collects object-shaped configuration sources with deterministic precedence.
#[derive(Clone, Default)]
pub struct ConfigBuilder {
    sources: BTreeMap<Source, Value>,
}

impl fmt::Debug for ConfigBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let configured_sources = self.sources.keys().copied().collect::<Vec<_>>();
        formatter
            .debug_struct("ConfigBuilder")
            .field("configured_sources", &configured_sources)
            .finish()
    }
}

impl ConfigBuilder {
    /// Creates an empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one JSON object for an explicit source.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not a JSON object or `source` was already configured.
    pub fn source(mut self, source: Source, value: Value) -> Result<Self, ConfigError> {
        self.ensure_source_available(source)?;
        if !value.is_object() {
            return Err(ConfigError::new(
                source.key(),
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
    /// Returns an error when `prefix` is empty, matching variables have a non-Unicode name or
    /// value, exceed configured count/key/path/value bounds, contain an empty, duplicate, or
    /// conflicting normalized nested path, or the environment source was already configured.
    ///
    /// A previously configured environment source is rejected before process environment input is
    /// inspected.
    pub fn environment(mut self, prefix: &str) -> Result<Self, ConfigError> {
        self.ensure_source_available(Source::Environment)?;
        let values = collect_environment_values(prefix, std::env::vars_os())?;
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
                merge_values(&mut merged, value);
            }
        }
        serde_json::from_value(merged).map_err(|_| {
            ConfigError::new(
                "configuration",
                "the value does not match the expected schema",
            )
        })
    }

    fn ensure_source_available(&self, source: Source) -> Result<(), ConfigError> {
        if self.sources.contains_key(&source) {
            return Err(ConfigError::new(
                source.key(),
                "a configuration source may only be configured once",
            ));
        }
        Ok(())
    }
}

fn merge_values(target: &mut Value, source: &Value) {
    match (target, source) {
        (Value::Object(target), Value::Object(source)) => {
            for (key, value) in source {
                merge_values(target.entry(key.clone()).or_insert(Value::Null), value);
            }
        }
        (target, source) => *target = source.clone(),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};

    use serde::Deserialize;
    use serde_json::json;

    use super::{
        ConfigBuilder, MAX_ENVIRONMENT_KEY_BYTES, MAX_ENVIRONMENT_PATH_SEGMENTS,
        MAX_ENVIRONMENT_VALUE_BYTES, MAX_ENVIRONMENT_VARIABLES, collect_environment_values,
    };
    use crate::Source;

    #[test]
    fn builder_debug_lists_sources_without_configuration_keys_or_values() {
        let builder = ConfigBuilder::new()
            .source(
                Source::Deployment,
                json!({"database": {"url": "postgres://user:password@database.internal/app"}}),
            )
            .unwrap();

        let debug = format!("{builder:?}");
        assert!(debug.contains("configured_sources: [Deployment]"));
        assert!(!debug.contains("database"));
        assert!(!debug.contains("postgres://user:password@database.internal/app"));
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
        let builder = ConfigBuilder::new()
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
            .unwrap();

        let settings = builder.build::<Settings>().unwrap();
        let repeated = builder.build::<Settings>().unwrap();

        assert_eq!(settings.http.port, 8443);
        assert_eq!(settings.token, "environment");
        assert_eq!(repeated, settings);
    }

    #[test]
    fn sources_cannot_be_replaced_silently() {
        let private_value = "postgres://user:password@database.internal/app";
        let error = ConfigBuilder::new()
            .source(Source::Deployment, json!({"database_url": private_value}))
            .expect("first source is accepted")
            .source(Source::Deployment, json!({"database_url": "replacement"}))
            .expect_err("a source must be configured exactly once");

        assert_eq!(error.key(), "deployment");
        assert_eq!(
            error.to_string(),
            "invalid configuration for deployment: a configuration source may only be configured once"
        );
        assert!(!error.to_string().contains(private_value));
        assert!(!format!("{error:?}").contains(private_value));
    }

    #[test]
    fn duplicate_environment_sources_fail_before_environment_input_is_examined() {
        let error = ConfigBuilder::new()
            .source(Source::Environment, json!({"token": "configured"}))
            .expect("first environment source is accepted")
            .environment("")
            .expect_err("duplicate sources are rejected before prefix validation or collection");

        assert_eq!(error.key(), "environment");
        assert_eq!(
            error.to_string(),
            "invalid configuration for environment: a configuration source may only be configured once"
        );
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
    fn environment_paths_normalize_nested_json_values() {
        let values = collect_environment_values(
            "RUSTEE_",
            [
                ("RUSTEE_HTTP__PORT".to_owned(), "3000".to_owned()),
                ("RUSTEE_FEATURES__ENABLED".to_owned(), "true".to_owned()),
            ],
        )
        .expect("distinct nested environment paths are accepted");

        assert_eq!(
            serde_json::Value::Object(values),
            json!({"http": {"port": 3000}, "features": {"enabled": true}})
        );
    }

    #[test]
    fn environment_collection_rejects_an_empty_prefix_without_reading_values() {
        let private_value = "postgres://user:password@database.internal/app";
        let error =
            collect_environment_values("", [("DATABASE_URL".to_owned(), private_value.to_owned())])
                .expect_err("an empty prefix would admit the whole process environment");

        assert_eq!(error.key(), "environment");
        assert_eq!(
            error.to_string(),
            "invalid configuration for environment: environment variable prefix must not be empty"
        );
        assert!(!error.to_string().contains(private_value));
        assert!(!format!("{error:?}").contains(private_value));
    }

    #[test]
    fn environment_paths_reject_empty_or_ambiguous_normalized_segments() {
        for variables in [
            vec![("RUSTEE___PORT".to_owned(), "3000".to_owned())],
            vec![
                ("RUSTEE_HTTP".to_owned(), "3000".to_owned()),
                ("RUSTEE_HTTP__PORT".to_owned(), "4000".to_owned()),
            ],
            vec![
                ("RUSTEE_HTTP__PORT".to_owned(), "4000".to_owned()),
                ("RUSTEE_HTTP".to_owned(), "3000".to_owned()),
            ],
            vec![
                ("RUSTEE_HTTP__PORT".to_owned(), "3000".to_owned()),
                ("RUSTEE_http__port".to_owned(), "4000".to_owned()),
            ],
        ] {
            let error = collect_environment_values("RUSTEE_", variables)
                .expect_err("ambiguous environment paths must be rejected");
            assert_eq!(error.key(), "environment");
            assert!(!error.to_string().contains("3000"));
            assert!(!error.to_string().contains("4000"));
        }
    }

    #[test]
    fn environment_paths_reject_nonportable_segment_characters_without_reading_values() {
        let private_value = "postgres://user:password@database.internal/app";
        let non_ascii_path = format!("HTTP__P{}RT", '\u{00D6}');
        for path in ["HTTP__TRACE ID", "HTTP__PORT.NAME", &non_ascii_path] {
            let error = collect_environment_values(
                "RUSTEE_",
                [(format!("RUSTEE_{path}"), private_value.to_owned())],
            )
            .expect_err("environment path segments must use the documented portable grammar");
            assert_eq!(error.key(), "environment");
            assert!(error.to_string().contains("ASCII letters"));
            assert!(!error.to_string().contains(private_value));
            assert!(!format!("{error:?}").contains(private_value));
        }
    }

    #[test]
    fn environment_collection_stops_at_fixed_input_bounds_without_exposing_values() {
        let variables = (0..=MAX_ENVIRONMENT_VARIABLES)
            .map(|index| {
                (
                    format!("RUSTEE_SERVICE__SETTING_{index}"),
                    "true".to_owned(),
                )
            })
            .chain(std::iter::once_with(|| {
                panic!("environment collection must stop after the first excess variable")
            }));
        let error = collect_environment_values("RUSTEE_", variables).unwrap_err();
        assert_eq!(error.key(), "environment");
        assert!(error.to_string().contains("variable count exceeds"));

        let oversized_key = format!("RUSTEE_{}", "x".repeat(MAX_ENVIRONMENT_KEY_BYTES));
        let error = collect_environment_values("RUSTEE_", [(oversized_key, "true".to_owned())])
            .unwrap_err();
        assert!(error.to_string().contains("variable name exceeds"));

        let path = vec!["LEVEL"; MAX_ENVIRONMENT_PATH_SEGMENTS + 1].join("__");
        let error = collect_environment_values(
            "RUSTEE_",
            [(format!("RUSTEE_{path}"), "private-value".to_owned())],
        )
        .unwrap_err();
        assert!(error.to_string().contains("path depth exceeds"));
        assert!(!error.to_string().contains("private-value"));

        let private_value = format!("private-{}", "x".repeat(MAX_ENVIRONMENT_VALUE_BYTES));
        let error = collect_environment_values(
            "RUSTEE_",
            [("RUSTEE_TOKEN".to_owned(), private_value.clone())],
        )
        .unwrap_err();
        assert!(error.to_string().contains("variable value exceeds"));
        assert!(!error.to_string().contains(&private_value));
    }

    #[cfg(unix)]
    #[test]
    fn matching_non_unicode_environment_names_and_values_fail_without_panicking_or_leaking() {
        let invalid_name = OsString::from_vec(b"RUSTEE_\xFF".to_vec());
        let invalid_value = OsString::from_vec(b"private-value-\xFF".to_vec());
        for variables in [
            vec![(invalid_name, OsString::from("value"))],
            vec![(OsString::from("RUSTEE_TOKEN"), invalid_value)],
        ] {
            let error = collect_environment_values("RUSTEE_", variables)
                .expect_err("matching non-Unicode environment input must fail");
            assert_eq!(error.key(), "environment");
            assert!(error.to_string().contains("not valid Unicode"));
            assert!(!error.to_string().contains("private-value"));
        }
    }
}
