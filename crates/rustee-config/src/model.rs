use std::fmt;

/// A configuration error that identifies a key without exposing its value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError {
    key: String,
    message: &'static str,
}

impl ConfigError {
    /// Creates a configuration error from a crate-defined value-free category.
    ///
    /// Construction is crate-private so arbitrary runtime input cannot become public error text.
    pub(crate) fn new(key: impl Into<String>, message: &'static str) -> Self {
        Self {
            key: key.into(),
            message,
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

/// The fixed configuration precedence order used by [`crate::ConfigBuilder`].
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

impl Source {
    pub(crate) const fn key(self) -> &'static str {
        match self {
            Self::Defaults => "defaults",
            Self::Local => "local",
            Self::Environment => "environment",
            Self::Deployment => "deployment",
        }
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

#[cfg(test)]
mod tests {
    use super::{ConfigError, Secret};

    #[test]
    fn configuration_errors_render_the_identifying_key_and_fixed_category_only() {
        let error = ConfigError::new("RUSTEE_API_TOKEN", "the value could not be parsed");

        assert_eq!(
            error.to_string(),
            "invalid configuration for RUSTEE_API_TOKEN: the value could not be parsed"
        );
        assert_eq!(
            format!("{error:?}"),
            "ConfigError { key: \"RUSTEE_API_TOKEN\", message: \"the value could not be parsed\" }"
        );
    }

    #[test]
    fn secret_redacts_debug_and_display() {
        let secret = Secret::new("do-not-log-me");
        assert!(!format!("{secret:?}").contains("do-not-log-me"));
        assert!(!secret.to_string().contains("do-not-log-me"));
    }
}
