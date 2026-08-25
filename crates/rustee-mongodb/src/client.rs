//! `MongoDB` client configuration, lifecycle, readiness, and transaction helpers.

use std::{fmt, time::Duration};

use mongodb::{
    Client, ClientSession, Database,
    bson::doc,
    options::{ClientOptions, TransactionOptions},
};

/// Connection settings for a long-lived `MongoDB` [`Client`].
#[derive(Clone, Eq, PartialEq)]
pub struct MongoConfig {
    uri: String,
    database: String,
    app_name: Option<String>,
    connect_timeout: Duration,
    server_selection_timeout: Duration,
}

impl MongoConfig {
    /// Creates settings with finite driver timeouts and no default application name.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::EmptyDatabase`] when `database` is blank.
    pub fn new(uri: impl Into<String>, database: impl Into<String>) -> Result<Self, ConfigError> {
        let database = database.into();
        if database.trim().is_empty() {
            return Err(ConfigError::EmptyDatabase);
        }

        Ok(Self {
            uri: uri.into(),
            database,
            app_name: None,
            connect_timeout: Duration::from_secs(5),
            server_selection_timeout: Duration::from_secs(5),
        })
    }

    /// Sets the driver-visible application name.
    #[must_use]
    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = Some(app_name.into());
        self
    }

    /// Sets the TCP connection deadline used by the `MongoDB` driver.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroConnectTimeout`] when `timeout` is zero.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Result<Self, ConfigError> {
        if timeout.is_zero() {
            return Err(ConfigError::ZeroConnectTimeout);
        }
        self.connect_timeout = timeout;
        Ok(self)
    }

    /// Sets the server selection deadline used by the `MongoDB` driver.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ZeroServerSelectionTimeout`] when `timeout` is zero.
    pub fn with_server_selection_timeout(mut self, timeout: Duration) -> Result<Self, ConfigError> {
        if timeout.is_zero() {
            return Err(ConfigError::ZeroServerSelectionTimeout);
        }
        self.server_selection_timeout = timeout;
        Ok(self)
    }

    /// Returns the database selected for application collection handles and readiness checks.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }
}

impl fmt::Debug for MongoConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoConfig")
            .field("uri", &"[REDACTED]")
            .field("database", &"[REDACTED]")
            .field("database_length", &self.database.len())
            .field("app_name", &self.app_name.as_ref().map(|_| "[REDACTED]"))
            .field("app_name_length", &self.app_name.as_ref().map(String::len))
            .field("connect_timeout", &self.connect_timeout)
            .field("server_selection_timeout", &self.server_selection_timeout)
            .finish()
    }
}

/// Errors in `MongoDB` connection configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// The database name was empty or whitespace only.
    #[error("MongoDB database name must not be blank")]
    EmptyDatabase,
    /// The TCP connection deadline was zero.
    #[error("MongoDB connection timeout must be non-zero")]
    ZeroConnectTimeout,
    /// The server selection deadline was zero.
    #[error("MongoDB server selection timeout must be non-zero")]
    ZeroServerSelectionTimeout,
}

/// Failure while configuring the `MongoDB` client.
///
/// Display and debug output retain only a safe failure category. The driver source remains
/// available through [`std::error::Error::source`] for trusted startup diagnostics.
#[derive(thiserror::Error)]
pub enum MongoConnectError {
    /// The driver rejected the URI or resolved client options.
    #[error("MongoDB client configuration failed")]
    Driver(#[source] mongodb::error::Error),
}

impl fmt::Debug for MongoConnectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoConnectError")
            .field("kind", &"client_configuration_failed")
            .finish()
    }
}

/// Failure while executing a `MongoDB` readiness probe.
///
/// Display and debug output retain only a safe failure category. The driver source remains
/// available through [`std::error::Error::source`] for trusted diagnostics.
#[derive(thiserror::Error)]
pub enum MongoReadinessError {
    /// The driver could not select a server or execute the `ping` command.
    #[error("MongoDB readiness failed")]
    Driver(#[source] mongodb::error::Error),
}

impl fmt::Debug for MongoReadinessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoReadinessError")
            .field("kind", &"readiness_failed")
            .finish()
    }
}

/// Parses driver options and creates the shared `MongoDB` client.
///
/// A returned client is safe to clone and share between concurrent handlers. The driver owns its
/// connection pools, so applications must not wrap it in a separate pool.
///
/// # Errors
///
/// Returns a content-free [`MongoConnectError`] when the URI is invalid or its DNS configuration
/// cannot be resolved. The trusted driver source remains available through the error chain.
pub async fn connect(config: &MongoConfig) -> Result<Client, MongoConnectError> {
    let mut options = ClientOptions::parse(config.uri.as_str())
        .await
        .map_err(MongoConnectError::Driver)?;
    options.app_name.clone_from(&config.app_name);
    options.connect_timeout = Some(config.connect_timeout);
    options.server_selection_timeout = Some(config.server_selection_timeout);
    Client::with_options(options).map_err(MongoConnectError::Driver)
}

/// Creates a typed driver database handle using the configured database name.
#[must_use]
pub fn database(client: &Client, config: &MongoConfig) -> Database {
    client.database(config.database())
}

/// Sends a `ping` command to the configured database for readiness evaluation.
///
/// # Errors
///
/// Returns a content-free [`MongoReadinessError`] when `MongoDB` cannot select a server or rejects
/// the command. The trusted driver source remains available through the error chain.
pub async fn readiness(client: &Client, config: &MongoConfig) -> Result<(), MongoReadinessError> {
    database(client, config)
        .run_command(doc! { "ping": 1 })
        .await
        .map(|_| ())
        .map_err(MongoReadinessError::Driver)
}

/// Starts one explicit transaction session with the driver's default transaction options.
///
/// Every operation that must be atomic must use the returned [`ClientSession`]. Applications must
/// commit or abort it explicitly, must not run transaction operations in parallel, and must keep
/// external side effects out of a retriable transaction body.
///
/// # Errors
///
/// Returns a driver error when a server cannot create a session or start the transaction. `MongoDB`
/// transactions require a replica set or sharded deployment.
pub async fn begin_transaction(client: &Client) -> mongodb::error::Result<ClientSession> {
    let mut session = client.start_session().await?;
    session.start_transaction().await?;
    Ok(session)
}

/// Starts one explicit transaction session with caller-selected driver transaction options.
///
/// The returned session has the same ownership and retry boundary as [`begin_transaction`].
/// Rustee deliberately does not retry the complete transaction callback because it cannot know
/// whether application code includes external side effects or is idempotent.
///
/// # Errors
///
/// Returns a driver error when a server cannot create a session or start the transaction.
pub async fn begin_transaction_with_options(
    client: &Client,
    options: TransactionOptions,
) -> mongodb::error::Result<ClientSession> {
    let mut session = client.start_session().await?;
    session.start_transaction().with_options(options).await?;
    Ok(session)
}

/// Stops the driver's background workers and closes its connections during graceful shutdown.
pub async fn shutdown(client: Client) {
    client.shutdown().await;
}
