//! `MongoDB` client lifecycle, transaction, change-stream, and tenant BSON-boundary helpers.
//!
//! This crate intentionally leaves collection queries, BSON persistence models, and HTTP DTOs in
//! application code. It configures one long-lived official driver client per process.

use std::{error::Error as StdError, fmt, future::Future, time::Duration};

use futures_util::future::BoxFuture;
use mongodb::{
    Client, ClientSession, Database,
    bson::{Bson, Document, doc},
    change_stream::{ChangeStream, event::ResumeToken},
    options::{ClientOptions, TransactionOptions},
};
pub use rustee_tenant::TenantContext;
use serde::de::DeserializeOwned;

pub use mongodb;

/// The required BSON field for documents protected by [`MongoTenantScope`].
pub const MONGO_TENANT_FIELD: &str = "tenant_id";

const MAX_CHANGE_STREAM_CONSUMER_BYTES: usize = 255;

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
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the server selection deadline used by the `MongoDB` driver.
    #[must_use]
    pub fn with_server_selection_timeout(mut self, timeout: Duration) -> Self {
        self.server_selection_timeout = timeout;
        self
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
            .field("database", &self.database)
            .field("app_name", &self.app_name)
            .field("connect_timeout", &self.connect_timeout)
            .field("server_selection_timeout", &self.server_selection_timeout)
            .finish()
    }
}

/// Errors in `MongoDB` connection configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The database name was empty or whitespace only.
    #[error("MongoDB database name must not be blank")]
    EmptyDatabase,
}

/// A trusted tenant boundary for `MongoDB` BSON reads and mutations.
///
/// Construct this only from a verified [`TenantContext`]. Use [`Self::filter`] for every read,
/// update, and delete, and [`Self::aggregation_pipeline`] to insert the target collection's first
/// aggregation match. Use [`Self::document`] before inserting or replacing an application
/// document. The raw driver remains available for deliberately unscoped administration and
/// migration work, so this helper cannot turn `MongoDB` into database-enforced row-level security.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MongoTenantScope {
    tenant: TenantContext,
}

impl MongoTenantScope {
    /// Creates a BSON boundary for the supplied trusted tenant.
    #[must_use]
    pub fn new(tenant: TenantContext) -> Self {
        Self { tenant }
    }

    /// Returns the trusted tenant context used by this scope.
    #[must_use]
    pub fn tenant(&self) -> &TenantContext {
        &self.tenant
    }

    /// Adds the trusted tenant equality as an outer `AND` condition.
    ///
    /// This composition remains authoritative even when `filter` contains logical operators such
    /// as `$or`: every matching document must still have the scope's [`MONGO_TENANT_FIELD`].
    #[must_use]
    pub fn filter(&self, filter: Document) -> Document {
        let mut tenant_filter = Document::new();
        tenant_filter.insert(MONGO_TENANT_FIELD, self.tenant.tenant());
        doc! { "$and": [tenant_filter, filter] }
    }

    /// Builds an aggregation pipeline whose first stage authoritatively scopes the input
    /// collection to the trusted tenant.
    ///
    /// This protects the collection passed to `Collection::aggregate`. Use [`Self::lookup_stage`]
    /// and [`Self::union_with_stage`] to scope one direct foreign collection with the same trusted
    /// tenant. Nested foreign pipelines and documents supplied through raw driver APIs remain
    /// application access-control boundaries.
    #[must_use]
    pub fn aggregation_pipeline(
        &self,
        filter: Document,
        stages: impl IntoIterator<Item = Document>,
    ) -> Vec<Document> {
        let mut pipeline = vec![doc! { "$match": self.filter(filter) }];
        pipeline.extend(stages);
        pipeline
    }

    /// Builds a `$lookup` stage whose foreign collection begins with this trusted tenant scope.
    ///
    /// `filter` normally contains the `$expr` join predicate that references `let_variables`.
    /// Rustee prepends the authoritative tenant match before that predicate and before every
    /// caller-supplied stage. `from`, `as_field`, `let_variables`, and later stages are
    /// application-owned query shape, not request input. Nested foreign pipelines still need an
    /// explicit scope and access-control review.
    ///
    /// # Errors
    ///
    /// Returns [`TenantScopeError::InvalidAggregationIdentifier`] when the collection or output
    /// field is blank, contains a NUL byte, or starts with `$`.
    pub fn lookup_stage(
        &self,
        from: impl AsRef<str>,
        as_field: impl AsRef<str>,
        let_variables: Document,
        filter: Document,
        stages: impl IntoIterator<Item = Document>,
    ) -> Result<Document, TenantScopeError> {
        let from = from.as_ref();
        let as_field = as_field.as_ref();
        validate_aggregation_identifier(from)?;
        validate_aggregation_identifier(as_field)?;

        let pipeline = self
            .aggregation_pipeline(filter, stages)
            .into_iter()
            .map(Bson::Document)
            .collect::<Vec<_>>();
        Ok(doc! {
            "$lookup": {
                "from": from,
                "let": let_variables,
                "pipeline": pipeline,
                "as": as_field,
            },
        })
    }

    /// Builds a `$unionWith` stage whose foreign collection begins with this trusted tenant scope.
    ///
    /// The stage scopes the named foreign collection only. Caller-supplied nested lookups or raw
    /// driver queries remain separate application authorization boundaries.
    ///
    /// # Errors
    ///
    /// Returns [`TenantScopeError::InvalidAggregationIdentifier`] when the collection name is
    /// blank, contains a NUL byte, or starts with `$`.
    pub fn union_with_stage(
        &self,
        collection: impl AsRef<str>,
        filter: Document,
        stages: impl IntoIterator<Item = Document>,
    ) -> Result<Document, TenantScopeError> {
        let collection = collection.as_ref();
        validate_aggregation_identifier(collection)?;

        let pipeline = self
            .aggregation_pipeline(filter, stages)
            .into_iter()
            .map(Bson::Document)
            .collect::<Vec<_>>();
        Ok(doc! {
            "$unionWith": {
                "coll": collection,
                "pipeline": pipeline,
            },
        })
    }

    /// Adds the trusted tenant field to an inserted or replaced BSON document.
    ///
    /// A document that already carries the same string tenant is accepted to support typed models
    /// that include the field. A missing field is inserted. Any other value is rejected instead of
    /// silently rewriting a client-controlled tenant identifier.
    ///
    /// # Errors
    ///
    /// Returns [`TenantScopeError::TenantMismatch`] when the document already contains a different
    /// or non-string tenant value.
    pub fn document(&self, mut document: Document) -> Result<Document, TenantScopeError> {
        match document.get(MONGO_TENANT_FIELD) {
            None => {
                document.insert(MONGO_TENANT_FIELD, self.tenant.tenant());
                Ok(document)
            }
            Some(Bson::String(value)) if value == self.tenant.tenant() => Ok(document),
            Some(_) => Err(TenantScopeError::TenantMismatch),
        }
    }

    /// Validates an operator-style update that must not change the tenant field.
    ///
    /// Use [`Self::document`] for replacement documents. This accepts classic update operators
    /// such as `$set`, `$inc`, and `$push`, then rejects direct mutations of
    /// [`MONGO_TENANT_FIELD`]. Aggregation-pipeline updates and `$rename` are deliberately not
    /// accepted because their computed document shape cannot be made tenant-safe generically.
    ///
    /// # Errors
    ///
    /// Returns [`TenantScopeError::ReplacementRequiresDocument`] for a replacement-style document
    /// and [`TenantScopeError::TenantFieldMutation`] when an update could change the tenant field.
    pub fn update(&self, update: Document) -> Result<Document, TenantScopeError> {
        if update.keys().any(|operator| !operator.starts_with('$')) {
            return Err(TenantScopeError::ReplacementRequiresDocument);
        }
        if update.contains_key("$rename") || contains_tenant_field(&update) {
            return Err(TenantScopeError::TenantFieldMutation);
        }
        Ok(update)
    }
}

fn contains_tenant_field(document: &Document) -> bool {
    document.iter().any(|(field, value)| {
        field == MONGO_TENANT_FIELD
            || field
                .strip_prefix(MONGO_TENANT_FIELD)
                .is_some_and(|suffix| suffix.starts_with('.'))
            || value_contains_tenant_field(value)
    })
}

fn value_contains_tenant_field(value: &Bson) -> bool {
    match value {
        Bson::Document(document) => contains_tenant_field(document),
        Bson::Array(values) => values.iter().any(value_contains_tenant_field),
        _ => false,
    }
}

fn validate_aggregation_identifier(identifier: &str) -> Result<(), TenantScopeError> {
    if identifier.trim().is_empty() || identifier.contains('\0') || identifier.starts_with('$') {
        return Err(TenantScopeError::InvalidAggregationIdentifier);
    }
    Ok(())
}

/// A document supplied to [`MongoTenantScope::document`] conflicted with its trusted tenant.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TenantScopeError {
    /// The document's tenant field was missing a string value equal to the trusted context.
    #[error("MongoDB document tenant must match the trusted tenant context")]
    TenantMismatch,
    /// A replacement update must use [`MongoTenantScope::document`] to retain the tenant field.
    #[error("MongoDB replacement updates must use the tenant-scoped document helper")]
    ReplacementRequiresDocument,
    /// An update could modify, remove, or rename the tenant field.
    #[error("MongoDB tenant-scoped updates must not modify the tenant field")]
    TenantFieldMutation,
    /// A `$lookup`/`$unionWith` collection or `$lookup` output field was not a safe identifier.
    #[error("MongoDB aggregation identifiers must be non-blank, NUL-free, and not start with $")]
    InvalidAggregationIdentifier,
}

/// A bounded stable identity for one durable change-stream consumer checkpoint.
///
/// A consumer identifies the exact watched scope and pipeline contract, not merely a process. Do
/// not reuse it after changing the watched collection, filter, event decoding, or durable handler
/// semantics. Run at most one active worker for an identity unless a deployment-owned leader
/// writer coordination prevents stale workers from saving an older token.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ChangeStreamConsumer(String);

impl ChangeStreamConsumer {
    /// Creates one non-blank, NUL-free, bounded consumer identity.
    ///
    /// # Errors
    ///
    /// Returns [`ChangeStreamConsumerError::InvalidConsumer`] when `consumer` is not safe for a
    /// durable checkpoint key.
    pub fn new(consumer: impl Into<String>) -> Result<Self, ChangeStreamConsumerError> {
        let consumer = consumer.into();
        if consumer.trim().is_empty()
            || consumer.contains('\0')
            || consumer.len() > MAX_CHANGE_STREAM_CONSUMER_BYTES
        {
            return Err(ChangeStreamConsumerError::InvalidConsumer);
        }
        Ok(Self(consumer))
    }

    /// Returns the stable consumer identity for a storage adapter key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ChangeStreamConsumer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ChangeStreamConsumer")
            .field(&"[REDACTED]")
            .finish()
    }
}

/// Invalid durable change-stream consumer identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ChangeStreamConsumerError {
    /// The identity was blank, contained a NUL byte, or exceeded the storage bound.
    #[error("change-stream consumer must be non-blank, NUL-free, and bounded")]
    InvalidConsumer,
}

/// Durable storage boundary for opaque `MongoDB` change-stream resume tokens.
///
/// Load the token before creating a new driver stream with `Watch::resume_after`. Save it only
/// after a received event's durable, idempotent handler succeeds. This contract deliberately does
/// not start workers, retry handlers, resolve invalidated tokens, or coordinate active workers;
/// those failure and exclusive-writer policies remain application deployment concerns.
pub trait ChangeStreamCheckpointStore: Clone + Send + Sync + 'static {
    /// Storage-specific failure type.
    type Error: StdError + Send + Sync + 'static;

    /// Loads the last durable resume token for a consumer identity.
    fn load(
        &self,
        consumer: ChangeStreamConsumer,
    ) -> BoxFuture<'static, Result<Option<ResumeToken>, Self::Error>>;

    /// Replaces the last durable resume token after successful event handling.
    fn save(
        &self,
        consumer: ChangeStreamConsumer,
        resume_token: ResumeToken,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Parses driver options and creates the shared `MongoDB` client.
///
/// A returned client is safe to clone and share between concurrent handlers. The driver owns its
/// connection pools, so applications must not wrap it in a separate pool.
///
/// # Errors
///
/// Returns a driver error when the URI is invalid or its DNS configuration cannot be resolved.
pub async fn connect(config: &MongoConfig) -> mongodb::error::Result<Client> {
    let mut options = ClientOptions::parse(config.uri.as_str()).await?;
    options.app_name.clone_from(&config.app_name);
    options.connect_timeout = Some(config.connect_timeout);
    options.server_selection_timeout = Some(config.server_selection_timeout);
    Client::with_options(options)
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
/// Returns a driver error when `MongoDB` cannot select a server or rejects the command.
pub async fn readiness(client: &Client, config: &MongoConfig) -> mongodb::error::Result<()> {
    database(client, config)
        .run_command(doc! { "ping": 1 })
        .await
        .map(|_| ())
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

/// The outcome of waiting for one `MongoDB` change stream item with a shutdown boundary.
#[derive(Debug)]
pub enum ChangeStreamNext<T> {
    /// One event was read. Persist `resume_token` only after durable event handling succeeds.
    Event {
        /// The driver-decoded change event.
        event: T,
        /// The opaque token that resumes from this observation point when the stream restarts.
        resume_token: Option<ResumeToken>,
    },
    /// The stream ended without an event. Its last observed token remains available for recovery.
    Ended {
        /// The most recent opaque resume token known by the driver.
        resume_token: Option<ResumeToken>,
    },
    /// Shutdown resolved before the next event was read. No event was handed to the application;
    /// stop using that stream and let the supervisor create a new one on its next start.
    Shutdown {
        /// The most recent opaque resume token known by the driver.
        resume_token: Option<ResumeToken>,
    },
}

/// Waits for one change-stream event or a shutdown signal without starting an unbounded worker.
///
/// Shutdown has priority when it is already ready. A shutdown result ends the stream's ownership
/// in the caller's worker; drop it and let the supervisor create a new stream on restart. When an
/// event wins, the returned token is only a checkpoint candidate: persist it after the event's
/// durable, idempotent handling succeeds. The official driver owns resume attempts for a live
/// stream; this helper does not invent queue delivery guarantees, retry failed handlers, or
/// persist tokens.
///
/// # Errors
///
/// Returns a driver error from the next change-stream operation. The caller owns supervisor
/// restart/backoff and decides whether a stored token remains valid for recovery.
pub async fn next_change_until<T, Shutdown>(
    stream: &mut ChangeStream<T>,
    shutdown: Shutdown,
) -> mongodb::error::Result<ChangeStreamNext<T>>
where
    T: DeserializeOwned,
    Shutdown: Future<Output = ()>,
{
    tokio::select! {
        biased;
        () = shutdown => Ok(ChangeStreamNext::Shutdown {
            resume_token: stream.resume_token(),
        }),
        next = stream.next_if_any() => match next? {
            Some(event) => Ok(ChangeStreamNext::Event {
                event,
                resume_token: stream.resume_token(),
            }),
            None => Ok(ChangeStreamNext::Ended {
                resume_token: stream.resume_token(),
            }),
        },
    }
}

/// Stops the driver's background workers and closes its connections during graceful shutdown.
pub async fn shutdown(client: Client) {
    client.shutdown().await;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mongodb::bson::{Document, doc};

    use super::{
        ChangeStreamConsumer, ChangeStreamConsumerError, ConfigError, MONGO_TENANT_FIELD,
        MongoConfig, MongoTenantScope, TenantContext, TenantScopeError,
    };

    #[test]
    fn secret_uri_is_not_exposed_in_debug_output() {
        let config = MongoConfig::new("mongodb://user:password@localhost:27017", "app").unwrap();
        assert!(!format!("{config:?}").contains("password"));
    }

    #[test]
    fn blank_database_is_rejected() {
        let error = MongoConfig::new("mongodb://localhost:27017", " ").unwrap_err();
        assert!(matches!(error, ConfigError::EmptyDatabase));
    }

    #[test]
    fn finite_driver_timeouts_are_shown_in_debug_output() {
        let config = MongoConfig::new("mongodb://localhost:27017", "app")
            .unwrap()
            .with_connect_timeout(Duration::from_secs(2))
            .with_server_selection_timeout(Duration::from_secs(3));
        let debug = format!("{config:?}");
        assert!(debug.contains("2s"));
        assert!(debug.contains("3s"));
    }

    #[test]
    fn tenant_scope_composes_an_authoritative_outer_filter() {
        let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

        assert_eq!(
            scope.filter(doc! { "$or": [{ "status": "open" }, { "owner": "alice" }] }),
            doc! {
                "$and": [
                    { MONGO_TENANT_FIELD: "tenant-a" },
                    { "$or": [{ "status": "open" }, { "owner": "alice" }] },
                ],
            }
        );
    }

    #[test]
    fn tenant_scope_starts_aggregation_with_an_authoritative_match() {
        let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

        assert_eq!(
            scope.aggregation_pipeline(
                doc! { "$or": [{ "status": "open" }, { "owner": "alice" }] },
                [doc! { "$group": { "_id": "$owner", "count": { "$sum": 1_i32 } } }],
            ),
            vec![
                doc! {
                    "$match": {
                        "$and": [
                            { MONGO_TENANT_FIELD: "tenant-a" },
                            { "$or": [{ "status": "open" }, { "owner": "alice" }] },
                        ],
                    },
                },
                doc! { "$group": { "_id": "$owner", "count": { "$sum": 1_i32 } } },
            ]
        );
    }

    #[test]
    fn tenant_scope_builds_scoped_lookup_and_union_stages() {
        let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

        assert_eq!(
            scope
                .lookup_stage(
                    "order_items",
                    "items",
                    doc! { "order_id": "$_id" },
                    doc! { "$expr": { "$eq": ["$order_id", "$$order_id"] } },
                    [doc! { "$project": { "sku": 1_i32 } }],
                )
                .unwrap(),
            doc! {
                "$lookup": {
                    "from": "order_items",
                    "let": { "order_id": "$_id" },
                    "pipeline": [
                        {
                            "$match": {
                                "$and": [
                                    { MONGO_TENANT_FIELD: "tenant-a" },
                                    { "$expr": { "$eq": ["$order_id", "$$order_id"] } },
                                ],
                            },
                        },
                        { "$project": { "sku": 1_i32 } },
                    ],
                    "as": "items",
                },
            }
        );
        assert_eq!(
            scope
                .union_with_stage("archived_orders", doc! { "state": "open" }, [])
                .unwrap(),
            doc! {
                "$unionWith": {
                    "coll": "archived_orders",
                    "pipeline": [
                        {
                            "$match": {
                                "$and": [
                                    { MONGO_TENANT_FIELD: "tenant-a" },
                                    { "state": "open" },
                                ],
                            },
                        },
                    ],
                },
            }
        );
    }

    #[test]
    fn tenant_scope_rejects_unsafe_foreign_aggregation_identifiers() {
        let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

        assert_eq!(
            scope
                .lookup_stage(" ", "items", Document::new(), Document::new(), [])
                .unwrap_err(),
            TenantScopeError::InvalidAggregationIdentifier
        );
        assert_eq!(
            scope
                .lookup_stage("items", "$items", Document::new(), Document::new(), [])
                .unwrap_err(),
            TenantScopeError::InvalidAggregationIdentifier
        );
        assert_eq!(
            scope
                .union_with_stage("archive\0orders", Document::new(), [])
                .unwrap_err(),
            TenantScopeError::InvalidAggregationIdentifier
        );
    }

    #[test]
    fn tenant_scope_adds_or_validates_the_document_tenant() {
        let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

        assert_eq!(
            scope.document(doc! { "status": "open" }).unwrap(),
            doc! { "status": "open", MONGO_TENANT_FIELD: "tenant-a" }
        );
        assert!(
            scope
                .document(doc! { MONGO_TENANT_FIELD: "tenant-a" })
                .is_ok()
        );
        assert_eq!(
            scope
                .document(doc! { MONGO_TENANT_FIELD: "tenant-b" })
                .unwrap_err(),
            TenantScopeError::TenantMismatch
        );
        assert_eq!(
            scope
                .document(doc! { MONGO_TENANT_FIELD: 7_i32 })
                .unwrap_err(),
            TenantScopeError::TenantMismatch
        );
    }

    #[test]
    fn tenant_scope_rejects_updates_that_can_change_the_tenant() {
        let scope = MongoTenantScope::new(TenantContext::new("tenant-a").unwrap());

        assert!(
            scope
                .update(doc! { "$set": { "status": "closed" } })
                .is_ok()
        );
        assert_eq!(
            scope
                .update(doc! { "$set": { MONGO_TENANT_FIELD: "tenant-b" } })
                .unwrap_err(),
            TenantScopeError::TenantFieldMutation
        );
        assert_eq!(
            scope
                .update(doc! { "$unset": { "tenant_id.profile": "" } })
                .unwrap_err(),
            TenantScopeError::TenantFieldMutation
        );
        assert_eq!(
            scope
                .update(doc! { "$rename": { "name": "renamed" } })
                .unwrap_err(),
            TenantScopeError::TenantFieldMutation
        );
        assert_eq!(
            scope.update(doc! { "status": "closed" }).unwrap_err(),
            TenantScopeError::ReplacementRequiresDocument
        );
    }

    #[test]
    fn change_stream_consumer_is_bounded_and_redacted() {
        assert_eq!(
            ChangeStreamConsumer::new(" ").unwrap_err(),
            ChangeStreamConsumerError::InvalidConsumer
        );
        assert_eq!(
            ChangeStreamConsumer::new("source\0consumer").unwrap_err(),
            ChangeStreamConsumerError::InvalidConsumer
        );
        assert_eq!(
            ChangeStreamConsumer::new("a".repeat(256)).unwrap_err(),
            ChangeStreamConsumerError::InvalidConsumer
        );

        let consumer = ChangeStreamConsumer::new("orders-projection-v1").unwrap();
        assert_eq!(consumer.as_str(), "orders-projection-v1");
        assert!(!format!("{consumer:?}").contains("orders-projection-v1"));
    }
}
