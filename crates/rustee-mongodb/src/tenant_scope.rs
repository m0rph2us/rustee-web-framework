//! Trusted tenant BSON filters, documents, updates, and aggregation stages.

use mongodb::bson::{Bson, Document, doc};

use crate::TenantContext;

/// The required BSON field for documents protected by [`MongoTenantScope`].
pub const MONGO_TENANT_FIELD: &str = "tenant_id";

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
    pub const fn new(tenant: TenantContext) -> Self {
        Self { tenant }
    }

    /// Returns the trusted tenant context used by this scope.
    #[must_use]
    pub const fn tenant(&self) -> &TenantContext {
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
