//! ACL-scoped retrieval identity and authorization values.

use std::{collections::BTreeSet, fmt};

use rustee_ai::MAX_TENANT_BYTES;

/// Largest number of document IDs accepted in one explicit retrieval authorization scope.
pub const MAX_RETRIEVAL_SCOPE_DOCUMENTS: usize = 10_000;
/// Largest UTF-8 byte length accepted for one retrieval document or chunk identifier.
pub const MAX_RETRIEVAL_IDENTIFIER_BYTES: usize = 255;

/// Document permission scope derived by application authorization before retrieval.
#[derive(Clone, Eq, PartialEq)]
pub struct RetrievalScope {
    tenant: String,
    allowed_document_ids: BTreeSet<String>,
}

impl RetrievalScope {
    /// Creates a tenant scope with an explicit bounded non-empty allowlist of document IDs.
    ///
    /// # Errors
    ///
    /// Returns [`RetrievalScopeError`] when the tenant or allowlist is invalid, or when more than
    /// [`MAX_RETRIEVAL_SCOPE_DOCUMENTS`] document IDs are supplied. Tenant values use the shared
    /// Rustee AI tenant bound; document identifiers use [`MAX_RETRIEVAL_IDENTIFIER_BYTES`].
    pub fn new(
        tenant: impl Into<String>,
        allowed_document_ids: impl IntoIterator<Item = String>,
    ) -> Result<Self, RetrievalScopeError> {
        let tenant = tenant.into();
        if tenant.trim().is_empty() {
            return Err(RetrievalScopeError::BlankTenant);
        }
        if tenant.len() > MAX_TENANT_BYTES {
            return Err(RetrievalScopeError::TenantTooLong);
        }
        if tenant.contains('\0') {
            return Err(RetrievalScopeError::TenantContainsNul);
        }
        let mut allowed = BTreeSet::new();
        for (received_document_ids, id) in allowed_document_ids.into_iter().enumerate() {
            if received_document_ids >= MAX_RETRIEVAL_SCOPE_DOCUMENTS {
                return Err(RetrievalScopeError::TooManyDocumentIds);
            }
            if id.trim().is_empty() {
                return Err(RetrievalScopeError::BlankDocumentId);
            }
            if id.len() > MAX_RETRIEVAL_IDENTIFIER_BYTES {
                return Err(RetrievalScopeError::DocumentIdTooLong);
            }
            if id.contains('\0') {
                return Err(RetrievalScopeError::DocumentIdContainsNul);
            }
            allowed.insert(id);
        }
        if allowed.is_empty() {
            return Err(RetrievalScopeError::EmptyAllowlist);
        }
        Ok(Self {
            tenant,
            allowed_document_ids: allowed,
        })
    }

    /// Returns the tenant constraint sent to a retrieval store.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns whether the scope permits this document identifier.
    #[must_use]
    pub fn permits_document(&self, document_id: &str) -> bool {
        self.allowed_document_ids.contains(document_id)
    }
}

impl fmt::Debug for RetrievalScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetrievalScope")
            .field("tenant", &"[REDACTED]")
            .field("allowed_document_count", &self.allowed_document_ids.len())
            .finish()
    }
}

/// Invalid retrieval-scope content.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RetrievalScopeError {
    /// Tenant context must originate in validated application identity.
    #[error("RAG retrieval tenant must not be blank")]
    BlankTenant,
    /// Tenant context must fit the shared Rustee AI tenant bound.
    #[error("RAG retrieval tenant exceeded the supported length")]
    TenantTooLong,
    /// Tenant context must remain representable across vector-store adapters.
    #[error("RAG retrieval tenant must not contain a NUL byte")]
    TenantContainsNul,
    /// The application must make document authorization explicit.
    #[error("RAG retrieval allowlist must not be empty")]
    EmptyAllowlist,
    /// An allowed document must have a stable identifier.
    #[error("RAG retrieval document ID must not be blank")]
    BlankDocumentId,
    /// An allowed document identifier exceeded the bounded store-filter size.
    #[error("RAG retrieval document ID exceeded the supported length")]
    DocumentIdTooLong,
    /// An allowed document identifier cannot be represented safely by every store adapter.
    #[error("RAG retrieval document ID must not contain a NUL byte")]
    DocumentIdContainsNul,
    /// Application authorization must not materialize an unbounded store filter.
    #[error("RAG retrieval scope exceeds the framework document ID limit")]
    TooManyDocumentIds,
}
