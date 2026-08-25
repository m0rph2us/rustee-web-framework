//! Trusted invocation identity and idempotent tool-execution context.

use std::fmt;

/// Maximum UTF-8 byte length accepted for a durable AI tenant identifier.
pub const MAX_TENANT_BYTES: usize = 255;
/// Maximum UTF-8 byte length accepted for a durable AI subject identifier.
pub const MAX_SUBJECT_BYTES: usize = 255;
/// Maximum UTF-8 byte length accepted for a durable AI idempotency key.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 512;

/// Trusted tenant and actor identity that accompanies one AI invocation or manual tool execution.
///
/// Construct this only from validated application identity and server-side tenant resolution, not
/// from model output or an arbitrary request header. It is intentionally not serialized into model
/// input or tool output, and its debug representation keeps both identifiers redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct AiExecutionContext {
    tenant: String,
    subject: String,
}

impl AiExecutionContext {
    /// Creates a bounded trusted tenant and subject context.
    ///
    /// # Errors
    ///
    /// Returns [`AiExecutionContextError`] when either stable identifier is invalid.
    pub fn new(
        tenant: impl Into<String>,
        subject: impl Into<String>,
    ) -> Result<Self, AiExecutionContextError> {
        let tenant = tenant.into();
        validate_tenant(&tenant)?;
        let subject = subject.into();
        validate_subject(&subject)?;
        Ok(Self { tenant, subject })
    }

    /// Returns the trusted tenant scope for application authorization and persistence filters.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns the validated actor identifier used by application authorization.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

impl fmt::Debug for AiExecutionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiExecutionContext")
            .field("tenant", &"[REDACTED]")
            .field("subject", &"[REDACTED]")
            .finish()
    }
}

/// Invalid trusted metadata supplied for a manual tool execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AiExecutionContextError {
    /// A tool action must be scoped to a server-validated tenant.
    #[error("AI tool execution tenant must not be blank")]
    BlankTenant,
    /// Tenant identifier was longer than durable AI metadata supports.
    #[error("AI tool execution tenant exceeded the supported length")]
    TenantTooLong,
    /// Tenant identifier contained a NUL byte.
    #[error("AI tool execution tenant must not contain a NUL byte")]
    TenantContainsNul,
    /// A tool action must retain its validated actor identity.
    #[error("AI tool execution subject must not be blank")]
    BlankSubject,
    /// Subject identifier was longer than durable AI metadata supports.
    #[error("AI tool execution subject exceeded the supported length")]
    SubjectTooLong,
    /// Subject identifier contained a NUL byte.
    #[error("AI tool execution subject must not contain a NUL byte")]
    SubjectContainsNul,
}

fn validate_tenant(tenant: &str) -> Result<(), AiExecutionContextError> {
    if tenant.trim().is_empty() {
        return Err(AiExecutionContextError::BlankTenant);
    }
    if tenant.len() > MAX_TENANT_BYTES {
        return Err(AiExecutionContextError::TenantTooLong);
    }
    if tenant.contains('\0') {
        return Err(AiExecutionContextError::TenantContainsNul);
    }
    Ok(())
}

fn validate_subject(subject: &str) -> Result<(), AiExecutionContextError> {
    if subject.trim().is_empty() {
        return Err(AiExecutionContextError::BlankSubject);
    }
    if subject.len() > MAX_SUBJECT_BYTES {
        return Err(AiExecutionContextError::SubjectTooLong);
    }
    if subject.contains('\0') {
        return Err(AiExecutionContextError::SubjectContainsNul);
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) enum IdempotencyKeyError {
    Blank,
    TooLong,
    ContainsNul,
}

pub(crate) fn validate_idempotency_key(idempotency_key: &str) -> Result<(), IdempotencyKeyError> {
    if idempotency_key.trim().is_empty() {
        return Err(IdempotencyKeyError::Blank);
    }
    if idempotency_key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
        return Err(IdempotencyKeyError::TooLong);
    }
    if idempotency_key.contains('\0') {
        return Err(IdempotencyKeyError::ContainsNul);
    }
    Ok(())
}

/// Trusted per-tool execution metadata for an idempotent external side effect.
///
/// The application chooses one stable idempotency key for the semantic action and reuses it only
/// when a retry must address that same action. It is never derived from model output. The key is
/// supplied to the typed handler and approval audit so an external provider and durable audit
/// record can reconcile the same execution without storing prompt, arguments, or result content.
#[derive(Clone, Eq, PartialEq)]
pub struct ToolExecutionContext {
    ai: AiExecutionContext,
    idempotency_key: String,
}

impl ToolExecutionContext {
    /// Creates one execution context from trusted invocation identity and an application key.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionContextError`] when `idempotency_key` is invalid.
    pub fn new(
        ai: AiExecutionContext,
        idempotency_key: impl Into<String>,
    ) -> Result<Self, ToolExecutionContextError> {
        let idempotency_key = idempotency_key.into();
        validate_idempotency_key(&idempotency_key).map_err(tool_execution_idempotency_key_error)?;
        Ok(Self {
            ai,
            idempotency_key,
        })
    }

    /// Returns the trusted tenant and actor identity shared with AI policy boundaries.
    #[must_use]
    pub const fn ai(&self) -> &AiExecutionContext {
        &self.ai
    }

    /// Returns the trusted tenant scope for application authorization and persistence filters.
    #[must_use]
    pub fn tenant(&self) -> &str {
        self.ai.tenant()
    }

    /// Returns the validated actor identifier used by application authorization.
    #[must_use]
    pub fn subject(&self) -> &str {
        self.ai.subject()
    }

    /// Returns the application-defined key to send to idempotent external side effects.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

impl fmt::Debug for ToolExecutionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolExecutionContext")
            .field("ai", &self.ai)
            .field("idempotency_key", &"[REDACTED]")
            .finish()
    }
}

/// Invalid metadata supplied for an idempotent tool execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ToolExecutionContextError {
    /// Retries cannot reconcile an unnamed external action.
    #[error("AI tool execution idempotency key must not be blank")]
    BlankIdempotencyKey,
    /// Key was longer than durable AI metadata supports.
    #[error("AI tool execution idempotency key exceeded the supported length")]
    IdempotencyKeyTooLong,
    /// Key contained a NUL byte.
    #[error("AI tool execution idempotency key must not contain a NUL byte")]
    IdempotencyKeyContainsNul,
}

fn tool_execution_idempotency_key_error(error: IdempotencyKeyError) -> ToolExecutionContextError {
    match error {
        IdempotencyKeyError::Blank => ToolExecutionContextError::BlankIdempotencyKey,
        IdempotencyKeyError::TooLong => ToolExecutionContextError::IdempotencyKeyTooLong,
        IdempotencyKeyError::ContainsNul => ToolExecutionContextError::IdempotencyKeyContainsNul,
    }
}
