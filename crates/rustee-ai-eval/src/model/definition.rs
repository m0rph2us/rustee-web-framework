//! Application-owned evaluation suite definitions and durable references.

use std::fmt;

use rustee_ai::ChatRequest;
use serde::{Deserialize, Serialize};

const MAX_CASES_PER_SUITE: usize = 10_000;
/// Maximum UTF-8 byte length accepted for a durable AI evaluation identifier.
pub const MAX_EVALUATION_IDENTIFIER_BYTES: usize = 128;

/// Content-free reference to an application-owned durable AI evaluation catalog entry.
///
/// `scope` is an opaque tenant/policy boundary, `catalog_id` selects application-owned prompts
/// and grading targets after authorization, and `run_key` is the stable durable delivery key.
/// None of these fields may contain request, target, completion, or grader content.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AiEvaluationReference {
    scope: String,
    catalog_id: String,
    run_key: String,
}

impl<'de> Deserialize<'de> for AiEvaluationReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawAiEvaluationReference {
            scope: String,
            catalog_id: String,
            run_key: String,
        }

        let raw = RawAiEvaluationReference::deserialize(deserializer)?;
        Self::new(raw.scope, raw.catalog_id, raw.run_key).map_err(serde::de::Error::custom)
    }
}

impl fmt::Debug for AiEvaluationReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationReference")
            .field("scope", &"[REDACTED]")
            .field("catalog_id", &"[REDACTED]")
            .field("run_key", &"[REDACTED]")
            .finish()
    }
}

impl AiEvaluationReference {
    /// Creates a bounded reference to one application catalog entry and durable evaluation run.
    ///
    /// # Errors
    ///
    /// Returns [`AiEvaluationConfigError::InvalidIdentifier`] for unsafe identifiers.
    pub fn new(
        scope: impl Into<String>,
        catalog_id: impl Into<String>,
        run_key: impl Into<String>,
    ) -> Result<Self, AiEvaluationConfigError> {
        let reference = Self {
            scope: scope.into(),
            catalog_id: catalog_id.into(),
            run_key: run_key.into(),
        };
        if !valid_identifier(&reference.scope)
            || !valid_identifier(&reference.catalog_id)
            || !valid_identifier(&reference.run_key)
        {
            return Err(AiEvaluationConfigError::InvalidIdentifier);
        }
        Ok(reference)
    }

    /// Returns the application-owned tenant/policy isolation scope.
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Returns the application catalog identifier without loading its contents.
    #[must_use]
    pub fn catalog_id(&self) -> &str {
        &self.catalog_id
    }

    /// Returns the stable idempotency key for the durable evaluation run.
    #[must_use]
    pub fn run_key(&self) -> &str {
        &self.run_key
    }
}

/// One application-owned test case in an AI evaluation suite.
///
/// `target` is intentionally generic. A trusted application grader defines its format, such as an
/// expected structured DTO, a fixed answer, a reference document ID, or a human-label reference.
/// Rustee does not serialize, log, or expose it from an evaluation report.
pub struct AiEvaluationCase<T> {
    id: String,
    request: ChatRequest,
    target: T,
}

impl<T> AiEvaluationCase<T> {
    /// Creates one bounded, stable case ID with a request and application-owned grading target.
    ///
    /// # Errors
    ///
    /// Returns [`AiEvaluationConfigError::InvalidIdentifier`] when `id` is not a bounded stable
    /// ASCII identifier.
    pub fn new(
        id: impl Into<String>,
        request: ChatRequest,
        target: T,
    ) -> Result<Self, AiEvaluationConfigError> {
        let id = id.into();
        if !valid_identifier(&id) {
            return Err(AiEvaluationConfigError::InvalidIdentifier);
        }
        Ok(Self {
            id,
            request,
            target,
        })
    }

    /// Returns the stable application-selected case ID.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the evaluation request. Do not log this value by default.
    #[must_use]
    pub const fn request(&self) -> &ChatRequest {
        &self.request
    }

    /// Returns the trusted application-owned grading target. Do not log this value by default.
    #[must_use]
    pub const fn target(&self) -> &T {
        &self.target
    }
}

impl<T> fmt::Debug for AiEvaluationCase<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationCase")
            .field("id", &self.id)
            .field("request", &"[REDACTED]")
            .field("target", &"[APPLICATION-OWNED]")
            .finish()
    }
}

/// A bounded, ordered set of application-owned evaluation cases.
pub struct AiEvaluationSuite<T> {
    name: String,
    cases: Vec<AiEvaluationCase<T>>,
}

impl<T> AiEvaluationSuite<T> {
    /// Creates a named evaluation suite with unique case identifiers.
    ///
    /// # Errors
    ///
    /// Returns [`AiEvaluationConfigError`] for an invalid suite name, empty/oversized suite, or a
    /// duplicated case ID. Case order is preserved and execution remains sequential.
    pub fn new(
        name: impl Into<String>,
        cases: impl IntoIterator<Item = AiEvaluationCase<T>>,
    ) -> Result<Self, AiEvaluationConfigError> {
        let name = name.into();
        if !valid_identifier(&name) {
            return Err(AiEvaluationConfigError::InvalidIdentifier);
        }
        let mut collected_cases = Vec::new();
        for case in cases {
            if collected_cases.len() == MAX_CASES_PER_SUITE {
                return Err(AiEvaluationConfigError::TooManyCases);
            }
            collected_cases.push(case);
        }
        if collected_cases.is_empty() {
            return Err(AiEvaluationConfigError::EmptySuite);
        }
        let mut ids = collected_cases
            .iter()
            .map(AiEvaluationCase::id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AiEvaluationConfigError::DuplicateCaseIdentifier);
        }
        Ok(Self {
            name,
            cases: collected_cases,
        })
    }

    /// Returns the stable application-selected suite name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns cases in the explicit evaluation order.
    #[must_use]
    pub fn cases(&self) -> &[AiEvaluationCase<T>] {
        &self.cases
    }
}

impl<T> fmt::Debug for AiEvaluationSuite<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiEvaluationSuite")
            .field("name", &self.name)
            .field("case_count", &self.cases.len())
            .finish()
    }
}

/// Invalid public evaluation-suite or grade configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AiEvaluationConfigError {
    /// Suite names and case IDs are intentionally bounded opaque identifiers.
    #[error(
        "AI evaluation identifier must use bounded ASCII letters, digits, underscore, hyphen, or dot"
    )]
    InvalidIdentifier,
    /// An evaluation suite needs an explicit least one case.
    #[error("AI evaluation suite must contain at least one case")]
    EmptySuite,
    /// Evaluation execution intentionally has a finite in-memory bound.
    #[error("AI evaluation suite supports at most 10,000 cases")]
    TooManyCases,
    /// A suite cannot report two different results under one case ID.
    #[error("AI evaluation suite contains a duplicate case identifier")]
    DuplicateCaseIdentifier,
    /// A grader label was malformed or unsafe for a content-free report.
    #[error(
        "AI evaluation grade label must use bounded visible ASCII without whitespace, quotes, or backslashes"
    )]
    InvalidGradeLabel,
    /// Grading scores use an integer per-mille scale to avoid unbounded float behavior.
    #[error("AI evaluation grade must not exceed 1,000 per mille")]
    ScoreOutOfRange,
    /// The in-memory development ledger has no safe fixed capacity.
    #[error("AI in-memory evaluation ledger capacity must be between one and 10,000 records")]
    InvalidInMemoryLedgerCapacity,
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_EVALUATION_IDENTIFIER_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}
