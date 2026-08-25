use std::{error::Error as StdError, fmt};

use futures_util::future::BoxFuture;

use crate::{
    AiAdvisor, AiExecutionContext, AiStreamEvent, BudgetAdvisorError, ChatRequest, ChatResponse,
};

/// Content-free input supplied to an application-owned AI budget gate or usage ledger.
#[derive(Clone, Eq, PartialEq)]
pub struct AiBudgetRequest {
    model: String,
    input_characters: usize,
    tool_count: usize,
    tool_result_count: usize,
}

impl fmt::Debug for AiBudgetRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiBudgetRequest")
            .field("model", &"[REDACTED]")
            .field("input_characters", &self.input_characters)
            .field("tool_count", &self.tool_count)
            .field("tool_result_count", &self.tool_result_count)
            .finish()
    }
}

impl AiBudgetRequest {
    pub(crate) fn from_request(request: &ChatRequest) -> Self {
        Self {
            model: request.model().to_owned(),
            input_characters: request.input_characters(),
            tool_count: request.tools().len(),
            tool_result_count: request.tool_results().len(),
        }
    }

    pub(super) fn from_metadata(
        model: String,
        input_characters: usize,
        tool_count: usize,
        tool_result_count: usize,
    ) -> Self {
        Self {
            model,
            input_characters,
            tool_count,
            tool_result_count,
        }
    }

    /// Returns the deployment-owned model alias.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns aggregate provider-bound input characters without retaining content.
    #[must_use]
    pub const fn input_characters(&self) -> usize {
        self.input_characters
    }

    /// Returns the number of declared tools.
    #[must_use]
    pub const fn tool_count(&self) -> usize {
        self.tool_count
    }

    /// Returns the number of approved tool results supplied to the provider.
    #[must_use]
    pub const fn tool_result_count(&self) -> usize {
        self.tool_result_count
    }
}

/// Application budget gate decision made before a provider receives a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiBudgetDecision {
    /// The provider call may proceed.
    Approved,
    /// The provider call must not start.
    Denied,
}

/// Application-owned tenant or actor budget admission boundary.
///
/// The gate receives only trusted identity and content-free request metadata. It may atomically
/// reserve quota in an application store, but Rustee does not infer provider billing or silently
/// retry a denied admission.
pub trait AiBudgetPolicy: Clone + Send + Sync + 'static {
    /// Failure type returned by the application's budget store or policy.
    type Error: StdError + Send + Sync + 'static;

    /// Approves or rejects one provider admission before the request leaves the application.
    fn admit(
        &self,
        context: AiExecutionContext,
        request: AiBudgetRequest,
    ) -> BoxFuture<'static, Result<AiBudgetDecision, Self::Error>>;
}

/// [`AiAdvisor`] that applies one application-owned budget gate before provider invocation.
#[derive(Clone)]
pub struct BudgetAdvisor<B> {
    context: AiExecutionContext,
    budget: B,
}

impl<B> fmt::Debug for BudgetAdvisor<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BudgetAdvisor")
            .field("context", &self.context)
            .field("budget_type", &std::any::type_name::<B>())
            .finish_non_exhaustive()
    }
}

impl<B> BudgetAdvisor<B> {
    /// Creates a budget advisor from trusted identity and one application budget policy.
    #[must_use]
    pub fn new(context: AiExecutionContext, budget: B) -> Self {
        Self { context, budget }
    }
}

impl<B> AiAdvisor for BudgetAdvisor<B>
where
    B: AiBudgetPolicy,
{
    type Error = BudgetAdvisorError<B::Error>;

    fn before_request(
        &self,
        request: ChatRequest,
    ) -> BoxFuture<'static, Result<ChatRequest, Self::Error>> {
        let budget = self.budget.clone();
        let context = self.context.clone();
        let budget_request = AiBudgetRequest::from_request(&request);
        Box::pin(async move {
            match budget
                .admit(context, budget_request)
                .await
                .map_err(BudgetAdvisorError::Policy)?
            {
                AiBudgetDecision::Approved => Ok(request),
                AiBudgetDecision::Denied => Err(BudgetAdvisorError::Denied),
            }
        })
    }

    fn after_response(
        &self,
        response: ChatResponse,
    ) -> BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        Box::pin(futures_util::future::ready(Ok(response)))
    }

    fn on_stream_event(
        &self,
        event: AiStreamEvent,
    ) -> BoxFuture<'static, Result<AiStreamEvent, Self::Error>> {
        Box::pin(futures_util::future::ready(Ok(event)))
    }
}
