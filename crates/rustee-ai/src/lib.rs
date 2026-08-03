//! Provider-neutral AI application contracts.
//!
//! Model output and tool arguments are untrusted input. This crate returns requested tool calls
//! but never executes a side effect automatically.

use std::{
    collections::BTreeMap, error::Error as StdError, fmt, future::Future, marker::PhantomData,
    sync::Arc, time::Instant,
};

use futures_util::{StreamExt, future::BoxFuture, stream::BoxStream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tracing::{Instrument, Span};

/// The role of one chat message.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// System instruction.
    System,
    /// End-user input.
    User,
    /// Model output.
    Assistant,
    /// An approved tool result.
    Tool,
}

/// One message supplied to a provider.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChatMessage {
    role: MessageRole,
    content: String,
}

impl ChatMessage {
    /// Creates a message with non-blank content.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError::BlankMessage`] when `content` is blank.
    pub fn new(role: MessageRole, content: impl Into<String>) -> Result<Self, RequestError> {
        let content = content.into();
        if content.trim().is_empty() {
            return Err(RequestError::BlankMessage);
        }
        Ok(Self { role, content })
    }

    /// Returns the message role.
    #[must_use]
    pub const fn role(&self) -> MessageRole {
        self.role
    }

    /// Returns message content. Do not record this value in logs by default.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }
}

impl fmt::Debug for ChatMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatMessage")
            .field("role", &self.role)
            .field("content_length", &self.content.len())
            .finish()
    }
}

/// Provider-visible tool declaration. The schema is an application validation input, not permission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolDefinition {
    name: String,
    input_schema: Value,
}

impl ToolDefinition {
    /// Creates a tool declaration with a stable ASCII name.
    ///
    /// Names may contain ASCII letters, digits, underscore, hyphen, and dot so application tools
    /// can preserve explicitly registered remote-tool namespaces.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::InvalidName`] when `name` is invalid.
    pub fn new(name: impl Into<String>, input_schema: Value) -> Result<Self, ToolError> {
        let name = name.into();
        validate_tool_name(&name)?;
        Ok(Self { name, input_schema })
    }

    /// Returns the stable tool name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns JSON schema for application-side argument validation.
    #[must_use]
    pub const fn input_schema(&self) -> &Value {
        &self.input_schema
    }
}

/// A model-requested tool call that still requires application approval.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolCall {
    id: String,
    name: String,
    arguments: Value,
}

impl ToolCall {
    /// Creates a tool call with validated call and tool names.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError`] for invalid IDs or names.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        arguments: Value,
    ) -> Result<Self, ToolError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(ToolError::BlankCallId);
        }
        let name = name.into();
        validate_tool_name(&name)?;
        Ok(Self {
            id,
            name,
            arguments,
        })
    }

    /// Returns provider call ID.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns requested tool name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns untrusted tool arguments.
    #[must_use]
    pub const fn arguments(&self) -> &Value {
        &self.arguments
    }
}

/// A request expressed through deployment-owned model alias and application messages.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    tools: Vec<ToolDefinition>,
    tool_results: Vec<ToolResult>,
}

impl ChatRequest {
    /// Creates a non-empty request.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError`] when the alias or messages are invalid.
    pub fn new(
        model: impl Into<String>,
        messages: impl IntoIterator<Item = ChatMessage>,
    ) -> Result<Self, RequestError> {
        let model = model.into();
        if model.trim().is_empty() {
            return Err(RequestError::BlankModel);
        }
        let messages = messages.into_iter().collect::<Vec<_>>();
        if messages.is_empty() {
            return Err(RequestError::EmptyMessages);
        }
        Ok(Self {
            model,
            messages,
            tools: Vec::new(),
            tool_results: Vec::new(),
        })
    }

    /// Adds manually executed tool declarations.
    #[must_use]
    pub fn with_tools(mut self, tools: impl IntoIterator<Item = ToolDefinition>) -> Self {
        self.tools = tools.into_iter().collect();
        self
    }

    /// Appends one validated application context message.
    ///
    /// Advisors use this to add authorized retrieval or policy context. The pipeline validates the
    /// final request after every advisor has run and before it reaches a provider.
    #[must_use]
    pub fn with_added_message(mut self, message: ChatMessage) -> Self {
        self.messages.push(message);
        self
    }

    /// Adds application-approved results for provider-specific function-call continuation.
    #[must_use]
    pub fn with_tool_results(mut self, tool_results: impl IntoIterator<Item = ToolResult>) -> Self {
        self.tool_results = tool_results.into_iter().collect();
        self
    }

    /// Returns the model alias.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns messages in request order.
    #[must_use]
    pub fn messages(&self) -> &[ChatMessage] {
        &self.messages
    }

    /// Returns declared tools.
    #[must_use]
    pub fn tools(&self) -> &[ToolDefinition] {
        &self.tools
    }

    /// Returns application-approved tool results that are safe to send back to a provider.
    #[must_use]
    pub fn tool_results(&self) -> &[ToolResult] {
        &self.tool_results
    }
}

impl fmt::Debug for ChatRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatRequest")
            .field("model", &self.model)
            .field("messages", &self.messages)
            .field(
                "tool_names",
                &self
                    .tools
                    .iter()
                    .map(ToolDefinition::name)
                    .collect::<Vec<_>>(),
            )
            .field(
                "tool_result_call_ids",
                &self
                    .tool_results
                    .iter()
                    .map(ToolResult::call_id)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Provider-reported token usage.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Usage {
    /// Input token count.
    pub input_tokens: u64,
    /// Output token count.
    pub output_tokens: u64,
}

impl Usage {
    /// Returns total token use.
    #[must_use]
    pub const fn total_tokens(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
}

/// Completed response from a provider.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChatResponse {
    id: String,
    model: String,
    content: String,
    tool_calls: Vec<ToolCall>,
    usage: Usage,
}

impl ChatResponse {
    /// Creates a provider response with non-blank ID and model.
    ///
    /// # Errors
    ///
    /// Returns [`ResponseError`] when provider metadata is blank.
    pub fn new(
        id: impl Into<String>,
        model: impl Into<String>,
        content: impl Into<String>,
        tool_calls: impl IntoIterator<Item = ToolCall>,
        usage: Usage,
    ) -> Result<Self, ResponseError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(ResponseError::BlankId);
        }
        let model = model.into();
        if model.trim().is_empty() {
            return Err(ResponseError::BlankModel);
        }
        Ok(Self {
            id,
            model,
            content: content.into(),
            tool_calls: tool_calls.into_iter().collect(),
            usage,
        })
    }

    /// Returns provider response ID.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns resolved provider model.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns untrusted text content.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Returns requested but unexecuted tool calls.
    #[must_use]
    pub fn tool_calls(&self) -> &[ToolCall] {
        &self.tool_calls
    }

    /// Returns provider usage.
    #[must_use]
    pub const fn usage(&self) -> Usage {
        self.usage
    }

    /// Deserializes text content as structured JSON.
    ///
    /// # Errors
    ///
    /// Returns [`StructuredOutputError`] when the model content is not valid JSON for `T`.
    pub fn parse_json<T>(&self) -> Result<T, StructuredOutputError>
    where
        T: DeserializeOwned,
    {
        serde_json::from_str(&self.content).map_err(StructuredOutputError::Deserialize)
    }
}

impl fmt::Debug for ChatResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatResponse")
            .field("id", &self.id)
            .field("model", &self.model)
            .field("content_length", &self.content.len())
            .field("tool_calls", &self.tool_calls)
            .field("usage", &self.usage)
            .finish()
    }
}

/// Side-effect classification declared for one application tool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolRisk {
    /// The tool is intended to read already-authorized data only.
    ReadOnly,
    /// The tool requires an explicit user confirmation before execution.
    RequiresConfirmation,
    /// The tool can make a privileged or consequential change.
    Privileged,
}

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
    /// Creates a non-blank trusted tenant and subject context.
    ///
    /// # Errors
    ///
    /// Returns [`AiExecutionContextError`] when either stable identifier is blank.
    pub fn new(
        tenant: impl Into<String>,
        subject: impl Into<String>,
    ) -> Result<Self, AiExecutionContextError> {
        let tenant = tenant.into();
        if tenant.trim().is_empty() {
            return Err(AiExecutionContextError::BlankTenant);
        }
        let subject = subject.into();
        if subject.trim().is_empty() {
            return Err(AiExecutionContextError::BlankSubject);
        }
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
    /// A tool action must retain its validated actor identity.
    #[error("AI tool execution subject must not be blank")]
    BlankSubject,
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
    /// Returns [`ToolExecutionContextError::BlankIdempotencyKey`] when `idempotency_key` is blank.
    pub fn new(
        ai: AiExecutionContext,
        idempotency_key: impl Into<String>,
    ) -> Result<Self, ToolExecutionContextError> {
        let idempotency_key = idempotency_key.into();
        if idempotency_key.trim().is_empty() {
            return Err(ToolExecutionContextError::BlankIdempotencyKey);
        }
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
}

/// Application implementation of one provider-visible tool.
///
/// Tool arguments are model output and must be decoded and validated by the implementation before
/// a side effect occurs. The registry never invokes this trait without an approval decision.
pub trait ToolExecutor: Send + Sync + 'static {
    /// Returns the provider-visible declaration.
    fn definition(&self) -> &ToolDefinition;

    /// Returns the tool's application-side risk classification.
    fn risk(&self) -> ToolRisk;

    /// Executes validated arguments after approval with trusted identity and an idempotency key.
    fn execute(
        &self,
        context: ToolExecutionContext,
        arguments: Value,
    ) -> BoxFuture<'static, Result<Value, ToolExecutionError>>;
}

/// Typed [`ToolExecutor`] that decodes JSON before invoking an application handler.
pub struct TypedTool<Input, Output, Handler> {
    definition: ToolDefinition,
    risk: ToolRisk,
    handler: Arc<Handler>,
    marker: PhantomData<fn(Input) -> Output>,
}

impl<Input, Output, Handler> TypedTool<Input, Output, Handler> {
    /// Creates a typed tool from a validated declaration, risk classification, and handler.
    #[must_use]
    pub fn new(definition: ToolDefinition, risk: ToolRisk, handler: Handler) -> Self {
        Self {
            definition,
            risk,
            handler: Arc::new(handler),
            marker: PhantomData,
        }
    }
}

impl<Input, Output, Handler> fmt::Debug for TypedTool<Input, Output, Handler> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedTool")
            .field("definition", &self.definition)
            .field("risk", &self.risk)
            .field("handler", &"[REDACTED]")
            .finish()
    }
}

impl<Input, Output, Handler, HandlerFuture, HandlerError> ToolExecutor
    for TypedTool<Input, Output, Handler>
where
    Input: serde::de::DeserializeOwned + Send + 'static,
    Output: Serialize + Send + 'static,
    Handler: Fn(ToolExecutionContext, Input) -> HandlerFuture + Send + Sync + 'static,
    HandlerFuture: Future<Output = Result<Output, HandlerError>> + Send + 'static,
    HandlerError: StdError + Send + Sync + 'static,
{
    fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    fn risk(&self) -> ToolRisk {
        self.risk
    }

    fn execute(
        &self,
        context: ToolExecutionContext,
        arguments: Value,
    ) -> BoxFuture<'static, Result<Value, ToolExecutionError>> {
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            let arguments = serde_json::from_value(arguments)
                .map_err(|_| ToolExecutionError::InvalidArguments)?;
            let output = handler(context, arguments)
                .await
                .map_err(|_| ToolExecutionError::HandlerFailed)?;
            serde_json::to_value(output).map_err(|_| ToolExecutionError::InvalidResult)
        })
    }
}

/// One application-side approval decision for a requested tool call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolApprovalDecision {
    /// The request may proceed to typed argument validation and handler execution.
    Approved,
    /// The request must not invoke the handler.
    Denied,
}

/// Policy boundary for user confirmation, tenant authorization, and side-effect approval.
pub trait ToolApprovalPolicy: Clone + Send + Sync + 'static {
    /// Failure type returned by the application's approval system.
    type Error: StdError + Send + Sync + 'static;

    /// Approves or rejects one requested call before its handler can run.
    fn approve(
        &self,
        context: AiExecutionContext,
        call: ToolCall,
        risk: ToolRisk,
    ) -> BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>>;
}

/// Default approval policy that never permits a tool execution.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAllToolApproval;

impl ToolApprovalPolicy for DenyAllToolApproval {
    type Error = std::convert::Infallible;

    fn approve(
        &self,
        _: AiExecutionContext,
        _: ToolCall,
        _: ToolRisk,
    ) -> BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>> {
        Box::pin(futures_util::future::ready(Ok(
            ToolApprovalDecision::Denied,
        )))
    }
}

/// Result of an approved tool execution.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolResult {
    call_id: String,
    name: String,
    content: Value,
}

impl ToolResult {
    fn from_call(call: &ToolCall, content: Value) -> Self {
        Self {
            call_id: call.id.clone(),
            name: call.name.clone(),
            content,
        }
    }

    /// Returns the provider call ID that this result satisfies.
    #[must_use]
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// Returns the tool name selected by the registry.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns tool output for an application to redact, bound, and send back to a provider.
    #[must_use]
    pub const fn content(&self) -> &Value {
        &self.content
    }
}

/// Content-free record written before an approved tool handler is allowed to run.
#[derive(Clone, Eq, PartialEq)]
pub struct ToolApprovalAuditEvent {
    execution: ToolExecutionContext,
    call_id: String,
    tool_name: String,
    risk: ToolRisk,
}

impl ToolApprovalAuditEvent {
    fn from_call(execution: ToolExecutionContext, call: &ToolCall, risk: ToolRisk) -> Self {
        Self {
            execution,
            call_id: call.id().to_owned(),
            tool_name: call.name().to_owned(),
            risk,
        }
    }

    /// Returns the trusted identity associated with this approved action.
    #[must_use]
    pub fn context(&self) -> &AiExecutionContext {
        self.execution.ai()
    }

    /// Returns the execution context shared with the tool handler.
    #[must_use]
    pub fn execution(&self) -> &ToolExecutionContext {
        &self.execution
    }

    /// Returns the application-defined external side-effect idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        self.execution.idempotency_key()
    }

    /// Returns the provider call identifier for application-owned reconciliation.
    #[must_use]
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// Returns the approved provider-visible tool name.
    #[must_use]
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    /// Returns the declared side-effect classification.
    #[must_use]
    pub const fn risk(&self) -> ToolRisk {
        self.risk
    }
}

impl fmt::Debug for ToolApprovalAuditEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolApprovalAuditEvent")
            .field("execution", &self.execution)
            .field("call_id", &"[REDACTED]")
            .field("tool_name", &self.tool_name)
            .field("risk", &self.risk)
            .finish()
    }
}

/// Application-owned durable audit boundary for a tool approved to execute.
///
/// A sink should persist its record before returning success. The registry invokes it after the
/// approval policy says `Approved` and before the typed handler starts, so an audit failure blocks
/// the side effect. The event never includes model arguments, tool output, prompt, or completion.
pub trait ToolApprovalAuditSink: Clone + Send + Sync + 'static {
    /// Failure type returned by the application's audit store.
    type Error: StdError + Send + Sync + 'static;

    /// Records one approved action before its handler starts.
    fn record_approved(
        &self,
        event: ToolApprovalAuditEvent,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Content-free terminal state of one tool execution after its approval audit persisted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolExecutionOutcome {
    /// The typed handler returned a serializable result.
    Succeeded,
    /// Argument decoding or the typed handler returned a normalized execution failure.
    Failed,
}

/// Content-free terminal audit record for an approved tool execution.
#[derive(Clone, Eq, PartialEq)]
pub struct ToolExecutionAuditEvent {
    approval: ToolApprovalAuditEvent,
    outcome: ToolExecutionOutcome,
}

impl ToolExecutionAuditEvent {
    fn new(approval: ToolApprovalAuditEvent, outcome: ToolExecutionOutcome) -> Self {
        Self { approval, outcome }
    }

    /// Returns the approved action identity and idempotency key shared with the handler.
    #[must_use]
    pub fn approval(&self) -> &ToolApprovalAuditEvent {
        &self.approval
    }

    /// Returns the terminal handler outcome without prompt, arguments, or tool result content.
    #[must_use]
    pub const fn outcome(&self) -> ToolExecutionOutcome {
        self.outcome
    }
}

impl fmt::Debug for ToolExecutionAuditEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolExecutionAuditEvent")
            .field("approval", &self.approval)
            .field("outcome", &self.outcome)
            .finish()
    }
}

/// Durable audit boundary that records both approval and terminal execution outcome.
///
/// [`ToolRegistry::execute_with_execution_audit`] writes approval before the handler and outcome
/// after it. An outcome-write failure cannot undo an external side effect, so the registry returns
/// the redacted event in [`ExecutionAuditedToolRunError::OutcomeAudit`] for application-owned
/// retry and reconciliation.
pub trait ToolExecutionAuditSink: ToolApprovalAuditSink {
    /// Records the terminal outcome for a previously approved action.
    fn record_outcome(
        &self,
        event: ToolExecutionAuditEvent,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

impl fmt::Debug for ToolResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolResult")
            .field("call_id", &self.call_id)
            .field("name", &self.name)
            .field("content", &"[REDACTED]")
            .finish()
    }
}

/// Registry of application tools that can be advertised to a provider.
#[derive(Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, Arc<dyn ToolExecutor>>,
}

impl ToolRegistry {
    /// Creates an empty tool registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one tool with a unique provider-visible name.
    ///
    /// # Errors
    ///
    /// Returns [`ToolRegistryError::DuplicateTool`] when a tool with the same name is already
    /// registered.
    pub fn register<T>(&mut self, tool: T) -> Result<(), ToolRegistryError>
    where
        T: ToolExecutor,
    {
        let name = tool.definition().name().to_owned();
        if self.tools.contains_key(&name) {
            return Err(ToolRegistryError::DuplicateTool);
        }
        self.tools.insert(name, Arc::new(tool));
        Ok(())
    }

    /// Returns declared tools in deterministic name order for a provider request.
    #[must_use]
    pub fn definitions(&self) -> impl ExactSizeIterator<Item = &ToolDefinition> {
        self.tools.values().map(|tool| tool.definition())
    }

    /// Executes one requested call only after an application approval policy permits it.
    ///
    /// The policy receives the invocation identity and the handler receives that identity plus an
    /// application-defined idempotency key. The policy also receives the full call, including
    /// untrusted arguments, so it can apply tenant, user-confirmation, and audit rules before JSON
    /// decoding invokes the tool handler.
    ///
    /// # Errors
    ///
    /// Returns a normalized registry, approval, or execution failure without exposing tool output
    /// or a handler's internal error.
    pub async fn execute<P>(
        &self,
        context: ToolExecutionContext,
        call: ToolCall,
        approval: &P,
    ) -> Result<ToolResult, ToolRunError<P::Error>>
    where
        P: ToolApprovalPolicy,
    {
        let span = tool_execution_span();
        let started_at = Instant::now();
        let execution_span = span.clone();
        let result = async {
            let tool = self
                .tools
                .get(call.name())
                .ok_or(ToolRunError::UnknownTool)?;
            let risk = tool.risk();
            record_tool_risk(&execution_span, risk);
            match approval
                .approve(context.ai().clone(), call.clone(), risk)
                .await
                .map_err(ToolRunError::Approval)?
            {
                ToolApprovalDecision::Approved => {}
                ToolApprovalDecision::Denied => return Err(ToolRunError::Denied { risk }),
            }
            let result_value = tool
                .execute(context, call.arguments().clone())
                .await
                .map_err(ToolRunError::Execution)?;
            Ok(ToolResult::from_call(&call, result_value))
        }
        .instrument(span.clone())
        .await;
        record_tool_run_result(&span, started_at, &result);
        result
    }

    /// Executes one approved tool only after its approval audit record persists.
    ///
    /// The audit sink is invoked after approval but before argument decoding or handler execution.
    /// Therefore an [`AuditedToolRunError::Audit`] result guarantees that the tool handler did not
    /// start. The application owns sink retention, reconciliation, and any post-execution audit.
    ///
    /// # Errors
    ///
    /// Returns a normalized approval/execution failure or an audit failure that prevented the
    /// handler from starting.
    pub async fn execute_with_approval_audit<P, A>(
        &self,
        context: ToolExecutionContext,
        call: ToolCall,
        approval: &P,
        audit: &A,
    ) -> Result<ToolResult, AuditedToolRunError<P::Error, A::Error>>
    where
        P: ToolApprovalPolicy,
        A: ToolApprovalAuditSink,
    {
        let span = tool_execution_span();
        let started_at = Instant::now();
        let execution_span = span.clone();
        let result = async {
            let tool = self
                .tools
                .get(call.name())
                .ok_or(AuditedToolRunError::Run(ToolRunError::UnknownTool))?;
            let risk = tool.risk();
            record_tool_risk(&execution_span, risk);
            match approval
                .approve(context.ai().clone(), call.clone(), risk)
                .await
                .map_err(ToolRunError::Approval)
                .map_err(AuditedToolRunError::Run)?
            {
                ToolApprovalDecision::Approved => {}
                ToolApprovalDecision::Denied => {
                    return Err(AuditedToolRunError::Run(ToolRunError::Denied { risk }));
                }
            }
            audit
                .record_approved(ToolApprovalAuditEvent::from_call(
                    context.clone(),
                    &call,
                    risk,
                ))
                .await
                .map_err(AuditedToolRunError::Audit)?;
            let result_value = tool
                .execute(context, call.arguments().clone())
                .await
                .map_err(ToolRunError::Execution)
                .map_err(AuditedToolRunError::Run)?;
            Ok(ToolResult::from_call(&call, result_value))
        }
        .instrument(span.clone())
        .await;
        record_audited_tool_run_result(&span, started_at, &result);
        result
    }

    /// Executes one tool with both pre-execution approval and post-execution outcome audit.
    ///
    /// The approval event is persisted before argument decoding and handler start. After the
    /// handler resolves, the same application idempotency key and a content-free terminal outcome
    /// are persisted. If that final write fails, the returned error exposes a redacted event for
    /// application-owned retry; it does not claim the side effect was rolled back.
    ///
    /// # Errors
    ///
    /// Returns an approval/handler failure, an approval-audit failure that prevented handler
    /// start, or an outcome-audit failure that requires reconciliation because the handler ran.
    pub async fn execute_with_execution_audit<P, A>(
        &self,
        context: ToolExecutionContext,
        call: ToolCall,
        approval: &P,
        audit: &A,
    ) -> Result<ToolResult, ExecutionAuditedToolRunError<P::Error, A::Error>>
    where
        P: ToolApprovalPolicy,
        A: ToolExecutionAuditSink,
    {
        let span = tool_execution_span();
        let started_at = Instant::now();
        let execution_span = span.clone();
        let result = async {
            let tool = self
                .tools
                .get(call.name())
                .ok_or(ExecutionAuditedToolRunError::Run(ToolRunError::UnknownTool))?;
            let risk = tool.risk();
            record_tool_risk(&execution_span, risk);
            match approval
                .approve(context.ai().clone(), call.clone(), risk)
                .await
                .map_err(ToolRunError::Approval)
                .map_err(ExecutionAuditedToolRunError::Run)?
            {
                ToolApprovalDecision::Approved => {}
                ToolApprovalDecision::Denied => {
                    return Err(ExecutionAuditedToolRunError::Run(ToolRunError::Denied {
                        risk,
                    }));
                }
            }
            let approval_event = ToolApprovalAuditEvent::from_call(context.clone(), &call, risk);
            audit
                .record_approved(approval_event.clone())
                .await
                .map_err(ExecutionAuditedToolRunError::ApprovalAudit)?;

            let result = tool.execute(context, call.arguments().clone()).await;
            let outcome = if result.is_ok() {
                ToolExecutionOutcome::Succeeded
            } else {
                ToolExecutionOutcome::Failed
            };
            let outcome_event = ToolExecutionAuditEvent::new(approval_event, outcome);
            audit
                .record_outcome(outcome_event.clone())
                .await
                .map_err(|source| ExecutionAuditedToolRunError::OutcomeAudit {
                    event: outcome_event,
                    source,
                })?;
            let result_value = result
                .map_err(ToolRunError::Execution)
                .map_err(ExecutionAuditedToolRunError::Run)?;
            Ok(ToolResult::from_call(&call, result_value))
        }
        .instrument(span.clone())
        .await;
        record_execution_audited_tool_run_result(&span, started_at, &result);
        result
    }
}

impl fmt::Debug for ToolRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolRegistry")
            .field("tool_names", &self.tools.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// Failure while registering an application tool.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ToolRegistryError {
    /// A provider-visible tool name may be registered only once.
    #[error("AI tool name is already registered")]
    DuplicateTool,
}

/// Failure returned by the typed tool executor before a tool result is produced.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ToolExecutionError {
    /// Untrusted model arguments did not match the typed input model.
    #[error("AI tool arguments were invalid")]
    InvalidArguments,
    /// The application tool handler failed; its details remain internal.
    #[error("AI tool execution failed")]
    HandlerFailed,
    /// The typed handler output could not be converted to JSON.
    #[error("AI tool result could not be serialized")]
    InvalidResult,
}

/// Failure while approving or executing a model-requested tool call.
#[derive(Debug, thiserror::Error)]
pub enum ToolRunError<ApprovalError>
where
    ApprovalError: StdError + Send + Sync + 'static,
{
    /// The provider requested a tool that this application did not register.
    #[error("AI requested an unknown tool")]
    UnknownTool,
    /// The application's approval system could not decide whether execution is allowed.
    #[error("AI tool approval failed")]
    Approval(#[source] ApprovalError),
    /// The application deliberately rejected this requested action.
    #[error("AI tool execution was not approved")]
    Denied {
        /// Risk classification of the denied action.
        risk: ToolRisk,
    },
    /// Argument decoding, handler execution, or result serialization failed.
    #[error(transparent)]
    Execution(#[from] ToolExecutionError),
}

/// Failure while executing a tool with a required pre-execution approval audit.
#[derive(Debug, thiserror::Error)]
pub enum AuditedToolRunError<ApprovalError, AuditError>
where
    ApprovalError: StdError + Send + Sync + 'static,
    AuditError: StdError + Send + Sync + 'static,
{
    /// Approval, lookup, argument, or handler execution failed.
    #[error(transparent)]
    Run(ToolRunError<ApprovalError>),
    /// The audit record did not persist, so the handler was not started.
    #[error("AI approved tool audit could not be recorded: {0}")]
    Audit(AuditError),
}

/// Failure while executing a tool with both approval and terminal outcome audit.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionAuditedToolRunError<ApprovalError, AuditError>
where
    ApprovalError: StdError + Send + Sync + 'static,
    AuditError: StdError + Send + Sync + 'static,
{
    /// Approval, lookup, argument, or handler execution failed after any required audit writes.
    #[error(transparent)]
    Run(ToolRunError<ApprovalError>),
    /// Approval audit did not persist, so the handler was not started.
    #[error("AI approved tool audit could not be recorded: {0}")]
    ApprovalAudit(AuditError),
    /// Terminal audit did not persist after the handler ran; reconciliation is required.
    #[error("AI tool outcome audit could not be recorded; reconciliation is required")]
    OutcomeAudit {
        /// Redacted event that identifies the action and observed terminal handler outcome.
        event: ToolExecutionAuditEvent,
        /// Durable audit store failure.
        #[source]
        source: AuditError,
    },
}

fn tool_execution_span() -> Span {
    tracing::info_span!(
        "rustee.ai.tool",
        otel.name = "AI tool execution",
        otel.kind = "internal",
        ai.operation = "tool_execute",
        ai.tool.risk = tracing::field::Empty,
        ai.tool.handler_outcome = tracing::field::Empty,
        ai.outcome = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    )
}

fn record_tool_risk(span: &Span, risk: ToolRisk) {
    span.record("ai.tool.risk", tool_risk_name(risk));
}

fn tool_risk_name(risk: ToolRisk) -> &'static str {
    match risk {
        ToolRisk::ReadOnly => "read_only",
        ToolRisk::RequiresConfirmation => "requires_confirmation",
        ToolRisk::Privileged => "privileged",
    }
}

fn tool_outcome_name(outcome: ToolExecutionOutcome) -> &'static str {
    match outcome {
        ToolExecutionOutcome::Succeeded => "succeeded",
        ToolExecutionOutcome::Failed => "failed",
    }
}

fn record_tool_run_result<ApprovalError>(
    span: &Span,
    started_at: Instant,
    result: &Result<ToolResult, ToolRunError<ApprovalError>>,
) where
    ApprovalError: StdError + Send + Sync + 'static,
{
    match result {
        Ok(_) => record_outcome(span, started_at, "succeeded", false),
        Err(error) => record_tool_run_error(span, started_at, error),
    }
}

fn record_tool_run_error<ApprovalError>(
    span: &Span,
    started_at: Instant,
    error: &ToolRunError<ApprovalError>,
) where
    ApprovalError: StdError + Send + Sync + 'static,
{
    match error {
        ToolRunError::UnknownTool => record_outcome(span, started_at, "unknown_tool", true),
        ToolRunError::Approval(_) => record_outcome(span, started_at, "approval_failed", true),
        ToolRunError::Denied { .. } => record_outcome(span, started_at, "denied", false),
        ToolRunError::Execution(_) => record_outcome(span, started_at, "execution_failed", true),
    }
}

fn record_audited_tool_run_result<ApprovalError, AuditError>(
    span: &Span,
    started_at: Instant,
    result: &Result<ToolResult, AuditedToolRunError<ApprovalError, AuditError>>,
) where
    ApprovalError: StdError + Send + Sync + 'static,
    AuditError: StdError + Send + Sync + 'static,
{
    match result {
        Ok(_) => record_outcome(span, started_at, "succeeded", false),
        Err(AuditedToolRunError::Run(error)) => record_tool_run_error(span, started_at, error),
        Err(AuditedToolRunError::Audit(_)) => {
            record_outcome(span, started_at, "approval_audit_failed", true);
        }
    }
}

fn record_execution_audited_tool_run_result<ApprovalError, AuditError>(
    span: &Span,
    started_at: Instant,
    result: &Result<ToolResult, ExecutionAuditedToolRunError<ApprovalError, AuditError>>,
) where
    ApprovalError: StdError + Send + Sync + 'static,
    AuditError: StdError + Send + Sync + 'static,
{
    match result {
        Ok(_) => record_outcome(span, started_at, "succeeded", false),
        Err(ExecutionAuditedToolRunError::Run(error)) => {
            record_tool_run_error(span, started_at, error);
        }
        Err(ExecutionAuditedToolRunError::ApprovalAudit(_)) => {
            record_outcome(span, started_at, "approval_audit_failed", true);
        }
        Err(ExecutionAuditedToolRunError::OutcomeAudit { event, .. }) => {
            span.record(
                "ai.tool.handler_outcome",
                tool_outcome_name(event.outcome()),
            );
            record_outcome(span, started_at, "outcome_audit_failed", true);
        }
    }
}

/// Provider-neutral streaming events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AiStreamEvent {
    /// Text fragment.
    TextDelta(String),
    /// A requested but unexecuted tool call.
    ToolCall(ToolCall),
    /// An application-approved tool result that may be returned to a provider in a later request.
    ToolResult(ToolResult),
    /// Stream completion and usage accounting.
    Completed(Usage),
}

/// A provider-normalized stream of AI events.
pub type AiEventStream<E> = BoxStream<'static, Result<AiStreamEvent, E>>;

/// Future returned while a provider opens an [`AiEventStream`].
pub type AiEventStreamFuture<E> = BoxFuture<'static, Result<AiEventStream<E>, E>>;

/// Provider-neutral AI client contract.
pub trait AiProvider: Clone + Send + Sync + 'static {
    /// Provider-specific failure.
    type Error: StdError + Send + Sync + 'static;

    /// Performs one non-streaming completion.
    fn complete(
        &self,
        request: ChatRequest,
    ) -> BoxFuture<'static, Result<ChatResponse, Self::Error>>;

    /// Opens a normalized text stream.
    fn stream(&self, request: ChatRequest) -> AiEventStreamFuture<Self::Error>;
}

/// Ordered application hook around one AI request, response, and stream event.
///
/// Advisors receive owned values so they can add authorized context or redact application-owned
/// output before it reaches the next stage. They must not log prompt, completion, or tool content
/// by default. The pipeline applies its request policy after [`AiAdvisor::before_request`] and
/// before provider invocation.
pub trait AiAdvisor: Clone + Send + Sync + 'static {
    /// Error returned by this application's advisor implementation.
    type Error: StdError + Send + Sync + 'static;

    /// Adds or validates application context before the provider sees a request.
    fn before_request(
        &self,
        request: ChatRequest,
    ) -> BoxFuture<'static, Result<ChatRequest, Self::Error>>;

    /// Transforms an application-visible non-streaming response after provider completion.
    fn after_response(
        &self,
        response: ChatResponse,
    ) -> BoxFuture<'static, Result<ChatResponse, Self::Error>>;

    /// Transforms one application-visible stream event after the provider emits it.
    fn on_stream_event(
        &self,
        event: AiStreamEvent,
    ) -> BoxFuture<'static, Result<AiStreamEvent, Self::Error>>;
}

/// Advisor that passes every request, response, and stream event through unchanged.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAiAdvisor;

impl AiAdvisor for NoopAiAdvisor {
    type Error = std::convert::Infallible;

    fn before_request(
        &self,
        request: ChatRequest,
    ) -> BoxFuture<'static, Result<ChatRequest, Self::Error>> {
        Box::pin(futures_util::future::ready(Ok(request)))
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
    fn from_request(request: &ChatRequest) -> Self {
        Self {
            model: request.model().to_owned(),
            input_characters: request
                .messages()
                .iter()
                .map(|message| message.content().chars().count())
                .sum(),
            tool_count: request.tools().len(),
            tool_result_count: request.tool_results().len(),
        }
    }

    /// Returns the deployment-owned model alias.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns aggregate input characters without retaining message content.
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
#[derive(Clone, Debug)]
pub struct BudgetAdvisor<B> {
    context: AiExecutionContext,
    budget: B,
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

/// One trusted, idempotent provider-attempt reservation for usage accounting.
///
/// The application creates this before the provider call from verified identity and a stable
/// request key. The key identifies one semantic provider attempt, rather than an HTTP retry. It
/// never contains prompt or completion content, and its debug representation redacts identity,
/// the key, and the model alias.
#[derive(Clone, Eq, PartialEq)]
pub struct AiUsageReservation {
    context: AiExecutionContext,
    idempotency_key: String,
    request: AiBudgetRequest,
}

impl AiUsageReservation {
    /// Creates a content-free reservation for one chat request.
    ///
    /// # Errors
    ///
    /// Returns [`AiUsageReservationError::BlankIdempotencyKey`] when `idempotency_key` is blank.
    pub fn for_request(
        context: AiExecutionContext,
        idempotency_key: impl Into<String>,
        request: &ChatRequest,
    ) -> Result<Self, AiUsageReservationError> {
        let idempotency_key = idempotency_key.into();
        if idempotency_key.trim().is_empty() {
            return Err(AiUsageReservationError::BlankIdempotencyKey);
        }
        Ok(Self {
            context,
            idempotency_key,
            request: AiBudgetRequest::from_request(request),
        })
    }

    /// Reconstructs a reservation from previously persisted content-free metadata.
    ///
    /// This is intended for a durable ledger's reconciliation query. Applications must use only
    /// metadata that was originally written by a trusted reservation path.
    ///
    /// # Errors
    ///
    /// Returns [`AiUsageReservationError`] when the idempotency key or model alias is blank.
    pub fn from_metadata(
        context: AiExecutionContext,
        idempotency_key: impl Into<String>,
        model: impl Into<String>,
        input_characters: usize,
        tool_count: usize,
        tool_result_count: usize,
    ) -> Result<Self, AiUsageReservationError> {
        let idempotency_key = idempotency_key.into();
        if idempotency_key.trim().is_empty() {
            return Err(AiUsageReservationError::BlankIdempotencyKey);
        }
        let model = model.into();
        if model.trim().is_empty() {
            return Err(AiUsageReservationError::BlankModel);
        }
        Ok(Self {
            context,
            idempotency_key,
            request: AiBudgetRequest {
                model,
                input_characters,
                tool_count,
                tool_result_count,
            },
        })
    }

    /// Returns the verified tenant and subject scope of the provider attempt.
    #[must_use]
    pub const fn context(&self) -> &AiExecutionContext {
        &self.context
    }

    /// Returns the application-owned idempotency key for this semantic provider attempt.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns content-free request metadata captured when the reservation was created.
    #[must_use]
    pub const fn request(&self) -> &AiBudgetRequest {
        &self.request
    }

    /// Creates the terminal provider-usage record for this reservation.
    #[must_use]
    pub fn settlement(&self, usage: Usage) -> AiUsageSettlement {
        AiUsageSettlement {
            reservation: self.clone(),
            usage,
        }
    }
}

impl fmt::Debug for AiUsageReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiUsageReservation")
            .field("context", &self.context)
            .field("idempotency_key", &"[REDACTED]")
            .field("request", &self.request)
            .finish()
    }
}

/// Invalid application metadata for a provider-usage reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AiUsageReservationError {
    /// A provider attempt must have a stable application-owned idempotency key.
    #[error("AI usage reservation idempotency key must not be blank")]
    BlankIdempotencyKey,
    /// A durable usage reservation must retain the deployment-owned model alias.
    #[error("AI usage reservation model alias must not be blank")]
    BlankModel,
}

/// Content-free terminal usage reported by a provider for one reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AiUsageSettlement {
    reservation: AiUsageReservation,
    usage: Usage,
}

impl AiUsageSettlement {
    /// Returns the reservation being settled.
    #[must_use]
    pub const fn reservation(&self) -> &AiUsageReservation {
        &self.reservation
    }

    /// Returns the provider-reported token usage to record durably.
    #[must_use]
    pub const fn usage(&self) -> Usage {
        self.usage
    }
}

/// A usage-ledger decision before a provider call starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AiUsageReservationDecision {
    /// This caller owns the reservation and may start exactly one provider attempt.
    Reserved,
    /// The application deliberately refused this attempt before provider invocation.
    Denied,
    /// A previous attempt with this key has no durable terminal usage and must be reconciled.
    PendingReconciliation,
    /// A previous attempt with this key already has durable terminal usage.
    AlreadySettled,
}

/// Application-owned durable reservation and actual-usage boundary.
///
/// A ledger atomically decides whether this caller may make a provider attempt and later records
/// provider-reported [`Usage`]. A provider transport error or a dropped stream deliberately does
/// not produce an automatic refund: delivery may have reached the provider, so the reservation
/// remains pending for an application-owned provider lookup, timeout policy, or manual review.
///
/// Implementations that enforce tenant or actor quota should make admission and reservation one
/// durable transaction. [`AiUsageReservationDecision::Reserved`] is the only decision that lets
/// [`AiPipeline::complete_with_usage_ledger`] or [`AiPipeline::stream_with_usage_ledger`] call a
/// provider.
pub trait AiUsageLedger: Clone + Send + Sync + 'static {
    /// Failure type returned by the application's durable ledger.
    type Error: StdError + Send + Sync + 'static;

    /// Atomically reserves one provider attempt or returns a non-starting decision.
    fn reserve(
        &self,
        reservation: AiUsageReservation,
    ) -> BoxFuture<'static, Result<AiUsageReservationDecision, Self::Error>>;

    /// Records actual usage after a provider completes successfully.
    ///
    /// This operation must be replay-safe for the same reservation and usage, and must reject a
    /// changed identity or changed terminal usage rather than overwriting a prior record.
    fn record_usage(
        &self,
        settlement: AiUsageSettlement,
    ) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Cost and safety bounds applied before a provider receives a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AiPolicy {
    /// Maximum total Unicode scalar count in message content.
    pub max_input_characters: usize,
    /// Maximum declared tools.
    pub max_tools: usize,
}

impl Default for AiPolicy {
    fn default() -> Self {
        Self {
            max_input_characters: 100_000,
            max_tools: 16,
        }
    }
}

impl AiPolicy {
    /// Validates a request before the provider call.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyError`] when configured bounds are exceeded.
    pub fn validate(self, request: &ChatRequest) -> Result<(), PolicyError> {
        let actual = request
            .messages()
            .iter()
            .map(|message| message.content().chars().count())
            .sum();
        if actual > self.max_input_characters {
            return Err(PolicyError::InputTooLarge {
                limit: self.max_input_characters,
                actual,
            });
        }
        if request.tools().len() > self.max_tools {
            return Err(PolicyError::TooManyTools {
                limit: self.max_tools,
                actual: request.tools().len(),
            });
        }
        Ok(())
    }
}

/// Pipeline that enforces policy before one provider call.
#[derive(Clone, Debug)]
pub struct AiPipeline<P> {
    provider: P,
    policy: AiPolicy,
}

impl<P> AiPipeline<P> {
    /// Creates a pipeline with default bounds.
    #[must_use]
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            policy: AiPolicy::default(),
        }
    }

    /// Replaces pre-provider request bounds.
    #[must_use]
    pub fn with_policy(mut self, policy: AiPolicy) -> Self {
        self.policy = policy;
        self
    }
}

impl<P: AiProvider> AiPipeline<P> {
    /// Validates and completes a chat request; tool calls remain unexecuted.
    ///
    /// # Errors
    ///
    /// Returns policy or provider failure.
    pub async fn complete(
        &self,
        request: ChatRequest,
    ) -> Result<ChatResponse, PipelineError<P::Error>> {
        let span = ai_operation_span("complete", &request);
        let started_at = Instant::now();
        let result = async {
            self.policy
                .validate(&request)
                .map_err(PipelineError::Policy)?;
            self.provider
                .complete(request)
                .await
                .map_err(PipelineError::Provider)
        }
        .instrument(span.clone())
        .await;
        record_pipeline_result(&span, started_at, &result);
        result
    }

    /// Validates and opens an AI text stream.
    ///
    /// # Errors
    ///
    /// Returns policy or provider failure before stream creation.
    pub async fn stream(
        &self,
        request: ChatRequest,
    ) -> Result<AiEventStream<P::Error>, PipelineError<P::Error>> {
        let span = ai_operation_span("stream", &request);
        let started_at = Instant::now();
        let result = async {
            self.policy
                .validate(&request)
                .map_err(PipelineError::Policy)?;
            self.provider
                .stream(request)
                .await
                .map_err(PipelineError::Provider)
        }
        .instrument(span.clone())
        .await;
        match result {
            Ok(stream) => {
                record_outcome(&span, started_at, "opened", false);
                Ok(observe_stream(span, started_at, stream))
            }
            Err(error) => {
                record_pipeline_error(&span, started_at, &error);
                Err(error)
            }
        }
    }

    /// Validates, reserves, completes, and durably records provider usage for one chat request.
    ///
    /// The supplied reservation must describe the exact content-free metadata of `request`.
    /// Only a ledger [`AiUsageReservationDecision::Reserved`] decision permits provider
    /// invocation. If provider completion succeeds but durable usage recording fails, the error
    /// retains the response for application-owned reconciliation and does not retry the provider.
    /// Provider failures leave the reservation pending because Rustee cannot safely infer whether
    /// the provider processed or billed the attempt.
    ///
    /// # Errors
    ///
    /// Returns a policy, reservation, ledger, or provider failure.
    pub async fn complete_with_usage_ledger<L>(
        &self,
        request: ChatRequest,
        reservation: AiUsageReservation,
        ledger: &L,
    ) -> Result<ChatResponse, UsageLedgerPipelineError<P::Error, L::Error>>
    where
        L: AiUsageLedger,
    {
        let span = ai_operation_span("complete", &request);
        let started_at = Instant::now();
        let ledger = ledger.clone();
        let result = async {
            self.policy
                .validate(&request)
                .map_err(UsageLedgerPipelineError::Policy)?;
            ensure_reservation_matches(&reservation, &request)?;
            match ledger
                .reserve(reservation.clone())
                .await
                .map_err(UsageLedgerPipelineError::Reservation)?
            {
                AiUsageReservationDecision::Reserved => {}
                AiUsageReservationDecision::Denied => {
                    return Err(UsageLedgerPipelineError::Denied);
                }
                AiUsageReservationDecision::PendingReconciliation => {
                    return Err(UsageLedgerPipelineError::PendingReconciliation);
                }
                AiUsageReservationDecision::AlreadySettled => {
                    return Err(UsageLedgerPipelineError::AlreadySettled);
                }
            }
            let response = self
                .provider
                .complete(request)
                .await
                .map_err(UsageLedgerPipelineError::Provider)?;
            if let Err(source) = ledger
                .record_usage(reservation.settlement(response.usage()))
                .await
            {
                return Err(UsageLedgerPipelineError::Settlement { response, source });
            }
            Ok(response)
        }
        .instrument(span.clone())
        .await;
        record_usage_ledger_pipeline_result(&span, started_at, &result);
        result
    }

    /// Validates, reserves, and opens an AI text stream with durable terminal-usage recording.
    ///
    /// The ledger records usage only after an application-visible [`AiStreamEvent::Completed`].
    /// If the provider fails, the response body is dropped, or the stream ends without a terminal
    /// event, the reservation remains pending for reconciliation; Rustee never guesses a refund.
    ///
    /// # Errors
    ///
    /// Returns a policy, reservation, ledger, or provider-open failure before a stream is
    /// returned. A terminal usage-ledger failure is emitted inside the returned stream.
    pub async fn stream_with_usage_ledger<L>(
        &self,
        request: ChatRequest,
        reservation: AiUsageReservation,
        ledger: &L,
    ) -> Result<
        AiEventStream<UsageLedgerStreamError<P::Error, L::Error>>,
        UsageLedgerPipelineError<P::Error, L::Error>,
    >
    where
        L: AiUsageLedger,
    {
        let span = ai_operation_span("stream", &request);
        let started_at = Instant::now();
        let ledger = ledger.clone();
        let reservation_for_stream = reservation.clone();
        let result = async {
            self.policy
                .validate(&request)
                .map_err(UsageLedgerPipelineError::Policy)?;
            ensure_reservation_matches(&reservation, &request)?;
            match ledger
                .reserve(reservation)
                .await
                .map_err(UsageLedgerPipelineError::Reservation)?
            {
                AiUsageReservationDecision::Reserved => {}
                AiUsageReservationDecision::Denied => {
                    return Err(UsageLedgerPipelineError::Denied);
                }
                AiUsageReservationDecision::PendingReconciliation => {
                    return Err(UsageLedgerPipelineError::PendingReconciliation);
                }
                AiUsageReservationDecision::AlreadySettled => {
                    return Err(UsageLedgerPipelineError::AlreadySettled);
                }
            }
            self.provider
                .stream(request)
                .await
                .map_err(UsageLedgerPipelineError::Provider)
        }
        .instrument(span.clone())
        .await;
        match result {
            Ok(stream) => {
                record_outcome(&span, started_at, "opened", false);
                let stream = settle_usage_stream(stream, ledger, reservation_for_stream);
                Ok(observe_stream(span, started_at, stream))
            }
            Err(error) => {
                record_usage_ledger_pipeline_error(&span, started_at, &error);
                Err(error)
            }
        }
    }

    /// Runs an advisor and a durable usage ledger around one provider completion.
    ///
    /// The advisor first creates the final request, then Rustee validates it and creates a
    /// reservation from that exact content-free metadata. Actual provider usage is settled before
    /// [`AiAdvisor::after_response`] receives application-visible output, so a response advisor
    /// failure cannot erase a completed provider attempt from the durable ledger.
    ///
    /// # Errors
    ///
    /// Returns advisor, policy, reservation, ledger, or provider failure. A terminal usage write
    /// failure retains the completed response for application-owned reconciliation.
    pub async fn complete_with_advisor_and_usage_ledger<A, L>(
        &self,
        request: ChatRequest,
        advisor: &A,
        context: AiExecutionContext,
        idempotency_key: impl Into<String>,
        ledger: &L,
    ) -> Result<ChatResponse, AdvisedUsageLedgerPipelineError<P::Error, A::Error, L::Error>>
    where
        A: AiAdvisor,
        L: AiUsageLedger,
    {
        let span = ai_operation_span("complete", &request);
        let started_at = Instant::now();
        let idempotency_key = idempotency_key.into();
        let ledger = ledger.clone();
        let request_span = span.clone();
        let result = async {
            let request = advisor
                .before_request(request)
                .await
                .map_err(AdvisedUsageLedgerPipelineError::Advisor)?;
            record_request_metadata(&request_span, &request);
            self.policy
                .validate(&request)
                .map_err(UsageLedgerPipelineError::Policy)
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?;
            let reservation = AiUsageReservation::for_request(context, idempotency_key, &request)
                .map_err(AdvisedUsageLedgerPipelineError::ReservationMetadata)?;
            match ledger
                .reserve(reservation.clone())
                .await
                .map_err(UsageLedgerPipelineError::Reservation)
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?
            {
                AiUsageReservationDecision::Reserved => {}
                AiUsageReservationDecision::Denied => {
                    return Err(AdvisedUsageLedgerPipelineError::Usage(
                        UsageLedgerPipelineError::Denied,
                    ));
                }
                AiUsageReservationDecision::PendingReconciliation => {
                    return Err(AdvisedUsageLedgerPipelineError::Usage(
                        UsageLedgerPipelineError::PendingReconciliation,
                    ));
                }
                AiUsageReservationDecision::AlreadySettled => {
                    return Err(AdvisedUsageLedgerPipelineError::Usage(
                        UsageLedgerPipelineError::AlreadySettled,
                    ));
                }
            }
            let response = self
                .provider
                .complete(request)
                .await
                .map_err(UsageLedgerPipelineError::Provider)
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?;
            if let Err(source) = ledger
                .record_usage(reservation.settlement(response.usage()))
                .await
            {
                return Err(AdvisedUsageLedgerPipelineError::Usage(
                    UsageLedgerPipelineError::Settlement { response, source },
                ));
            }
            advisor
                .after_response(response)
                .await
                .map_err(AdvisedUsageLedgerPipelineError::Advisor)
        }
        .instrument(span.clone())
        .await;
        record_advised_usage_ledger_pipeline_result(&span, started_at, &result);
        result
    }

    /// Runs an advisor and a durable usage ledger around an AI text stream.
    ///
    /// The ledger settles raw provider terminal usage before the advisor transforms the terminal
    /// event. Provider errors, incomplete streams, and dropped response bodies remain pending for
    /// reconciliation instead of being silently released or retried.
    ///
    /// # Errors
    ///
    /// Returns advisor, policy, reservation, ledger, or provider-open failure before a stream is
    /// returned. Provider, ledger, or advisor event failures are emitted inside the stream.
    pub async fn stream_with_advisor_and_usage_ledger<A, L>(
        &self,
        request: ChatRequest,
        advisor: &A,
        context: AiExecutionContext,
        idempotency_key: impl Into<String>,
        ledger: &L,
    ) -> Result<
        AiEventStream<AdvisedUsageLedgerStreamError<P::Error, A::Error, L::Error>>,
        AdvisedUsageLedgerPipelineError<P::Error, A::Error, L::Error>,
    >
    where
        A: AiAdvisor,
        L: AiUsageLedger,
    {
        let span = ai_operation_span("stream", &request);
        let started_at = Instant::now();
        let idempotency_key = idempotency_key.into();
        let ledger = ledger.clone();
        let request_span = span.clone();
        let result = async {
            let request = advisor
                .before_request(request)
                .await
                .map_err(AdvisedUsageLedgerPipelineError::Advisor)?;
            record_request_metadata(&request_span, &request);
            self.policy
                .validate(&request)
                .map_err(UsageLedgerPipelineError::Policy)
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?;
            let reservation = AiUsageReservation::for_request(context, idempotency_key, &request)
                .map_err(AdvisedUsageLedgerPipelineError::ReservationMetadata)?;
            match ledger
                .reserve(reservation.clone())
                .await
                .map_err(UsageLedgerPipelineError::Reservation)
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?
            {
                AiUsageReservationDecision::Reserved => {}
                AiUsageReservationDecision::Denied => {
                    return Err(AdvisedUsageLedgerPipelineError::Usage(
                        UsageLedgerPipelineError::Denied,
                    ));
                }
                AiUsageReservationDecision::PendingReconciliation => {
                    return Err(AdvisedUsageLedgerPipelineError::Usage(
                        UsageLedgerPipelineError::PendingReconciliation,
                    ));
                }
                AiUsageReservationDecision::AlreadySettled => {
                    return Err(AdvisedUsageLedgerPipelineError::Usage(
                        UsageLedgerPipelineError::AlreadySettled,
                    ));
                }
            }
            let stream = self
                .provider
                .stream(request)
                .await
                .map_err(UsageLedgerPipelineError::Provider)
                .map_err(AdvisedUsageLedgerPipelineError::Usage)?;
            Ok::<_, AdvisedUsageLedgerPipelineError<P::Error, A::Error, L::Error>>((
                stream,
                reservation,
            ))
        }
        .instrument(span.clone())
        .await;
        match result {
            Ok((stream, reservation)) => {
                record_outcome(&span, started_at, "opened", false);
                let stream = settle_usage_stream(stream, ledger, reservation);
                let advisor = advisor.clone();
                let stream = Box::pin(stream.then(move |event| {
                    let advisor = advisor.clone();
                    async move {
                        let event = event.map_err(AdvisedUsageLedgerStreamError::Usage)?;
                        advisor
                            .on_stream_event(event)
                            .await
                            .map_err(AdvisedUsageLedgerStreamError::Advisor)
                    }
                }));
                Ok(observe_stream(span, started_at, stream))
            }
            Err(error) => {
                record_advised_usage_ledger_pipeline_error(&span, started_at, &error);
                Err(error)
            }
        }
    }

    /// Runs an advisor around a non-streaming provider completion.
    ///
    /// # Errors
    ///
    /// Returns an advisor, request-policy, or provider failure. A provider is never called when
    /// the advisor or final request-policy validation fails.
    pub async fn complete_with_advisor<A>(
        &self,
        request: ChatRequest,
        advisor: &A,
    ) -> Result<ChatResponse, AdvisedPipelineError<P::Error, A::Error>>
    where
        A: AiAdvisor,
    {
        let span = ai_operation_span("complete", &request);
        let started_at = Instant::now();
        let request_span = span.clone();
        let result = async {
            let request = advisor
                .before_request(request)
                .await
                .map_err(AdvisedPipelineError::Advisor)?;
            record_request_metadata(&request_span, &request);
            self.policy
                .validate(&request)
                .map_err(AdvisedPipelineError::Policy)?;
            let response = self
                .provider
                .complete(request)
                .await
                .map_err(AdvisedPipelineError::Provider)?;
            advisor
                .after_response(response)
                .await
                .map_err(AdvisedPipelineError::Advisor)
        }
        .instrument(span.clone())
        .await;
        record_advised_pipeline_result(&span, started_at, &result);
        result
    }

    /// Runs an advisor around a provider stream.
    ///
    /// The advisor validates or enriches the request before the stream opens, then receives every
    /// application-visible event. Provider stream failures and advisor event failures remain
    /// distinct in the returned stream.
    ///
    /// # Errors
    ///
    /// Returns an advisor, request-policy, or provider-open failure before a stream is returned.
    pub async fn stream_with_advisor<A>(
        &self,
        request: ChatRequest,
        advisor: &A,
    ) -> Result<
        AiEventStream<AdvisedStreamError<P::Error, A::Error>>,
        AdvisedPipelineError<P::Error, A::Error>,
    >
    where
        A: AiAdvisor,
    {
        let span = ai_operation_span("stream", &request);
        let started_at = Instant::now();
        let request_span = span.clone();
        let result = async {
            let request = advisor
                .before_request(request)
                .await
                .map_err(AdvisedPipelineError::Advisor)?;
            record_request_metadata(&request_span, &request);
            self.policy
                .validate(&request)
                .map_err(AdvisedPipelineError::Policy)?;
            self.provider
                .stream(request)
                .await
                .map_err(AdvisedPipelineError::Provider)
        }
        .instrument(span.clone())
        .await;
        match result {
            Ok(stream) => {
                let advisor = advisor.clone();
                let stream = Box::pin(stream.then(move |event| {
                    let advisor = advisor.clone();
                    async move {
                        let event = event.map_err(AdvisedStreamError::Provider)?;
                        advisor
                            .on_stream_event(event)
                            .await
                            .map_err(AdvisedStreamError::Advisor)
                    }
                }));
                record_outcome(&span, started_at, "opened", false);
                Ok(observe_stream(span, started_at, stream))
            }
            Err(error) => {
                record_advised_pipeline_error(&span, started_at, &error);
                Err(error)
            }
        }
    }
}

fn ai_operation_span(operation: &'static str, request: &ChatRequest) -> Span {
    tracing::info_span!(
        "rustee.ai",
        otel.name = "AI request",
        otel.kind = "client",
        ai.operation = operation,
        ai.request.message_count = request.messages().len(),
        ai.request.tool_count = request.tools().len(),
        ai.request.tool_result_count = request.tool_results().len(),
        ai.request.input_characters = request_input_characters(request),
        ai.usage.input_tokens = tracing::field::Empty,
        ai.usage.output_tokens = tracing::field::Empty,
        ai.outcome = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    )
}

fn record_request_metadata(span: &Span, request: &ChatRequest) {
    span.record(
        "ai.request.message_count",
        tracing::field::display(request.messages().len()),
    );
    span.record(
        "ai.request.tool_count",
        tracing::field::display(request.tools().len()),
    );
    span.record(
        "ai.request.tool_result_count",
        tracing::field::display(request.tool_results().len()),
    );
    span.record(
        "ai.request.input_characters",
        tracing::field::display(request_input_characters(request)),
    );
}

fn request_input_characters(request: &ChatRequest) -> usize {
    request
        .messages()
        .iter()
        .map(|message| message.content().chars().count())
        .sum()
}

fn record_usage(span: &Span, usage: Usage) {
    span.record(
        "ai.usage.input_tokens",
        tracing::field::display(usage.input_tokens),
    );
    span.record(
        "ai.usage.output_tokens",
        tracing::field::display(usage.output_tokens),
    );
}

fn record_success(span: &Span, started_at: Instant, usage: Usage) {
    record_usage(span, usage);
    record_outcome(span, started_at, "succeeded", false);
}

fn record_outcome(span: &Span, started_at: Instant, outcome: &'static str, failed: bool) {
    span.record("ai.outcome", outcome);
    span.record("otel.status_code", if failed { "ERROR" } else { "UNSET" });
    span.record(
        "duration_ms",
        tracing::field::display(started_at.elapsed().as_millis()),
    );
}

fn observe_stream<E: 'static>(
    span: Span,
    started_at: Instant,
    stream: AiEventStream<E>,
) -> AiEventStream<E> {
    Box::pin(stream.inspect(move |event| match event {
        Ok(AiStreamEvent::Completed(usage)) => record_success(&span, started_at, *usage),
        Err(_) => record_outcome(&span, started_at, "stream_failed", true),
        Ok(_) => {}
    }))
}

fn settle_usage_stream<ProviderError, L>(
    stream: AiEventStream<ProviderError>,
    ledger: L,
    reservation: AiUsageReservation,
) -> AiEventStream<UsageLedgerStreamError<ProviderError, L::Error>>
where
    ProviderError: StdError + Send + Sync + 'static,
    L: AiUsageLedger,
{
    let mut unsettled_reservation = Some(reservation);
    Box::pin(stream.then(move |event| {
        let is_completion = matches!(&event, Ok(AiStreamEvent::Completed(_)));
        let reservation = is_completion
            .then(|| unsettled_reservation.take())
            .flatten();
        let ledger = ledger.clone();
        async move {
            let event = event.map_err(UsageLedgerStreamError::Provider)?;
            let AiStreamEvent::Completed(usage) = event else {
                return Ok(event);
            };
            let Some(reservation) = reservation else {
                return Err(UsageLedgerStreamError::DuplicateCompletion);
            };
            ledger
                .record_usage(reservation.settlement(usage))
                .await
                .map_err(UsageLedgerStreamError::Ledger)?;
            Ok(AiStreamEvent::Completed(usage))
        }
    }))
}

fn ensure_reservation_matches<ProviderError, LedgerError>(
    reservation: &AiUsageReservation,
    request: &ChatRequest,
) -> Result<(), UsageLedgerPipelineError<ProviderError, LedgerError>> {
    (reservation.request == AiBudgetRequest::from_request(request))
        .then_some(())
        .ok_or(UsageLedgerPipelineError::ReservationRequestMismatch)
}

fn record_pipeline_result<E>(
    span: &Span,
    started_at: Instant,
    result: &Result<ChatResponse, PipelineError<E>>,
) {
    match result {
        Ok(response) => record_success(span, started_at, response.usage()),
        Err(error) => record_pipeline_error(span, started_at, error),
    }
}

fn record_pipeline_error<E>(span: &Span, started_at: Instant, error: &PipelineError<E>) {
    match error {
        PipelineError::Policy(_) => record_outcome(span, started_at, "policy_rejected", false),
        PipelineError::Provider(_) => record_outcome(span, started_at, "provider_failed", true),
    }
}

fn record_advised_pipeline_result<ProviderError, AdvisorError>(
    span: &Span,
    started_at: Instant,
    result: &Result<ChatResponse, AdvisedPipelineError<ProviderError, AdvisorError>>,
) {
    match result {
        Ok(response) => record_success(span, started_at, response.usage()),
        Err(error) => record_advised_pipeline_error(span, started_at, error),
    }
}

fn record_advised_pipeline_error<ProviderError, AdvisorError>(
    span: &Span,
    started_at: Instant,
    error: &AdvisedPipelineError<ProviderError, AdvisorError>,
) {
    match error {
        AdvisedPipelineError::Policy(_) => {
            record_outcome(span, started_at, "policy_rejected", false);
        }
        AdvisedPipelineError::Provider(_) => {
            record_outcome(span, started_at, "provider_failed", true);
        }
        AdvisedPipelineError::Advisor(_) => {
            record_outcome(span, started_at, "advisor_failed", true);
        }
    }
}

fn record_usage_ledger_pipeline_result<ProviderError, LedgerError>(
    span: &Span,
    started_at: Instant,
    result: &Result<ChatResponse, UsageLedgerPipelineError<ProviderError, LedgerError>>,
) {
    match result {
        Ok(response) => record_success(span, started_at, response.usage()),
        Err(error) => record_usage_ledger_pipeline_error(span, started_at, error),
    }
}

fn record_usage_ledger_pipeline_error<ProviderError, LedgerError>(
    span: &Span,
    started_at: Instant,
    error: &UsageLedgerPipelineError<ProviderError, LedgerError>,
) {
    match error {
        UsageLedgerPipelineError::Policy(_)
        | UsageLedgerPipelineError::ReservationRequestMismatch => {
            record_outcome(span, started_at, "policy_rejected", false);
        }
        UsageLedgerPipelineError::Denied => {
            record_outcome(span, started_at, "budget_denied", false);
        }
        UsageLedgerPipelineError::PendingReconciliation
        | UsageLedgerPipelineError::AlreadySettled => {
            record_outcome(span, started_at, "usage_reconciliation_required", true);
        }
        UsageLedgerPipelineError::Reservation(_) => {
            record_outcome(span, started_at, "usage_reservation_failed", true);
        }
        UsageLedgerPipelineError::Provider(_) => {
            record_outcome(span, started_at, "provider_failed", true);
        }
        UsageLedgerPipelineError::Settlement { .. } => {
            record_outcome(span, started_at, "usage_settlement_failed", true);
        }
    }
}

fn record_advised_usage_ledger_pipeline_result<ProviderError, AdvisorError, LedgerError>(
    span: &Span,
    started_at: Instant,
    result: &Result<
        ChatResponse,
        AdvisedUsageLedgerPipelineError<ProviderError, AdvisorError, LedgerError>,
    >,
) {
    match result {
        Ok(response) => record_success(span, started_at, response.usage()),
        Err(error) => record_advised_usage_ledger_pipeline_error(span, started_at, error),
    }
}

fn record_advised_usage_ledger_pipeline_error<ProviderError, AdvisorError, LedgerError>(
    span: &Span,
    started_at: Instant,
    error: &AdvisedUsageLedgerPipelineError<ProviderError, AdvisorError, LedgerError>,
) {
    match error {
        AdvisedUsageLedgerPipelineError::Advisor(_) => {
            record_outcome(span, started_at, "advisor_failed", true);
        }
        AdvisedUsageLedgerPipelineError::ReservationMetadata(_) => {
            record_outcome(span, started_at, "usage_reservation_invalid", false);
        }
        AdvisedUsageLedgerPipelineError::Usage(error) => {
            record_usage_ledger_pipeline_error(span, started_at, error);
        }
    }
}

/// Invalid request construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RequestError {
    /// Alias was blank.
    #[error("AI model alias must not be blank")]
    BlankModel,
    /// No messages were supplied.
    #[error("AI request must contain at least one message")]
    EmptyMessages,
    /// A message was blank.
    #[error("AI message content must not be blank")]
    BlankMessage,
}

/// Invalid provider response metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ResponseError {
    /// Response ID was blank.
    #[error("AI provider response ID must not be blank")]
    BlankId,
    /// Model was blank.
    #[error("AI provider model must not be blank")]
    BlankModel,
}

/// Invalid tool definition or call metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ToolError {
    /// Name was blank or had unsupported characters.
    #[error("AI tool name must use ASCII letters, digits, underscore, hyphen, or dot")]
    InvalidName,
    /// Provider call ID was blank.
    #[error("AI tool call ID must not be blank")]
    BlankCallId,
}

/// Structured JSON parsing failure.
#[derive(Debug, thiserror::Error)]
pub enum StructuredOutputError {
    /// Model output failed to deserialize.
    #[error("AI structured output was invalid JSON: {0}")]
    Deserialize(serde_json::Error),
}

/// Request policy failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PolicyError {
    /// Input exceeded the configured character bound.
    #[error("AI input has {actual} characters, exceeding the limit of {limit}")]
    InputTooLarge {
        /// Configured limit.
        limit: usize,
        /// Observed count.
        actual: usize,
    },
    /// Too many tool declarations were supplied.
    #[error("AI request has {actual} tools, exceeding the limit of {limit}")]
    TooManyTools {
        /// Configured limit.
        limit: usize,
        /// Observed count.
        actual: usize,
    },
}

/// Policy or provider failure.
#[derive(Debug, thiserror::Error)]
pub enum PipelineError<E> {
    /// Request was rejected before provider invocation.
    #[error(transparent)]
    Policy(PolicyError),
    /// Provider failed.
    #[error("AI provider failed: {0}")]
    Provider(E),
}

/// Failure while a budget advisor admits one provider request.
#[derive(Debug, thiserror::Error)]
pub enum BudgetAdvisorError<E> {
    /// The application deliberately refused this request before provider invocation.
    #[error("AI request exceeded the application budget")]
    Denied,
    /// The application budget store or policy could not decide safely.
    #[error("AI budget policy failed: {0}")]
    Policy(E),
}

/// Failure while a usage-ledger pipeline reserves or settles one provider attempt.
#[derive(Debug, thiserror::Error)]
pub enum UsageLedgerPipelineError<ProviderError, LedgerError> {
    /// The request exceeded the pipeline's explicit bounds before a reservation was created.
    #[error(transparent)]
    Policy(PolicyError),
    /// The supplied reservation does not describe the request that would reach the provider.
    #[error("AI usage reservation does not match the provider request")]
    ReservationRequestMismatch,
    /// The durable ledger could not decide whether the provider may start.
    #[error("AI usage reservation could not be recorded: {0}")]
    Reservation(LedgerError),
    /// The application deliberately refused this attempt before provider invocation.
    #[error("AI request exceeded the application budget")]
    Denied,
    /// A prior attempt may have reached the provider but has no durable terminal usage yet.
    #[error("AI usage reservation requires reconciliation before another provider attempt")]
    PendingReconciliation,
    /// A prior attempt already has durable terminal usage, so this call must not be repeated.
    #[error("AI usage reservation is already settled")]
    AlreadySettled,
    /// The provider failed after reservation; usage remains pending for reconciliation.
    #[error("AI provider failed: {0}")]
    Provider(ProviderError),
    /// The provider completed, but its actual usage was not durably recorded.
    #[error("AI provider usage could not be recorded; reconciliation is required")]
    Settlement {
        /// The completed response. Reuse it or reconcile the ledger; do not repeat the provider.
        response: ChatResponse,
        /// Durable ledger failure.
        #[source]
        source: LedgerError,
    },
}

/// Provider or ledger failure emitted after a usage-ledger stream has opened.
#[derive(Debug, thiserror::Error)]
pub enum UsageLedgerStreamError<ProviderError, LedgerError> {
    /// The provider emitted a stream failure; usage remains pending for reconciliation.
    #[error("AI provider stream failed: {0}")]
    Provider(ProviderError),
    /// The provider completed, but terminal usage could not be persisted.
    #[error("AI provider stream usage could not be recorded; reconciliation is required")]
    Ledger(#[source] LedgerError),
    /// The provider emitted more than one terminal completion event.
    #[error("AI provider stream emitted multiple completion events")]
    DuplicateCompletion,
}

/// Failure while an advisor and usage ledger wrap a non-streaming provider call.
#[derive(Debug, thiserror::Error)]
pub enum AdvisedUsageLedgerPipelineError<ProviderError, AdvisorError, LedgerError> {
    /// The application advisor could not enrich, validate, or transform the call.
    #[error("AI advisor failed: {0}")]
    Advisor(AdvisorError),
    /// The stable provider-attempt metadata was invalid before a reservation could be written.
    #[error("AI usage reservation metadata was invalid: {0}")]
    ReservationMetadata(AiUsageReservationError),
    /// Policy, durable ledger, or provider lifecycle failure.
    #[error(transparent)]
    Usage(UsageLedgerPipelineError<ProviderError, LedgerError>),
}

/// Provider, ledger, or advisor failure emitted after an advised usage-ledger stream opens.
#[derive(Debug, thiserror::Error)]
pub enum AdvisedUsageLedgerStreamError<ProviderError, AdvisorError, LedgerError> {
    /// Provider or durable-usage lifecycle failure.
    #[error(transparent)]
    Usage(UsageLedgerStreamError<ProviderError, LedgerError>),
    /// Application stream-event processing failure.
    #[error("AI advisor stream processing failed: {0}")]
    Advisor(AdvisorError),
}

/// Failure while an advisor wraps a non-streaming provider call.
#[derive(Debug, thiserror::Error)]
pub enum AdvisedPipelineError<ProviderError, AdvisorError> {
    /// The final advisor-produced request exceeded an explicit pipeline bound.
    #[error(transparent)]
    Policy(PolicyError),
    /// The provider rejected or could not complete the request.
    #[error("AI provider failed: {0}")]
    Provider(ProviderError),
    /// Application advisor enrichment, validation, or response processing failed.
    #[error("AI advisor failed: {0}")]
    Advisor(AdvisorError),
}

/// Provider or advisor failure emitted after an advised stream has opened.
#[derive(Debug, thiserror::Error)]
pub enum AdvisedStreamError<ProviderError, AdvisorError> {
    /// The provider emitted a stream failure.
    #[error("AI provider stream failed: {0}")]
    Provider(ProviderError),
    /// Application advisor stream processing failed.
    #[error("AI advisor stream processing failed: {0}")]
    Advisor(AdvisorError),
}

fn validate_tool_name(name: &str) -> Result<(), ToolError> {
    if name.is_empty()
        || !name
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | '-' | '.'))
    {
        return Err(ToolError::InvalidName);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AdvisedPipelineError, AdvisedUsageLedgerPipelineError, AiAdvisor, AiBudgetDecision,
        AiBudgetPolicy, AiBudgetRequest, AiExecutionContext, AiExecutionContextError, AiPipeline,
        AiPolicy, AiProvider, AiStreamEvent, AiUsageLedger, AiUsageReservation,
        AiUsageReservationDecision, AiUsageReservationError, AiUsageSettlement,
        AuditedToolRunError, BudgetAdvisor, BudgetAdvisorError, ChatMessage, ChatRequest,
        ChatResponse, DenyAllToolApproval, ExecutionAuditedToolRunError, MessageRole, PolicyError,
        ToolApprovalAuditEvent, ToolApprovalAuditSink, ToolApprovalDecision, ToolApprovalPolicy,
        ToolCall, ToolDefinition, ToolExecutionAuditEvent, ToolExecutionAuditSink,
        ToolExecutionContext, ToolExecutionContextError, ToolExecutionError, ToolExecutionOutcome,
        ToolExecutor, ToolRegistry, ToolRisk, ToolRunError, Usage, UsageLedgerPipelineError,
        UsageLedgerStreamError,
    };
    use futures_util::{StreamExt, future, stream};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::{
        convert::Infallible,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[derive(Clone, Debug)]
    struct Fake;
    impl AiProvider for Fake {
        type Error = Infallible;
        fn complete(
            &self,
            _: ChatRequest,
        ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
            Box::pin(future::ready(Ok(ChatResponse::new(
                "response",
                "fake",
                r#"{"answer":42}"#,
                [],
                Usage {
                    input_tokens: 3,
                    output_tokens: 2,
                },
            )
            .unwrap())))
        }
        fn stream(&self, _: ChatRequest) -> super::AiEventStreamFuture<Self::Error> {
            let events: super::AiEventStream<Self::Error> = Box::pin(stream::empty());
            Box::pin(future::ready(Ok(events)))
        }
    }
    fn request() -> ChatRequest {
        ChatRequest::new(
            "support.default",
            [ChatMessage::new(MessageRole::User, "status?").unwrap()],
        )
        .unwrap()
    }
    #[derive(Deserialize)]
    struct Output {
        answer: u64,
    }
    #[tokio::test]
    async fn pipeline_parses_structured_output() {
        assert_eq!(
            AiPipeline::new(Fake)
                .complete(request())
                .await
                .unwrap()
                .parse_json::<Output>()
                .unwrap()
                .answer,
            42
        );
    }
    #[test]
    fn policy_rejects_input_before_provider() {
        assert!(
            AiPolicy {
                max_input_characters: 2,
                max_tools: 1
            }
            .validate(&request())
            .is_err()
        );
    }

    #[derive(Clone)]
    struct CountingProvider {
        invocations: Arc<AtomicUsize>,
    }

    impl AiProvider for CountingProvider {
        type Error = Infallible;

        fn complete(
            &self,
            _: ChatRequest,
        ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
            self.invocations.fetch_add(1, Ordering::SeqCst);
            Box::pin(future::ready(Ok(ChatResponse::new(
                "response",
                "fake",
                "complete",
                [],
                Usage::default(),
            )
            .unwrap())))
        }

        fn stream(&self, _: ChatRequest) -> super::AiEventStreamFuture<Self::Error> {
            self.invocations.fetch_add(1, Ordering::SeqCst);
            let events: super::AiEventStream<Self::Error> = Box::pin(stream::iter(vec![
                Ok(AiStreamEvent::TextDelta("delta".to_owned())),
                Ok(AiStreamEvent::Completed(Usage {
                    input_tokens: 3,
                    output_tokens: 2,
                })),
            ]));
            Box::pin(future::ready(Ok(events)))
        }
    }

    #[derive(Clone)]
    struct ContextAdvisor {
        before: Arc<AtomicUsize>,
        response: Arc<AtomicUsize>,
        stream: Arc<AtomicUsize>,
    }

    impl ContextAdvisor {
        fn new() -> Self {
            Self {
                before: Arc::new(AtomicUsize::new(0)),
                response: Arc::new(AtomicUsize::new(0)),
                stream: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl AiAdvisor for ContextAdvisor {
        type Error = Infallible;

        fn before_request(
            &self,
            request: ChatRequest,
        ) -> futures_util::future::BoxFuture<'static, Result<ChatRequest, Self::Error>> {
            self.before.fetch_add(1, Ordering::SeqCst);
            let request = request.with_added_message(
                ChatMessage::new(MessageRole::System, "x").expect("test message is valid"),
            );
            Box::pin(future::ready(Ok(request)))
        }

        fn after_response(
            &self,
            response: ChatResponse,
        ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
            self.response.fetch_add(1, Ordering::SeqCst);
            Box::pin(future::ready(Ok(response)))
        }

        fn on_stream_event(
            &self,
            event: AiStreamEvent,
        ) -> futures_util::future::BoxFuture<'static, Result<AiStreamEvent, Self::Error>> {
            self.stream.fetch_add(1, Ordering::SeqCst);
            let event = match event {
                AiStreamEvent::TextDelta(text) => AiStreamEvent::TextDelta(format!("{text}!")),
                event => event,
            };
            Box::pin(future::ready(Ok(event)))
        }
    }

    #[tokio::test]
    async fn advisor_context_is_checked_before_provider_invocation() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let advisor = ContextAdvisor::new();
        let error = AiPipeline::new(CountingProvider {
            invocations: Arc::clone(&invocations),
        })
        .with_policy(AiPolicy {
            max_input_characters: 7,
            max_tools: 1,
        })
        .complete_with_advisor(request(), &advisor)
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            AdvisedPipelineError::Policy(PolicyError::InputTooLarge {
                limit: 7,
                actual: 8,
            })
        ));
        assert_eq!(advisor.before.load(Ordering::SeqCst), 1);
        assert_eq!(invocations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn advisor_runs_for_complete_and_each_stream_event() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let pipeline = AiPipeline::new(CountingProvider {
            invocations: Arc::clone(&invocations),
        });
        let advisor = ContextAdvisor::new();

        let response = pipeline
            .complete_with_advisor(request(), &advisor)
            .await
            .unwrap();
        assert_eq!(response.content(), "complete");

        let events = pipeline
            .stream_with_advisor(request(), &advisor)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].as_ref().unwrap(),
            &AiStreamEvent::TextDelta("delta!".to_owned())
        );
        assert!(matches!(
            events[1].as_ref().unwrap(),
            AiStreamEvent::Completed(Usage {
                input_tokens: 3,
                output_tokens: 2,
            })
        ));
        assert_eq!(advisor.before.load(Ordering::SeqCst), 2);
        assert_eq!(advisor.response.load(Ordering::SeqCst), 1);
        assert_eq!(advisor.stream.load(Ordering::SeqCst), 2);
        assert_eq!(invocations.load(Ordering::SeqCst), 2);
    }

    #[derive(Clone)]
    struct DenyingBudget {
        admissions: Arc<Mutex<Vec<(AiExecutionContext, AiBudgetRequest)>>>,
    }

    impl AiBudgetPolicy for DenyingBudget {
        type Error = Infallible;

        fn admit(
            &self,
            context: AiExecutionContext,
            request: AiBudgetRequest,
        ) -> futures_util::future::BoxFuture<'static, Result<AiBudgetDecision, Self::Error>>
        {
            let admissions = Arc::clone(&self.admissions);
            Box::pin(async move {
                admissions
                    .lock()
                    .expect("test budget lock is available")
                    .push((context, request));
                Ok(AiBudgetDecision::Denied)
            })
        }
    }

    #[tokio::test]
    async fn budget_advisor_denies_before_provider_invocation_without_prompt_content() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let admissions = Arc::new(Mutex::new(Vec::new()));
        let advisor = BudgetAdvisor::new(
            ai_context(),
            DenyingBudget {
                admissions: Arc::clone(&admissions),
            },
        );

        let error = AiPipeline::new(CountingProvider {
            invocations: Arc::clone(&invocations),
        })
        .complete_with_advisor(request(), &advisor)
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            AdvisedPipelineError::Advisor(BudgetAdvisorError::Denied)
        ));
        assert_eq!(invocations.load(Ordering::SeqCst), 0);

        let admissions = admissions.lock().expect("test budget lock is available");
        assert_eq!(admissions.len(), 1);
        let (context, request) = &admissions[0];
        assert_eq!(context.tenant(), "tenant-a");
        assert_eq!(context.subject(), "user-7");
        assert_eq!(request.model(), "support.default");
        assert_eq!(request.input_characters(), 7);
        assert_eq!(request.tool_count(), 0);
        assert_eq!(request.tool_result_count(), 0);
        assert!(!format!("{request:?}").contains("status?"));
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
    enum TestUsageLedgerError {
        #[error("test usage ledger is unavailable")]
        Unavailable,
    }

    #[derive(Clone)]
    struct CapturingUsageLedger {
        decision: AiUsageReservationDecision,
        reservations: Arc<Mutex<Vec<AiUsageReservation>>>,
        settlements: Arc<Mutex<Vec<AiUsageSettlement>>>,
        fail_settlement: bool,
    }

    impl AiUsageLedger for CapturingUsageLedger {
        type Error = TestUsageLedgerError;

        fn reserve(
            &self,
            reservation: AiUsageReservation,
        ) -> futures_util::future::BoxFuture<'static, Result<AiUsageReservationDecision, Self::Error>>
        {
            let reservations = Arc::clone(&self.reservations);
            let decision = self.decision;
            Box::pin(async move {
                reservations
                    .lock()
                    .expect("test usage ledger lock is available")
                    .push(reservation);
                Ok(decision)
            })
        }

        fn record_usage(
            &self,
            settlement: AiUsageSettlement,
        ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
            let settlements = Arc::clone(&self.settlements);
            let fail_settlement = self.fail_settlement;
            Box::pin(async move {
                if fail_settlement {
                    return Err(TestUsageLedgerError::Unavailable);
                }
                settlements
                    .lock()
                    .expect("test usage ledger lock is available")
                    .push(settlement);
                Ok(())
            })
        }
    }

    fn usage_ledger(
        decision: AiUsageReservationDecision,
        fail_settlement: bool,
    ) -> CapturingUsageLedger {
        CapturingUsageLedger {
            decision,
            reservations: Arc::new(Mutex::new(Vec::new())),
            settlements: Arc::new(Mutex::new(Vec::new())),
            fail_settlement,
        }
    }

    fn usage_reservation(key: &str) -> AiUsageReservation {
        AiUsageReservation::for_request(ai_context(), key, &request())
            .expect("test usage reservation is valid")
    }

    #[tokio::test]
    async fn usage_ledger_reserves_before_completion_and_records_actual_usage() {
        let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
        let response = AiPipeline::new(Fake)
            .complete_with_usage_ledger(request(), usage_reservation("ai:request:1"), &ledger)
            .await
            .unwrap();

        assert_eq!(response.usage().total_tokens(), 5);
        let reservations = ledger
            .reservations
            .lock()
            .expect("test usage ledger lock is available");
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0].context().tenant(), "tenant-a");
        assert_eq!(reservations[0].idempotency_key(), "ai:request:1");
        assert!(!format!("{:?}", reservations[0]).contains("ai:request:1"));
        drop(reservations);
        let settlements = ledger
            .settlements
            .lock()
            .expect("test usage ledger lock is available");
        assert_eq!(settlements.len(), 1);
        assert_eq!(settlements[0].usage(), response.usage());
    }

    #[tokio::test]
    async fn usage_ledger_blocks_a_pending_retry_before_provider_invocation() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let ledger = usage_ledger(AiUsageReservationDecision::PendingReconciliation, false);
        let error = AiPipeline::new(CountingProvider {
            invocations: Arc::clone(&invocations),
        })
        .complete_with_usage_ledger(request(), usage_reservation("ai:request:pending"), &ledger)
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            UsageLedgerPipelineError::PendingReconciliation
        ));
        assert_eq!(invocations.load(Ordering::SeqCst), 0);
        assert_eq!(
            ledger
                .reservations
                .lock()
                .expect("test usage ledger lock is available")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn usage_ledger_settles_stream_only_after_terminal_usage_event() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
        let events = AiPipeline::new(CountingProvider {
            invocations: Arc::clone(&invocations),
        })
        .stream_with_usage_ledger(request(), usage_reservation("ai:stream:1"), &ledger)
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[1],
            Ok(AiStreamEvent::Completed(Usage {
                input_tokens: 3,
                output_tokens: 2,
            }))
        ));
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        let settlements = ledger
            .settlements
            .lock()
            .expect("test usage ledger lock is available");
        assert_eq!(settlements.len(), 1);
        assert_eq!(settlements[0].usage().total_tokens(), 5);
    }

    #[tokio::test]
    async fn usage_ledger_settlement_failure_returns_the_completed_response_without_retrying() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let ledger = usage_ledger(AiUsageReservationDecision::Reserved, true);
        let error = AiPipeline::new(CountingProvider {
            invocations: Arc::clone(&invocations),
        })
        .complete_with_usage_ledger(
            request(),
            usage_reservation("ai:request:reconcile"),
            &ledger,
        )
        .await
        .unwrap_err();

        match error {
            UsageLedgerPipelineError::Settlement { response, source } => {
                assert_eq!(response.content(), "complete");
                assert_eq!(source, TestUsageLedgerError::Unavailable);
            }
            error => panic!("expected settlement failure, received {error:?}"),
        }
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        assert!(
            ledger
                .settlements
                .lock()
                .expect("test usage ledger lock is available")
                .is_empty()
        );
    }

    #[test]
    fn usage_reservation_requires_a_stable_key_and_redacts_metadata() {
        assert_eq!(
            AiUsageReservation::for_request(ai_context(), " ", &request()).unwrap_err(),
            AiUsageReservationError::BlankIdempotencyKey
        );
        let reservation = usage_reservation("ai:request:redacted");
        let debug = format!("{reservation:?}");
        assert!(!debug.contains("tenant-a"));
        assert!(!debug.contains("ai:request:redacted"));
        assert!(!debug.contains("support.default"));
    }

    #[tokio::test]
    async fn usage_ledger_stream_settlement_failure_is_returned_after_the_completion_event() {
        let ledger = usage_ledger(AiUsageReservationDecision::Reserved, true);
        let events = AiPipeline::new(CountingProvider {
            invocations: Arc::new(AtomicUsize::new(0)),
        })
        .stream_with_usage_ledger(request(), usage_reservation("ai:stream:reconcile"), &ledger)
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[1],
            Err(UsageLedgerStreamError::Ledger(
                TestUsageLedgerError::Unavailable
            ))
        ));
    }

    #[tokio::test]
    async fn advisor_and_usage_ledger_reserve_the_final_enriched_request_before_completion() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let advisor = ContextAdvisor::new();
        let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
        let response = AiPipeline::new(CountingProvider {
            invocations: Arc::clone(&invocations),
        })
        .complete_with_advisor_and_usage_ledger(
            request(),
            &advisor,
            ai_context(),
            "ai:advised:completion",
            &ledger,
        )
        .await
        .unwrap();

        assert_eq!(response.content(), "complete");
        assert_eq!(advisor.before.load(Ordering::SeqCst), 1);
        assert_eq!(advisor.response.load(Ordering::SeqCst), 1);
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        let reservations = ledger
            .reservations
            .lock()
            .expect("test usage ledger lock is available");
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0].request().input_characters(), 8);
        assert_eq!(reservations[0].idempotency_key(), "ai:advised:completion");
        drop(reservations);
        assert_eq!(
            ledger
                .settlements
                .lock()
                .expect("test usage ledger lock is available")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn advisor_and_usage_ledger_settle_raw_stream_usage_before_advisor_output() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let advisor = ContextAdvisor::new();
        let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
        let events = AiPipeline::new(CountingProvider {
            invocations: Arc::clone(&invocations),
        })
        .stream_with_advisor_and_usage_ledger(
            request(),
            &advisor,
            ai_context(),
            "ai:advised:stream",
            &ledger,
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].as_ref().unwrap(),
            &AiStreamEvent::TextDelta("delta!".to_owned())
        );
        assert!(matches!(
            events[1],
            Ok(AiStreamEvent::Completed(Usage {
                input_tokens: 3,
                output_tokens: 2,
            }))
        ));
        assert_eq!(advisor.before.load(Ordering::SeqCst), 1);
        assert_eq!(advisor.stream.load(Ordering::SeqCst), 2);
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        let settlements = ledger
            .settlements
            .lock()
            .expect("test usage ledger lock is available");
        assert_eq!(settlements.len(), 1);
        assert_eq!(settlements[0].usage().total_tokens(), 5);
    }

    #[tokio::test]
    async fn advisor_and_usage_ledger_reject_blank_provider_keys_before_invocation() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let advisor = ContextAdvisor::new();
        let ledger = usage_ledger(AiUsageReservationDecision::Reserved, false);
        let error = AiPipeline::new(CountingProvider {
            invocations: Arc::clone(&invocations),
        })
        .complete_with_advisor_and_usage_ledger(request(), &advisor, ai_context(), " ", &ledger)
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            AdvisedUsageLedgerPipelineError::ReservationMetadata(
                AiUsageReservationError::BlankIdempotencyKey
            )
        ));
        assert_eq!(advisor.before.load(Ordering::SeqCst), 1);
        assert_eq!(invocations.load(Ordering::SeqCst), 0);
        assert!(
            ledger
                .reservations
                .lock()
                .expect("test usage ledger lock is available")
                .is_empty()
        );
    }

    #[derive(Deserialize)]
    struct LookupArguments {
        id: u64,
    }

    #[derive(Serialize)]
    struct LookupResult {
        status: &'static str,
    }

    #[derive(Clone, Copy)]
    struct ApproveAll;

    impl ToolApprovalPolicy for ApproveAll {
        type Error = Infallible;

        fn approve(
            &self,
            _: AiExecutionContext,
            _: ToolCall,
            _: ToolRisk,
        ) -> futures_util::future::BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>>
        {
            Box::pin(future::ready(Ok(ToolApprovalDecision::Approved)))
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
    enum TestAuditError {
        #[error("test audit store is unavailable")]
        Unavailable,
    }

    #[derive(Clone)]
    struct CapturingAudit {
        events: Arc<Mutex<Vec<ToolApprovalAuditEvent>>>,
        should_fail: bool,
    }

    impl ToolApprovalAuditSink for CapturingAudit {
        type Error = TestAuditError;

        fn record_approved(
            &self,
            event: ToolApprovalAuditEvent,
        ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
            let events = Arc::clone(&self.events);
            let should_fail = self.should_fail;
            Box::pin(async move {
                if should_fail {
                    return Err(TestAuditError::Unavailable);
                }
                events
                    .lock()
                    .expect("test audit lock is available")
                    .push(event);
                Ok(())
            })
        }
    }

    #[derive(Clone)]
    struct CapturingExecutionAudit {
        approvals: Arc<Mutex<Vec<ToolApprovalAuditEvent>>>,
        outcomes: Arc<Mutex<Vec<ToolExecutionAuditEvent>>>,
        fail_approval: bool,
        fail_outcome: bool,
    }

    impl ToolApprovalAuditSink for CapturingExecutionAudit {
        type Error = TestAuditError;

        fn record_approved(
            &self,
            event: ToolApprovalAuditEvent,
        ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
            let approvals = Arc::clone(&self.approvals);
            let fail_approval = self.fail_approval;
            Box::pin(async move {
                if fail_approval {
                    return Err(TestAuditError::Unavailable);
                }
                approvals
                    .lock()
                    .expect("test audit lock is available")
                    .push(event);
                Ok(())
            })
        }
    }

    impl ToolExecutionAuditSink for CapturingExecutionAudit {
        fn record_outcome(
            &self,
            event: ToolExecutionAuditEvent,
        ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
            let outcomes = Arc::clone(&self.outcomes);
            let fail_outcome = self.fail_outcome;
            Box::pin(async move {
                if fail_outcome {
                    return Err(TestAuditError::Unavailable);
                }
                outcomes
                    .lock()
                    .expect("test audit lock is available")
                    .push(event);
                Ok(())
            })
        }
    }

    fn lookup_tool(calls: Arc<AtomicUsize>) -> impl ToolExecutor {
        super::TypedTool::new(
            ToolDefinition::new("lookup_order", json!({"type":"object"})).unwrap(),
            ToolRisk::ReadOnly,
            move |context: ToolExecutionContext, arguments: LookupArguments| {
                assert_eq!(arguments.id, 7);
                assert_eq!(context.tenant(), "tenant-a");
                assert_eq!(context.subject(), "user-7");
                assert_eq!(context.idempotency_key(), "external:order:7");
                calls.fetch_add(1, Ordering::SeqCst);
                future::ready(Ok::<LookupResult, Infallible>(LookupResult {
                    status: "found",
                }))
            },
        )
    }

    fn ai_context() -> AiExecutionContext {
        AiExecutionContext::new("tenant-a", "user-7").expect("test context is valid")
    }

    fn tool_context() -> ToolExecutionContext {
        ToolExecutionContext::new(ai_context(), "external:order:7")
            .expect("test execution context is valid")
    }

    #[test]
    fn ai_execution_context_requires_trusted_identity_and_redacts_debug_output() {
        assert_eq!(
            AiExecutionContext::new(" ", "user").unwrap_err(),
            AiExecutionContextError::BlankTenant
        );
        assert_eq!(
            AiExecutionContext::new("tenant", " ").unwrap_err(),
            AiExecutionContextError::BlankSubject
        );
        let debug = format!("{:?}", ai_context());
        assert!(!debug.contains("tenant-a"));
        assert!(!debug.contains("user-7"));
    }

    #[test]
    fn tool_execution_context_requires_an_idempotency_key_and_redacts_debug_output() {
        assert_eq!(
            ToolExecutionContext::new(ai_context(), " ").unwrap_err(),
            ToolExecutionContextError::BlankIdempotencyKey
        );
        let debug = format!("{:?}", tool_context());
        assert!(!debug.contains("tenant-a"));
        assert!(!debug.contains("external:order:7"));
    }

    #[test]
    fn tool_names_allow_explicit_remote_namespaces() {
        let definition = ToolDefinition::new("orders.lookup.v1", json!({"type":"object"}))
            .expect("dotted remote tool name is portable");
        let call = ToolCall::new("call-remote-1", "orders.lookup.v1", json!({"id":7}))
            .expect("dotted remote tool call is portable");

        assert_eq!(definition.name(), "orders.lookup.v1");
        assert_eq!(call.name(), "orders.lookup.v1");
    }

    #[tokio::test]
    async fn tool_registry_requires_approval_before_handler_execution() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

        let error = registry
            .execute(
                tool_context(),
                ToolCall::new("call-1", "lookup_order", json!({"id": 7})).unwrap(),
                &DenyAllToolApproval,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, ToolRunError::Denied { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn typed_tool_rejects_invalid_arguments_before_handler_execution() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

        let error = registry
            .execute(
                tool_context(),
                ToolCall::new("call-2", "lookup_order", json!({"id":"not-a-number"})).unwrap(),
                &ApproveAll,
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ToolRunError::Execution(ToolExecutionError::InvalidArguments)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn approved_typed_tool_returns_redacted_debug_result() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::new();
        registry.register(lookup_tool(calls)).unwrap();

        let result = registry
            .execute(
                tool_context(),
                ToolCall::new("call-3", "lookup_order", json!({"id": 7})).unwrap(),
                &ApproveAll,
            )
            .await
            .unwrap();

        assert_eq!(result.content(), &json!({"status":"found"}));
        assert!(!format!("{result:?}").contains("found"));
    }

    #[tokio::test]
    async fn approved_tool_audit_persists_before_the_handler_runs() {
        let calls = Arc::new(AtomicUsize::new(0));
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ToolRegistry::new();
        registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

        let result = registry
            .execute_with_approval_audit(
                tool_context(),
                ToolCall::new("call-audit-1", "lookup_order", json!({"id": 7})).unwrap(),
                &ApproveAll,
                &CapturingAudit {
                    events: Arc::clone(&events),
                    should_fail: false,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.content(), &json!({"status":"found"}));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let events = events.lock().expect("test audit lock is available");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].context().tenant(), "tenant-a");
        assert_eq!(events[0].context().subject(), "user-7");
        assert_eq!(events[0].idempotency_key(), "external:order:7");
        assert_eq!(events[0].call_id(), "call-audit-1");
        assert_eq!(events[0].tool_name(), "lookup_order");
        assert_eq!(events[0].risk(), ToolRisk::ReadOnly);
        let debug = format!("{:?}", events[0]);
        assert!(!debug.contains("tenant-a"));
        assert!(!debug.contains("call-audit-1"));
        assert!(!debug.contains("external:order:7"));
    }

    #[tokio::test]
    async fn execution_audit_records_a_terminal_success_after_the_handler() {
        let calls = Arc::new(AtomicUsize::new(0));
        let approvals = Arc::new(Mutex::new(Vec::new()));
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ToolRegistry::new();
        registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

        let result = registry
            .execute_with_execution_audit(
                tool_context(),
                ToolCall::new("call-execution-1", "lookup_order", json!({"id": 7})).unwrap(),
                &ApproveAll,
                &CapturingExecutionAudit {
                    approvals: Arc::clone(&approvals),
                    outcomes: Arc::clone(&outcomes),
                    fail_approval: false,
                    fail_outcome: false,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.content(), &json!({"status":"found"}));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            approvals
                .lock()
                .expect("test audit lock is available")
                .len(),
            1
        );
        let outcomes = outcomes.lock().expect("test audit lock is available");
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].outcome(), ToolExecutionOutcome::Succeeded);
        assert_eq!(outcomes[0].approval().idempotency_key(), "external:order:7");
        assert!(!format!("{:?}", outcomes[0]).contains("external:order:7"));
    }

    #[tokio::test]
    async fn execution_audit_records_a_terminal_failure_before_returning_handler_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ToolRegistry::new();
        registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

        let error = registry
            .execute_with_execution_audit(
                tool_context(),
                ToolCall::new("call-execution-2", "lookup_order", json!({"id":"bad"})).unwrap(),
                &ApproveAll,
                &CapturingExecutionAudit {
                    approvals: Arc::new(Mutex::new(Vec::new())),
                    outcomes: Arc::clone(&outcomes),
                    fail_approval: false,
                    fail_outcome: false,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ExecutionAuditedToolRunError::Run(ToolRunError::Execution(
                ToolExecutionError::InvalidArguments
            ))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let outcomes = outcomes.lock().expect("test audit lock is available");
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].outcome(), ToolExecutionOutcome::Failed);
    }

    #[tokio::test]
    async fn failed_execution_outcome_audit_requires_reconciliation_after_the_handler() {
        let calls = Arc::new(AtomicUsize::new(0));
        let approvals = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ToolRegistry::new();
        registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

        let error = registry
            .execute_with_execution_audit(
                tool_context(),
                ToolCall::new("call-execution-3", "lookup_order", json!({"id": 7})).unwrap(),
                &ApproveAll,
                &CapturingExecutionAudit {
                    approvals: Arc::clone(&approvals),
                    outcomes: Arc::new(Mutex::new(Vec::new())),
                    fail_approval: false,
                    fail_outcome: true,
                },
            )
            .await
            .unwrap_err();

        match error {
            ExecutionAuditedToolRunError::OutcomeAudit { event, source } => {
                assert_eq!(source, TestAuditError::Unavailable);
                assert_eq!(event.outcome(), ToolExecutionOutcome::Succeeded);
                assert_eq!(event.approval().call_id(), "call-execution-3");
                assert_eq!(event.approval().idempotency_key(), "external:order:7");
            }
            error => panic!("expected outcome audit failure, received {error:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            approvals
                .lock()
                .expect("test audit lock is available")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn failed_approval_audit_blocks_the_tool_handler() {
        let calls = Arc::new(AtomicUsize::new(0));
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ToolRegistry::new();
        registry.register(lookup_tool(Arc::clone(&calls))).unwrap();

        let error = registry
            .execute_with_approval_audit(
                tool_context(),
                ToolCall::new("call-audit-2", "lookup_order", json!({"id": 7})).unwrap(),
                &ApproveAll,
                &CapturingAudit {
                    events: Arc::clone(&events),
                    should_fail: true,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AuditedToolRunError::Audit(TestAuditError::Unavailable)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            events
                .lock()
                .expect("test audit lock is available")
                .is_empty()
        );
    }
}
