//! Public AI observability contract in an isolated tracing test binary.

use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::{StreamExt, future, stream};
use opentelemetry::trace::{SpanKind, TracerProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use rustee_ai::{
    AiAdvisor, AiEventStream, AiEventStreamFuture, AiExecutionContext, AiPipeline, AiPolicy,
    AiProvider, AiStreamEvent, AiUsageLedger, AiUsageReservation, AiUsageReservationDecision,
    AiUsageSettlement, ChatMessage, ChatRequest, ChatResponse, MessageRole, ToolApprovalAuditEvent,
    ToolApprovalAuditSink, ToolApprovalDecision, ToolApprovalPolicy, ToolCall, ToolDefinition,
    ToolExecutionAuditEvent, ToolExecutionAuditSink, ToolExecutionContext, ToolRegistry,
    ToolResult, ToolRisk, TypedTool, Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Clone)]
struct Provider;

impl AiProvider for Provider {
    type Error = Infallible;

    fn complete(
        &self,
        _: ChatRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        Box::pin(future::ready(Ok(ChatResponse::new(
            "response",
            "provider-model",
            "provider completion",
            [],
            Usage {
                input_tokens: 3,
                output_tokens: 2,
            },
        )
        .unwrap())))
    }

    fn stream(&self, _: ChatRequest) -> AiEventStreamFuture<Self::Error> {
        let events: AiEventStream<Self::Error> = Box::pin(stream::iter([
            Ok(AiStreamEvent::TextDelta("provider fragment".to_owned())),
            Ok(AiStreamEvent::Completed(Usage {
                input_tokens: 3,
                output_tokens: 2,
            })),
        ]));
        Box::pin(future::ready(Ok(events)))
    }
}

#[derive(Clone, Copy)]
struct Approve;

impl ToolApprovalPolicy for Approve {
    type Error = Infallible;

    fn approve(
        &self,
        _: AiExecutionContext,
        _: ToolCall,
        _: ToolRisk,
    ) -> futures_util::future::BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>> {
        Box::pin(future::ready(Ok(ToolApprovalDecision::Approved)))
    }
}

#[derive(Clone, Copy)]
struct Audit;

impl ToolApprovalAuditSink for Audit {
    type Error = Infallible;

    fn record_approved(
        &self,
        _: ToolApprovalAuditEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(future::ready(Ok(())))
    }
}

impl ToolExecutionAuditSink for Audit {
    fn record_outcome(
        &self,
        _: ToolExecutionAuditEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(future::ready(Ok(())))
    }
}

#[derive(Clone, Copy)]
struct Ledger;

impl AiUsageLedger for Ledger {
    type Error = Infallible;

    fn reserve(
        &self,
        _: AiUsageReservation,
    ) -> futures_util::future::BoxFuture<'static, Result<AiUsageReservationDecision, Self::Error>>
    {
        Box::pin(future::ready(Ok(AiUsageReservationDecision::Reserved)))
    }

    fn record_usage(
        &self,
        _: AiUsageSettlement,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(future::ready(Ok(())))
    }
}

#[derive(Clone, Copy)]
struct ContextAdvisor;

impl AiAdvisor for ContextAdvisor {
    type Error = Infallible;

    fn before_request(
        &self,
        request: ChatRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<ChatRequest, Self::Error>> {
        let request = request.with_added_message(
            ChatMessage::new(MessageRole::System, "private advisor context").unwrap(),
        );
        Box::pin(future::ready(Ok(request)))
    }

    fn after_response(
        &self,
        response: ChatResponse,
    ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        Box::pin(future::ready(Ok(response)))
    }

    fn on_stream_event(
        &self,
        event: AiStreamEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<AiStreamEvent, Self::Error>> {
        Box::pin(future::ready(Ok(event)))
    }
}

#[derive(Deserialize)]
struct LookupArguments {
    id: u64,
}

#[derive(Serialize)]
struct LookupResult {
    status: &'static str,
}

fn request() -> ChatRequest {
    ChatRequest::new(
        "internal-model-alias",
        [ChatMessage::new(MessageRole::User, "private customer question").unwrap()],
    )
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn exports_only_redacted_bounded_metadata_for_pipeline_and_tools() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("rustee-ai-contract")));
    let dispatch = tracing::Dispatch::new(subscriber);
    let guard = tracing::dispatcher::set_default(&dispatch);

    run_public_workflow().await;
    drop(guard);
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    assert_exported_spans(&spans);
    provider.shutdown().unwrap();
}

async fn run_public_workflow() {
    let response = AiPipeline::new(Provider).complete(request()).await.unwrap();
    assert_eq!(response.usage().total_tokens(), 5);
    let stream_events = AiPipeline::new(Provider)
        .stream(request())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(stream_events.len(), 2);

    let rejected = AiPipeline::new(Provider)
        .with_policy(AiPolicy {
            max_input_characters: 1,
            max_tools: 1,
            max_tool_results: 1,
        })
        .complete(
            request().with_tool_results([
                serde_json::from_value::<ToolResult>(json!({
                    "call_id":"call-one",
                    "name":"lookup_order",
                    "content":{"private":"x".repeat(1_024)},
                }))
                .unwrap(),
                serde_json::from_value::<ToolResult>(json!({
                    "call_id":"call-two",
                    "name":"lookup_order",
                    "content":{"private":"y".repeat(1_024)},
                }))
                .unwrap(),
            ]),
        )
        .await;
    assert!(rejected.is_err());

    let ledger = Ledger;
    let advisor = ContextAdvisor;
    let response = AiPipeline::new(Provider)
        .complete_with_advisor_and_usage_ledger(
            request(),
            &advisor,
            AiExecutionContext::new("tenant-ledger-private", "subject-ledger-private").unwrap(),
            "usage-ledger-private",
            &ledger,
        )
        .await
        .unwrap();
    assert_eq!(response.usage().total_tokens(), 5);
    let stream_events = AiPipeline::new(Provider)
        .stream_with_advisor_and_usage_ledger(
            request(),
            &advisor,
            AiExecutionContext::new("tenant-ledger-private", "subject-ledger-private").unwrap(),
            "usage-ledger-stream-private",
            &ledger,
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(stream_events.len(), 2);

    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry
        .register(TypedTool::new(
            ToolDefinition::new("lookup_order", json!({"type": "object"})).unwrap(),
            ToolRisk::ReadOnly,
            {
                let calls = Arc::clone(&calls);
                move |context: ToolExecutionContext, arguments: LookupArguments| {
                    assert_eq!(arguments.id, 7);
                    assert_eq!(context.tenant(), "tenant-private");
                    assert_eq!(context.subject(), "subject-private");
                    assert_eq!(context.idempotency_key(), "side-effect-private");
                    calls.fetch_add(1, Ordering::SeqCst);
                    future::ready(Ok::<LookupResult, Infallible>(LookupResult {
                        status: "private result",
                    }))
                }
            },
        ))
        .unwrap();
    registry
        .execute_with_execution_audit(
            ToolExecutionContext::new(
                AiExecutionContext::new("tenant-private", "subject-private").unwrap(),
                "side-effect-private",
            )
            .unwrap(),
            ToolCall::new("call-private", "lookup_order", json!({"id": 7})).unwrap(),
            &Approve,
            &Audit,
        )
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

fn assert_exported_spans(spans: &[SpanData]) {
    let completion_span = spans
        .iter()
        .find(|span| {
            span.name == "AI request"
                && format!("{:?}", span.attributes).contains("ai.operation")
                && format!("{:?}", span.attributes).contains("complete")
        })
        .unwrap_or_else(|| panic!("AI completion span must be exported: {spans:#?}"));
    assert_eq!(completion_span.span_kind, SpanKind::Client);
    let completion_attributes = format!("{:?}", completion_span.attributes);
    assert!(completion_attributes.contains("ai.request.message_count"));
    assert!(completion_attributes.contains("ai.request.input_characters"));
    assert!(completion_attributes.contains("ai.usage.input_tokens"));
    assert!(completion_attributes.contains("ai.usage.output_tokens"));
    assert!(completion_attributes.contains("succeeded"));

    let stream_span = spans
        .iter()
        .find(|span| {
            span.name == "AI request" && format!("{:?}", span.attributes).contains("stream")
        })
        .unwrap_or_else(|| panic!("AI stream span must be exported: {spans:#?}"));
    let stream_attributes = format!("{:?}", stream_span.attributes);
    assert!(stream_attributes.contains("ai.usage.input_tokens"));
    assert!(stream_attributes.contains("succeeded"));

    let rejected_span = spans
        .iter()
        .find(|span| {
            span.name == "AI request"
                && format!("{:?}", span.attributes).contains("policy_rejected")
        })
        .unwrap_or_else(|| panic!("AI rejected-request span must be exported: {spans:#?}"));
    let rejected_attributes = format!("{:?}", rejected_span.attributes);
    assert!(rejected_attributes.contains("ai.request.tool_result_count"));
    assert!(!rejected_attributes.contains("ai.request.input_characters"));

    let tool_span = spans
        .iter()
        .find(|span| span.name == "AI tool execution")
        .unwrap_or_else(|| panic!("AI tool span must be exported: {spans:#?}"));
    assert_eq!(tool_span.span_kind, SpanKind::Internal);
    let tool_attributes = format!("{:?}", tool_span.attributes);
    assert!(tool_attributes.contains("ai.tool.risk"));
    assert!(tool_attributes.contains("read_only"));
    assert!(tool_attributes.contains("succeeded"));

    let span_dump = format!("{spans:#?}");
    for secret in [
        "private customer question",
        "internal-model-alias",
        "tenant-private",
        "subject-private",
        "side-effect-private",
        "call-private",
        "lookup_order",
        "private result",
        "provider completion",
        "private advisor context",
        "tenant-ledger-private",
        "subject-ledger-private",
        "usage-ledger-private",
        "usage-ledger-stream-private",
    ] {
        assert!(
            !span_dump.contains(secret),
            "redacted AI span unexpectedly exposed {secret}"
        );
    }
}
