use std::{
    collections::BTreeSet,
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use http::{HeaderValue, Request as HttpRequest};
use http_body_util::BodyExt;
use rustee_ai::{
    AiExecutionContext, ToolApprovalAuditEvent, ToolApprovalDecision, ToolApprovalPolicy,
    ToolDefinition, ToolExecutionAuditEvent, ToolExecutionAuditSink, ToolRisk, TypedTool,
};
use rustee_core::full_body;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{MCP_PROTOCOL_VERSION, McpServer, McpServerConfig, McpToolAccessPolicy};

#[derive(Clone, Deserialize)]
struct LookupInput {
    id: u64,
}

#[derive(Serialize)]
struct LookupOutput {
    status: &'static str,
}

#[derive(Debug, thiserror::Error)]
#[error("handler secret")]
struct HandlerFailure;

#[derive(Clone)]
pub(super) struct Access {
    names: BTreeSet<String>,
}

impl McpToolAccessPolicy for Access {
    type Error = Infallible;

    fn permitted_tools(&self, _: &rustee_core::Request) -> Result<BTreeSet<String>, Self::Error> {
        Ok(self.names.clone())
    }

    fn execution_context(
        &self,
        _: &rustee_core::Request,
        _: &str,
    ) -> Result<rustee_ai::ToolExecutionContext, Self::Error> {
        Ok(rustee_ai::ToolExecutionContext::new(
            AiExecutionContext::new("tenant-a", "user-7").unwrap(),
            "mcp-semantic-action",
        )
        .unwrap())
    }
}

#[derive(Clone)]
pub(super) struct Approval {
    approved: bool,
}

impl ToolApprovalPolicy for Approval {
    type Error = Infallible;

    fn approve(
        &self,
        _: AiExecutionContext,
        _: rustee_ai::ToolCall,
        _: ToolRisk,
    ) -> futures_util::future::BoxFuture<'static, Result<ToolApprovalDecision, Self::Error>> {
        let decision = self.approved;
        Box::pin(futures_util::future::ready(Ok(if decision {
            ToolApprovalDecision::Approved
        } else {
            ToolApprovalDecision::Denied
        })))
    }
}

#[derive(Clone, Default)]
pub(super) struct Audit {
    approvals: Arc<AtomicUsize>,
    outcomes: Arc<AtomicUsize>,
}

impl Audit {
    pub(super) fn approval_count(&self) -> usize {
        self.approvals.load(Ordering::SeqCst)
    }

    pub(super) fn outcome_count(&self) -> usize {
        self.outcomes.load(Ordering::SeqCst)
    }
}

impl ToolExecutionAuditSink for Audit {
    fn record_outcome(
        &self,
        _: ToolExecutionAuditEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        let outcomes = Arc::clone(&self.outcomes);
        Box::pin(async move {
            outcomes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

impl rustee_ai::ToolApprovalAuditSink for Audit {
    type Error = Infallible;

    fn record_approved(
        &self,
        _: ToolApprovalAuditEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<(), Self::Error>> {
        let approvals = Arc::clone(&self.approvals);
        Box::pin(async move {
            approvals.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

pub(super) fn server<const N: usize>(
    names: [&str; N],
) -> (McpServer<Access, Approval, Audit>, Arc<AtomicUsize>, Audit) {
    server_with_config(names, true, 1024 * 1024)
}

pub(super) fn server_with_approval<const N: usize>(
    names: [&str; N],
    approved: bool,
) -> (McpServer<Access, Approval, Audit>, Arc<AtomicUsize>, Audit) {
    server_with_config(names, approved, 1024 * 1024)
}

pub(super) fn server_configured<const N: usize>(
    names: [&str; N],
    max_request_bytes: usize,
) -> (McpServer<Access, Approval, Audit>, Arc<AtomicUsize>, Audit) {
    server_with_config(names, true, max_request_bytes)
}

fn server_with_config<const N: usize>(
    names: [&str; N],
    approved: bool,
    max_request_bytes: usize,
) -> (McpServer<Access, Approval, Audit>, Arc<AtomicUsize>, Audit) {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = rustee_ai::ToolRegistry::new();
    register_lookup_tool(&mut registry, "orders.lookup", Arc::clone(&calls));
    for name in names
        .iter()
        .copied()
        .filter(|name| *name != "orders.lookup")
    {
        register_lookup_tool(&mut registry, name, Arc::clone(&calls));
    }
    let access = Access {
        names: names.into_iter().map(str::to_owned).collect(),
    };
    let audit = Audit::default();
    let config = McpServerConfig::new("rustee-mcp-test", "0.1.0")
        .unwrap()
        .with_max_request_bytes(max_request_bytes)
        .unwrap();
    (
        McpServer::new(
            config,
            registry,
            access,
            Approval { approved },
            audit.clone(),
        ),
        calls,
        audit,
    )
}

fn register_lookup_tool(
    registry: &mut rustee_ai::ToolRegistry,
    name: &str,
    calls: Arc<AtomicUsize>,
) {
    registry
        .register(TypedTool::new(
            ToolDefinition::new(name, json!({"type":"object"})).unwrap(),
            ToolRisk::ReadOnly,
            move |_context, input: LookupInput| {
                if input.id == 13 {
                    return futures_util::future::ready(Err::<LookupOutput, HandlerFailure>(
                        HandlerFailure,
                    ));
                }
                assert_eq!(input.id, 7);
                calls.fetch_add(1, Ordering::SeqCst);
                futures_util::future::ready(Ok::<LookupOutput, HandlerFailure>(LookupOutput {
                    status: "found",
                }))
            },
        ))
        .unwrap();
}

pub(super) fn request(value: &Value) -> rustee_core::Request {
    HttpRequest::builder()
        .method(http::Method::POST)
        .uri("/")
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(full_body(value.to_string()))
        .unwrap()
}

pub(super) fn protocol_request(value: &Value) -> rustee_core::Request {
    let mut request = request(value);
    request.headers_mut().insert(
        "mcp-protocol-version",
        HeaderValue::from_static(MCP_PROTOCOL_VERSION),
    );
    request
}

pub(super) async fn response_json(response: rustee_core::Response) -> Value {
    let (_, body) = response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}
