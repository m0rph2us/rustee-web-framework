//! Advisor and budget-policy fixtures.

use super::*;

#[derive(Clone)]
pub(in crate::tests) struct ContextAdvisor {
    pub(in crate::tests) before: Arc<AtomicUsize>,
    pub(in crate::tests) response: Arc<AtomicUsize>,
    pub(in crate::tests) stream: Arc<AtomicUsize>,
}

impl ContextAdvisor {
    pub(in crate::tests) fn new() -> Self {
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

#[derive(Clone, Copy, Debug)]
pub(in crate::tests) struct RejectingStreamAdvisor;

impl AiAdvisor for RejectingStreamAdvisor {
    type Error = TestUsageLedgerError;

    fn before_request(
        &self,
        request: ChatRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<ChatRequest, Self::Error>> {
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
        _: AiStreamEvent,
    ) -> futures_util::future::BoxFuture<'static, Result<AiStreamEvent, Self::Error>> {
        Box::pin(future::ready(Err(TestUsageLedgerError::Unavailable)))
    }
}

#[derive(Clone)]
pub(in crate::tests) struct DenyingBudget {
    pub(in crate::tests) admissions: Arc<Mutex<Vec<(AiExecutionContext, AiBudgetRequest)>>>,
}

impl AiBudgetPolicy for DenyingBudget {
    type Error = Infallible;

    fn admit(
        &self,
        context: AiExecutionContext,
        request: AiBudgetRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<AiBudgetDecision, Self::Error>> {
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
