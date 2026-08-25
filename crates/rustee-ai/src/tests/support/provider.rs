//! Deterministic provider and diagnostic fixtures.

use super::*;

#[derive(Clone, Debug)]
pub(in crate::tests) struct Fake;

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

    fn stream(&self, _: ChatRequest) -> crate::AiEventStreamFuture<Self::Error> {
        let events: crate::AiEventStream<Self::Error> = Box::pin(stream::empty());
        Box::pin(future::ready(Ok(events)))
    }
}

#[derive(Clone)]
pub(in crate::tests) struct LeakyDebugProvider;

impl fmt::Debug for LeakyDebugProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyDebugProvider(private-provider-credential)")
    }
}

impl AiProvider for LeakyDebugProvider {
    type Error = Infallible;

    fn complete(
        &self,
        request: ChatRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        Fake.complete(request)
    }

    fn stream(&self, request: ChatRequest) -> crate::AiEventStreamFuture<Self::Error> {
        Fake.stream(request)
    }
}

pub(in crate::tests) struct LeakyDiagnosticError;

impl fmt::Debug for LeakyDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeakyDiagnosticError(private-provider-or-ledger-detail)")
    }
}

impl fmt::Display for LeakyDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private-provider-or-ledger-detail")
    }
}

impl std::error::Error for LeakyDiagnosticError {}

pub(in crate::tests) fn request() -> ChatRequest {
    ChatRequest::new(
        "support.default",
        [ChatMessage::new(MessageRole::User, "status?").unwrap()],
    )
    .unwrap()
}

#[derive(Clone)]
pub(in crate::tests) struct CountingProvider {
    pub(in crate::tests) invocations: Arc<AtomicUsize>,
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

    fn stream(&self, _: ChatRequest) -> crate::AiEventStreamFuture<Self::Error> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        let events: crate::AiEventStream<Self::Error> = Box::pin(stream::iter(vec![
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
pub(in crate::tests) struct PostCompletionProvider {
    pub(in crate::tests) tail_polls: Arc<AtomicUsize>,
}

impl AiProvider for PostCompletionProvider {
    type Error = Infallible;

    fn complete(
        &self,
        request: ChatRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        Fake.complete(request)
    }

    fn stream(&self, _: ChatRequest) -> crate::AiEventStreamFuture<Self::Error> {
        let tail_polls = Arc::clone(&self.tail_polls);
        let events = stream::iter(vec![
            Ok(AiStreamEvent::Completed(Usage {
                input_tokens: 3,
                output_tokens: 2,
            })),
            Ok(AiStreamEvent::TextDelta("late event".to_owned())),
        ])
        .inspect(move |event| {
            if matches!(event, Ok(AiStreamEvent::TextDelta(_))) {
                tail_polls.fetch_add(1, Ordering::SeqCst);
            }
        });
        let events: crate::AiEventStream<Self::Error> = Box::pin(events);
        Box::pin(future::ready(Ok(events)))
    }
}

#[derive(Clone)]
pub(in crate::tests) struct PostAdvisorFailureProvider {
    pub(in crate::tests) tail_polls: Arc<AtomicUsize>,
}

impl AiProvider for PostAdvisorFailureProvider {
    type Error = Infallible;

    fn complete(
        &self,
        request: ChatRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        Fake.complete(request)
    }

    fn stream(&self, _: ChatRequest) -> crate::AiEventStreamFuture<Self::Error> {
        let tail_polls = Arc::clone(&self.tail_polls);
        let events = stream::iter(vec![
            Ok(AiStreamEvent::TextDelta("first event".to_owned())),
            Ok(AiStreamEvent::TextDelta("late event".to_owned())),
        ])
        .inspect(move |event| {
            if matches!(event, Ok(AiStreamEvent::TextDelta(text)) if text == "late event") {
                tail_polls.fetch_add(1, Ordering::SeqCst);
            }
        });
        let events: crate::AiEventStream<Self::Error> = Box::pin(events);
        Box::pin(future::ready(Ok(events)))
    }
}
