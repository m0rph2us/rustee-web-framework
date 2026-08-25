//! Explicit RAG retrieval integration for [`rustee_ai::AiAdvisor`].
//!
//! Applications own both the retrieval query and how untrusted retrieved text is delimited in a
//! prompt. This crate only retrieves ACL-revalidated chunks and appends the renderer's explicit
//! message before the AI pipeline applies its final request policy.

use std::{error::Error as StdError, fmt};

use futures_util::{future, future::BoxFuture};
use rustee_ai::{AiAdvisor, AiStreamEvent, ChatMessage, ChatRequest, ChatResponse};
use rustee_ai_rag::{RagError, RagRetriever, RetrievalContext, RetrievalQuery, RetrievalStore};

/// Application policy that derives one authorized retrieval query from an AI request.
///
/// Implementations own query extraction, tenant identity, and document authorization. They must
/// not trust model output or log request text by default.
pub trait RagQueryBuilder: Clone + Send + Sync + 'static {
    /// Application query-policy failure.
    type Error: StdError + Send + Sync + 'static;

    /// Builds a bounded, ACL-scoped retrieval query for this request.
    ///
    /// # Errors
    ///
    /// Returns the application query-policy error when authorization or query derivation fails.
    fn build(&self, request: &ChatRequest) -> Result<RetrievalQuery, Self::Error>;
}

/// Application policy that converts ACL-revalidated chunks into one explicit AI message.
///
/// Retrieved text can contain prompt-injection attempts. Renderers must delimit sources, retain
/// citations through application-owned response handling, and choose a message role deliberately;
/// no default renderer is provided by this crate.
pub trait RagContextRenderer: Clone + Send + Sync + 'static {
    /// Application rendering failure.
    type Error: StdError + Send + Sync + 'static;

    /// Renders one message from a fully ACL-revalidated retrieval context.
    ///
    /// # Errors
    ///
    /// Returns the application rendering error when safe context construction fails.
    fn render(&self, context: &RetrievalContext) -> Result<ChatMessage, Self::Error>;
}

/// Explicit one-call RAG advisor with application-owned query and rendering policy.
#[derive(Clone)]
pub struct RagAdvisor<S, Q, R> {
    retriever: RagRetriever<S>,
    query_builder: Q,
    renderer: R,
}

impl<S, Q, R> RagAdvisor<S, Q, R> {
    /// Creates an advisor from one retrieval store, query builder, and message renderer.
    #[must_use]
    pub fn new(store: S, query_builder: Q, renderer: R) -> Self {
        Self {
            retriever: RagRetriever::new(store),
            query_builder,
            renderer,
        }
    }
}

impl<S, Q, R> fmt::Debug for RagAdvisor<S, Q, R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RagAdvisor")
            .field("retriever", &"[REDACTED]")
            .field("query_builder", &"[REDACTED]")
            .field("renderer", &"[REDACTED]")
            .finish()
    }
}

impl<S, Q, R> AiAdvisor for RagAdvisor<S, Q, R>
where
    S: RetrievalStore,
    Q: RagQueryBuilder,
    R: RagContextRenderer,
{
    type Error = RagAdvisorError<S::Error, Q::Error, R::Error>;

    fn before_request(
        &self,
        request: ChatRequest,
    ) -> BoxFuture<'static, Result<ChatRequest, Self::Error>> {
        let query = match self.query_builder.build(&request) {
            Ok(query) => query,
            Err(error) => return Box::pin(future::ready(Err(RagAdvisorError::Query(error)))),
        };
        let retriever = self.retriever.clone();
        let renderer = self.renderer.clone();
        Box::pin(async move {
            let context = retriever
                .retrieve(query)
                .await
                .map_err(RagAdvisorError::Retrieval)?;
            let message = renderer.render(&context).map_err(RagAdvisorError::Render)?;
            Ok(request.with_added_message(message))
        })
    }

    fn after_response(
        &self,
        response: ChatResponse,
    ) -> BoxFuture<'static, Result<ChatResponse, Self::Error>> {
        Box::pin(future::ready(Ok(response)))
    }

    fn on_stream_event(
        &self,
        event: AiStreamEvent,
    ) -> BoxFuture<'static, Result<AiStreamEvent, Self::Error>> {
        Box::pin(future::ready(Ok(event)))
    }
}

/// Failure while deriving, retrieving, or rendering explicit RAG context.
///
/// Its display and debug forms retain only the failure category. Application sources remain
/// available through [`std::error::Error::source`] for trusted handling without exposing query or
/// retrieved-content details in routine diagnostics.
#[derive(thiserror::Error)]
pub enum RagAdvisorError<StoreError, QueryError, RenderError>
where
    StoreError: StdError + Send + Sync + 'static,
    QueryError: StdError + Send + Sync + 'static,
    RenderError: StdError + Send + Sync + 'static,
{
    /// Application policy could not derive an authorized retrieval query.
    #[error("RAG query policy failed")]
    Query(#[source] QueryError),
    /// The store failed or violated retrieval authorization scope.
    #[error("RAG retrieval failed")]
    Retrieval(#[source] RagError<StoreError>),
    /// Application prompt rendering could not safely construct its context message.
    #[error("RAG context rendering failed")]
    Render(#[source] RenderError),
}

impl<StoreError, QueryError, RenderError> fmt::Debug
    for RagAdvisorError<StoreError, QueryError, RenderError>
where
    StoreError: StdError + Send + Sync + 'static,
    QueryError: StdError + Send + Sync + 'static,
    RenderError: StdError + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(_) => formatter.write_str("RagAdvisorError::Query"),
            Self::Retrieval(_) => formatter.write_str("RagAdvisorError::Retrieval"),
            Self::Render(_) => formatter.write_str("RagAdvisorError::Render"),
        }
    }
}

#[cfg(test)]
mod tests;
