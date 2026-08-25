use std::convert::Infallible;

use futures_util::{future, future::BoxFuture};
use rustee_ai::MAX_TENANT_BYTES;

use crate::{
    Citation, CitationError, DEFAULT_RETRIEVAL_CONTEXT_BYTES, MAX_RETRIEVAL_CHUNKS,
    MAX_RETRIEVAL_CITATION_FIELD_BYTES, MAX_RETRIEVAL_CONTEXT_BYTES,
    MAX_RETRIEVAL_IDENTIFIER_BYTES, MAX_RETRIEVAL_QUERY_BYTES, MAX_RETRIEVAL_SCOPE_DOCUMENTS,
    MAX_RETRIEVED_CHUNK_CONTENT_BYTES, RagError, RagRetriever, RetrievalQuery, RetrievalQueryError,
    RetrievalScope, RetrievalScopeError, RetrievalStore, RetrievedChunk, RetrievedChunkError,
};

#[derive(Clone)]
struct FakeStore {
    chunks: Vec<RetrievedChunk>,
}

impl RetrievalStore for FakeStore {
    type Error = Infallible;

    fn search(
        &self,
        _: RetrievalQuery,
    ) -> BoxFuture<'static, Result<Vec<RetrievedChunk>, Self::Error>> {
        Box::pin(future::ready(Ok(self.chunks.clone())))
    }
}

fn chunk(tenant: &str, document_id: &str) -> RetrievedChunk {
    RetrievedChunk::new(
        "chunk-1",
        document_id,
        tenant,
        "account policy text",
        Citation::new("kb://policy", "Account policy").unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn retriever_returns_only_scope_valid_bounded_context() {
    let retriever = RagRetriever::new(FakeStore {
        chunks: vec![chunk("acme", "doc-1"), chunk("acme", "doc-1")],
    });
    let query = RetrievalQuery::new(
        "refund policy",
        RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
        1,
    )
    .unwrap();

    let context = retriever.retrieve(query).await.unwrap();

    assert_eq!(context.chunks().len(), 1);
    assert_eq!(context.citations().len(), 1);
    assert!(!format!("{:?}", context.chunks()[0]).contains("account policy text"));
    let context_debug = format!("{context:?}");
    assert_eq!(context_debug, "RetrievalContext { chunk_count: 1 }");
    for sensitive in [
        "account policy text",
        "acme",
        "doc-1",
        "kb://policy",
        "Account policy",
    ] {
        assert!(!context_debug.contains(sensitive));
    }
}

#[tokio::test]
async fn retriever_rejects_any_chunk_outside_the_authorized_scope() {
    let retriever = RagRetriever::new(FakeStore {
        chunks: vec![chunk("other-tenant", "doc-1")],
    });
    let query = RetrievalQuery::new(
        "refund policy",
        RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
        5,
    )
    .unwrap();

    assert!(matches!(
        retriever.retrieve(query).await,
        Err(RagError::ScopeViolation)
    ));
}

#[tokio::test]
async fn retriever_rejects_context_larger_than_its_query_limit() {
    let context_chunk = RetrievedChunk::new(
        "chunk-1",
        "doc-1",
        "acme",
        "account policy text",
        Citation::new("kb://policy", "Account policy").unwrap(),
    )
    .unwrap();
    let rag_retriever = RagRetriever::new(FakeStore {
        chunks: vec![context_chunk],
    });
    let query = RetrievalQuery::new(
        "refund policy",
        RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
        1,
    )
    .unwrap()
    .with_max_context_bytes(8)
    .unwrap();

    assert!(matches!(
        rag_retriever.retrieve(query).await,
        Err(RagError::ContextLimit)
    ));
}

#[test]
fn retrieval_context_byte_limit_is_finite_and_configurable() {
    let query = RetrievalQuery::new(
        "refund policy",
        RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
        1,
    )
    .unwrap();
    assert_eq!(query.max_context_bytes(), DEFAULT_RETRIEVAL_CONTEXT_BYTES);
    assert_eq!(
        query.clone().with_max_context_bytes(0).unwrap_err(),
        RetrievalQueryError::ZeroMaxContextBytes
    );
    assert_eq!(
        query
            .with_max_context_bytes(MAX_RETRIEVAL_CONTEXT_BYTES + 1)
            .unwrap_err(),
        RetrievalQueryError::MaxContextBytesLimit
    );
}

#[test]
fn retrieval_query_bounds_provider_input_and_candidate_count() {
    assert!(
        RetrievalQuery::new(
            "q".repeat(MAX_RETRIEVAL_QUERY_BYTES),
            RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
            MAX_RETRIEVAL_CHUNKS,
        )
        .is_ok()
    );
    assert_eq!(
        RetrievalQuery::new(
            "q".repeat(MAX_RETRIEVAL_QUERY_BYTES + 1),
            RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
            1,
        )
        .unwrap_err(),
        RetrievalQueryError::TextTooLong
    );
    assert_eq!(
        RetrievalQuery::new(
            "refund policy",
            RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
            MAX_RETRIEVAL_CHUNKS + 1,
        )
        .unwrap_err(),
        RetrievalQueryError::MaxChunksLimit
    );
}

#[test]
fn retrieval_scope_bounds_allowlist_input_before_deduplication() {
    assert!(
        RetrievalScope::new(
            "acme",
            (0..MAX_RETRIEVAL_SCOPE_DOCUMENTS).map(|index| format!("doc-{index}")),
        )
        .is_ok()
    );
    assert_eq!(
        RetrievalScope::new(
            "acme",
            std::iter::repeat_n("doc-1".to_owned(), MAX_RETRIEVAL_SCOPE_DOCUMENTS + 1),
        )
        .unwrap_err(),
        RetrievalScopeError::TooManyDocumentIds
    );
}

#[test]
fn retrieval_values_reject_oversized_or_nul_metadata_before_store_access() {
    assert!(
        RetrievalScope::new(
            "t".repeat(MAX_TENANT_BYTES),
            ["d".repeat(MAX_RETRIEVAL_IDENTIFIER_BYTES)],
        )
        .is_ok()
    );
    assert!(matches!(
        RetrievalScope::new("t".repeat(MAX_TENANT_BYTES + 1), ["doc-1".to_owned()],),
        Err(RetrievalScopeError::TenantTooLong)
    ));
    assert!(matches!(
        RetrievalScope::new("acme", ["d".repeat(MAX_RETRIEVAL_IDENTIFIER_BYTES + 1)]),
        Err(RetrievalScopeError::DocumentIdTooLong)
    ));
    assert!(matches!(
        RetrievalScope::new("acme\0", ["doc-1".to_owned()]),
        Err(RetrievalScopeError::TenantContainsNul)
    ));
    assert!(matches!(
        RetrievalQuery::new(
            "refund\0policy",
            RetrievalScope::new("acme", ["doc-1".to_owned()]).unwrap(),
            1,
        ),
        Err(RetrievalQueryError::TextContainsNul)
    ));
    assert!(matches!(
        Citation::new("u".repeat(MAX_RETRIEVAL_CITATION_FIELD_BYTES + 1), "title"),
        Err(CitationError::FieldTooLong)
    ));
    assert!(matches!(
        Citation::new("kb://policy\0", "title"),
        Err(CitationError::FieldContainsNul)
    ));

    let citation = Citation::new("kb://policy", "Account policy").unwrap();
    assert!(matches!(
        RetrievedChunk::new(
            "c".repeat(MAX_RETRIEVAL_IDENTIFIER_BYTES + 1),
            "doc-1",
            "acme",
            "text",
            citation.clone(),
        ),
        Err(RetrievedChunkError::FieldTooLong)
    ));
    assert!(matches!(
        RetrievedChunk::new(
            "chunk-1",
            "doc-1",
            "acme",
            "x".repeat(MAX_RETRIEVED_CHUNK_CONTENT_BYTES + 1),
            citation.clone(),
        ),
        Err(RetrievedChunkError::FieldTooLong)
    ));
    assert!(matches!(
        RetrievedChunk::new("chunk\0-1", "doc-1", "acme", "text", citation,),
        Err(RetrievedChunkError::FieldContainsNul)
    ));
}
