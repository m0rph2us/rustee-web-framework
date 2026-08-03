# Rustee Agent Bootstrap

This file is a small bootstrap for coding agents. The canonical project documentation is HTML, not Markdown.

Before making changes, read:

- `docs/ai-context.html`
- `docs/index.html`
- `docs/release-plan.html`
- `docs/release-inventory.html`
- `docs/release-checklist.html`
- `docs/supply-chain.html`
- `docs/release-notes.html`
- `docs/migration-guide.html`
- `docs/qualification-register.html`
- `docs/support-matrix.html`
- `docs/implementation-review.html`
- The specific HTML documents referenced by the change area, including `docs/openapi.html` and ADR 0039/0043/0045/0046/0047 for OpenAPI route/schema/security-metadata/OAuth-default/mutual-TLS/API-key-header work, `docs/authentication.html` and ADR 0005/0048 for runtime API-key or authentication/authorization work, `docs/macros.html` and ADR 0040/0041 for optional macro work, `docs/testing.html` and ADR 0042/0044 for TestApp/cookie-jar/test strategy, property/fuzz, or CI work, `docs/events.html`, `docs/event-schema-operations.html`, and `docs/kafka-delayed-retry-operations.html` for Kafka, event schema, or delayed-retry work, `docs/ai.html`, `docs/mcp-oauth.html`, ADR 0020, ADR 0021, ADR 0022, ADR 0023, ADR 0024, ADR 0025, and ADR 0038 for AI/MCP authorization, evaluation, cache, provider batch, OpenAI lifecycle, batch artifact reconciliation, PostgreSQL batch-ledger, or durable evaluation-run work, ADR 0026 for panic-response boundaries, `docs/edge-delivery.html` and ADR 0027 for CDN/TLS/cache/purge work, ADR 0028 for PostgreSQL outbox priority work, ADR 0029 for PostgreSQL recurring job scheduling, ADR 0030 for scheduler rate-governor work, ADR 0031 for scheduler pass observability, ADR 0032 for job-delivery Prometheus export, ADR 0033 for recurring IANA time-zone/DST work, ADR 0034 for Kafka PostgreSQL delayed retry work, ADR 0035 for Kafka delayed-retry observability, ADR 0036 for event-schema registry boundaries, ADR 0037 for Kafka event-delivery observability, and `docs/operations.html` for configuration, deployment, or lifecycle work

Rules for future agents:

- Keep project documentation in `docs/**/*.html`.
- Update the relevant HTML document whenever architecture, roadmap, release/support policy, public API direction, or design philosophy changes. Queue delivery, event-stream topology, AI provider/tool/RAG/evaluation/cache/batch policy, testing/CI policy, and deployment trust policy are separate concerns and must be documented separately.
- Add architectural decisions as `docs/adr/NNNN-short-title.html`.
- Do not create new Markdown documentation unless the user explicitly asks for it or a tool requires it.
- Treat `scripts/package-release-candidates.mjs` as the pre-publish candidate-only archive check. Treat `scripts/verify-published-release-candidates.mjs` as a post-publish registry consumer check; do not claim either one proves the other boundary.
- Keep every direct third-party `uses:` reference in `.github/workflows` pinned to a full commit SHA with a version comment. Run `node scripts/check-workflow-action-pins.mjs` whenever workflow files change; this verifier does not cover transitive composite actions or container image digests.
