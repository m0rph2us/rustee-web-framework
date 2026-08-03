CREATE TABLE IF NOT EXISTS rustee_ai_usage_ledger (
    tenant TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    subject TEXT NOT NULL,
    model_alias TEXT NOT NULL,
    input_characters BIGINT NOT NULL CHECK (input_characters >= 0),
    tool_count BIGINT NOT NULL CHECK (tool_count >= 0),
    tool_result_count BIGINT NOT NULL CHECK (tool_result_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'completed')),
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    input_tokens BIGINT CHECK (input_tokens >= 0),
    output_tokens BIGINT CHECK (output_tokens >= 0),
    settled_at TIMESTAMPTZ,
    PRIMARY KEY (tenant, idempotency_key),
    CHECK (
        (status = 'pending' AND input_tokens IS NULL AND output_tokens IS NULL AND settled_at IS NULL)
        OR
        (status = 'completed' AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND settled_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS rustee_ai_usage_ledger_pending_idx
    ON rustee_ai_usage_ledger (reserved_at, tenant, idempotency_key)
    WHERE status = 'pending';
