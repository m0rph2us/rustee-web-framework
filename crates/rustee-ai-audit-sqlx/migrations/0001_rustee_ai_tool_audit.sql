CREATE TABLE IF NOT EXISTS rustee_ai_tool_audit (
    tenant TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    subject TEXT NOT NULL,
    call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    risk TEXT NOT NULL CHECK (risk IN ('read_only', 'requires_confirmation', 'privileged')),
    approved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    terminal_outcome TEXT CHECK (terminal_outcome IN ('succeeded', 'failed')),
    outcome_recorded_at TIMESTAMPTZ,
    PRIMARY KEY (tenant, idempotency_key)
);

CREATE INDEX IF NOT EXISTS rustee_ai_tool_audit_pending_idx
    ON rustee_ai_tool_audit (approved_at, tenant, idempotency_key)
    WHERE terminal_outcome IS NULL;
