CREATE TABLE IF NOT EXISTS rustee_ai_evaluation_run_ledger (
    scope TEXT NOT NULL CHECK (octet_length(scope) BETWEEN 1 AND 128),
    run_key TEXT NOT NULL CHECK (octet_length(run_key) BETWEEN 1 AND 128),
    catalog_id TEXT NOT NULL CHECK (octet_length(catalog_id) BETWEEN 1 AND 128),
    status TEXT NOT NULL CHECK (status IN ('pending', 'completed')),
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (scope, run_key)
);

CREATE INDEX IF NOT EXISTS rustee_ai_evaluation_run_ledger_pending_idx
    ON rustee_ai_evaluation_run_ledger (reserved_at ASC, scope ASC, run_key ASC)
    WHERE status = 'pending';
