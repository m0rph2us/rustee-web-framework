CREATE TABLE IF NOT EXISTS rustee_ai_batch_artifact_ledger (
    scope TEXT NOT NULL,
    reconciliation_key TEXT NOT NULL,
    catalog_id TEXT NOT NULL,
    run_key TEXT NOT NULL,
    artifact_kind TEXT NOT NULL CHECK (artifact_kind IN ('output', 'error')),
    provider_file_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'reconciled')),
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    reconciled_at TIMESTAMPTZ,
    PRIMARY KEY (scope, reconciliation_key),
    CHECK (
        (status = 'pending' AND reconciled_at IS NULL)
        OR
        (status = 'reconciled' AND reconciled_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS rustee_ai_batch_artifact_ledger_pending_idx
    ON rustee_ai_batch_artifact_ledger (reserved_at, scope, reconciliation_key)
    WHERE status = 'pending';
