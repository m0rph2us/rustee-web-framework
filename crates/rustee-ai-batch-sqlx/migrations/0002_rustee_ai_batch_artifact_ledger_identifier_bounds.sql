DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'rustee_ai_batch_artifact_ledger'::regclass
          AND conname = 'rustee_ai_batch_artifact_ledger_identifier_shape_check'
    ) THEN
        ALTER TABLE rustee_ai_batch_artifact_ledger
            ADD CONSTRAINT rustee_ai_batch_artifact_ledger_identifier_shape_check
            CHECK (
                octet_length(scope) BETWEEN 1 AND 128
                AND scope ~ '^[A-Za-z0-9_.-]+$'
                AND octet_length(reconciliation_key) BETWEEN 1 AND 128
                AND reconciliation_key ~ '^[A-Za-z0-9_.-]+$'
                AND octet_length(catalog_id) BETWEEN 1 AND 128
                AND catalog_id ~ '^[A-Za-z0-9_.-]+$'
                AND octet_length(run_key) BETWEEN 1 AND 128
                AND run_key ~ '^[A-Za-z0-9_.-]+$'
                AND octet_length(provider_file_id) BETWEEN 1 AND 128
                AND provider_file_id ~ '^[A-Za-z0-9_.-]+$'
            );
    END IF;
END $$;
