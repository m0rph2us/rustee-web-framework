DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'rustee_api_key_credentials'::regclass
          AND conname = 'rustee_api_key_credentials_principal_size_check'
    ) THEN
        ALTER TABLE rustee_api_key_credentials
            ADD CONSTRAINT rustee_api_key_credentials_principal_size_check
            CHECK (octet_length(principal::text) <= 524288);
    END IF;
END $$;
