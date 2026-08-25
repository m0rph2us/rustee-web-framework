DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'rustee_mongodb_change_stream_checkpoint'::regclass
          AND conname = 'rustee_mongodb_change_stream_checkpoint_resume_token_size_check'
    ) THEN
        ALTER TABLE rustee_mongodb_change_stream_checkpoint
            ADD CONSTRAINT rustee_mongodb_change_stream_checkpoint_resume_token_size_check
            CHECK (octet_length(resume_token) <= 1048576);
    END IF;
END $$;
