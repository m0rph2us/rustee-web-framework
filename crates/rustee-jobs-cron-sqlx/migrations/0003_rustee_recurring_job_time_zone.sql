ALTER TABLE rustee_recurring_jobs
  ADD COLUMN IF NOT EXISTS time_zone text NOT NULL DEFAULT 'UTC';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'rustee_recurring_jobs_time_zone_check'
  ) THEN
    ALTER TABLE rustee_recurring_jobs
      ADD CONSTRAINT rustee_recurring_jobs_time_zone_check
      CHECK (octet_length(time_zone) BETWEEN 1 AND 255);
  END IF;
END $$;
