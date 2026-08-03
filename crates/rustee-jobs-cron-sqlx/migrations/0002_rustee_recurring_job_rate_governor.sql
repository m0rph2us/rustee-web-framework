ALTER TABLE rustee_recurring_jobs
  ADD COLUMN IF NOT EXISTS rate_limit_key text,
  ADD COLUMN IF NOT EXISTS rate_limit_capacity integer,
  ADD COLUMN IF NOT EXISTS rate_limit_window_ms bigint;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'rustee_recurring_jobs_rate_limit_configuration_check'
  ) THEN
    ALTER TABLE rustee_recurring_jobs
      ADD CONSTRAINT rustee_recurring_jobs_rate_limit_configuration_check
      CHECK (
        (rate_limit_key IS NULL AND rate_limit_capacity IS NULL AND rate_limit_window_ms IS NULL)
        OR (
          octet_length(rate_limit_key) BETWEEN 1 AND 255
          AND rate_limit_capacity BETWEEN 1 AND 2147483647
          AND rate_limit_window_ms BETWEEN 1 AND 31536000000
        )
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS rustee_recurring_jobs_rate_limit_key_idx
  ON rustee_recurring_jobs (rate_limit_key)
  WHERE rate_limit_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS rustee_recurring_job_rate_windows (
  rate_limit_key text PRIMARY KEY,
  capacity integer NOT NULL CHECK (capacity BETWEEN 1 AND 2147483647),
  window_ms bigint NOT NULL CHECK (window_ms BETWEEN 1 AND 31536000000),
  window_started_at_ms bigint NOT NULL,
  consumed integer NOT NULL CHECK (consumed BETWEEN 0 AND 2147483647),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (octet_length(rate_limit_key) BETWEEN 1 AND 255),
  CHECK (consumed <= capacity)
);
