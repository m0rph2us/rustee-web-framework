CREATE TABLE IF NOT EXISTS rustee_recurring_jobs (
  id uuid PRIMARY KEY,
  schedule_key text NOT NULL UNIQUE,
  destination text NOT NULL,
  job_name text NOT NULL,
  schema_version integer NOT NULL CHECK (schema_version >= 0 AND schema_version <= 65535),
  payload bytea NOT NULL CHECK (octet_length(payload) > 0 AND octet_length(payload) <= 1048576),
  cron_expression text NOT NULL,
  priority smallint NOT NULL DEFAULT 0 CHECK (priority >= 0 AND priority <= 255),
  next_run_at timestamptz NOT NULL,
  last_fired_at timestamptz,
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (octet_length(schedule_key) BETWEEN 1 AND 255),
  CHECK (octet_length(destination) BETWEEN 1 AND 255),
  CHECK (octet_length(job_name) BETWEEN 1 AND 255),
  CHECK (octet_length(cron_expression) BETWEEN 1 AND 255)
);

CREATE INDEX IF NOT EXISTS rustee_recurring_jobs_due_idx
  ON rustee_recurring_jobs (next_run_at, id)
  WHERE enabled;
