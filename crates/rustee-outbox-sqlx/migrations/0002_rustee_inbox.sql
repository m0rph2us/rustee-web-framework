CREATE TABLE IF NOT EXISTS rustee_inbox (
  consumer text NOT NULL,
  message_id text NOT NULL,
  completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (consumer, message_id)
);
