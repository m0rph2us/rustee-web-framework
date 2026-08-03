ALTER TABLE rustee_outbox
  ADD COLUMN IF NOT EXISTS priority smallint NOT NULL DEFAULT 0;

DO $$
BEGIN
  ALTER TABLE rustee_outbox
    ADD CONSTRAINT rustee_outbox_priority_range
    CHECK (priority >= 0 AND priority <= 255) NOT VALID;
EXCEPTION
  WHEN duplicate_object THEN NULL;
END
$$;

ALTER TABLE rustee_outbox
  VALIDATE CONSTRAINT rustee_outbox_priority_range;

CREATE INDEX IF NOT EXISTS rustee_outbox_ready_priority_idx
  ON rustee_outbox (kind, destination, priority DESC, available_at, created_at, id)
  WHERE published_at IS NULL;
