CREATE TABLE IF NOT EXISTS rustee_outbox (
  id uuid PRIMARY KEY,
  kind text NOT NULL CHECK (kind IN ('event', 'job')),
  destination text NOT NULL,
  message_id text NOT NULL,
  message_type text NOT NULL,
  schema_version integer NOT NULL CHECK (schema_version >= 0 AND schema_version <= 65535),
  ordering_key text NOT NULL,
  delivery_attempt integer NOT NULL CHECK (delivery_attempt >= 1 AND delivery_attempt <= 65535),
  payload bytea NOT NULL CHECK (octet_length(payload) > 0),
  relay_attempt integer NOT NULL DEFAULT 0 CHECK (relay_attempt >= 0),
  available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  leased_until timestamptz,
  lease_token uuid,
  published_at timestamptz,
  last_failure_kind text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK ((lease_token IS NULL) = (leased_until IS NULL)),
  UNIQUE (kind, destination, message_id)
);

CREATE INDEX IF NOT EXISTS rustee_outbox_ready_idx
  ON rustee_outbox (kind, destination, available_at, created_at, id)
  WHERE published_at IS NULL;
