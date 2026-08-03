CREATE TABLE IF NOT EXISTS rustee_kafka_delayed_retries (
  id uuid PRIMARY KEY,
  origin_topic text NOT NULL CHECK (octet_length(origin_topic) BETWEEN 1 AND 255),
  origin_partition integer NOT NULL CHECK (origin_partition >= 0),
  origin_offset bigint NOT NULL CHECK (origin_offset >= 0),
  retry_topic text NOT NULL CHECK (octet_length(retry_topic) BETWEEN 1 AND 255),
  retry_attempt integer NOT NULL CHECK (retry_attempt BETWEEN 2 AND 65535),
  failure_kind text NOT NULL CHECK (failure_kind IN ('decode', 'handler')),
  event_key bytea,
  payload bytea NOT NULL CHECK (octet_length(payload) BETWEEN 1 AND 1048576),
  available_at timestamptz NOT NULL,
  leased_until timestamptz,
  lease_token uuid,
  relay_attempt integer NOT NULL DEFAULT 0 CHECK (relay_attempt >= 0),
  published_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK ((lease_token IS NULL) = (leased_until IS NULL)),
  UNIQUE (origin_topic, origin_partition, origin_offset, retry_attempt)
);

CREATE INDEX IF NOT EXISTS rustee_kafka_delayed_retries_ready_idx
  ON rustee_kafka_delayed_retries (available_at, created_at, id)
  WHERE published_at IS NULL;
