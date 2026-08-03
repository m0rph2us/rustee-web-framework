CREATE TABLE IF NOT EXISTS rustee_mongodb_change_stream_checkpoint (
    consumer TEXT PRIMARY KEY,
    resume_token BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (octet_length(consumer) BETWEEN 1 AND 255)
);

CREATE TABLE IF NOT EXISTS rustee_mongodb_change_stream_lease (
    consumer TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (octet_length(consumer) BETWEEN 1 AND 255),
    CHECK (octet_length(owner) BETWEEN 1 AND 255)
);
