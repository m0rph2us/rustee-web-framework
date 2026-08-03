CREATE TABLE IF NOT EXISTS rustee_api_key_credentials (
    key_id UUID PRIMARY KEY,
    fingerprint BYTEA NOT NULL UNIQUE CHECK (octet_length(fingerprint) = 32),
    principal JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked')),
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_used_at TIMESTAMPTZ,
    last_used_count BIGINT NOT NULL DEFAULT 0 CHECK (last_used_count >= 0),
    revoked_at TIMESTAMPTZ,
    CHECK (
        (status = 'active' AND revoked_at IS NULL)
        OR (status = 'revoked' AND revoked_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS rustee_api_key_credentials_active_fingerprint_idx
    ON rustee_api_key_credentials (fingerprint)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS rustee_api_key_authentication_audit (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    key_id UUID NOT NULL REFERENCES rustee_api_key_credentials (key_id),
    authenticated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS rustee_api_key_authentication_audit_key_time_idx
    ON rustee_api_key_authentication_audit (key_id, authenticated_at DESC);
