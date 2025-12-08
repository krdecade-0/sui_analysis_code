-- Your SQL goes here
CREATE TABLE object_changes (
    object_id BIGSERIAL PRIMARY KEY,
    address BYTEA NOT NULL,
    transaction_digest BYTEA NOT NULL REFERENCES transactions(transaction_digest),
    change_type TEXT NOT NULL,
    input_version INTEGER NOT NULL DEFAULT 0,
    input_digest BYTEA NOT NULL DEFAULT '',
    output_version INTEGER NOT NULL DEFAULT 0,
    output_digest BYTEA NOT NULL DEFAULT ''
);