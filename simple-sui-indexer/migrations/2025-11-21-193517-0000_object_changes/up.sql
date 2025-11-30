-- Your SQL goes here
CREATE TABLE object_changes (
    object_id SERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    transaction_digest TEXT NOT NULL REFERENCES transactions(transaction_digest),
    change_type TEXT NOT NULL,
    input_version BIGINT NOT NULL,
    input_digest TEXT NOT NULL,
    output_version BIGINT NOT NULL,
    output_digest TEXT NOT NULL
);