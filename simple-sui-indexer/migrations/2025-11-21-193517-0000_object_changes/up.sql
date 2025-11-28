-- Your SQL goes here
CREATE TABLE object_changes (
    id BIGSERIAL PRIMARY KEY,
    transaction_digest TEXT NOT NULL REFERENCES transactions(transaction_digest),
    change_type TEXT NOT NULL,
    address TEXT NOT NULL,
    id_created BOOLEAN NOT NULL DEFAULT false,
    id_deleted BOOLEAN NOT NULL DEFAULT false
);