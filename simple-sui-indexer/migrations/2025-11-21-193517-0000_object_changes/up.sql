-- Your SQL goes here
CREATE TABLE object_changes (
    address TEXT PRIMARY KEY NOT NULL,
    transaction_digest TEXT NOT NULL REFERENCES transactions(transaction_digest),
    change_type TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    output_digest TEXT NOT NULL
);