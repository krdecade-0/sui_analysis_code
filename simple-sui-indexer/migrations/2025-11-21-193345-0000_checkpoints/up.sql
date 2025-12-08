-- Your SQL goes here
CREATE TABLE checkpoints (
    sequence_number INTEGER PRIMARY KEY,
    digest BYTEA NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    epoch_id INTEGER NOT NULL,
    user_tx_count SMALLINT NOT NULL DEFAULT 0,
    system_tx_count SMALLINT NOT NULL DEFAULT 0
);
