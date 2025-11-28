-- Your SQL goes here
CREATE TABLE checkpoints (
    sequence_number BIGINT PRIMARY KEY,
    digest TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    epoch_id BIGINT NOT NULL,
    user_tx_count BIGINT NOT NULL DEFAULT 0,
    system_tx_count BIGINT NOT NULL DEFAULT 0
);
