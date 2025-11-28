-- Your SQL goes here
CREATE TABLE transactions (
    transaction_digest TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    checkpoint_sequence BIGINT NOT NULL REFERENCES checkpoints(sequence_number),
    status TEXT NOT NULL DEFAULT 'success',
    error TEXT,
    inputs_imm_or_owned BIGINT NOT NULL DEFAULT 0,
    inputs_pure BIGINT NOT NULL DEFAULT 0,
    inputs_receiving BIGINT NOT NULL DEFAULT 0,
    inputs_shared_mut BIGINT NOT NULL DEFAULT 0,
    inputs_shared_ro BIGINT NOT NULL DEFAULT 0,
    inputs_funds_withdrawal BIGINT NOT NULL DEFAULT 0,
    command_move_call BIGINT NOT NULL DEFAULT 0,
    command_transfer_objects BIGINT NOT NULL DEFAULT 0,
    command_split_coins BIGINT NOT NULL DEFAULT 0,
    command_merge_coins BIGINT NOT NULL DEFAULT 0,
    command_publish BIGINT NOT NULL DEFAULT 0,
    command_make_move_vec BIGINT NOT NULL DEFAULT 0,
    command_upgrade BIGINT NOT NULL DEFAULT 0,
    sui_transferred BIGINT NOT NULL DEFAULT 0,
    gas_used BIGINT NOT NULL DEFAULT 0,
    gas_price BIGINT NOT NULL DEFAULT 0
);