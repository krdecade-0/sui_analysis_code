-- Your SQL goes here
CREATE TABLE transactions (
    transaction_digest BYTEA PRIMARY KEY,
    kind TEXT NOT NULL,
    checkpoint_sequence INTEGER NOT NULL REFERENCES checkpoints(sequence_number),
    status TEXT NOT NULL,
    error BYTEA,
    inputs_imm_or_owned SMALLINT NOT NULL DEFAULT 0,
    inputs_pure SMALLINT NOT NULL DEFAULT 0,
    inputs_receiving SMALLINT NOT NULL DEFAULT 0,
    inputs_shared_mut SMALLINT NOT NULL DEFAULT 0,
    inputs_shared_ro SMALLINT NOT NULL DEFAULT 0,
    inputs_funds_withdrawal SMALLINT NOT NULL DEFAULT 0,
    command_move_call SMALLINT NOT NULL DEFAULT 0,
    command_transfer_objects SMALLINT NOT NULL DEFAULT 0,
    command_split_coins SMALLINT NOT NULL DEFAULT 0,
    command_merge_coins SMALLINT NOT NULL DEFAULT 0,
    command_publish SMALLINT NOT NULL DEFAULT 0,
    command_make_move_vec SMALLINT NOT NULL DEFAULT 0,
    command_upgrade SMALLINT NOT NULL DEFAULT 0,
    sui_transferred SMALLINT NOT NULL DEFAULT 0,
    gas_used INTEGER NOT NULL DEFAULT 0,
    gas_price INTEGER NOT NULL DEFAULT 0
);