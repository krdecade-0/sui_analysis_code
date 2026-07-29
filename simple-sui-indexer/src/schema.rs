// @generated automatically by Diesel CLI.

diesel::table! {
    checkpoints (sequence_number) {
        sequence_number -> Int4,
        digest -> Bytea,
        timestamp -> Timestamptz,
        epoch_id -> Int4,
        user_tx_count -> Int2,
        system_tx_count -> Int2,
    }
}

diesel::table! {
    gas_backfill_targets (transaction_digest) {
        transaction_digest -> Bytea,
        checkpoint_sequence -> Int4,
    }
}

diesel::table! {
    object_changes (object_id) {
        object_id -> Int8,
        address -> Bytea,
        transaction_digest -> Bytea,
        change_type -> Text,
        input_version -> Int4,
        input_digest -> Bytea,
        output_version -> Int4,
        output_digest -> Bytea,
    }
}

diesel::table! {
    transactions (transaction_digest) {
        transaction_digest -> Bytea,
        kind -> Text,
        checkpoint_sequence -> Int4,
        status -> Text,
        error -> Nullable<Bytea>,
        inputs_imm_or_owned -> Int2,
        inputs_pure -> Int2,
        inputs_receiving -> Int2,
        inputs_shared_mut -> Int2,
        inputs_shared_ro -> Int2,
        inputs_funds_withdrawal -> Int2,
        command_move_call -> Int2,
        command_transfer_objects -> Int2,
        command_split_coins -> Int2,
        command_merge_coins -> Int2,
        command_publish -> Int2,
        command_make_move_vec -> Int2,
        command_upgrade -> Int2,
        sui_transferred -> Int2,
        gas_used -> Int8,
        gas_price -> Int8,
    }
}

diesel::table! {
    watermarks (pipeline) {
        pipeline -> Text,
        epoch_hi_inclusive -> Int8,
        checkpoint_hi_inclusive -> Int8,
        tx_hi -> Int8,
        timestamp_ms_hi_inclusive -> Int8,
        reader_lo -> Int8,
        pruner_timestamp -> Timestamp,
        pruner_hi -> Int8,
    }
}

diesel::joinable!(object_changes -> transactions (transaction_digest));
diesel::joinable!(transactions -> checkpoints (checkpoint_sequence));

diesel::allow_tables_to_appear_in_same_query!(
    checkpoints,
    gas_backfill_targets,
    object_changes,
    transactions,
    watermarks,
);
