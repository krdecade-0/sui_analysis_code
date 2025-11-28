// @generated automatically by Diesel CLI.

diesel::table! {
    checkpoints (sequence_number) {
        sequence_number -> Int8,
        digest -> Text,
        timestamp -> Int8,
        epoch_id -> Int8,
        user_tx_count -> Int8,
        system_tx_count -> Int8,
    }
}

diesel::table! {
    object_changes (id) {
        id -> Int8,
        transaction_digest -> Text,
        change_type -> Text,
        address -> Text,
        id_created -> Bool,
        id_deleted -> Bool,
    }
}

diesel::table! {
    transactions (transaction_digest) {
        transaction_digest -> Text,
        kind -> Text,
        checkpoint_sequence -> Int8,
        status -> Text,
        error -> Nullable<Text>,
        inputs_imm_or_owned -> Int8,
        inputs_pure -> Int8,
        inputs_receiving -> Int8,
        inputs_shared_mut -> Int8,
        inputs_shared_ro -> Int8,
        inputs_funds_withdrawal -> Int8,
        command_move_call -> Int8,
        command_transfer_objects -> Int8,
        command_split_coins -> Int8,
        command_merge_coins -> Int8,
        command_publish -> Int8,
        command_make_move_vec -> Int8,
        command_upgrade -> Int8,
        sui_transferred -> Int8,
        gas_used -> Int8,
        gas_price -> Int8,
    }
}

diesel::joinable!(object_changes -> transactions (transaction_digest));
diesel::joinable!(transactions -> checkpoints (checkpoint_sequence));

diesel::allow_tables_to_appear_in_same_query!(checkpoints, object_changes, transactions,);
