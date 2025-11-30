// Rust structs representing each row that Diesel can insert/query.

use diesel::prelude::*;
use sui_indexer_alt_framework::FieldCount;
use crate::schema::{checkpoints, transactions, object_changes};

/// Checkpoint table
#[derive(Insertable, Queryable, Debug, Clone, FieldCount)]
#[diesel(table_name = checkpoints)]
pub struct StoredCheckpoint {
    pub sequence_number: i64,   // PK
    pub digest: String,
    pub timestamp: i64,
    pub epoch_id: i64,
    pub user_tx_count: i64,
    pub system_tx_count: i64,
}

/// Transactions table
#[derive(Insertable, Queryable, Debug, Clone, FieldCount)]
#[diesel(table_name = transactions)]
pub struct StoredTransaction {
    pub transaction_digest: String,   // PK
    pub kind: String,
    pub checkpoint_sequence: i64, // FK to StoredCheckpoint
    pub status: String,
    pub error: Option<String>,
    pub inputs_imm_or_owned: i64,
    pub inputs_pure: i64,
    pub inputs_receiving: i64,
    pub inputs_shared_mut: i64,
    pub inputs_shared_ro: i64,
    pub inputs_funds_withdrawal: i64,
    pub command_move_call: i64,
    pub command_transfer_objects: i64,
    pub command_split_coins: i64,
    pub command_merge_coins: i64,
    pub command_publish: i64,
    pub command_make_move_vec: i64,
    pub command_upgrade: i64,
    pub sui_transferred: i64,
    pub gas_used: i64,
    pub gas_price: i64,
}

/// Object changes table
#[derive(Insertable, Queryable, Debug, Clone, FieldCount)]
#[diesel(table_name = object_changes)]
pub struct StoredObjectChange {
    pub object_id: i32,    // PK
    pub address: String,
    pub transaction_digest: String, // FK to StoredTransaction
    pub change_type: String,
    pub input_version: i64,
    pub input_digest: String,
    pub output_version: i64,
    pub output_digest: String,
}

#[derive(Insertable)]
#[diesel(table_name = object_changes)]
pub struct ObjectChange {
    pub address: String,
    pub transaction_digest: String,
    pub change_type: String,
    pub input_version: i64,
    pub input_digest: String,
    pub output_version: i64,
    pub output_digest: String,
}