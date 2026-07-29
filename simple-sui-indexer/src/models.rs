use diesel::prelude::*;
use diesel::sql_types::Text;
use diesel::serialize::{ToSql, Output};
use diesel::deserialize::FromSql;
use diesel::pg::Pg;
use diesel::{AsExpression, FromSqlRow};
use sui_indexer_alt_framework::FieldCount;
use crate::schema::{checkpoints, transactions, object_changes};
use chrono::{DateTime, Utc};
use std::io::Write;

//
// ───────────────────────────────────────────────────────────────
//   ENUMS (TxKind, Status, ChangeType)
// ───────────────────────────────────────────────────────────────
//

#[derive(Debug, Clone, PartialEq, Eq, AsExpression, FromSqlRow)]
#[diesel(sql_type = Text)]
pub enum TxKind {
    ProgrammableTransaction,
    GenesisTransaction,
    ConsensusCommitPrologueTransaction,
    ConsensusCommitPrologueTransactionV2,
    ConsensusCommitPrologueTransactionV3,
    ConsensusCommitPrologueTransactionV4,
    ChangeEpochTransaction,
    RandomnessStateUpdateTransaction,
    AuthenticatorStateUpdateTransaction,
    EndOfEpochTransaction,
    ProgrammableSystemTransaction,
}

impl ToSql<Text, Pg> for TxKind {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>)
        -> diesel::serialize::Result
    {
        // Convert enum → owned String
        let s = match self {
            TxKind::ProgrammableTransaction => "ProgrammableTransaction",
            TxKind::GenesisTransaction => "GenesisTransaction",
            TxKind::ConsensusCommitPrologueTransaction => "ConsensusCommitPrologueTransaction",
            TxKind::ConsensusCommitPrologueTransactionV2 => "ConsensusCommitPrologueTransactionV2",
            TxKind::ConsensusCommitPrologueTransactionV3 => "ConsensusCommitPrologueTransactionV3",
            TxKind::ConsensusCommitPrologueTransactionV4 => "ConsensusCommitPrologueTransactionV4",
            TxKind::ChangeEpochTransaction => "ChangeEpochTransaction",
            TxKind::RandomnessStateUpdateTransaction => "RandomnessStateUpdateTransaction",
            TxKind::AuthenticatorStateUpdateTransaction => "AuthenticatorStateUpdateTransaction",
            TxKind::EndOfEpochTransaction => "EndOfEpochTransaction",
            TxKind::ProgrammableSystemTransaction => "ProgrammableSystemTransaction",
        };

        out.write_all(s.as_bytes())?;
        Ok(diesel::serialize::IsNull::No)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, AsExpression, FromSqlRow)]
#[diesel(sql_type = Text)]
pub enum Status {
    Success,
    Failure,
}

impl ToSql<Text, Pg> for Status {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>)
        -> diesel::serialize::Result
    {
        let s = match self {
            Status::Success => "success",
            Status::Failure => "failure",
        };

        out.write_all(s.as_bytes())?;
        Ok(diesel::serialize::IsNull::No)
    }
}

impl FromSql<Text, Pg> for Status {
    fn from_sql(bytes: diesel::pg::PgValue<'_>)
        -> diesel::deserialize::Result<Self>
    {
        let s = <String as FromSql<Text, Pg>>::from_sql(bytes)?;
        match s.as_str() {
            "success" => Ok(Status::Success),
            "failure" => Ok(Status::Failure),
            _ => Err(format!("Unknown Status: {}", s).into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, AsExpression, FromSqlRow)]
#[diesel(sql_type = Text)]
pub enum ChangeType {
    Created,
    Deleted,
    Mutated,
    Wrapped,
    Unwrapped,
    UnwrappedThenDeleted,
    Unknown,
}

impl ToSql<Text, Pg> for ChangeType {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>)
        -> diesel::serialize::Result
    {
        let s = match self {
            ChangeType::Created => "created",
            ChangeType::Deleted => "deleted",
            ChangeType::Mutated => "mutated",
            ChangeType::Wrapped => "wrapped",
            ChangeType::Unwrapped => "unwrapped",
            ChangeType::UnwrappedThenDeleted => "unwrapped_then_deleted",
            ChangeType::Unknown => "unknown",
        };

        out.write_all(s.as_bytes())?;
        Ok(diesel::serialize::IsNull::No)
    }
}

impl FromSql<Text, Pg> for ChangeType {
    fn from_sql(bytes: diesel::pg::PgValue<'_>)
        -> diesel::deserialize::Result<Self>
    {
        let s = <String as FromSql<Text, Pg>>::from_sql(bytes)?;
        match s.as_str() {
            "created" => Ok(ChangeType::Created),
            "deleted" => Ok(ChangeType::Deleted),
            "mutated" => Ok(ChangeType::Mutated),
            "wrapped" => Ok(ChangeType::Wrapped),
            "unwrapped" => Ok(ChangeType::Unwrapped),
            "unwrapped_then_deleted" => Ok(ChangeType::UnwrappedThenDeleted),
            "unknown" => Ok(ChangeType::Unknown),
            _ => Err(format!("Unknown ChangeType: {}", s).into()),
        }
    }
}

//
// ───────────────────────────────────────────────────────────────
//   TABLE STRUCTS (StoredCheckpoint, StoredTransaction, StoredObjectChange)
// ───────────────────────────────────────────────────────────────
//

#[derive(Insertable, Queryable, Debug, Clone, FieldCount)]
#[diesel(table_name = checkpoints)]
pub struct StoredCheckpoint {
    pub sequence_number: i32,
    pub digest: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub epoch_id: i32,
    pub user_tx_count: i16,
    pub system_tx_count: i16,
}

#[derive(Insertable, Queryable, Debug, Clone, FieldCount)]
#[diesel(table_name = transactions)]
pub struct StoredTransaction {
    pub transaction_digest: Vec<u8>,
    pub kind: TxKind,
    pub checkpoint_sequence: i32,
    pub status: Status,
    pub error: Option<Vec<u8>>,
    pub inputs_imm_or_owned: i16,
    pub inputs_pure: i16,
    pub inputs_receiving: i16,
    pub inputs_shared_mut: i16,
    pub inputs_shared_ro: i16,
    pub inputs_funds_withdrawal: i16,
    pub command_move_call: i16,
    pub command_transfer_objects: i16,
    pub command_split_coins: i16,
    pub command_merge_coins: i16,
    pub command_publish: i16,
    pub command_make_move_vec: i16,
    pub command_upgrade: i16,
    pub sui_transferred: i16,
    pub gas_used: i64,
    pub gas_price: i64,
}

#[derive(Insertable, Queryable, Debug, Clone, FieldCount)]
#[diesel(table_name = object_changes)]
pub struct StoredObjectChange {
    pub object_id: i64,
    pub address: Vec<u8>,
    pub transaction_digest: Vec<u8>,
    pub change_type: ChangeType,
    pub input_version: i32,
    pub input_digest: Vec<u8>,
    pub output_version: i32,
    pub output_digest: Vec<u8>,
}

#[derive(Clone, Debug, Queryable, Insertable)]
#[diesel(table_name = object_changes)]
pub struct ObjectChange {
    pub address: Vec<u8>,
    pub transaction_digest: Vec<u8>,
    pub change_type: ChangeType,
    pub input_version: i32,
    pub input_digest: Vec<u8>,
    pub output_version: i32,
    pub output_digest: Vec<u8>,
}

#[derive(Clone, Debug, FieldCount)]
pub struct IndexerBatch {
    pub checkpoints: Vec<StoredCheckpoint>,
    pub transactions: Vec<StoredTransaction>,
    pub object_changes: Vec<ObjectChange>,
}