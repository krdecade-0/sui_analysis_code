// Logic to fetch Sui checkpoints from mainnet, parse BCS, and insert rows into the DB.

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::Result;
use diesel_async::RunQueryDsl;
use diesel::ExpressionMethods;
use sui_indexer_alt_framework::{pipeline::Processor, postgres::Db};
use sui_indexer_alt_framework::pipeline::concurrent::Handler as ConcurrentHandler;
use sui_indexer_alt_framework::postgres::store::Store;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::effects::{TransactionEffects, TransactionEffectsAPI, IDOperation};
use sui_types::transaction::{TransactionKind, TransactionDataAPI, Command, CallArg, ObjectArg, SharedObjectMutability};
use sui_types::execution_status::ExecutionStatus;
use chrono::{DateTime, Utc};

use crate::models::{StoredCheckpoint, StoredTransaction, ObjectChange, TxKind, Status, ChangeType, IndexerBatch};
use crate::schema::transactions::dsl::{transactions, transaction_digest};
use crate::schema::checkpoints::dsl::{checkpoints, sequence_number};
use crate::schema::object_changes::dsl::object_changes;
use crate::schema::watermarks::dsl as wm;


/// Handler for inserting checkpoints + transactions + object changes
pub struct CheckpointHandler {
    pub start_checkpoint: u64,
    pub end_checkpoint: u64,
}


#[async_trait::async_trait]
impl Processor for CheckpointHandler {
    const NAME: &'static str = "checkpoint_handler";
    type Value = IndexerBatch;

    async fn process(
        &self,
        checkpoint: &Arc<CheckpointData>,
    ) -> Result<Vec<Self::Value>> {

        // Sample every ~691th checkpoint, assuming 345k checkpoints per day and 100 desired samples per day
        if checkpoint.checkpoint_summary.sequence_number % 3455 != 0 {
            return Ok(vec![]);
        }

        // Skip checkpoints before start
        if checkpoint.checkpoint_summary.sequence_number < self.start_checkpoint {
            return Ok(vec![]);
        }

        // Skip checkpoints after end
        if self.end_checkpoint > 0 && checkpoint.checkpoint_summary.sequence_number > self.end_checkpoint {
            return Ok(vec![]);
        }

        // Extract checkpoint metadata
        let seq_number = checkpoint.checkpoint_summary.sequence_number as i32;
        let digest_vec = checkpoint.checkpoint_summary.digest().inner().to_vec();
        let timestamp = DateTime::<Utc>::from(checkpoint.checkpoint_summary.timestamp());
        let epoch_number = checkpoint.checkpoint_summary.epoch as i32;
        let mut system_tx_count: i16 = 0;
        let mut user_tx_count: i16 = 0;
        let mut tx_count = 0;

        let txs = &checkpoint.transactions;

        for tx_checkpoint in txs.iter() {
            tx_count += 1;
            let kind = tx_checkpoint.transaction.data().intent_message().value.kind();

            if kind.is_system_tx() {
                system_tx_count += 1;
            } else {
                user_tx_count += 1;
            }
        }

        // Create checkpoint record
        let stored_checkpoint = StoredCheckpoint {
            sequence_number: seq_number,
            digest: digest_vec,
            timestamp,
            epoch_id: epoch_number,
            system_tx_count,
            user_tx_count,
        };

        let mut transactions_vec = Vec::with_capacity(tx_count);
        let obj_total: usize = txs.iter().map(|tx| match &tx.effects {
            TransactionEffects::V1(e) => e.object_changes().len(),
            TransactionEffects::V2(e) => e.object_changes().len(),
        }).sum();

        let mut object_changes_vec = Vec::with_capacity(obj_total);

        // Process each transaction in the checkpoint
        for tx in txs.iter() {

            // Determine the kind of the transaction
            let tx_data = &tx.transaction.data().intent_message().value;

            let kind = match tx_data.kind() {
                TransactionKind::ProgrammableTransaction(_) => TxKind::ProgrammableTransaction,
                TransactionKind::Genesis(_) => TxKind::GenesisTransaction,
                TransactionKind::ConsensusCommitPrologue(_) => TxKind::ConsensusCommitPrologueTransaction,
                TransactionKind::ConsensusCommitPrologueV2(_) => TxKind::ConsensusCommitPrologueTransactionV2,
                TransactionKind::ConsensusCommitPrologueV3(_) => TxKind::ConsensusCommitPrologueTransactionV3,
                TransactionKind::ConsensusCommitPrologueV4(_) => TxKind::ConsensusCommitPrologueTransactionV4,
                TransactionKind::ChangeEpoch(_) => TxKind::ChangeEpochTransaction,
                TransactionKind::RandomnessStateUpdate(_) => TxKind::RandomnessStateUpdateTransaction,
                TransactionKind::AuthenticatorStateUpdate(_) => TxKind::AuthenticatorStateUpdateTransaction,
                TransactionKind::EndOfEpochTransaction(_) => TxKind::EndOfEpochTransaction,
                TransactionKind::ProgrammableSystemTransaction(_) => TxKind::ProgrammableSystemTransaction,
            };

            let (status, error_str) = match tx.effects.status() {
                ExecutionStatus::Success => (Status::Success, None),
            
                ExecutionStatus::Failure { error, .. } => (
                    Status::Failure,
                    Some(error.to_string()),
                ),
            };

            let mut inputs_imm_or_owned: i16 = 0;
            let mut inputs_pure: i16 = 0;
            let mut inputs_receiving: i16 = 0;
            let mut inputs_shared_mut: i16 = 0;
            let mut inputs_shared_ro: i16 = 0;
            let mut inputs_funds_withdrawal: i16 = 0;

            let mut command_move_call: i16 = 0;
            let mut command_transfer_objects: i16 = 0;
            let mut command_split_coins: i16 = 0;
            let mut command_merge_coins: i16 = 0;
            let mut command_publish: i16 = 0;
            let mut command_make_move_vec: i16 = 0;
            let mut command_upgrade: i16 = 0;

            // cannot calculate sui_transferred directly from checkpoint
            let sui_transferred: i16 = 0;

            let gas_price = tx_data.gas_data().price as i32;

            let gas_used: i32 = match &tx.effects {
                TransactionEffects::V1(effects_v1) => {
                    let gas = effects_v1.gas_cost_summary();
                    (gas.computation_cost + gas.storage_cost) as i32 - gas.storage_rebate as i32
                }
                TransactionEffects::V2(effects_v2) => {
                    let gas = effects_v2.gas_cost_summary();
                    (gas.computation_cost + gas.storage_cost) as i32 - gas.storage_rebate as i32
                }
            };

            if let TransactionKind::ProgrammableTransaction(pt) = tx_data.kind() {
                for input in pt.inputs.iter() {
                    match input {
                        // Pure values
                        CallArg::Pure(_) => {
                            inputs_pure += 1;
                        }
                        // Object inputs
                        CallArg::Object(obj) => {
                            match obj {
                                // Owned or immutable object
                                ObjectArg::ImmOrOwnedObject(_) => {
                                    inputs_imm_or_owned += 1;
                                }
                                // Receiving object reference
                                ObjectArg::Receiving(_) => {
                                    inputs_receiving += 1;
                                }
                                // Shared object with mutability flag
                                ObjectArg::SharedObject { mutability, .. } => {
                                    match mutability {
                                        SharedObjectMutability::Mutable => {
                                            inputs_shared_mut += 1;
                                        }
                                        SharedObjectMutability::Immutable => {
                                            inputs_shared_ro += 1;
                                        }
                                        SharedObjectMutability::NonExclusiveWrite => {
                                            inputs_shared_mut += 1;
                                        }
                                    }
                                }
                            }
                        }
                        // FundsWithdrawal variant
                        CallArg::FundsWithdrawal(_) => {
                            inputs_funds_withdrawal += 1;
                        }
                    }
                }

                for cmd in pt.commands.iter() {
                    match cmd {
                        Command::MoveCall(_) => command_move_call += 1,
                        Command::TransferObjects(_, _) => command_transfer_objects += 1,
                        Command::SplitCoins(_, _) => command_split_coins += 1,
                        Command::MergeCoins(_, _) => command_merge_coins += 1,
                        Command::Publish(_, _) => command_publish += 1,
                        Command::MakeMoveVec(_, _) => command_make_move_vec += 1,
                        Command::Upgrade(..) => command_upgrade += 1,
                    }
                }
            }

            // Get transaction digest as vec
            let tx_digest_vec = tx.transaction.digest().inner().to_vec();

            // Build stored transaction
            let stored_tx = StoredTransaction {
                transaction_digest: tx_digest_vec.clone(),
                kind,
                checkpoint_sequence: seq_number,
                status,
                error: error_str.as_ref().map(|s| s.as_bytes().to_vec()),
                inputs_imm_or_owned,
                inputs_pure,
                inputs_receiving,
                inputs_shared_mut,
                inputs_shared_ro,
                inputs_funds_withdrawal,
                command_move_call,
                command_transfer_objects,
                command_split_coins,
                command_merge_coins,
                command_publish,
                command_make_move_vec,
                command_upgrade,
                sui_transferred,
                gas_used,
                gas_price,
            };
            transactions_vec.push(stored_tx);

            // Extract object changes depending on the effects version
            match &tx.effects {
                TransactionEffects::V1(effects_v1) => {
                    for change in effects_v1.object_changes() {
                        let change_type = if change.id_operation == IDOperation::Created {
                            ChangeType::Created
                        } else if change.id_operation == IDOperation::Deleted && change.input_version.is_none() && change.output_version.is_none() {
                            ChangeType::UnwrappedThenDeleted
                        } else if change.id_operation == IDOperation::Deleted {
                            ChangeType::Deleted
                        } else if change.input_version.is_some() && change.output_version.is_some() {
                            ChangeType::Mutated
                        } else if change.input_version.is_none() && change.output_version.is_some() {
                            ChangeType::Unwrapped
                        } else if change.input_version.is_some() && change.output_version.is_none() {
                            ChangeType::Wrapped
                        } else {
                            ChangeType::Unknown
                        };

                        let stored = ObjectChange {
                            address: change.id.into_bytes().to_vec(),
                            transaction_digest: tx_digest_vec.clone(),
                            change_type,
                            input_version: change.input_version.map(|seq| seq.value() as i32).unwrap_or_default(),
                            input_digest: change.input_digest.map(|d| d.inner().to_vec()).unwrap_or_default(),
                            output_version: change.output_version.map(|seq| seq.value() as i32).unwrap_or_default(),
                            output_digest: change.output_digest.map(|d| d.inner().to_vec()).unwrap_or_default(),
                        };                   
                        object_changes_vec.push(stored);
                    }
                }
                TransactionEffects::V2(effects_v2) => {
                    for change in effects_v2.object_changes() {
                        let change_type = if change.id_operation == IDOperation::Created {
                            ChangeType::Created
                        } else if change.id_operation == IDOperation::Deleted && change.input_version.is_none() && change.output_version.is_none() {
                            ChangeType::UnwrappedThenDeleted
                        } else if change.id_operation == IDOperation::Deleted {
                            ChangeType::Deleted
                        } else if change.input_version.is_some() && change.output_version.is_some() {
                            ChangeType::Mutated
                        } else if change.input_version.is_none() && change.output_version.is_some() {
                            ChangeType::Unwrapped
                        } else if change.input_version.is_some() && change.output_version.is_none() {
                            ChangeType::Wrapped
                        } else {
                            ChangeType::Unknown
                        };

                        let stored = ObjectChange {
                            address: change.id.into_bytes().to_vec(),
                            transaction_digest: tx_digest_vec.clone(),
                            change_type,
                            input_version: change.input_version.map(|seq| seq.value() as i32).unwrap_or_default(),
                            input_digest: change.input_digest.map(|d| d.inner().to_vec()).unwrap_or_default(),
                            output_version: change.output_version.map(|seq| seq.value() as i32).unwrap_or_default(),
                            output_digest: change.output_digest.map(|d| d.inner().to_vec()).unwrap_or_default(),
                        };
                        
                        object_changes_vec.push(stored);
                    }
                }
            }
        }

        Ok(vec![IndexerBatch {
            checkpoints: vec![stored_checkpoint],
            transactions: transactions_vec,
            object_changes: object_changes_vec,
        }])
    }
}

#[async_trait::async_trait]
impl ConcurrentHandler for CheckpointHandler {
    type Store = Db;

    async fn commit<'a>(
        values: &[Self::Value],
        conn: &mut <Self::Store as Store>::Connection<'a>,
    ) -> Result<usize> {
        let mut total_inserted = 0;

        for batch in values.iter() {
            let checkpoint_batch = &batch.checkpoints;
            let tx_batch = &batch.transactions;
            let obj_batch = &batch.object_changes;

            if !checkpoint_batch.is_empty() {
                for chunk in checkpoint_batch.chunks(200) {
                    let inserted = diesel::insert_into(checkpoints)
                        .values(chunk)
                        .on_conflict(sequence_number)
                        .do_nothing()
                        .execute(conn)
                        .await?;
                    total_inserted += inserted;
                }
            }

            if !tx_batch.is_empty() {
                for chunk in tx_batch.chunks(2000) {
                    let inserted = diesel::insert_into(transactions)
                        .values(chunk)
                        .on_conflict(transaction_digest)
                        .do_nothing()
                        .execute(conn)
                        .await?;
                    total_inserted += inserted;
                }
            }

            if !obj_batch.is_empty() {
                for chunk in obj_batch.chunks(3000) {
                    let inserted = diesel::insert_into(object_changes)
                        .values(chunk)
                        .execute(conn)
                        .await?;
                    total_inserted += inserted;
                }
            }

            // Advance watermark to last successfully committed checkpoint
            if let Some(last) = checkpoint_batch.last() {
                let seq = last.sequence_number as i64;

                diesel::update(wm::watermarks)
                    .filter(wm::pipeline.eq("checkpoint_handler"))
                    .set(wm::checkpoint_hi_inclusive.eq(seq))
                    .execute(conn)
                    .await?;
            }
        }

        Ok(total_inserted)
    }
}
