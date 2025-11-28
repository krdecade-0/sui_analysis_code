// Logic to fetch Sui checkpoints from mainnet, parse BCS, and insert rows into the DB.

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use anyhow::Result;
use diesel_async::RunQueryDsl;
use sui_indexer_alt_framework::{pipeline::Processor, postgres::{Connection, Db}};
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::effects::{TransactionEffects, TransactionEffectsAPI};
use sui_types::transaction::{TransactionKind, TransactionDataAPI, Command, CallArg, ObjectArg, SharedObjectMutability};
use sui_types::execution_status::ExecutionStatus;

use crate::models::{StoredCheckpoint, StoredTransaction, StoredObjectChange};
use crate::schema::transactions::dsl::{transactions, transaction_digest};
use crate::schema::checkpoints::dsl::{checkpoints, sequence_number};
use crate::schema::object_changes::dsl::{object_changes, id};


/// Handler for inserting checkpoints + transactions + object changes
pub struct CheckpointHandler;

#[async_trait::async_trait]
impl Processor for CheckpointHandler {
    const NAME: &'static str = "checkpoint_handler";
    type Value = (Vec<StoredCheckpoint>, Vec<StoredTransaction>, Vec<StoredObjectChange>);

    async fn process(
        &self,
        checkpoint: &Arc<CheckpointData>,
    ) -> Result<Vec<Self::Value>> {

        // Extract checkpoint metadata
        let seq_number = checkpoint.checkpoint_summary.sequence_number as i64;
        let digest_str = checkpoint.checkpoint_summary.digest().to_string();
        let timestamp = checkpoint
            .checkpoint_summary
            .timestamp()
            .duration_since(UNIX_EPOCH)
            .expect("Timestamp is before UNIX_EPOCH")
            .as_secs() as i64;
        let epoch_number = checkpoint.checkpoint_summary.epoch as i64;
        let mut system_tx_count = 0;
        let mut user_tx_count = 0;

        for tx_checkpoint in checkpoint.transactions.iter() {
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
            digest: digest_str,
            timestamp,
            epoch_id: epoch_number,
            system_tx_count,
            user_tx_count,
        };

        let mut transactions_vec = Vec::new();
        let mut object_changes_vec = Vec::new();

        // Process each transaction in the checkpoint
        for tx in checkpoint.transactions.iter() {

            // Determine the kind of the transaction
            let tx_data = &tx.transaction.data().intent_message().value;

            let kind_str = match tx_data.kind() {
                TransactionKind::ProgrammableTransaction(_) => "ProgrammableTransaction",
                TransactionKind::Genesis(_) => "GenesisTransaction",
                TransactionKind::ConsensusCommitPrologue(_) => "ConsensusCommitPrologueTransaction",
                TransactionKind::ConsensusCommitPrologueV2(_) => "ConsensusCommitPrologueTransactionV2",
                TransactionKind::ConsensusCommitPrologueV3(_) => "ConsensusCommitPrologueTransactionV3",
                TransactionKind::ConsensusCommitPrologueV4(_) => "ConsensusCommitPrologueTransactionV4",
                TransactionKind::ChangeEpoch(_) => "ChangeEpochTransaction",
                TransactionKind::RandomnessStateUpdate(_) => "RandomnessStateUpdateTransaction",
                TransactionKind::AuthenticatorStateUpdate(_) => "AuthenticatorStateUpdateTransaction",
                TransactionKind::EndOfEpochTransaction(_) => "EndOfEpochTransaction",
                TransactionKind::ProgrammableSystemTransaction(_) => "ProgrammableSystemTransaction",
            }
            .to_string();

            let (status, error) = match tx.effects.status() {
                ExecutionStatus::Success => ("success".to_string(), None),

                ExecutionStatus::Failure { error, command: _ } => (
                    "failure".to_string(),
                    Some(error.to_string())
                ),
            };

            let mut inputs_imm_or_owned = 0;
            let mut inputs_pure = 0;
            let mut inputs_receiving = 0;
            let mut inputs_shared_mut = 0;
            let mut inputs_shared_ro = 0;
            let mut inputs_funds_withdrawal = 0;

            let mut command_move_call = 0;
            let mut command_transfer_objects = 0;
            let mut command_split_coins = 0;
            let mut command_merge_coins = 0;
            let mut command_publish = 0;
            let mut command_make_move_vec = 0;
            let mut command_upgrade = 0;

            // cannot calculate sui_transferred directly from checkpoint
            let sui_transferred: i64 = 0;

            let gas_price = tx_data.gas_data().price as i64;

            let gas_used: i64 = match &tx.effects {
                TransactionEffects::V1(effects_v1) => {
                    let gas = effects_v1.gas_cost_summary();
                    (gas.computation_cost + gas.storage_cost) as i64 - gas.storage_rebate as i64
                }
                TransactionEffects::V2(effects_v2) => {
                    let gas = effects_v2.gas_cost_summary();
                    (gas.computation_cost + gas.storage_cost) as i64 - gas.storage_rebate as i64
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

            // Get transaction digest
            let tx_digest = tx.transaction.digest().to_string();

            // Build stored transaction
            let stored_tx = StoredTransaction {
                transaction_digest: tx_digest.clone(),
                kind: kind_str,
                checkpoint_sequence: seq_number,
                status,
                error,
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
                    // CREATED
                    for (obj_ref, _owner) in effects_v1.created() {
                        let (object_id, _, _) = obj_ref;
                        // (ObjectID, sui_types::base_types::SequenceNumber, ObjectDigest)

                        let stored = StoredObjectChange {
                            id: 0, // Auto-generated by database, ignored on insert
                            transaction_digest: tx_digest.clone(),
                            change_type: "created".to_string(),
                            address: object_id.to_string(),
                            id_created: true,
                            id_deleted: false,
                        };
                        object_changes_vec.push(stored);
                    }

                    // MUTATED
                    for (obj_ref, _owner) in effects_v1.mutated() {
                        let (object_id, _, _) = obj_ref;

                        let stored = StoredObjectChange {
                            id: 0, // Auto-generated by database, ignored on insert
                            transaction_digest: tx_digest.clone(),
                            change_type: "mutated".to_string(),
                            address: object_id.to_string(),
                            id_created: false,
                            id_deleted: false,
                        };
                        object_changes_vec.push(stored);
                    }

                    // UNWRAPPED
                    for (obj_ref, _owner) in effects_v1.unwrapped() {
                        let (object_id, _, _) = obj_ref;

                        let stored = StoredObjectChange {
                            id: 0, // Auto-generated by database, ignored on insert
                            transaction_digest: tx_digest.clone(),
                            change_type: "unwrapped".to_string(),
                            address: object_id.to_string(),
                            id_created: true,
                            id_deleted: false,
                        };
                        object_changes_vec.push(stored);
                    }

                    // DELETED
                    for obj_ref in effects_v1.deleted() {
                        let (object_id, _, _) = obj_ref;

                        let stored = StoredObjectChange {
                            id: 0, // Auto-generated by database, ignored on insert
                            transaction_digest: tx_digest.clone(),
                            change_type: "deleted".to_string(),
                            address: object_id.to_string(),
                            id_created: false,
                            id_deleted: true,
                        };
                        object_changes_vec.push(stored);
                    }

                    // WRAPPED
                    for obj_ref in effects_v1.wrapped() {
                        let (object_id, _, _) = obj_ref;

                        let stored = StoredObjectChange {
                            id: 0, // Auto-generated by database, ignored on insert
                            transaction_digest: tx_digest.clone(),
                            change_type: "wrapped".to_string(),
                            address: object_id.to_string(),
                            id_created: false,
                            id_deleted: false,
                        };
                        object_changes_vec.push(stored);
                    }
                }

                TransactionEffects::V2(effects_v2) => {
                    // EXACT SAME LOGIC for V2
                    // CREATED
                    for (obj_ref, _owner) in effects_v2.created() {
                        let (object_id, _, _) = obj_ref;

                        let stored = StoredObjectChange {
                            id: 0, // Auto-generated by database, ignored on insert
                            transaction_digest: tx_digest.clone(),
                            change_type: "created".to_string(),
                            address: object_id.to_string(),
                            id_created: true,
                            id_deleted: false,
                        };
                        object_changes_vec.push(stored);
                    }

                    // MUTATED
                    for (obj_ref, _owner) in effects_v2.mutated() {
                        let (object_id, _, _) = obj_ref;

                        let stored = StoredObjectChange {
                            id: 0, // Auto-generated by database, ignored on insert
                            transaction_digest: tx_digest.clone(),
                            change_type: "mutated".to_string(),
                            address: object_id.to_string(),
                            id_created: false,
                            id_deleted: false,
                        };
                        object_changes_vec.push(stored);
                    }

                    // UNWRAPPED
                    for (obj_ref, _owner) in effects_v2.unwrapped() {
                        let (object_id, _, _) = obj_ref;

                        let stored = StoredObjectChange {
                            id: 0, // Auto-generated by database, ignored on insert
                            transaction_digest: tx_digest.clone(),
                            change_type: "unwrapped".to_string(),
                            address: object_id.to_string(),
                            id_created: true,
                            id_deleted: false,
                        };
                        object_changes_vec.push(stored);
                    }

                    // DELETED
                    for obj_ref in effects_v2.deleted() {
                        let (object_id, _, _) = obj_ref;

                        let stored = StoredObjectChange {
                            id: 0, // Auto-generated by database, ignored on insert
                            transaction_digest: tx_digest.clone(),
                            change_type: "deleted".to_string(),
                            address: object_id.to_string(),
                            id_created: false,
                            id_deleted: true,
                        };
                        object_changes_vec.push(stored);
                    }

                    // WRAPPED
                    for obj_ref in effects_v2.wrapped() {
                        let (object_id, _, _) = obj_ref;

                        let stored = StoredObjectChange {
                            id: 0, // Auto-generated by database, ignored on insert
                            transaction_digest: tx_digest.clone(),
                            change_type: "wrapped".to_string(),
                            address: object_id.to_string(),
                            id_created: false,
                            id_deleted: false,
                        };
                        object_changes_vec.push(stored);
                    }
                }
            }
        }

        Ok(vec![(vec![stored_checkpoint], transactions_vec, object_changes_vec)])
    }
}

#[async_trait::async_trait]
impl sui_indexer_alt_framework::pipeline::sequential::Handler for CheckpointHandler {
    type Store = Db;
    type Batch = Vec<(Vec<StoredCheckpoint>, Vec<StoredTransaction>, Vec<StoredObjectChange>)>;

    fn batch(batch: &mut Self::Batch, values: Vec<Self::Value>) {
        batch.extend(values);
    }

    async fn commit<'a>(
        batch: &Self::Batch,
        conn: &mut Connection<'a>,
    ) -> Result<usize> {
        let mut total_inserted = 0;

        for (checkpoint_batch, tx_batch, obj_batch) in batch.iter() {
            if !checkpoint_batch.is_empty() {
                let inserted = diesel::insert_into(checkpoints)
                    .values(checkpoint_batch)
                    .on_conflict(sequence_number)
                    .do_nothing()
                    .execute(conn)
                    .await?;
                total_inserted += inserted;
            }

            if !tx_batch.is_empty() {
                let inserted = diesel::insert_into(transactions)
                    .values(tx_batch)
                    .on_conflict(transaction_digest)
                    .do_nothing()
                    .execute(conn)
                    .await?;
                total_inserted += inserted;
            }

            if !obj_batch.is_empty() {
                let inserted = diesel::insert_into(object_changes)
                    .values(obj_batch)
                    .on_conflict(id)
                    .do_nothing()
                    .execute(conn)
                    .await?;
                total_inserted += inserted;
            }
        }

        Ok(total_inserted)
    }
}
