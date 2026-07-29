use std::sync::Arc;

use anyhow::{anyhow, Result};
use diesel::Insertable;
use diesel_async::RunQueryDsl;
use sui_indexer_alt_framework::pipeline::concurrent::{
    BatchStatus,
    Handler as ConcurrentHandler,
};
use sui_indexer_alt_framework::postgres::store::Store;
use sui_indexer_alt_framework::{pipeline::Processor, postgres::Db};
use sui_types::balance_change::{derive_balance_changes, BalanceChange};
use sui_types::full_checkpoint_content::Checkpoint;
use sui_types::object::Object;
use tokio::time::{timeout, Duration};

use crate::schema::balance_changes::dsl::balance_changes as balance_changes_table;

#[derive(Debug, Clone)]
pub struct IndexerBatch {
    pub checkpoint_sequence_number: i64,
    pub balance_changes: Vec<IndexedBalanceChange>,
}

#[derive(Debug, Clone)]
pub struct IndexedBalanceChange {
    pub tx_digest: Vec<u8>,
    pub coin_type: String,
    pub amount: i128,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = crate::schema::balance_changes)]
struct NewBalanceChange {
    pub tx_digest: Vec<u8>,
    pub coin_type: String,
    pub amount: i64,
}

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
        checkpoint: &Arc<Checkpoint>,
    ) -> Result<Vec<Self::Value>> {
        let seq = checkpoint.summary.sequence_number;

        let result = timeout(Duration::from_secs(600), async {
            self.process_inner(checkpoint).await
        })
        .await;

        match result {
            Ok(Ok(batch)) => Ok(batch),
            Ok(Err(e)) => Err(e),
            Err(_) => {
                eprintln!("TIMEOUT processing checkpoint {} — skipping", seq);
                Ok(vec![])
            }
        }
    }
}

impl CheckpointHandler {
    async fn process_inner(
        &self,
        checkpoint: &Arc<Checkpoint>,
    ) -> Result<Vec<IndexerBatch>> {
        let seq_number = checkpoint.summary.sequence_number;

        if seq_number % 3455 != 0 {
            return Ok(vec![]);
        }

        if seq_number < self.start_checkpoint {
            return Ok(vec![]);
        }

        if self.end_checkpoint > 0 && seq_number > self.end_checkpoint {
            return Ok(vec![]);
        }

        let txs = &checkpoint.transactions;

        let mut indexed_balance_changes = Vec::new();

        for tx in txs.iter() {
            let tx_digest_vec = tx.transaction.digest().inner().to_vec();
            let effects = &tx.effects;

            let input_objects: Vec<Object> =
                tx.input_objects(&checkpoint.object_set).cloned().collect();

            let output_objects: Vec<Object> =
                tx.output_objects(&checkpoint.object_set).cloned().collect();

            let derived: Vec<BalanceChange> = derive_balance_changes(
                effects,
                &input_objects,
                &output_objects,
            );

            for bc in derived {
                indexed_balance_changes.push(IndexedBalanceChange {
                    tx_digest: tx_digest_vec.clone(),
                    coin_type: bc.coin_type.to_string(),
                    amount: bc.amount,
                });
            }
        }

        Ok(vec![IndexerBatch {
            checkpoint_sequence_number: seq_number as i64,
            balance_changes: indexed_balance_changes,
        }])
    }
}

#[async_trait::async_trait]
impl ConcurrentHandler for CheckpointHandler {
    type Store = Db;
    type Batch = Vec<IndexedBalanceChange>;

    const MIN_EAGER_ROWS: usize = 1000;
    const MAX_PENDING_ROWS: usize = 20_000;
    const MAX_WATERMARK_UPDATES: usize = 1000;

    fn batch(
        &self,
        batch: &mut Self::Batch,
        values: &mut std::vec::IntoIter<Self::Value>,
    ) -> BatchStatus {
        while let Some(value) = values.next() {
            batch.extend(value.balance_changes);
        }

        BatchStatus::Pending
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        conn: &mut <Self::Store as Store>::Connection<'a>,
    ) -> Result<usize> {
        if batch.is_empty() {
            return Ok(0);
        }

        let mut total_inserted = 0;
        let mut new_balance_rows = Vec::with_capacity(batch.len());

        for row in batch {
            let amount = i64::try_from(row.amount).map_err(|_| {
                anyhow!(
                    "balance change amount overflow for coin_type {}: {}",
                    row.coin_type,
                    row.amount
                )
            })?;

            new_balance_rows.push(NewBalanceChange {
                tx_digest: row.tx_digest.clone(),
                coin_type: row.coin_type.clone(),
                amount,
            });
        }

        for chunk in new_balance_rows.chunks(1000) {
            let inserted = diesel::insert_into(balance_changes_table)
                .values(chunk)
                .execute(conn)
                .await?;
            total_inserted += inserted;
        }

        Ok(total_inserted)
    }
}