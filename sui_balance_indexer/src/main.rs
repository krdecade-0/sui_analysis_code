mod models;
mod handlers;
pub mod schema;

use anyhow::{bail, Result};
use clap::Parser;
use diesel_migrations::{embed_migrations, EmbeddedMigrations};
use handlers::CheckpointHandler;
use sui_indexer_alt_framework::{
    cluster::{Args as ClusterArgs, IndexerCluster},
    pipeline::concurrent::ConcurrentConfig,
    service::Error,
};
use url::Url;

#[derive(Parser, Debug)]
struct MyArgs {
    #[command(flatten)]
    cluster_args: ClusterArgs,

    #[arg(long, default_value = "0")]
    from_checkpoint: u64,

    #[arg(long, default_value_t = u64::MAX)]
    to_checkpoint: u64,
}

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("BALANCE_DB_URL")
        .expect("BALANCE_DB_URL must be set")
        .parse::<Url>()
        .expect("Invalid database URL");

    let mut args = MyArgs::parse();

    let start_checkpoint = args.from_checkpoint;
    let end_checkpoint = args.to_checkpoint;

    // Override framework checkpoint range from our custom flags.
    args.cluster_args.indexer_args.first_checkpoint = Some(start_checkpoint);
    args.cluster_args.indexer_args.last_checkpoint = if end_checkpoint == u64::MAX {
        None
    } else {
        Some(end_checkpoint)
    };

    let handler = CheckpointHandler {
        start_checkpoint,
        end_checkpoint,
    };

    let mut cluster = IndexerCluster::builder()
        .with_args(args.cluster_args)
        .with_database_url(database_url)
        .with_migrations(&MIGRATIONS)
        .build()
        .await?;

    cluster
        .concurrent_pipeline(handler, ConcurrentConfig::default())
        .await?;

    match cluster.run().await?.main().await {
        Ok(()) | Err(Error::Terminated) => Ok(()),
        Err(Error::Aborted) => {
            bail!("Indexer aborted due to an unexpected error")
        }
        Err(Error::Task(e)) => {
            bail!(e)
        }
    }
}