mod models;
mod handlers;

pub mod schema;

use anyhow::Result;
use clap::Parser;
use diesel_migrations::{embed_migrations, EmbeddedMigrations};
use handlers::CheckpointHandler; // updated to use combined handler
use sui_indexer_alt_framework::{
    cluster::{Args as ClusterArgs, IndexerCluster},
    pipeline::concurrent::ConcurrentConfig,
};
use sui_indexer_alt_framework::ingestion::ClientArgs;
use sui_indexer_alt_framework::IndexerArgs;

use tokio;
use url::Url;

#[derive(Parser)]
struct MyArgs {
    #[arg(long)]
    remote_store_url: String,

    #[arg(long, default_value = "0")]
    from_checkpoint: u64,

    #[arg(long, default_value_t = u64::MAX)]
    to_checkpoint: u64,
}

// Embed database migrations into the binary so they run automatically on startup
const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env data
    dotenvy::dotenv().ok();

    // Get local database URL from environment variable
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in the environment")
        .parse::<Url>()
        .expect("Invalid database URL");

    // Parse command-line arguments (checkpoint range, URLs, performance settings)
    let args = MyArgs::parse();
    let start_checkpoint = args.from_checkpoint;
    let end_checkpoint = args.to_checkpoint;

    let handler = CheckpointHandler {
        start_checkpoint,
        end_checkpoint,
    };

    // Map your custom args to the framework's Args struct
    let framework_args = ClusterArgs {
        indexer_args: IndexerArgs {
            first_checkpoint: Some(start_checkpoint),
            last_checkpoint: Some(end_checkpoint),
            ..Default::default()
        },
        client_args: Some(ClientArgs {
            remote_store_url: Some(args.remote_store_url.parse::<Url>()?),
            local_ingestion_path: None,
            rpc_api_url: None,
            rpc_username: None,
            rpc_password: None,
        }),
        ..Default::default()
    };

    // Build and configure the indexer cluster
    let mut cluster = IndexerCluster::builder()
        .with_args(framework_args)                    // Apply command-line configuration
        .with_database_url(database_url)    // Set up database URL
        .with_migrations(&MIGRATIONS)       // Enable automatic schema migrations
        .build()
        .await?;

    // Register our custom sequential pipeline with the cluster
    // using the new CheckpointHandler that handles all three tables
    cluster.concurrent_pipeline(
        handler,                            // Combined handler
        ConcurrentConfig::default(),
    ).await?;

    // Start the indexer and wait for completion
    let handle = cluster.run().await?;
    handle.await?;

    Ok(())
}
