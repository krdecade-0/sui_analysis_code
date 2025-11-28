mod models;
mod handlers;

pub mod schema;

use anyhow::Result;
use clap::Parser;
use diesel_migrations::{embed_migrations, EmbeddedMigrations};
use handlers::CheckpointHandler; // updated to use combined handler
use sui_indexer_alt_framework::{
    cluster::{Args, IndexerCluster},
    pipeline::sequential::SequentialConfig,
};
use tokio;
use url::Url;

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
    let args = Args::parse();
    let start_checkpoint = args.from_checkpoint;
    let end_checkpoint = args.to_checkpoint;

    // Build and configure the indexer cluster
    let mut cluster = IndexerCluster::builder()
        .with_args(args)                    // Apply command-line configuration
        .with_database_url(database_url)    // Set up database URL
        .with_migrations(&MIGRATIONS)       // Enable automatic schema migrations
        .build()
        .await?;

    // Register our custom sequential pipeline with the cluster
    // using the new CheckpointHandler that handles all three tables
    cluster.sequential_pipeline(
        CheckpointHandler,                 // Combined handler
        SequentialConfig::default(),       // Default batch sizes and checkpoint lag
    ).await?;

    // Start the indexer and wait for completion
    let handle = cluster.run().await?;
    handle.await?;

    Ok(())
}
