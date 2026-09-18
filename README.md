# Sui Checkpoint Data Analysis

This repository contains the code and data-processing pipeline used to collect and analyse transaction and object data from the Sui blockchain. The project focuses on transaction conflicts within Sui checkpoints and examines the degree of potential parallelism available during transaction execution.

The analysis constructs transaction conflict graphs based on shared object access and evaluates their structure across sampled Sui mainnet checkpoints. It also includes analysis of transaction activity, object contention, and related on-chain metrics.

The project uses the Sui indexer framework to process checkpoint data and PostgreSQL/Diesel for data storage and querying.

## Sui Version

Sui commit:

```text
649e5e2ad4b7a48dd49fd7c0990a98f13ef5359e
```

## Dependencies

### Core Sui dependencies

```toml
sui-indexer-alt-framework = { git = "https://github.com/MystenLabs/sui.git", branch = "mainnet" }
sui-types = { git = "https://github.com/MystenLabs/sui.git", branch = "mainnet", package = "sui-types" }
```

### Rust dependencies

```toml
tokio = { version = "1.0", features = ["full"] }
anyhow = "1.0"
diesel = { version = "2.0", features = ["postgres", "r2d2"] }
diesel-async = { version = "0.5", features = ["bb8", "postgres", "async-connection-wrapper"] }
diesel_migrations = "2.0"
async-trait = "0.1"
url = "2.0"
dotenvy = "0.15"
clap = { version = "4.0", features = ["derive"] }
base64 = "0.22.1"
bcs = "0.1.6"
chrono = { version = "0.4", features = ["serde"] }
```

## Data Sources

* **Sui Mainnet:** checkpoint and transaction data collected using the Sui indexer framework
* **CoinMarketCap:** cryptocurrency price data
* **CoinMarketCap dataset snapshot:** February 2026
* **Database:** PostgreSQL 18

## Database

The collected checkpoint and transaction data is stored in PostgreSQL. Diesel and Diesel Async are used for database interaction and asynchronous indexing.

The database contains information including:

* Checkpoints
* Transactions
* Object changes
* Transaction/object relationships
* Gas usage
* Balance changes

## Analysis

The analysis focuses on transaction contention and the structure of transaction conflict graphs. Transactions that access common objects are used to identify potential conflicts, allowing the degree of concurrency available within sampled checkpoints to be examined.

The repository includes the code used for data collection, database indexing, conflict-graph construction, and subsequent analysis.

## Notes

The datasets and results in this repository correspond to the specific Sui commit and data snapshots documented above. Results may differ when using newer versions of Sui or different checkpoint and market-data snapshots.
