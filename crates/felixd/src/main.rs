//! The felixd command line.

mod connection;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use connection::Connection;
use felixd_agent::submit::{Submission, submit};
use felixd_runtime::Limits;

/// Runs deterministic WebAssembly workloads on a Felix log.
#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run every workload in the cluster on this node.
    Agent {
        #[command(flatten)]
        connection: Connection,
        /// Fuel given to each call into a workload.
        #[arg(long, default_value_t = Limits::default().fuel_per_call)]
        fuel: u64,
        /// The most memory a workload may use, in bytes.
        #[arg(long, default_value_t = Limits::default().max_memory_bytes)]
        max_memory: usize,
    },
    /// Submit a workload: store its module, create its streams and start it.
    Run {
        #[command(flatten)]
        connection: Connection,
        /// The workload's name, used in its stream names.
        #[arg(long)]
        id: String,
        /// The stream the workload reads. It must be a durable stream.
        #[arg(long)]
        input: String,
        /// Write a snapshot after this many inputs.
        #[arg(long, default_value_t = felixd_core::spec::DEFAULT_SNAPSHOT_EVERY)]
        snapshot_every: u64,
        /// Replicas of the workload's output and snapshot streams. More than
        /// one makes them quorum streams.
        #[arg(long, default_value_t = 1)]
        replication_factor: u32,
        /// The workload, a WebAssembly component.
        module: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "felixd=info,felixd_agent=info".into()),
        )
        .init();
    match Cli::parse().command {
        Command::Agent {
            connection,
            fuel,
            max_memory,
        } => {
            let limits = Limits {
                fuel_per_call: fuel,
                max_memory_bytes: max_memory,
            };
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            felixd_agent::agent::run(&connection.settings()?, limits, shutdown).await
        }
        Command::Run {
            connection,
            id,
            input,
            snapshot_every,
            replication_factor,
            module,
        } => {
            let bytes =
                std::fs::read(&module).with_context(|| format!("read {}", module.display()))?;
            let mut submission = Submission::new(&id, bytes, &input);
            submission.snapshot_every = snapshot_every;
            submission.replication_factor = replication_factor;
            let hash = submit(&connection.settings()?, submission).await?;
            println!("workload {id} running module {hash}");
            Ok(())
        }
    }
}
