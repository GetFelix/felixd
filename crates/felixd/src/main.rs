//! The felixd command line.

mod connection;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use connection::Connection;
use felixd_agent::agent::AgentConfig;
use felixd_agent::check::check;
use felixd_agent::submit::{Submission, submit};
use felixd_core::lease::LeaseTiming;
use felixd_core::placement::{self, FirstClaimWins};
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
    /// Run workloads on this node: claim the lease of each one that is free
    /// and run it while the lease holds.
    Agent {
        #[command(flatten)]
        connection: Connection,
        /// This node's name in the leases it holds. Defaults to the host name
        /// and process id, so two agents on one host differ.
        #[arg(long, env = "FELIXD_NODE_ID")]
        node_id: Option<String>,
        /// How long a lease lives without renewal, in milliseconds, at least
        /// 2000. Every node in a cluster should use the same value.
        #[arg(
            long,
            default_value_t = felixd_core::lease::DEFAULT_TTL.as_millis() as u64,
            value_parser = clap::value_parser!(u64).range(felixd_core::lease::MIN_TTL.as_millis() as u64..),
        )]
        lease_ttl_ms: u64,
        /// The most workloads this node holds at once.
        #[arg(long, default_value_t = placement::DEFAULT_MAX_WORKLOADS)]
        max_workloads: usize,
        /// The longest random wait, in milliseconds, before claiming a free
        /// workload, so that the first node to see it does not take everything.
        #[arg(long, default_value_t = placement::DEFAULT_CLAIM_DELAY.as_millis() as u64)]
        claim_delay_ms: u64,
    },
    /// Check output streams: replay each workload's module over its input and
    /// report gaps, duplicates, records out of order, epoch regressions and
    /// outputs that differ. Exits with 1 if anything is found.
    Check {
        #[command(flatten)]
        connection: Connection,
        /// The workload to check. Repeat for several; every workload when
        /// left out.
        #[arg(long = "id")]
        ids: Vec<String>,
        /// Also report outputs missing from the end of a stream, for when the
        /// workloads should have caught up with their input.
        #[arg(long)]
        complete: bool,
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
        /// Also write a snapshot at the first input of every interval this
        /// many milliseconds long, by the inputs' append times.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        snapshot_every_ms: Option<u64>,
        /// Fuel given to each call into the workload. It is stored in the
        /// spec, so every node runs the workload under the same limit.
        #[arg(long, default_value_t = Limits::default().fuel_per_call)]
        fuel: u64,
        /// The most memory the workload may use, in bytes. Stored in the spec.
        #[arg(long, default_value_t = Limits::default().max_memory_bytes)]
        max_memory: u64,
        /// Replicas of felixd's caches and the workload's output and snapshot
        /// streams. More than one makes them `Quorum`. Use 1 against a single
        /// broker. Caches that already exist keep their replicas.
        #[arg(long, default_value_t = felixd_agent::submit::DEFAULT_REPLICATION_FACTOR)]
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
            node_id,
            lease_ttl_ms,
            max_workloads,
            claim_delay_ms,
        } => {
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let config = AgentConfig {
                node_id: node_id.unwrap_or_else(default_node_id),
                lease: LeaseTiming {
                    ttl: Duration::from_millis(lease_ttl_ms),
                },
                placement: FirstClaimWins {
                    max_workloads,
                    max_delay: Duration::from_millis(claim_delay_ms),
                },
            };
            felixd_agent::agent::run(&connection.settings()?, config, shutdown).await
        }
        Command::Check {
            connection,
            ids,
            complete,
        } => {
            let reports = check(&connection.settings()?, &ids, complete).await?;
            let mut found = false;
            for report in &reports {
                if report.findings.is_empty() {
                    println!("{}: {} records, ok", report.output, report.records);
                }
                for finding in &report.findings {
                    println!("{}: {finding}", report.output);
                    found = true;
                }
            }
            if found {
                std::process::exit(1);
            }
            Ok(())
        }
        Command::Run {
            connection,
            id,
            input,
            snapshot_every,
            snapshot_every_ms,
            fuel,
            max_memory,
            replication_factor,
            module,
        } => {
            let bytes =
                std::fs::read(&module).with_context(|| format!("read {}", module.display()))?;
            let mut submission = Submission::new(&id, bytes, &input);
            submission.snapshot_every = snapshot_every;
            submission.snapshot_every_ms = snapshot_every_ms;
            submission.limits = Limits {
                fuel_per_call: fuel,
                max_memory_bytes: max_memory,
            };
            submission.replication_factor = replication_factor;
            let hash = submit(&connection.settings()?, submission).await?;
            println!("workload {id} running module {hash}");
            Ok(())
        }
    }
}

fn default_node_id() -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "node".to_owned());
    format!("{host}-{}", std::process::id())
}

#[cfg(test)]
mod tests;
