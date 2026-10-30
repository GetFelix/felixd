//! The felixd command line.

mod connection;
mod ps;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use connection::Connection;
use felixd_agent::agent::AgentConfig;
use felixd_agent::check::check;
use felixd_agent::drain;
use felixd_agent::replay::{Target, diff, replay};
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
        /// Serve Prometheus metrics at `/metrics` on this address, such as
        /// 127.0.0.1:9464. Off when left out. Plain HTTP with no
        /// authentication: it names workloads and streams, so keep it on an
        /// internal network.
        #[arg(long, env = "FELIXD_METRICS_BIND")]
        metrics_bind: Option<std::net::SocketAddr>,
        /// The most workloads that get metric series of their own. Those past
        /// it are reported together as `workload="_other"`.
        #[arg(
            long,
            default_value_t = felixd_agent::metrics::DEFAULT_MAX_WORKLOADS,
            requires = "metrics_bind"
        )]
        metrics_max_workloads: usize,
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
        /// Also check every workload whose output stream a checked one
        /// reads, up the whole pipeline. Stages are checked upstream first.
        #[arg(long)]
        upstream: bool,
    },
    /// Rebuild a workload's state as of an input offset or a time and print
    /// what its `inspect` returns, or with `--diff` compare the outputs a
    /// replay produces with the recorded ones. Writes nothing.
    Replay {
        #[command(flatten)]
        connection: Connection,
        /// The workload to replay.
        #[arg(long)]
        id: String,
        /// An input offset, for the state after that input, or an RFC 3339
        /// time such as 2026-10-29T14:02:00Z, for the state after every input
        /// appended before it. For a workload with several inputs, an offset on
        /// its merge log, or a time compared with when entries were merged. A
        /// diff runs to the last input by default.
        #[arg(long, value_parser = parse_target, required_unless_present = "diff")]
        to: Option<Target>,
        /// Compare each output the replay produces with the one on the output
        /// stream and report the first divergence. Exits with 1 if it finds
        /// one.
        #[arg(long)]
        diff: bool,
        /// The first input a diff compares.
        #[arg(long, default_value_t = 0, requires = "diff")]
        from: u64,
        /// Run this module, the hash of one in the `modules` cache, over the
        /// recorded inputs instead of the workload's own, from its initial
        /// state. With `--diff` this checks an upgrade.
        #[arg(long)]
        module: Option<String>,
    },
    /// Show each workload's state, holder, epoch, output stream tail and how
    /// far it is along each input, and each node: whether it is up, the
    /// leases it holds and whether it is draining. Reads the caches and
    /// streams only, so it needs read grants and writes nothing.
    Ps {
        #[command(flatten)]
        connection: Connection,
        /// The workload to show. Repeat for several; every workload when
        /// left out.
        #[arg(long = "id")]
        ids: Vec<String>,
    },
    /// Mark a node as draining. It stops each workload it holds and releases
    /// the lease, so another node takes it over without waiting for the
    /// lease to expire, and claims nothing until `felixd undrain`.
    Drain {
        #[command(flatten)]
        connection: Connection,
        /// The node, by the id it runs with (`felixd agent --node-id`).
        node: String,
        /// Wait until no live lease names the node and it has said it will
        /// claim nothing, or it is not up.
        #[arg(long)]
        wait: bool,
        /// With `--wait`, give up after this many seconds and exit with 1.
        #[arg(long, default_value_t = 300, requires = "wait")]
        timeout_secs: u64,
        /// Replicas of the `drains` cache, if this creates it.
        #[arg(long, default_value_t = felixd_agent::submit::DEFAULT_REPLICATION_FACTOR)]
        replication_factor: u32,
    },
    /// Lift a node's drain mark, so it claims work again.
    Undrain {
        #[command(flatten)]
        connection: Connection,
        /// The node, by the id it runs with.
        node: String,
    },
    /// Submit a workload: store its module, create its streams and start it.
    Run {
        #[command(flatten)]
        connection: Connection,
        /// The workload's name, used in its stream names.
        #[arg(long)]
        id: String,
        /// The stream the workload reads. It must be a durable stream. Repeat
        /// for several: their records are merged in the order the lease
        /// holder records on the workload's merge log.
        #[arg(long = "input", required = true)]
        inputs: Vec<String>,
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
        /// Fuel for a whole snapshot or restore, every chunk call together.
        /// Stored in the spec.
        #[arg(long, default_value_t = Limits::default().snapshot_fuel)]
        snapshot_fuel: u64,
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
            metrics_bind,
            metrics_max_workloads,
        } => {
            let node_id = node_id.unwrap_or_else(default_node_id);
            if let Some(bind) = metrics_bind {
                felixd_agent::metrics::start(bind, &node_id, metrics_max_workloads).await?;
            }
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let config = AgentConfig {
                node_id,
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
            upstream,
        } => {
            let reports = check(&connection.settings()?, &ids, complete, upstream).await?;
            let mut found = false;
            for report in &reports {
                if report.findings.is_empty() {
                    println!(
                        "{}: {} records, ok, from {}",
                        report.output,
                        report.records,
                        report.inputs.join(", ")
                    );
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
        Command::Ps { connection, ids } => {
            let report = felixd_agent::ps::ps(&connection.settings()?, &ids).await?;
            print!("{}", ps::render(&report));
            Ok(())
        }
        Command::Drain {
            connection,
            node,
            wait,
            timeout_secs,
            replication_factor,
        } => {
            let settings = connection.settings()?;
            drain::mark(&settings, &node, replication_factor).await?;
            println!("node {node} is draining");
            let report = felixd_agent::ps::ps(&settings, &[]).await?;
            if !report
                .nodes
                .iter()
                .any(|n| n.id != node && n.up && !n.draining)
            {
                eprintln!(
                    "no other node is up and not draining, so its workloads wait until one is"
                );
            }
            if !wait {
                return Ok(());
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
            let felix = felixd_agent::felix::Felix::connect(&settings).await?;
            let mut last = None;
            loop {
                // Each read is bounded so the timeout holds; a failed one is
                // tried again until then.
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                match tokio::time::timeout(left, drain::progress(&felix, &node)).await {
                    Ok(Ok(progress)) if progress.done() => {
                        println!("node {node} holds no leases and claims nothing");
                        return Ok(());
                    }
                    Ok(Ok(progress)) => last = Some(progress),
                    Ok(Err(error)) => eprintln!("reading the drain's progress: {error:#}"),
                    Err(_) => {}
                }
                if tokio::time::Instant::now() >= deadline {
                    eprintln!("node {node} has not finished draining after {timeout_secs} s");
                    if let Some(progress) = last {
                        if !progress.held.is_empty() {
                            eprintln!("it still holds {}", progress.held.join(", "));
                        }
                        if !progress.acknowledged {
                            eprintln!(
                                "it has not said it read this drain mark; a node of an older \
                                 build, or one without read on the drains cache, never does"
                            );
                        }
                    }
                    std::process::exit(1);
                }
                tokio::time::sleep(Duration::from_millis(500).min(left)).await;
            }
        }
        Command::Undrain { connection, node } => {
            if drain::lift(&connection.settings()?, &node).await? {
                println!("node {node} is no longer draining");
            } else {
                println!("node {node} was not draining");
            }
            Ok(())
        }
        Command::Replay {
            connection,
            id,
            to,
            diff: false,
            module,
            ..
        } => {
            let to = to.context("--to is required without --diff")?;
            let replayed = replay(&connection.settings()?, &id, to, module.as_deref()).await?;
            let from = match replayed.restored {
                Some(offset) => format!("the snapshot of input {offset}"),
                None => "the initial state".to_owned(),
            };
            eprintln!(
                "from {from}, ran {} more of the inputs below {}",
                replayed.steps, replayed.end
            );
            println!("{}", replayed.state);
            Ok(())
        }
        Command::Replay {
            connection,
            id,
            to,
            from,
            module,
            ..
        } => {
            let diffed = diff(&connection.settings()?, &id, from, to, module.as_deref()).await?;
            if let Some(divergence) = diffed.divergence {
                println!("first divergence at {divergence}");
                std::process::exit(1);
            }
            println!(
                "inputs from {} below {}: {} outputs match",
                diffed.from, diffed.end, diffed.matched
            );
            Ok(())
        }
        Command::Run {
            connection,
            id,
            inputs,
            snapshot_every,
            snapshot_every_ms,
            fuel,
            max_memory,
            snapshot_fuel,
            replication_factor,
            module,
        } => {
            let bytes =
                std::fs::read(&module).with_context(|| format!("read {}", module.display()))?;
            let mut submission = Submission::new(&id, bytes, &inputs[0]);
            submission.inputs = inputs;
            submission.snapshot_every = snapshot_every;
            submission.snapshot_every_ms = snapshot_every_ms;
            submission.limits = Limits {
                fuel_per_call: fuel,
                max_memory_bytes: max_memory,
                snapshot_fuel,
            };
            submission.replication_factor = replication_factor;
            let hash = submit(&connection.settings()?, submission).await?;
            println!("workload {id} running module {hash}");
            Ok(())
        }
    }
}

fn parse_target(to: &str) -> Result<Target, String> {
    if let Ok(offset) = to.parse() {
        return Ok(Target::Offset(offset));
    }
    let at = time::OffsetDateTime::parse(to, &time::format_description::well_known::Rfc3339)
        .map_err(|e| format!("neither an offset nor an RFC 3339 time: {e}"))?;
    let micros = at.unix_timestamp_nanos() / 1000;
    u64::try_from(micros)
        .map(Target::Time)
        .map_err(|_| "the time is before 1970".to_owned())
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
