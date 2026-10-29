//! Faults the integration tests inject through `FELIXD_FAULTS`, a comma
//! separated list such as `crash-before-snapshot=3,drop-input=17`:
//!
//! - `crash-before-snapshot=N`: abort the process just before its Nth snapshot.
//! - `crash-after-snapshot=N`: abort it just after its Nth snapshot is written.
//! - `skip-snapshots=K`: write none of the first K snapshots that are due, so
//!   a later owner has more than `snapshot_every` inputs to replay.
//! - `crash-after-append-sent=N`: abort once the Nth output append has been
//!   sent, before its answer is acted on. The write may have landed.
//! - `drop-input=K`: ignore the subscription's delivery of input offset `K`, as
//!   if Felix had dropped it.
//! - `freeze-before-snapshot-write=N`: stop the process with `SIGSTOP` once
//!   its Nth snapshot is taken, after the lease was checked and before it is
//!   written. It carries on at `SIGCONT`.
//! - `freeze-before-append=N`: stop the whole process with `SIGSTOP` just before
//!   its Nth output append, after the lease was checked. It carries on when
//!   the test sends `SIGCONT`, as a node out of a long GC pause would.
//! - `delay-failure-record=MS`: wait MS milliseconds between reading a failed
//!   workload's spec and writing the failure into it, as a slow broker would.
//! - `freeze-before-failure-record=1`: stop the process with `SIGSTOP` at the
//!   same point. It carries on at `SIGCONT`.
//! - `fail-workload=1`: fail every workload this node claims, as a trap would,
//!   but only on this node.
//! - `drop-watch=MS`: when the key `drop-watch` is written to the workloads
//!   cache, end the workloads watch and wait MS milliseconds before opening a
//!   new one.
//! - `slow-compile=MS`: block the thread compiling a module for MS
//!   milliseconds, as a slow machine would.
//! - `ignore-lease-deletes=1`: ignore every delete the leases watch delivers,
//!   including the broker's expiry events, so takeover has to go by the
//!   lease's expiry time alone.
//!
//! Only a build with the `faults` feature reads the variable. The felixd
//! crate turns it on for its own tests through a dev-dependency, so a binary
//! built with `cargo build` or `cargo install` ignores `FELIXD_FAULTS`.

#[cfg(feature = "faults")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

static SNAPSHOTS: AtomicU64 = AtomicU64::new(0);
static APPENDS: AtomicU64 = AtomicU64::new(0);
static SKIPPED: AtomicU64 = AtomicU64::new(0);

#[cfg(not(feature = "faults"))]
fn fault(_name: &str) -> Option<u64> {
    None
}

#[cfg(feature = "faults")]
fn fault(name: &str) -> Option<u64> {
    static FAULTS: OnceLock<Vec<(String, u64)>> = OnceLock::new();
    FAULTS
        .get_or_init(|| {
            std::env::var("FELIXD_FAULTS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|f| {
                    let (name, value) = f.trim().split_once('=')?;
                    Some((name.to_owned(), value.parse().ok()?))
                })
                .collect()
        })
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, value)| *value)
}

fn crash(name: &str) -> ! {
    tracing::error!(fault = name, "injected crash");
    std::process::abort()
}

pub(crate) fn before_snapshot() {
    let n = SNAPSHOTS.fetch_add(1, Ordering::Relaxed) + 1;
    if fault("crash-before-snapshot") == Some(n) {
        crash("crash-before-snapshot");
    }
}

pub(crate) fn before_snapshot_write() {
    if fault("freeze-before-snapshot-write") == Some(SNAPSHOTS.load(Ordering::Relaxed)) {
        freeze();
    }
}

pub(crate) fn after_snapshot() {
    if fault("crash-after-snapshot") == Some(SNAPSHOTS.load(Ordering::Relaxed)) {
        crash("crash-after-snapshot");
    }
}

/// Whether to leave out this snapshot.
pub(crate) fn skip_snapshot() -> bool {
    let skip = fault("skip-snapshots").unwrap_or(0);
    SKIPPED
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            (n < skip).then_some(n + 1)
        })
        .is_ok()
}

pub(crate) fn after_append_sent() {
    if fault("crash-after-append-sent") == Some(APPENDS.load(Ordering::Relaxed)) {
        crash("crash-after-append-sent");
    }
}

pub(crate) fn drop_input(offset: u64) -> bool {
    fault("drop-input") == Some(offset)
}

pub(crate) fn before_append() {
    let n = APPENDS.fetch_add(1, Ordering::Relaxed) + 1;
    if fault("freeze-before-append") == Some(n) {
        freeze();
    }
}

fn freeze() {
    tracing::warn!("injected freeze");
    // Stopping ourselves stops every thread at once, renewals included.
    let _ = std::process::Command::new("kill")
        .args(["-STOP", &std::process::id().to_string()])
        .status();
}

pub(crate) async fn before_failure_record() {
    if let Some(ms) = fault("delay-failure-record") {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
    if fault("freeze-before-failure-record") == Some(1) {
        freeze();
    }
}

pub(crate) fn fail_workload() -> bool {
    fault("fail-workload") == Some(1)
}

/// How long to stay without a watch, if the change to `key` should end it.
pub(crate) fn drop_watch(key: &str, written: bool) -> Option<std::time::Duration> {
    if key != "drop-watch" || !written {
        return None;
    }
    fault("drop-watch").map(std::time::Duration::from_millis)
}

pub(crate) fn ignore_lease_delete() -> bool {
    fault("ignore-lease-deletes") == Some(1)
}

pub(crate) fn during_compile() {
    if let Some(ms) = fault("slow-compile") {
        std::thread::sleep(std::time::Duration::from_millis(ms));
    }
}
