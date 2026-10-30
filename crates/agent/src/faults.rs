//! Faults the integration tests inject through `FELIXD_FAULTS`, a comma
//! separated list such as `crash-before-snapshot=3,drop-input=17`:
//!
//! - `crash-before-snapshot=N`: abort the process just before its Nth snapshot.
//! - `crash-after-snapshot=N`: abort it just after its Nth snapshot is written.
//! - `skip-snapshots=K`: write none of the first K snapshots that are due, so
//!   a later owner has more than `snapshot_every` inputs to replay.
//! - `crash-after-append-sent=N`: abort once the Nth output append has been
//!   sent, before its answer is acted on. The write may have landed.
//! - `append-prefix=N`: send only the first half (at least one record) of the
//!   Nth output append when it holds more than one, and answer it with an
//!   error, as when a `Quorum` leader keeps part of a batch through a
//!   failover. With `crash-after-append-sent=N` the process aborts instead.
//! - `freeze-after-append-sent=N`: stop the process with `SIGSTOP` once the
//!   Nth output append has been answered, before the answer is acted on. It
//!   carries on at `SIGCONT`.
//! - `drop-append=N`, `drop-claim=N`, `drop-failure=N`: the Nth output
//!   append, claim or failure record never reaches Felix, and the caller
//!   gets an error.
//! - `lose-append-answer=N`, `lose-claim-answer=N`, `lose-failure-answer=N`:
//!   the Nth one lands, and the caller gets an error instead of the answer.
//! - `drop-input=K`: ignore the subscription's delivery of input offset `K`, as
//!   if Felix had dropped it.
//! - `restart-input-after=K`: once input offset `K` is stepped, subscribe to
//!   the input again from offset 0, so the subscription goes below the
//!   workload's position as one on a stream that started over would.
//! - `freeze-before-snapshot-write=N`: stop the process with `SIGSTOP` once
//!   its Nth snapshot is taken, after the lease was checked and before it is
//!   written. It carries on at `SIGCONT`.
//! - `freeze-before-append=N`: stop the whole process with `SIGSTOP` just before
//!   its Nth output append, after the lease was checked. It carries on when
//!   the test sends `SIGCONT`, as a node out of a long GC pause would.
//! - `freeze-before-merge=N`: stop the process with `SIGSTOP` just before its
//!   Nth merge log append, after the lease was checked. It carries on at
//!   `SIGCONT`.
//! - `crash-after-merge=N`: abort once the Nth merge log append has landed,
//!   before any of its entries is stepped.
//! - `freeze-before-merge-claim=N`: stop the process with `SIGSTOP` just
//!   before its Nth merge log claim is sent, after the tail was read. It
//!   carries on at `SIGCONT`.
//! - `drop-merge=N`, `drop-merge-claim=N`, `lose-merge-answer=N`,
//!   `lose-merge-claim-answer=N`: as for output appends and claims, for the
//!   Nth merge log append or claim.
//! - `merge-write-limit=K`: refuse, with an error, any merge log write of
//!   more than K records (0 counts as 1), as Felix refuses a write too large for a frame, and
//!   write at most K entries at a time.
//! - `freeze-before-claim=N`: stop the process with `SIGSTOP` just before its
//!   Nth claim append, after the lease was checked. It carries on at `SIGCONT`.
//! - `delay-failure-record=MS`: wait MS milliseconds between reading a failed
//!   workload's spec and confirming the lease to record the failure, or that
//!   it needs an operator, as a slow broker would.
//! - `freeze-before-failure-record=N`: stop the process with `SIGSTOP` at the
//!   same point, the Nth time it is reached. It carries on at `SIGCONT`.
//! - `freeze-before-failure-append=1`: stop the process with `SIGSTOP` after
//!   the lease was confirmed and the output tail read, just before the failure
//!   record is appended. It carries on at `SIGCONT`.
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
//! - `miss-drain-marks=1`: the agent's regular read of its drain mark never
//!   finds one, as when that read has not come round yet, so only the read
//!   before each claim sees it.
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
static CLAIMS: AtomicU64 = AtomicU64::new(0);
static FAILURES: AtomicU64 = AtomicU64::new(0);
static FAILURE_RECORDS: AtomicU64 = AtomicU64::new(0);
static MERGES: AtomicU64 = AtomicU64::new(0);
static MERGE_CLAIMS: AtomicU64 = AtomicU64::new(0);

/// A conditional write the answer faults count.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Write {
    Append,
    Claim,
    Failure,
    Merge,
    MergeClaim,
}

/// What the answer faults do to the current write of a kind.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Answer {
    Sent,
    /// The request never reaches Felix.
    Dropped,
    /// The request lands and its answer is lost.
    Lost,
}

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
    let n = APPENDS.load(Ordering::Relaxed);
    if fault("crash-after-append-sent") == Some(n) {
        crash("crash-after-append-sent");
    }
    if fault("freeze-after-append-sent") == Some(n) {
        freeze();
    }
}

/// What happens to the current `write`, counted by the `before_*` hook
/// called just before it.
pub(crate) fn answer(write: Write) -> Answer {
    let (n, dropped, lost) = match write {
        Write::Append => (&APPENDS, "drop-append", "lose-append-answer"),
        Write::Claim => (&CLAIMS, "drop-claim", "lose-claim-answer"),
        Write::Failure => (&FAILURES, "drop-failure", "lose-failure-answer"),
        Write::Merge => (&MERGES, "drop-merge", "lose-merge-answer"),
        Write::MergeClaim => (&MERGE_CLAIMS, "drop-merge-claim", "lose-merge-claim-answer"),
    };
    let n = n.load(Ordering::Relaxed);
    if fault(dropped) == Some(n) {
        tracing::warn!(?write, "injected dropped request");
        Answer::Dropped
    } else if fault(lost) == Some(n) {
        tracing::warn!(?write, "injected lost answer");
        Answer::Lost
    } else {
        Answer::Sent
    }
}

/// The part of `batch` the Nth append sends, when `append-prefix=N` cuts it.
pub(crate) fn append_prefix(batch: &[Vec<u8>]) -> Option<Vec<Vec<u8>>> {
    if batch.len() < 2 || fault("append-prefix") != Some(APPENDS.load(Ordering::Relaxed)) {
        return None;
    }
    tracing::warn!("injected partial append");
    Some(batch[..batch.len() / 2].to_vec())
}

pub(crate) fn drop_input(offset: u64) -> bool {
    fault("drop-input") == Some(offset)
}

pub(crate) fn restart_input_after(offset: u64) -> bool {
    fault("restart-input-after") == Some(offset)
}

pub(crate) fn before_append() {
    let n = APPENDS.fetch_add(1, Ordering::Relaxed) + 1;
    if fault("freeze-before-append") == Some(n) {
        freeze();
    }
}

pub(crate) fn before_merge() {
    let n = MERGES.fetch_add(1, Ordering::Relaxed) + 1;
    if fault("freeze-before-merge") == Some(n) {
        freeze();
    }
}

pub(crate) fn before_merge_claim() {
    let n = MERGE_CLAIMS.fetch_add(1, Ordering::Relaxed) + 1;
    if fault("freeze-before-merge-claim") == Some(n) {
        freeze();
    }
}

/// The most records one merge log write may hold, under `merge-write-limit`.
pub(crate) fn merge_write_limit() -> Option<usize> {
    // Every write holds at least one record, so 0 means 1.
    fault("merge-write-limit").map(|k| usize::try_from(k).unwrap_or(usize::MAX).max(1))
}

pub(crate) fn after_merge() {
    if fault("crash-after-merge") == Some(MERGES.load(Ordering::Relaxed)) {
        crash("crash-after-merge");
    }
}

pub(crate) fn before_claim() {
    let n = CLAIMS.fetch_add(1, Ordering::Relaxed) + 1;
    if fault("freeze-before-claim") == Some(n) {
        freeze();
    }
}

/// Stops the whole process, renewals included, and returns once it is
/// continued.
fn freeze() {
    tracing::warn!("injected freeze");
    until_continued(|| {
        let _ = std::process::Command::new("kill")
            .args(["-STOP", &std::process::id().to_string()])
            .status();
    });
}

/// Runs `stop`, then blocks until `SIGCONT`. `kill -STOP` is aimed at the
/// process, so the kernel may hand it to another thread, and this one can
/// return from `stop` and run on until that thread stops the group. That is
/// long enough to send the write the freeze was meant to hold back. It
/// blocks rather than awaits so the task cannot be dropped in between, as by
/// a select on the renewals, and the frozen write is still sent on waking.
fn until_continued(stop: impl FnOnce()) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut continued = signal(SignalKind::from_raw(libc::SIGCONT)).expect("listen for SIGCONT");
    stop();
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(continued.recv());
    });
}

pub(crate) async fn before_failure_record() {
    if let Some(ms) = fault("delay-failure-record") {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
    let n = FAILURE_RECORDS.fetch_add(1, Ordering::Relaxed) + 1;
    if fault("freeze-before-failure-record") == Some(n) {
        freeze();
    }
}

pub(crate) fn before_failure_append() {
    FAILURES.fetch_add(1, Ordering::Relaxed);
    if fault("freeze-before-failure-append") == Some(1) {
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

pub(crate) fn miss_drain_marks() -> bool {
    fault("miss-drain-marks") == Some(1)
}

pub(crate) fn during_compile() {
    if let Some(ms) = fault("slow-compile") {
        std::thread::sleep(std::time::Duration::from_millis(ms));
    }
}

#[cfg(test)]
mod tests;
