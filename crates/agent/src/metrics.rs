//! Prometheus metrics for one node, off unless `felixd agent --metrics-bind`
//! is given.
//!
//! Counters and gauges are atomics read when `/metrics` is scraped, and
//! nothing here is read back by the node, so it cannot change what a node
//! does. With metrics off, every call below is one failed `OnceLock` read.
//!
//! The series are per node: each node reports what it holds and does, and
//! Prometheus adds the instance. Series labelled by workload are bounded by
//! `--metrics-max-workloads` (default [`DEFAULT_MAX_WORKLOADS`]). A node that
//! meets more workloads than that reports the rest together as
//! `workload="_other"`, without input lag or snapshot size. A workload that
//! is no longer held is dropped to make room for a new one. Everything else has a fixed label
//! set. See docs/metrics.md.

mod render;
mod serve;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use felixd_core::class::OperatorReason;

pub use serve::start;

/// The most workloads that get series of their own unless told otherwise.
pub const DEFAULT_MAX_WORKLOADS: usize = 100;

/// The workload label of everything past the limit.
pub(crate) const OTHER: &str = "_other";

/// Unknown tail of an input, before the first sample.
const UNKNOWN: u64 = u64::MAX;

/// How long a workload's input tails go between reads, so lag is at most
/// this stale when an input is quiet.
pub(crate) const SAMPLE_TAILS_EVERY: Duration = Duration::from_secs(5);

/// Failed tail reads in a row after which an input's lag is dropped.
pub(crate) const FORGET_TAIL_AFTER: u32 = 3;

/// `SAMPLE_TAILS_EVERY` plus up to a second, so the workloads of a node do
/// not all read at once.
pub(crate) fn sample_wait() -> Duration {
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_millis());
    SAMPLE_TAILS_EVERY + Duration::from_millis(u64::from(jitter))
}

/// Step latencies, in seconds.
const STEP_BUCKETS: &[f64] = &[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];

/// Snapshot times, in seconds.
const SNAPSHOT_BUCKETS: &[f64] = &[0.005, 0.025, 0.1, 0.5, 1.0, 5.0, 30.0, 120.0];

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Every operator reason, in the order its series are written.
const REASONS: [OperatorReason; 10] = [
    OperatorReason::ModuleRefused,
    OperatorReason::ModuleMissing,
    OperatorReason::SpecUnrecorded,
    OperatorReason::SpecConflict,
    OperatorReason::SpecStale,
    OperatorReason::RestoreFuel,
    OperatorReason::InputUnreadable,
    OperatorReason::InputBehind,
    OperatorReason::HistoryTrimmed,
    OperatorReason::SpecUnadmitted,
];

fn reason_label(reason: OperatorReason) -> &'static str {
    match reason {
        OperatorReason::ModuleRefused => "module_refused",
        OperatorReason::ModuleMissing => "module_missing",
        OperatorReason::SpecUnrecorded => "spec_unrecorded",
        OperatorReason::SpecConflict => "spec_conflict",
        OperatorReason::SpecStale => "spec_stale",
        OperatorReason::RestoreFuel => "restore_fuel",
        OperatorReason::InputUnreadable => "input_unreadable",
        OperatorReason::InputBehind => "input_behind",
        OperatorReason::HistoryTrimmed => "history_trimmed",
        OperatorReason::SpecUnadmitted => "spec_unadmitted",
    }
}

/// How a lease claim ended.
#[derive(Clone, Copy)]
pub(crate) enum Claim {
    Won,
    /// Another node's lease was there.
    Contended,
    Error,
}

/// Why a node let go of a lease it held, with another node expected to take
/// the workload over.
#[derive(Clone, Copy)]
pub(crate) enum Handoff {
    /// The node was drained.
    Drained,
    /// The lease was lost: not renewed in time, or taken.
    Lost,
}

/// A histogram with fixed buckets.
pub(crate) struct Histogram {
    bounds: &'static [f64],
    /// Observations in each bucket, the last for those above every bound.
    buckets: Vec<AtomicU64>,
    sum_nanos: AtomicU64,
}

impl Histogram {
    fn new(bounds: &'static [f64]) -> Histogram {
        Histogram {
            bounds,
            buckets: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum_nanos: AtomicU64::new(0),
        }
    }

    pub(crate) fn observe(&self, took: Duration) {
        let seconds = took.as_secs_f64();
        let at = self.bounds.partition_point(|&b| b < seconds);
        self.buckets[at].fetch_add(1, Ordering::Relaxed);
        let nanos = u64::try_from(took.as_nanos()).unwrap_or(u64::MAX);
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
    }
}

/// One input of a workload this node has run.
pub(crate) struct InputMetrics {
    pub(crate) stream: String,
    /// The next offset the workload steps on this input.
    next: AtomicU64,
    /// The input's tail as last read.
    tail: AtomicU64,
}

impl InputMetrics {
    fn lag(&self) -> Option<u64> {
        match self.tail.load(Ordering::Relaxed) {
            UNKNOWN => None,
            tail => Some(tail.saturating_sub(self.next.load(Ordering::Relaxed))),
        }
    }
}

/// What this node counts for one workload.
pub(crate) struct WorkloadMetrics {
    held: AtomicI64,
    steps: AtomicU64,
    step_seconds: Histogram,
    output_records: AtomicU64,
    /// Refused appends: reconciled, then fenced.
    output_refused: [AtomicU64; 2],
    merge_entries: AtomicU64,
    restarts: AtomicU64,
    snapshot_bytes: AtomicU64,
    inputs: Mutex<Vec<Arc<InputMetrics>>>,
}

impl WorkloadMetrics {
    fn new() -> WorkloadMetrics {
        WorkloadMetrics {
            held: AtomicI64::new(0),
            steps: AtomicU64::new(0),
            step_seconds: Histogram::new(STEP_BUCKETS),
            output_records: AtomicU64::new(0),
            output_refused: Default::default(),
            merge_entries: AtomicU64::new(0),
            restarts: AtomicU64::new(0),
            snapshot_bytes: AtomicU64::new(0),
            inputs: Mutex::new(Vec::new()),
        }
    }

    /// Names the inputs a run reads, in the spec's order. The workload past
    /// the label limit has none.
    pub(crate) fn set_inputs(&self, streams: &[String], shared: bool) {
        let inputs: Vec<Arc<InputMetrics>> = if shared {
            Vec::new()
        } else {
            streams
                .iter()
                .map(|stream| {
                    Arc::new(InputMetrics {
                        stream: stream.clone(),
                        next: AtomicU64::new(0),
                        tail: AtomicU64::new(UNKNOWN),
                    })
                })
                .collect()
        };
        *self.inputs.lock().unwrap_or_else(|e| e.into_inner()) = inputs;
    }

    pub(crate) fn inputs(&self) -> Vec<Arc<InputMetrics>> {
        self.inputs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub(crate) fn step(&self, took: Duration) {
        self.steps.fetch_add(1, Ordering::Relaxed);
        self.step_seconds.observe(took);
    }

    pub(crate) fn output_written(&self, records: usize) {
        self.output_records
            .fetch_add(records as u64, Ordering::Relaxed);
    }

    /// An append the output stream refused because its tail had moved,
    /// and whether what was found there fenced this holder.
    pub(crate) fn output_refused(&self, fenced: bool) {
        self.output_refused[usize::from(fenced)].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn merged(&self, entries: usize) {
        self.merge_entries
            .fetch_add(entries as u64, Ordering::Relaxed);
    }

    pub(crate) fn restarted(&self) {
        self.restarts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot_size(&self, bytes: u64) {
        self.snapshot_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Where `input` is, as the offset it steps next.
    pub(crate) fn position(inputs: &[Arc<InputMetrics>], input: usize, next: u64) {
        if let Some(input) = inputs.get(input) {
            input.next.store(next, Ordering::Relaxed);
            // A tail below what was stepped is stale. An unknown one stays
            // unknown, as it is the largest value.
            input.tail.fetch_max(next, Ordering::Relaxed);
        }
    }

    /// Records the tail read for `input`. Tails do not shrink, so a lower
    /// one is a stale read.
    pub(crate) fn tail(inputs: &[Arc<InputMetrics>], input: usize, tail: u64) {
        if let Some(input) = inputs.get(input) {
            let _ = input
                .tail
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |now| {
                    Some(if now == UNKNOWN { tail } else { now.max(tail) })
                });
        }
    }

    /// Drops the lag of `input` until a tail is read again.
    pub(crate) fn forget_tail(inputs: &[Arc<InputMetrics>], input: usize) {
        if let Some(input) = inputs.get(input) {
            input.tail.store(UNKNOWN, Ordering::Relaxed);
        }
    }
}

/// Everything one node reports.
pub(crate) struct Metrics {
    node: String,
    max_workloads: usize,
    draining: AtomicBool,
    leases_held: AtomicI64,
    claims: [AtomicU64; 3],
    handoffs: [AtomicU64; 2],
    needs_operator: [AtomicU64; REASONS.len()],
    failures: AtomicU64,
    snapshots_written: AtomicU64,
    snapshots_skipped: AtomicU64,
    snapshot_seconds: Histogram,
    workloads: Mutex<BTreeMap<String, Arc<WorkloadMetrics>>>,
}

/// Turns metrics on for this process, for the node `node`. Does nothing if
/// they are on already.
pub(crate) fn enable(node: &str, max_workloads: usize) -> &'static Metrics {
    METRICS.get_or_init(|| Metrics {
        node: node.to_owned(),
        max_workloads,
        draining: AtomicBool::new(false),
        leases_held: AtomicI64::new(0),
        claims: Default::default(),
        handoffs: Default::default(),
        needs_operator: Default::default(),
        failures: AtomicU64::new(0),
        snapshots_written: AtomicU64::new(0),
        snapshots_skipped: AtomicU64::new(0),
        snapshot_seconds: Histogram::new(SNAPSHOT_BUCKETS),
        workloads: Mutex::new(BTreeMap::new()),
    })
}

fn get() -> Option<&'static Metrics> {
    METRICS.get()
}

/// The counters for `id`, or `None` with metrics off. The workloads past the
/// limit share one entry.
pub(crate) fn workload(id: &str) -> Option<(Arc<WorkloadMetrics>, bool)> {
    get().map(|m| m.workload(id))
}

pub(crate) fn claim(result: Claim) {
    if let Some(m) = get() {
        m.claims[result as usize].fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn handoff(reason: Handoff) {
    if let Some(m) = get() {
        m.handoffs[reason as usize].fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn needs_operator(reason: OperatorReason) {
    if let Some(m) = get() {
        let at = REASONS.iter().position(|&r| r == reason).unwrap_or(0);
        m.needs_operator[at].fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn failed() {
    if let Some(m) = get() {
        m.failures.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn draining(draining: bool) {
    if let Some(m) = get() {
        m.draining.store(draining, Ordering::Relaxed);
    }
}

/// A snapshot written, with its size and how long it took, or abandoned.
pub(crate) fn snapshot(written: Option<Duration>) {
    if let Some(m) = get() {
        match written {
            Some(took) => {
                m.snapshots_written.fetch_add(1, Ordering::Relaxed);
                m.snapshot_seconds.observe(took);
            }
            None => {
                m.snapshots_skipped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Counts a lease held until dropped.
pub(crate) struct Holding(Option<Arc<WorkloadMetrics>>);

impl Holding {
    pub(crate) fn start(workload: &Option<(Arc<WorkloadMetrics>, bool)>) -> Holding {
        if let Some(m) = get() {
            m.leases_held.fetch_add(1, Ordering::Relaxed);
        }
        let workload = workload.as_ref().map(|(w, _)| w.clone());
        if let Some(w) = &workload {
            w.held.fetch_add(1, Ordering::Relaxed);
        }
        Holding(workload)
    }
}

impl Drop for Holding {
    fn drop(&mut self) {
        if let Some(m) = get() {
            m.leases_held.fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(w) = &self.0 {
            w.held.fetch_sub(1, Ordering::Relaxed);
            // What it was behind is no longer this node's to report.
            w.inputs.lock().unwrap_or_else(|e| e.into_inner()).clear();
        }
    }
}

impl Metrics {
    fn workload(&self, id: &str) -> (Arc<WorkloadMetrics>, bool) {
        let mut workloads = self.workloads.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(found) = workloads.get(id) {
            return (found.clone(), false);
        }
        // `_other` is not a valid workload id, so it never collides.
        let named = |w: &BTreeMap<String, Arc<WorkloadMetrics>>| {
            w.len() - usize::from(w.contains_key(OTHER))
        };
        if named(&workloads) >= self.max_workloads {
            // Make room by dropping one that is neither held nor in use. Its
            // counters start over if it returns, which a rate absorbs.
            let idle = workloads
                .iter()
                .find(|(key, w)| {
                    key.as_str() != OTHER
                        && w.held.load(Ordering::Relaxed) <= 0
                        && Arc::strong_count(w) == 1
                })
                .map(|(key, _)| key.clone());
            if let Some(idle) = idle {
                workloads.remove(&idle);
            }
        }
        let key = if named(&workloads) >= self.max_workloads {
            OTHER
        } else {
            id
        };
        let entry = workloads
            .entry(key.to_owned())
            .or_insert_with(|| Arc::new(WorkloadMetrics::new()))
            .clone();
        (entry, key == OTHER)
    }
}

#[cfg(test)]
mod tests;
