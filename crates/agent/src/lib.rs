//! The node loop: reading workloads, feeding input, writing output and
//! snapshots.
//!
//! - [`settings`] and [`felix`]: how to reach Felix, and the calls felixd makes.
//! - [`admin`]: the control plane's REST API, used to create streams and caches.
//! - [`submit`]: what `felixd run` does.
//! - [`check`]: what `felixd check` does.
//! - [`replay`]: what `felixd replay` does.
//! - [`ps`]: what `felixd ps` does.
//! - [`metrics`]: the Prometheus endpoint and what feeds it.
//! - [`drain`]: drain marks, which make a node give up its leases.
//! - [`assignments`]: the scheduler's assignments as a node follows them, and
//!   the node facts it writes for the scheduler.
//! - [`agent`]: watches the workloads and leases caches and runs what it claims.
//! - [`lease`]: claims, renews and releases a workload's lease.
//! - [`runner`]: runs one workload: resume, then step, append and snapshot.
//! - [`output`]: appends a step's outputs exactly once.
//! - [`merge`]: appends to the merge log of a workload with several inputs.
//! - [`input`]: what a step is given and what it produced, the same for every
//!   caller.
//! - [`snapshots`]: reads snapshots and their chunks back.
//! - [`specs`]: spec records on `sys.specs` and their projection into
//!   `workloads`.
//! - [`faults`]: crashes and dropped inputs the integration tests inject.

pub mod admin;
pub mod agent;
pub mod assignments;
pub mod check;
pub mod drain;
pub mod felix;
pub mod metrics;
pub mod ps;
pub mod replay;
pub mod settings;
pub mod submit;

mod faults;
mod input;
mod lease;
mod merge;
mod output;
mod runner;
mod snapshots;
mod specs;
