//! The node loop: reading workloads, feeding input, writing output and
//! snapshots.
//!
//! - [`settings`] and [`felix`]: how to reach Felix, and the calls felixd makes.
//! - [`admin`]: the control plane's REST API, used to create streams and caches.
//! - [`submit`]: what `felixd run` does.
//! - [`check`]: what `felixd check` does.
//! - [`agent`]: watches the workloads and leases caches and runs what it claims.
//! - [`lease`]: claims, renews and releases a workload's lease.
//! - [`runner`]: runs one workload: resume, then step, append and snapshot.
//! - [`output`]: appends a step's outputs exactly once.
//! - [`faults`]: crashes and dropped inputs the integration tests inject.

pub mod admin;
pub mod agent;
pub mod check;
pub mod felix;
pub mod settings;
pub mod submit;

mod faults;
mod lease;
mod output;
mod runner;
