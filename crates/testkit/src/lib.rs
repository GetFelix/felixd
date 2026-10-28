//! Helpers shared by tests. Not part of the shipped binary.
//!
//! [`workload`] builds an example workload from `workloads/` into a component,
//! so a test runs the real thing instead of a stand-in.

mod workloads;

pub use workloads::workload;
