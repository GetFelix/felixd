//! Helpers shared by tests. Not part of the shipped binary.
//!
//! [`workload`] builds an example workload from `workloads/` into a component,
//! so a test runs the real thing instead of a stand-in. [`workload_with`]
//! builds it with cargo features, for a workload that has variants.
//! [`contract_01_component`] is a workload of the old contract, which the host
//! refuses.

mod contract01;
mod workloads;

pub use contract01::contract_01_component;
pub use workloads::{workload, workload_with};
