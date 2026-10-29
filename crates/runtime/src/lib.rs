//! The wasmtime host that runs a workload component.
//!
//! A [`Runtime`] loads a component of workload contract 0.2
//! (`docs/contract.md`) into a [`Workload`] with no imports, NaN
//! canonicalization, a fuel limit on every call and a memory cap. A workload
//! that traps, runs out of fuel, returns `poison` or breaks the contract is
//! marked failed and never called again.

mod bindings;
mod runtime;
mod workload;

pub use felixd_core::spec::Limits;
pub use runtime::{Compiled, LoadError, Runtime};
pub use workload::{Failure, FailureKind, Input, Output, Stepped, Workload, WorkloadError};
