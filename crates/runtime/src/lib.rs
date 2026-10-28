//! The wasmtime host that runs a workload component.
//!
//! A [`Runtime`] loads a component into a [`Workload`] with no imports, NaN
//! canonicalization, a fuel limit on every call and a memory cap. A workload
//! that traps or runs out of fuel is marked failed and never called again.

mod bindings;
mod limits;
mod runtime;
mod workload;

pub use limits::Limits;
pub use runtime::{LoadError, Runtime};
pub use workload::{Failure, FailureKind, Workload, WorkloadError};
