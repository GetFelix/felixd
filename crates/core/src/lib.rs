//! Record formats, key and stream names, lease rules and the resume fold.
//! Does no I/O.
//!
//! - [`names`]: the caches and streams felixd uses, and how a workload id maps
//!   to its streams.
//! - [`output`] and [`snapshot`]: the records on output and snapshot streams.
//! - [`spec`], [`lease`] and [`node`]: the values in the `workloads`, `leases`
//!   and `nodes` caches.
//! - [`class`]: error classes and what an operator has to fix.
//! - [`placement`]: which free workloads a node claims.
//! - [`module`]: how a module is identified.
//! - [`resume`]: where a new owner picks a workload up.
//! - [`replay`]: where a replay starts and stops.
//! - [`check`]: what is wrong with an output stream, against a replay.

pub mod check;
pub mod class;
pub mod lease;
pub mod module;
pub mod names;
pub mod node;
pub mod output;
pub mod placement;
pub mod replay;
pub mod resume;
pub mod snapshot;
pub mod spec;

mod codec;

pub use codec::{DecodeError, MAX_WRITE_BYTES};
