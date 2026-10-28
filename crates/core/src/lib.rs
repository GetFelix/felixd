//! Record formats, key and stream names, lease rules and the resume fold.
//! Does no I/O.
//!
//! - [`names`]: the caches and streams felixd uses, and how a workload id maps
//!   to its streams.
//! - [`output`] and [`snapshot`]: the headers on output and snapshot records.
//! - [`spec`] and [`lease`]: the values in the `workloads` and `leases` caches.
//! - [`resume`]: where a new owner picks a workload up.

pub mod lease;
pub mod names;
pub mod output;
pub mod resume;
pub mod snapshot;
pub mod spec;

mod codec;

pub use codec::DecodeError;
