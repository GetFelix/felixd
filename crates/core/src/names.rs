//! Cache and stream names. Every name felixd uses is built here.

/// Cache of workload specs, keyed by workload id.
pub const WORKLOADS: &str = "workloads";
/// Cache of leases, keyed by workload id, with a TTL.
pub const LEASES: &str = "leases";
/// Cache of the last lease epoch handed out for each workload, keyed by
/// workload id. No TTL, so an epoch outlives the lease that used it.
pub const EPOCHS: &str = "epochs";
/// Cache of live nodes, keyed by node id, with a TTL.
pub const NODES: &str = "nodes";
/// Cache of module bytes, keyed by the module's sha256 in hex.
pub const MODULES: &str = "modules";

const MAX_ID_LEN: usize = 64;

/// Checks that `id` is usable in a stream name: 1 to 64 characters of
/// lowercase letters, digits and `-`.
pub fn validate_id(id: &str) -> Result<(), InvalidId> {
    let ok = !id.is_empty()
        && id.len() <= MAX_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if ok { Ok(()) } else { Err(InvalidId) }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("a workload id is 1 to 64 characters of a-z, 0-9 and '-'")]
pub struct InvalidId;

/// The stream a workload writes its outputs to.
pub fn output_stream(id: &str) -> String {
    format!("wl.{id}.out")
}

/// The stream a workload writes its snapshots to.
pub fn snapshot_stream(id: &str) -> String {
    format!("wl.{id}.snap")
}

#[cfg(test)]
mod tests;
