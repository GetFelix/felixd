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
/// Cache of drain marks, keyed by node id. No TTL: a mark stays until an
/// operator lifts it.
pub const DRAINS: &str = "drains";

/// Streams named with this prefix live in the system namespace
/// ([`system_namespace`]), apart from workload streams, so a grant on every
/// stream of the workload namespace, which every node needs, reaches none of
/// them. Felix grants a stream by name or by whole namespace only.
pub const SYSTEM_PREFIX: &str = "sys.";

/// Stream of spec records: every version of every workload's spec.
pub const SPECS: &str = "sys.specs";
/// Stream of node facts: what each node says about itself while a scheduler
/// runs ([`crate::schedule`]).
pub const FACTS: &str = "sys.facts";

/// Stream of requests to run, change or stop workloads, which admission
/// decides ([`crate::admission`]).
pub const INTAKE: &str = "sys.intake";

/// The system streams as they were named before they moved to the system
/// namespace, where `felixd init` and the agent look for leftovers.
pub const LEGACY_SYSTEM_STREAMS: [&str; 3] = [SPECS, INTAKE, FACTS];

/// The id of the admission workload, which reads [`INTAKE`]. Its spec is the
/// only one written directly, by `felixd init`.
pub const ADMISSION: &str = "admission";

/// The id of the scheduler workload, whose outputs are placement
/// assignments. It reads [`SPECS`] and [`FACTS`].
pub const SCHEDULER: &str = "scheduler";

/// The system workloads. Their specs are written directly by an operator,
/// never submitted through admission, which refuses these ids.
pub const SYSTEM: [&str; 2] = [ADMISSION, SCHEDULER];

/// Whether `id` is a system workload's.
pub fn is_system(id: &str) -> bool {
    SYSTEM.contains(&id)
}

/// The namespace holding the system streams of workload namespace
/// `namespace`.
pub fn system_namespace(namespace: &str) -> String {
    format!("{namespace}{SYSTEM_SUFFIX}")
}

/// The suffix of a system namespace.
const SYSTEM_SUFFIX: &str = ".sys";

/// Checks that `namespace` can be a workload namespace: not empty, and not
/// ending in `.sys`. A namespace named `x.sys` is the system namespace of `x`
/// ([`system_namespace`]), so as a workload namespace it would give nodes
/// their workload-stream grant over `x`'s system streams.
pub fn validate_namespace(namespace: &str) -> Result<(), InvalidNamespace> {
    if namespace.is_empty() || namespace.ends_with(SYSTEM_SUFFIX) {
        Err(InvalidNamespace(namespace.to_owned()))
    } else {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("namespace {0:?} is empty or ends in \".sys\", which names a system namespace")]
pub struct InvalidNamespace(String);

/// The namespace `stream` lives in, for workload namespace `namespace`.
pub fn namespace_of(namespace: &str, stream: &str) -> String {
    if stream.starts_with(SYSTEM_PREFIX) {
        system_namespace(namespace)
    } else {
        namespace.to_owned()
    }
}

/// The longest workload id.
pub const MAX_ID_LEN: usize = 64;

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

/// Whether `stream` is a workload's output stream, by its name.
pub fn is_output_stream(stream: &str) -> bool {
    output_of(stream).is_some()
}

/// The id of the workload whose output stream `stream` is, by its name.
pub fn output_of(stream: &str) -> Option<&str> {
    stream
        .strip_prefix("wl.")
        .and_then(|s| s.strip_suffix(".out"))
        .filter(|id| validate_id(id).is_ok())
}

/// The stream a workload with several inputs records their merge order on.
pub fn merge_stream(id: &str) -> String {
    format!("wl.{id}.merge")
}

/// The stream a workload writes its snapshots to.
pub fn snapshot_stream(id: &str) -> String {
    format!("wl.{id}.snap")
}

#[cfg(test)]
mod tests;
