//! What `felixd ps` says about a workload. Everything comes from records and
//! cache entries any node or operator can read: the spec, the lease, the
//! output, snapshot and merge streams and the inputs' tails. No node's own
//! view goes into it.

use std::fmt;

use crate::class::Failure;
use crate::lease::Lease;
use crate::output::{self, OutputHeader};
use crate::snapshot::SnapshotHeader;
use crate::spec::{WorkloadSpec, WorkloadState};

/// What the records say about one workload.
#[derive(Clone, Copy, Debug)]
pub struct Found<'a> {
    pub spec: &'a WorkloadSpec,
    /// The live lease, if any.
    pub lease: Option<&'a Lease>,
    /// The last record on the output stream, of any kind.
    pub last_record: Option<&'a [u8]>,
}

/// Where a workload is, in the order the checks below decide it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    /// The spec says `stopped`.
    Stopped,
    /// The spec says `failed`.
    Failed(Failure),
    /// The last record on the output stream says that the spec record the
    /// spec comes from needs an operator. Nothing runs it until the spec
    /// changes, whoever holds the lease meanwhile.
    NeedsOperator(Parked),
    /// A node holds the lease and has taken its epoch.
    Running,
    /// A node has claimed the lease and not yet taken an epoch.
    Claiming,
    /// The spec is `running` and no node holds the lease.
    Waiting,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Stopped => f.write_str("stopped"),
            State::Failed(failure) => write!(f, "failed ({}): {}", failure.class, failure.detail),
            State::NeedsOperator(parked) => write!(f, "needs operator: {parked}"),
            State::Running => f.write_str("running"),
            State::Claiming => f.write_str("claiming"),
            State::Waiting => f.write_str("waiting"),
        }
    }
}

/// Why a needs-operator record parks a spec record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parked {
    pub reason: String,
    pub detail: String,
}

impl fmt::Display for Parked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.detail.is_empty() {
            f.write_str(&self.reason)
        } else {
            write!(f, "{} ({})", self.reason, self.detail)
        }
    }
}

/// The workload's state, from its spec, lease and last output record.
pub fn state(found: Found<'_>) -> State {
    match found.spec.state {
        WorkloadState::Stopped => return State::Stopped,
        WorkloadState::Failed => {
            return State::Failed(found.spec.failure.clone().unwrap_or(Failure {
                class: crate::class::ErrorClass::Poison,
                detail: String::new(),
            }));
        }
        WorkloadState::Running => {}
    }
    if let (Some(record), Some(last)) = (found.spec.record, found.last_record)
        && let Some(parked) = parked(last, record)
    {
        return State::NeedsOperator(parked);
    }
    match found.lease {
        Some(lease) if lease.epoch > 0 => State::Running,
        Some(_) => State::Claiming,
        None => State::Waiting,
    }
}

/// Why `last` parks spec record `record`, if it is a needs-operator record
/// naming it. A newer build may record a reason this one cannot read, and
/// that still parks it, so an older build never runs what a newer one parked.
pub fn parked(last: &[u8], record: u64) -> Option<Parked> {
    match output::decode(last) {
        Ok(output::Record::NeedsOperator {
            spec,
            reason,
            detail,
            ..
        }) if spec == record => Some(Parked {
            reason: reason.to_string(),
            detail: String::from_utf8_lossy(detail).into_owned(),
        }),
        Ok(_) => None,
        Err(error) => (output::needs_operator_spec_of(last) == Some(record)).then(|| Parked {
            reason: format!("a reason this build cannot read ({error})"),
            detail: String::new(),
        }),
    }
}

/// The next step a workload takes, as far as its records show: past the
/// input of the last output or rejection, or of the latest snapshot,
/// whichever is later. For a workload with several inputs it is a merge log
/// offset. A step that produced no output and is not in a snapshot does not
/// show, so this can be behind the holder by the steps since.
pub fn next_step(last_written: Option<OutputHeader>, snapshot: Option<SnapshotHeader>) -> u64 {
    let after = |offset: u64| offset.saturating_add(1);
    let written = last_written.map_or(0, |h| after(h.input_offset));
    let snapshot = snapshot.map_or(0, |s| after(s.input_offset));
    written.max(snapshot)
}

#[cfg(test)]
mod tests;
