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
    /// The last record on the output stream says the holder of the spec
    /// record the spec comes from released the workload for being stuck. It
    /// is an observation: any node may claim, and the next holder's claim
    /// record replaces this state.
    Degraded(Degraded),
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
            State::Degraded(degraded) => write!(f, "degraded {degraded}"),
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

/// What a degraded record says: the class of the error the holder was stuck
/// on, the node that held the workload, and the error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Degraded {
    pub class: String,
    /// Empty when the record does not say, or this build cannot read it.
    pub node: String,
    pub detail: String,
}

impl fmt::Display for Degraded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.class)?;
        if !self.node.is_empty() {
            write!(f, " on {}", self.node)?;
        }
        if !self.detail.is_empty() {
            write!(f, ": {}", self.detail)?;
        }
        Ok(())
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
    if let (Some(record), Some(last)) = (found.spec.record, found.last_record)
        && let Some(degraded) = degraded(last, record)
    {
        return State::Degraded(degraded);
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

/// What `last` says if it is a degraded record naming spec record `record`.
/// A newer build may record a class this one cannot read, which still shows.
pub fn degraded(last: &[u8], record: u64) -> Option<Degraded> {
    match output::decode(last) {
        Ok(output::Record::Degraded {
            spec,
            class,
            node,
            detail,
            ..
        }) if spec == record => Some(Degraded {
            class: class.to_string(),
            node: String::from_utf8_lossy(node).into_owned(),
            detail: String::from_utf8_lossy(detail).into_owned(),
        }),
        Ok(_) => None,
        Err(error) => (output::degraded_spec_of(last) == Some(record)).then(|| Degraded {
            class: format!("a class this build cannot read: {error}"),
            node: String::new(),
            detail: String::new(),
        }),
    }
}

/// What a divergence record says: where a replay and the output stream
/// disagreed and which build found it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diverged {
    pub node: String,
    pub epoch: u64,
    pub input_offset: u64,
    pub index: u32,
    /// The input offset of the snapshot the replay restored, or `None` for
    /// the initial state.
    pub snapshot: Option<u64>,
    pub contract: String,
    pub wasmtime: String,
    pub felixd: String,
    /// The sha256 of what the stream holds there, in hex, when it holds an
    /// output or a rejection.
    pub expected: Option<String>,
    /// The sha256 of what the replay produced there.
    pub actual: Option<String>,
    pub detail: String,
}

impl fmt::Display for Diverged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "diverged at input {} output {}",
            self.input_offset, self.index
        )?;
        if !self.node.is_empty() {
            write!(f, " on {}", self.node)?;
        }
        write!(f, " at epoch {}: the stream holds ", self.epoch)?;
        digest(f, self.expected.as_deref())?;
        f.write_str(", the replay produced ")?;
        digest(f, self.actual.as_deref())?;
        match self.snapshot {
            Some(input) => write!(f, ", started from the snapshot of input {input}")?,
            None => f.write_str(", started from the initial state")?,
        }
        write!(
            f,
            ", contract {}, wasmtime {}, felixd {}",
            self.contract, self.wasmtime, self.felixd
        )?;
        if !self.detail.is_empty() {
            write!(f, ": {}", self.detail)?;
        }
        Ok(())
    }
}

fn digest(f: &mut fmt::Formatter<'_>, digest: Option<&str>) -> fmt::Result {
    match digest {
        Some(digest) => write!(f, "sha256 {digest}"),
        None => f.write_str("nothing that compares"),
    }
}

/// The divergence record under spec record `record` that the workload's
/// failure followed, from `recent`, the last records of its output stream
/// oldest first. Failure, degraded and needs-operator records after it are
/// passed over. Any other record, a claim included, ends the search: a later
/// holder has run since, so the divergence is no longer what the stream
/// ends on. A record this build cannot read still shows, as far as its
/// header goes.
pub fn divergence<'a>(
    recent: impl DoubleEndedIterator<Item = &'a [u8]>,
    record: u64,
) -> Option<Diverged> {
    for last in recent.rev() {
        match output::decode(last) {
            Ok(output::Record::Divergence { epoch, found }) if found.spec == record => {
                return Some(Diverged {
                    node: lossy(found.node),
                    epoch,
                    input_offset: found.header.input_offset,
                    index: found.header.index,
                    snapshot: found.snapshot,
                    contract: lossy(found.abi.contract),
                    wasmtime: lossy(found.abi.wasmtime),
                    felixd: lossy(found.abi.felixd),
                    expected: found.expected.map(hex),
                    actual: found.actual.map(hex),
                    detail: lossy(found.detail),
                });
            }
            Ok(
                output::Record::Failure { .. }
                | output::Record::Degraded { .. }
                | output::Record::NeedsOperator { .. },
            ) => {}
            Ok(_) => return None,
            Err(error) if output::divergence_spec_of(last) == Some(record) => {
                return Some(Diverged {
                    node: String::new(),
                    epoch: 0,
                    input_offset: 0,
                    index: 0,
                    snapshot: None,
                    contract: "?".into(),
                    wasmtime: "?".into(),
                    felixd: "?".into(),
                    expected: None,
                    actual: None,
                    detail: format!("a divergence record this build cannot read: {error}"),
                });
            }
            Err(_)
                if output::needs_operator_spec_of(last).is_some()
                    || output::degraded_spec_of(last).is_some() => {}
            Err(_) => return None,
        }
    }
    None
}

/// Whether `last`, the last record of an output stream, is one a divergence
/// record can sit behind, so [`divergence`] is worth the read.
pub fn may_follow_divergence(last: &[u8]) -> bool {
    match output::decode(last) {
        Ok(output::Record::Failure { .. })
        | Ok(output::Record::Degraded { .. })
        | Ok(output::Record::NeedsOperator { .. })
        | Ok(output::Record::Divergence { .. }) => true,
        Ok(_) => false,
        Err(_) => output::divergence_spec_of(last).is_some(),
    }
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn hex(digest: &[u8; 32]) -> String {
    digest.iter().map(|b| format!("{b:02x}")).collect()
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
