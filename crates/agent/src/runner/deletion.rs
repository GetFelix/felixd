//! Deleting what other workloads share with this one: its `.out` and the
//! inputs it reads. Each goes only as far as the lowest floor of every
//! workload that reads it ([`felixd_core::readers`]).

use felixd_core::readers::{self as rule, Known};
use felixd_core::{names, trim::Trim};

use super::Steps;
use crate::readers;
use crate::retention::{self, Deletion, Kind};
use crate::snapshots;

impl Steps<'_> {
    /// Whether `.out` is up for deleting ([`Trim::cuts_output`]).
    pub(super) fn deletes_output(&self, trim: &Trim) -> bool {
        self.spec.keep_snapshots.is_some()
            && names::output_of(&self.spec.output).is_some_and(|id| trim.cuts_output(id))
    }

    /// Deletes `.out` if `output`, and the `shared` inputs, by position and
    /// this workload's floor, each below the lowest floor of its readers.
    /// A reader that cannot be found out leaves the stream alone.
    pub(super) async fn delete_shared(
        &mut self,
        trim: &Trim,
        id: &str,
        output: bool,
        shared: &[(usize, u64)],
    ) {
        let own = Known {
            inputs: Some(self.spec.inputs.clone()),
            floors: Some(trim.input_floors.clone()),
        };
        let mut streams: Vec<&str> = shared
            .iter()
            .map(|&(at, _)| self.spec.inputs[at].as_str())
            .collect();
        if output {
            streams.push(&self.spec.output);
        }
        let known = match readers::known(self.felix, id, own, &streams).await {
            Ok(known) => known,
            Err(error) => {
                tracing::warn!(workload = id, %error, "finding the readers failed, deleting nothing shared");
                return;
            }
        };
        if output && self.delete_output(trim, &known).await == Deletion::Unavailable {
            return;
        }
        if shared.is_empty() || !retention::may_delete_shared() {
            return;
        }
        for &(at, _) in shared {
            let stream = self.spec.inputs[at].clone();
            let done = self.deleted_inputs.get(at).copied().unwrap_or(0);
            let Some(before) = rule::lowest_floor(&stream, &known).filter(|&b| b > done) else {
                continue;
            };
            match self.trim_at_tail(&stream, before, Kind::Shared).await {
                Deletion::Done { .. } => {
                    self.deleted_inputs.resize(self.spec.inputs.len(), 0);
                    self.deleted_inputs[at] = before;
                }
                Deletion::Unavailable => return,
                Deletion::Later => {}
            }
        }
    }

    /// Trims `.out` below the lowest of this workload's own need and its
    /// readers' floors. It lands at the tail this owner expects, so a
    /// superseded holder is refused, which a retry would undo.
    async fn delete_output(&mut self, trim: &Trim, known: &[Known]) -> Deletion {
        let Some(need) = self.output_need(trim).await else {
            return Deletion::Later;
        };
        let floor = rule::output_floor(need, &self.spec.output, known);
        if floor <= self.deleted.out_floor {
            return Deletion::Done { landed: None };
        }
        let deletion = retention::trim_stream(
            self.felix,
            &self.spec.output,
            floor,
            self.log.tail(),
            Kind::Owned,
        )
        .await;
        if let Deletion::Done { landed } = deletion {
            if let Some(at) = landed {
                self.log.trim_landed(at);
            }
            self.deleted.out_floor = floor;
        }
        deletion
    }

    /// The output tail of the oldest kept snapshot `trim` names: a resume or
    /// a check from that snapshot reads `.out` from there.
    async fn output_need(&mut self, trim: &Trim) -> Option<u64> {
        if let Some((snapshot, tail)) = self.out_need
            && snapshot == trim.snapshot
        {
            return Some(tail);
        }
        match snapshots::header_at(self.felix, self.snap_stream, trim.snapshot).await {
            Ok(Some(header)) => {
                self.out_need = Some((trim.snapshot, header.output_tail));
                Some(header.output_tail)
            }
            Ok(None) => {
                tracing::warn!(
                    stream = self.snap_stream,
                    offset = trim.snapshot,
                    "the last trim names a record that is not a snapshot, keeping .out"
                );
                None
            }
            Err(error) => {
                tracing::warn!(%error, "reading the oldest kept snapshot failed, keeping .out");
                None
            }
        }
    }
}
