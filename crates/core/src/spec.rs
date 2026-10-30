//! Specs: what should run. Each spec is a record on `sys.specs`, and the
//! `workloads` cache holds the latest one per workload with the offset of
//! the record it came from, plus the state an owner writes.

use serde::{Deserialize, Serialize};

use crate::class::{ErrorClass, Failure};
use crate::merge::MAX_INPUTS;
use crate::{DecodeError, codec};

/// How often a workload is snapshotted unless its spec says otherwise.
pub const DEFAULT_SNAPSHOT_EVERY: u64 = 1000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSpec {
    /// Hex sha256 of the module, the key in the `modules` cache.
    pub module: String,
    /// The streams whose records the workload is fed: one, or several merged
    /// in the order the merge log records ([`crate::merge`]). One is written
    /// as `input` and several as `inputs`, so a one-input spec reads as it
    /// always has and a build that knows only `input` refuses several.
    #[serde(flatten, with = "inputs_field")]
    pub inputs: Vec<String>,
    /// The stream its outputs are appended to.
    pub output: String,
    /// A snapshot is written after every this many inputs.
    #[serde(default = "default_snapshot_every")]
    pub snapshot_every: u64,
    /// A snapshot is also written at the first input of each interval this
    /// many milliseconds long, by the input's append time, so a slow input
    /// still gets snapshots. Either trigger restarts the count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_every_ms: Option<u64>,
    /// How many snapshots to keep. With it set, history before the oldest of
    /// the last this many is recorded as trimmable ([`crate::trim`]). Unset
    /// keeps everything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_snapshots: Option<u32>,
    pub state: WorkloadState,
    /// What the workload may use. It is in the spec, not node configuration,
    /// so every node runs the workload under the same limits and fails it at
    /// the same step.
    #[serde(default)]
    pub limits: Limits,
    /// Why the workload failed. Set with [`WorkloadState::Failed`].
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "failure_or_text"
    )]
    pub failure: Option<Failure>,
    /// The `sys.specs` offset of the spec record this cache value comes from.
    /// Only the cache value has it. An agent runs a spec only with its record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<u64>,
}

/// What a workload may use.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Fuel given to each call into the workload. Running out of it fails the
    /// workload.
    pub fuel_per_call: u64,
    /// The most linear memory the workload may grow to, in bytes.
    pub max_memory_bytes: u64,
    /// Fuel for a whole snapshot, `snapshot` and every `snapshot-chunk`
    /// call, and for a whole restore. A snapshot that runs out is skipped; a
    /// restore that runs out leaves the workload to an operator.
    #[serde(default = "default_snapshot_fuel")]
    pub snapshot_fuel: u64,
}

/// The default fuel for one call into a workload.
pub const DEFAULT_FUEL_PER_CALL: u64 = 50_000_000;

impl Default for Limits {
    fn default() -> Self {
        Limits {
            fuel_per_call: DEFAULT_FUEL_PER_CALL,
            max_memory_bytes: 64 << 20,
            snapshot_fuel: default_snapshot_fuel(),
        }
    }
}

fn default_snapshot_fuel() -> u64 {
    10 * DEFAULT_FUEL_PER_CALL
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkloadState {
    Running,
    Stopped,
    Failed,
}

fn default_snapshot_every() -> u64 {
    DEFAULT_SNAPSHOT_EVERY
}

impl WorkloadSpec {
    /// Whether the workload has several inputs, and so a merge log.
    pub fn merged(&self) -> bool {
        self.inputs.len() > 1
    }

    /// Why `next` cannot replace this spec on streams that already hold its
    /// output or snapshots, if it cannot. Another module or inputs would not
    /// reproduce what is written, and the next owner would find the stream
    /// diverged. The other fields may change: a running workload restarts
    /// under the new spec.
    pub fn conflicts_with_history(&self, next: &WorkloadSpec) -> Option<String> {
        if self.module != next.module {
            Some(format!("it runs module {}", self.module))
        } else if self.inputs != next.inputs {
            Some(format!("it reads {}", self.inputs.join(", ")))
        } else {
            None
        }
    }

    /// Whether the cache value `self` says what `record` does. The state may
    /// have moved on to `failed`, which only the cache holds.
    pub fn matches_record(&self, record: &WorkloadSpec) -> bool {
        let mut cached = self.clone();
        if cached.state == WorkloadState::Failed {
            cached.state = WorkloadState::Running;
        }
        cached.failure = None;
        cached.record = None;
        cached == *record
    }

    /// The `workloads` cache value.
    pub fn encode(&self) -> Vec<u8> {
        codec::encode_json(CACHE_FORMAT, self)
    }

    pub fn decode(bytes: &[u8]) -> Result<WorkloadSpec, DecodeError> {
        codec::decode_json(CACHE_FORMAT, bytes)
    }
}

/// Checks a list of inputs: 1 to [`MAX_INPUTS`] streams, none twice.
pub fn validate_inputs(inputs: &[String]) -> Result<(), InvalidInputs> {
    if inputs.is_empty() || inputs.len() > MAX_INPUTS {
        return Err(InvalidInputs::Count(inputs.len()));
    }
    for (at, input) in inputs.iter().enumerate() {
        if inputs[..at].contains(input) {
            return Err(InvalidInputs::Twice(input.clone()));
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidInputs {
    #[error("a workload reads 1 to {MAX_INPUTS} inputs, not {0}")]
    Count(usize),
    #[error("input {0} is listed twice")]
    Twice(String),
}

/// `inputs` in JSON: `"input": "a"` for one, `"inputs": ["a", "b"]` for
/// several.
mod inputs_field {
    use serde::de::Error as _;
    use serde::ser::SerializeMap;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(inputs: &[String], s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(1))?;
        match inputs {
            [one] => map.serialize_entry("input", one)?,
            several => map.serialize_entry("inputs", several)?,
        }
        map.end()
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
        #[derive(Deserialize)]
        struct Found {
            input: Option<String>,
            inputs: Option<Vec<String>>,
        }
        let inputs = match Found::deserialize(d)? {
            Found {
                input: Some(one),
                inputs: None,
            } => vec![one],
            Found {
                input: None,
                inputs: Some(several),
            } if several.len() > 1 => several,
            _ => {
                return Err(D::Error::custom(
                    "a spec names one `input` or several `inputs`",
                ));
            }
        };
        super::validate_inputs(&inputs).map_err(D::Error::custom)?;
        Ok(inputs)
    }
}

/// The `workloads` value format this build writes and reads.
const CACHE_FORMAT: u32 = 1;

/// A failure as it is, or as text from a build before error classes, which
/// failed a workload only for poison or a lease epoch gone backwards.
fn failure_or_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Failure>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Found {
        Typed(Failure),
        Text(String),
    }
    Ok(
        Option::<Found>::deserialize(deserializer)?.map(|found| match found {
            Found::Typed(failure) => failure,
            Found::Text(detail) => Failure {
                class: ErrorClass::Poison,
                detail,
            },
        }),
    )
}

/// Whether a `workloads` value naming spec record `ours` may replace one
/// naming `current`. The cache only moves forward, so a submitter that loses
/// a race, or one whose `sys.specs` was recreated and restarted its offsets,
/// changes nothing. A value naming no record predates spec records.
pub fn moves_forward(current: Option<u64>, ours: u64) -> bool {
    current.is_none_or(|current| current < ours)
}

/// The spec record format this build writes and reads.
const RECORD_FORMAT: u32 = 1;

/// A record on `sys.specs`: one version of one workload's spec. Its offset on
/// the stream is its version, so later records supersede earlier ones and the
/// stream is the spec history. Its state is `running` or `stopped`; `failed`
/// is an observation and stays in the cache.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecRecord {
    pub format: u32,
    pub id: String,
    pub spec: WorkloadSpec,
}

impl SpecRecord {
    pub fn new(id: &str, spec: WorkloadSpec) -> SpecRecord {
        SpecRecord {
            format: RECORD_FORMAT,
            id: id.to_owned(),
            spec,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a spec record is always valid JSON")
    }

    /// Reads a spec record. One of a format this build does not know is
    /// refused rather than misread.
    pub fn decode(bytes: &[u8]) -> Result<SpecRecord, DecodeError> {
        let record: SpecRecord =
            serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))?;
        if record.format != RECORD_FORMAT {
            return Err(DecodeError::UnknownVersion(
                u8::try_from(record.format).unwrap_or(u8::MAX),
            ));
        }
        Ok(record)
    }
}

/// One workload's spec records in stream order, as `(offset, spec)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct History(Vec<(u64, WorkloadSpec)>);

impl History {
    /// Folds the records of `sys.specs`, `(offset, bytes)` in stream order,
    /// into workload `id`'s history. Records of other workloads are passed
    /// over; a record that does not decode is an error, since it may be one
    /// of `id`'s.
    pub fn read<'a>(
        id: &str,
        records: impl IntoIterator<Item = (u64, &'a [u8])>,
    ) -> Result<History, DecodeError> {
        let mut history = Vec::new();
        for (offset, bytes) in records {
            let record = SpecRecord::decode(bytes)?;
            if record.id == id {
                history.push((offset, record.spec));
            }
        }
        Ok(History(history))
    }

    pub fn latest(&self) -> Option<&(u64, WorkloadSpec)> {
        self.0.last()
    }

    /// The spec recorded at `offset`.
    pub fn at(&self, offset: u64) -> Option<&WorkloadSpec> {
        self.0
            .iter()
            .find(|(at, _)| *at == offset)
            .map(|(_, spec)| spec)
    }
}

#[cfg(test)]
mod tests;
