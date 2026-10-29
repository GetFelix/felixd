//! The value in the `workloads` cache: what should run.

use serde::{Deserialize, Serialize};

use crate::DecodeError;

/// How often a workload is snapshotted unless its spec says otherwise.
pub const DEFAULT_SNAPSHOT_EVERY: u64 = 1000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSpec {
    /// Hex sha256 of the module, the key in the `modules` cache.
    pub module: String,
    /// The stream whose records the workload is fed.
    pub input: String,
    /// The stream its outputs are appended to.
    pub output: String,
    /// A snapshot is written after every this many inputs.
    #[serde(default = "default_snapshot_every")]
    pub snapshot_every: u64,
    pub state: WorkloadState,
    /// What the workload may use. It is in the spec, not node configuration,
    /// so every node runs the workload under the same limits and fails it at
    /// the same step.
    #[serde(default)]
    pub limits: Limits,
    /// Why the workload failed. Set with [`WorkloadState::Failed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

/// What a workload may use.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Fuel given to each call into the workload. Running out of it fails the
    /// workload.
    pub fuel_per_call: u64,
    /// The most linear memory the workload may grow to, in bytes.
    pub max_memory_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            fuel_per_call: 50_000_000,
            max_memory_bytes: 64 << 20,
        }
    }
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
    /// Why `next` cannot replace this spec on streams that already hold its
    /// output or snapshots, if it cannot. Another module or input would not
    /// reproduce what is written, and the next owner would find the stream
    /// diverged. The other fields may change: a running workload restarts
    /// under the new spec.
    pub fn conflicts_with_history(&self, next: &WorkloadSpec) -> Option<String> {
        if self.module != next.module {
            Some(format!("it runs module {}", self.module))
        } else if self.input != next.input {
            Some(format!("it reads input {}", self.input))
        } else {
            None
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a spec is always valid JSON")
    }

    pub fn decode(bytes: &[u8]) -> Result<WorkloadSpec, DecodeError> {
        serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))
    }
}

#[cfg(test)]
mod tests;
