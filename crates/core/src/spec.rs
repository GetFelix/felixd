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
    /// Why the workload failed. Set with [`WorkloadState::Failed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
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
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a spec is always valid JSON")
    }

    pub fn decode(bytes: &[u8]) -> Result<WorkloadSpec, DecodeError> {
        serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))
    }
}

#[cfg(test)]
mod tests;
