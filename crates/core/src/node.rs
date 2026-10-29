//! The value in the `nodes` cache: a live agent.

use serde::{Deserialize, Serialize};

use crate::DecodeError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// The felixd build the node runs.
    pub version: String,
    /// The most workloads it will hold.
    pub max_workloads: usize,
    /// How many it holds now.
    pub held: usize,
}

impl NodeInfo {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("node info is always valid JSON")
    }

    pub fn decode(bytes: &[u8]) -> Result<NodeInfo, DecodeError> {
        serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))
    }
}

#[cfg(test)]
mod tests;
