//! The value in the `nodes` cache: a live agent.

use serde::{Deserialize, Serialize};

use crate::DecodeError;

/// The `nodes` value format this build writes and reads.
const FORMAT: u32 = 1;

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
        crate::codec::encode_json(FORMAT, self)
    }

    pub fn decode(bytes: &[u8]) -> Result<NodeInfo, DecodeError> {
        crate::codec::decode_json(FORMAT, bytes)
    }
}

#[cfg(test)]
mod tests;
