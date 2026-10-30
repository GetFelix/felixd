//! The values in the `nodes` cache, a live agent, and the `drains` cache, a
//! node an operator is draining.

use serde::{Deserialize, Serialize};

use crate::DecodeError;

/// The `nodes` value format this build writes and reads.
const FORMAT: u32 = 1;
/// The `drains` value format.
const DRAIN_FORMAT: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// The felixd build the node runs.
    pub version: String,
    /// The most workloads it will hold.
    pub max_workloads: usize,
    /// How many it holds now.
    pub held: usize,
    /// It has read its drain mark and has no claim under way, so it will
    /// claim nothing until the mark is gone. Left out when false, so an
    /// entry that is not draining reads the same as before the field.
    #[serde(default, skip_serializing_if = "is_false")]
    pub draining: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl NodeInfo {
    pub fn encode(&self) -> Vec<u8> {
        crate::codec::encode_json(FORMAT, self)
    }

    pub fn decode(bytes: &[u8]) -> Result<NodeInfo, DecodeError> {
        crate::codec::decode_json(FORMAT, bytes)
    }
}

/// The value in `drains` under a node's id: an operator wants the node to
/// give up its leases and claim nothing until the mark is deleted.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drain {}

impl Drain {
    pub fn encode(&self) -> Vec<u8> {
        crate::codec::encode_json(DRAIN_FORMAT, self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Drain, DecodeError> {
        crate::codec::decode_json(DRAIN_FORMAT, bytes)
    }
}

#[cfg(test)]
mod tests;
