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
    /// The nonce of the drain mark it has read, once it also has no claim
    /// under way, so it will claim nothing until that mark is gone. Left
    /// out when there is none, so an entry that is not draining reads the
    /// same as before the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draining: Option<u64>,
    /// It cannot claim now: it cannot read its drain mark. Left out when
    /// false.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub blocked: bool,
}

impl NodeInfo {
    /// Whether the node would claim one more workload now: it has room, is
    /// not draining and can claim. Another node waits for an assignee only
    /// while this holds.
    pub fn takes_work(&self) -> bool {
        self.held < self.max_workloads && self.draining.is_none() && !self.blocked
    }

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
pub struct Drain {
    /// Picked by whoever writes the mark, never 0. A node echoes it in
    /// [`NodeInfo::draining`], so an acknowledgement of an earlier mark is
    /// not taken for this one.
    #[serde(default)]
    pub nonce: u64,
}

/// Whether `info` acknowledges `mark`: the node has read that very mark and
/// has no claim under way. A mark this build cannot read is never
/// acknowledged.
pub fn acknowledges(info: Option<&NodeInfo>, mark: Option<&Drain>) -> bool {
    match (info.and_then(|i| i.draining), mark) {
        (Some(acked), Some(mark)) => mark.nonce != 0 && acked == mark.nonce,
        _ => false,
    }
}

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
