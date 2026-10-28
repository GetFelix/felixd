//! The value in the `leases` cache: which node runs a workload.

use serde::{Deserialize, Serialize};

use crate::DecodeError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub owner: String,
    /// Counts how many times the workload has changed owner, from 1.
    pub epoch: u64,
}

impl Lease {
    /// The lease `owner` writes when it claims a workload. `previous` is the
    /// lease it replaces, if one was still readable.
    pub fn claim(owner: &str, previous: Option<&Lease>) -> Lease {
        Lease {
            owner: owner.to_owned(),
            epoch: previous.map_or(1, |p| p.epoch.saturating_add(1)),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a lease is always valid JSON")
    }

    pub fn decode(bytes: &[u8]) -> Result<Lease, DecodeError> {
        serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))
    }
}

#[cfg(test)]
mod tests;
