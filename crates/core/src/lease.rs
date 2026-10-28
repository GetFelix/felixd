//! The value in the `leases` cache, and the timing rules a holder follows.
//!
//! A lease's epoch is one more than the cache version of the put that claimed
//! it. Felix versions only grow within a shard and are never reused, so a
//! later claim always has a higher epoch than every earlier one, including
//! claims whose entries have since expired. The first write to a new cache has
//! version 0, and epoch 0 is kept for "not yet known". Owners stamp their epoch on every output
//! record, which is how a stale owner learns it has been replaced.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::DecodeError;

/// How long a lease lives without renewal unless the agent is told otherwise.
pub const DEFAULT_TTL: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub owner: String,
    /// The claim's epoch. A claim does not know its version until the put is
    /// applied, so it writes 0 and its renewals write the version it got.
    pub epoch: u64,
}

impl Lease {
    /// The value `owner` writes to claim a workload.
    pub fn claim(owner: &str) -> Lease {
        Lease {
            owner: owner.to_owned(),
            epoch: 0,
        }
    }

    /// The value the holder of the claim at `epoch` writes to renew it.
    pub fn renewal(owner: &str, epoch: u64) -> Lease {
        Lease {
            owner: owner.to_owned(),
            epoch,
        }
    }

    /// The epoch of a claim the cache applied at `version`.
    pub fn claim_epoch(version: u64) -> u64 {
        version + 1
    }

    /// The epoch of this lease, given the version the cache reports for it.
    pub fn epoch_at(&self, version: u64) -> u64 {
        if self.epoch == 0 {
            Lease::claim_epoch(version)
        } else {
            self.epoch
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a lease is always valid JSON")
    }

    pub fn decode(bytes: &[u8]) -> Result<Lease, DecodeError> {
        serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))
    }
}

/// When a holder renews and when it gives up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseTiming {
    pub ttl: Duration,
}

impl LeaseTiming {
    /// How long after its last successful renewal a holder renews again.
    pub fn renew_after(&self) -> Duration {
        self.ttl / 3
    }

    /// How long after it sent its last successful renewal a holder stops
    /// running the workload if it has not renewed since. Counted from the
    /// send, which is no later than the broker set the expiry, so the holder
    /// stops a third of a TTL before any other node can claim.
    pub fn stop_after(&self) -> Duration {
        self.ttl * 2 / 3
    }

    pub fn ttl_millis(&self) -> u64 {
        u64::try_from(self.ttl.as_millis()).unwrap_or(u64::MAX)
    }
}

impl Default for LeaseTiming {
    fn default() -> Self {
        LeaseTiming { ttl: DEFAULT_TTL }
    }
}

#[cfg(test)]
mod tests;
