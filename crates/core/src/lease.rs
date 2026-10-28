//! The value in the `leases` cache, and the timing rules a holder follows.
//!
//! The value in the `leases` cache, and the timing rules a holder follows.
//!
//! A claim takes its epoch from the `epochs` cache, one more than the larger
//! of the last epoch handed out and the epoch on the output stream's tail. The
//! counter makes epochs unique and growing; the output tail keeps them above
//! everything already written if the counter is ever lost. Owners stamp their
//! epoch on every output record, which is how a stale owner learns it has been
//! replaced.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::DecodeError;

/// How long a lease lives without renewal unless the agent is told otherwise.
pub const DEFAULT_TTL: Duration = Duration::from_secs(10);

/// The shortest TTL an agent accepts. A holder stops a third of a TTL before
/// anyone else may claim, and that margin has to cover a renewal's round trip
/// to a busy broker and the scheduler's delay in waking the holder.
pub const MIN_TTL: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub owner: String,
    /// The holder's epoch. The claim itself writes 0, since the epoch is
    /// taken only once the claim has won; the holder then rewrites the lease
    /// with its epoch before it runs anything.
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

    /// The value the holder at `epoch` writes once it has its epoch, and on
    /// every renewal.
    pub fn held(owner: &str, epoch: u64) -> Lease {
        Lease {
            owner: owner.to_owned(),
            epoch,
        }
    }

    /// The epoch for a new claim, given the last one handed out and the epoch
    /// of the last record on the output stream.
    pub fn next_epoch(last_handed_out: u64, output_tail: u64) -> u64 {
        last_handed_out.max(output_tail) + 1
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a lease is always valid JSON")
    }

    pub fn decode(bytes: &[u8]) -> Result<Lease, DecodeError> {
        serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))
    }
}

/// When a holder renews and when it gives up.
/// The value in the `epochs` cache: the last epoch handed out.
pub fn encode_epoch(epoch: u64) -> Vec<u8> {
    epoch.to_string().into_bytes()
}

pub fn decode_epoch(bytes: &[u8]) -> Result<u64, DecodeError> {
    serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))
}

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
