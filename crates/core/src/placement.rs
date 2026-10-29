//! Which free workloads a node claims.
//!
//! v0 is "first claim wins": every node with room tries to claim every free
//! workload after a random delay, and the lease decides. Whether placement
//! should stay in the agents or move to a scheduler workload is open (#26), so
//! the agent only sees this trait.

use std::time::Duration;

pub trait Placement: Send + Sync {
    /// Whether a node holding `held` workloads may claim another.
    fn has_room(&self, held: usize) -> bool;

    /// How long to wait before claiming the free workload `workload`.
    /// `random` is uniform in `[0, 1)`.
    fn claim_delay(&self, workload: &str, random: f64) -> Duration;
}

/// Claim anything while under `max_workloads`, after a random delay of up to
/// `max_delay` so that one node does not grab everything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FirstClaimWins {
    pub max_workloads: usize,
    pub max_delay: Duration,
}

/// How many workloads a node holds at most unless told otherwise.
pub const DEFAULT_MAX_WORKLOADS: usize = 64;
/// The longest random delay before a claim unless told otherwise.
pub const DEFAULT_CLAIM_DELAY: Duration = Duration::from_secs(1);

impl Default for FirstClaimWins {
    fn default() -> Self {
        FirstClaimWins {
            max_workloads: DEFAULT_MAX_WORKLOADS,
            max_delay: DEFAULT_CLAIM_DELAY,
        }
    }
}

impl Placement for FirstClaimWins {
    fn has_room(&self, held: usize) -> bool {
        held < self.max_workloads
    }

    fn claim_delay(&self, _workload: &str, random: f64) -> Duration {
        self.max_delay.mul_f64(random.clamp(0.0, 1.0))
    }
}

#[cfg(test)]
mod tests;
