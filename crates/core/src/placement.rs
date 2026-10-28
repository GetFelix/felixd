//! Which free workloads a node claims.
//!
//! v0 is "first claim wins": every node with room tries to claim every free
//! workload after a random delay, and the lease decides. Whether placement
//! should stay in the agents or move to a scheduler workload is open (#26), so
//! the agent only sees this trait.

use std::time::Duration;

/// What a node knows when it decides whether to claim a free workload.
#[derive(Clone, Copy, Debug)]
pub struct Offer<'a> {
    pub workload: &'a str,
    /// How many workloads this node holds now.
    pub held: usize,
}

pub trait Placement: Send + Sync {
    /// How long to wait before claiming, or `None` to leave the workload to
    /// other nodes. `random` is uniform in `[0, 1)`.
    fn claim_after(&self, offer: Offer<'_>, random: f64) -> Option<Duration>;
}

/// Claim anything while under `max_workloads`, after a random delay of up to
/// `max_delay` so that one node does not grab everything.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FirstClaimWins {
    pub max_workloads: usize,
    pub max_delay: Duration,
}

impl Default for FirstClaimWins {
    fn default() -> Self {
        FirstClaimWins {
            max_workloads: 64,
            max_delay: Duration::from_secs(1),
        }
    }
}

impl Placement for FirstClaimWins {
    fn claim_after(&self, offer: Offer<'_>, random: f64) -> Option<Duration> {
        (offer.held < self.max_workloads).then(|| self.max_delay.mul_f64(random.clamp(0.0, 1.0)))
    }
}

#[cfg(test)]
mod tests;
