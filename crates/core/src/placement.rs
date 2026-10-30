//! Which free workloads a node claims.
//!
//! With no scheduler it is first claim wins: every node with room tries to
//! claim every free workload after a random delay, and the lease decides.
//! While a scheduler runs ([`crate::schedule`]), a node claims what is
//! assigned to it at once and leaves the rest to their assignees, up to a
//! bound, then falls back to first claim wins. Either way the claim goes
//! through the lease, so placement decides where work runs, never whether
//! output is correct.

use std::time::Duration;

use crate::names;

pub trait Placement: Send + Sync {
    /// Whether a node holding `held` workloads may claim another.
    fn has_room(&self, held: usize) -> bool;

    /// What to do about the free workload `workload`, given what this node
    /// knows of its assignment. `random` is uniform in `[0, 1)`.
    fn turn(&self, workload: &str, assigned: &Assigned, random: f64) -> Turn;
}

/// What a node knows about a free workload's assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Assigned {
    /// Whether the `workloads` cache says a scheduler runs.
    pub scheduler: bool,
    pub to: AssignedTo,
    /// How long this node has seen the workload free under this assignment,
    /// or without one.
    pub waited: Duration,
}

impl Assigned {
    /// No scheduler, so no assignment.
    pub const NONE: Assigned = Assigned {
        scheduler: false,
        to: AssignedTo::Nobody,
        waited: Duration::ZERO,
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssignedTo {
    Nobody,
    ThisNode,
    /// Another node. `up` is whether it has an entry in `nodes`; a node
    /// that is not up is not coming for it.
    Elsewhere {
        up: bool,
    },
}

/// What a node does about a free workload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Turn {
    /// Claim it after this long.
    Claim(Duration),
    /// Leave it to its assignee, and decide again after this long, or sooner
    /// if the lease or the assignment changes.
    Defer(Duration),
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

impl FirstClaimWins {
    fn claim_delay(&self, random: f64) -> Duration {
        self.max_delay.mul_f64(random.clamp(0.0, 1.0))
    }
}

impl Placement for FirstClaimWins {
    fn has_room(&self, held: usize) -> bool {
        held < self.max_workloads
    }

    fn turn(&self, _workload: &str, _assigned: &Assigned, random: f64) -> Turn {
        Turn::Claim(self.claim_delay(random))
    }
}

/// How long a free workload waits for its assignee, or for an assignment,
/// unless told otherwise.
pub const DEFAULT_ASSIGNMENT_BOUND: Duration = Duration::from_secs(15);

/// Follows the scheduler's assignments while one runs, with first claim wins
/// as the fallback. A node claims a workload assigned to it at once. One
/// assigned elsewhere, or not yet assigned, it leaves alone for `bound`,
/// unless the assignee is not up, and then claims it by first claim wins.
/// The scheduler itself is always placed by first claim wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scheduled {
    pub fallback: FirstClaimWins,
    pub bound: Duration,
}

impl Placement for Scheduled {
    fn has_room(&self, held: usize) -> bool {
        self.fallback.has_room(held)
    }

    fn turn(&self, workload: &str, assigned: &Assigned, random: f64) -> Turn {
        if !assigned.scheduler || workload == names::SCHEDULER {
            return self.fallback.turn(workload, assigned, random);
        }
        match assigned.to {
            AssignedTo::ThisNode => Turn::Claim(Duration::ZERO),
            _ => Turn::Defer(self.bound),
        }
    }
}

#[cfg(test)]
mod tests;
