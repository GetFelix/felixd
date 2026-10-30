//! Which free workloads a node claims.
//!
//! With no scheduler it is first claim wins: every node with room tries to
//! claim every free workload after a random delay, and the lease decides.
//! While a scheduler runs and its holder renews ([`crate::schedule`]), a node
//! claims what is assigned to it at once. It leaves a workload to another
//! node only while that node can take it, for at most a bound, and to the
//! scheduler only while it has not yet read the workload's spec record, for
//! at most a short grace, both from when it first saw the workload free.
//! Otherwise it is first claim wins at once. Either way the claim goes through the lease, so placement decides
//! where work runs, never whether output is correct.

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
    /// Whether the `workloads` cache says the scheduler runs and its holder
    /// renewed its lease lately.
    pub scheduler: bool,
    pub to: AssignedTo,
    /// How long this node has seen the workload free, whatever its
    /// assignment, so a changing assignment cannot defer it for ever.
    pub waited: Duration,
}

impl Assigned {
    /// No scheduler, so no assignment.
    pub const NONE: Assigned = Assigned {
        scheduler: false,
        to: AssignedTo::Nobody { decided: true },
        waited: Duration::ZERO,
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssignedTo {
    /// No assignment. `decided` is whether the scheduler has read past the
    /// workload's spec record, so it left it unassigned on purpose.
    Nobody {
        decided: bool,
    },
    ThisNode,
    /// Another node. `coming` is whether its `nodes` entry, rewritten lately,
    /// says it can take the workload: it has room, is not draining and can
    /// claim.
    Elsewhere {
        coming: bool,
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

/// The longest a free workload waits for an assignee that can take it,
/// unless told otherwise.
pub const DEFAULT_ASSIGNMENT_BOUND: Duration = Duration::from_secs(15);
/// The longest a free workload waits for a healthy scheduler to read its
/// spec record and assign it, unless told otherwise. A healthy scheduler
/// takes well under a second.
pub const DEFAULT_ASSIGNMENT_GRACE: Duration = Duration::from_secs(2);

/// Follows the scheduler's assignments while it runs and its holder renews,
/// with first claim wins as the fallback. A node claims a workload assigned
/// to it at once. One assigned to a node that can take it, it leaves alone
/// until `bound` has passed since it first saw the workload free; one whose
/// spec record the scheduler has not read yet, until `grace` has. Anything
/// else is first claim wins at once. The scheduler itself is always placed
/// by first claim wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scheduled {
    pub fallback: FirstClaimWins,
    pub bound: Duration,
    pub grace: Duration,
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
            AssignedTo::Elsewhere { coming: true } if assigned.waited < self.bound => {
                Turn::Defer(self.bound - assigned.waited)
            }
            AssignedTo::Nobody { decided: false } if assigned.waited < self.grace => {
                Turn::Defer(self.grace - assigned.waited)
            }
            _ => self.fallback.turn(workload, assigned, random),
        }
    }
}

#[cfg(test)]
mod tests;
