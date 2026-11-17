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
//!
//! Every claim has one authority, [`Authority`], which the holder records on
//! `wl.<id>.placed` ([`PlacementRecord`]) right after its claim lands.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{DecodeError, codec, names};

/// The placement record format this build writes and reads.
const PLACEMENT_FORMAT: u32 = 1;

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
    /// For a group member, where its group's record places it and how long
    /// it has been held. A member follows only that, whatever `to` says.
    pub group: Option<GroupHold>,
}

impl Assigned {
    /// No scheduler, so no assignment.
    pub const NONE: Assigned = Assigned {
        scheduler: false,
        to: AssignedTo::Nobody { decided: true },
        waited: Duration::ZERO,
        group: None,
    };
}

/// What a node knows of a free group member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupHold {
    pub placed: GroupPlaced,
    /// How long this node has held the member: since it first saw it free
    /// with the whole group admitted, or, while some member is not admitted,
    /// since it first saw it free, so a group that never completes is not
    /// held for ever.
    pub held: Duration,
}

/// Where the `group` record in a node's view places a member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupPlaced {
    /// On this node, by the `wl.scheduler.out` record at `assignment`
    /// written by the scheduler holder at `scheduler_epoch`.
    ThisNode {
        assignment: u64,
        scheduler_epoch: u64,
    },
    /// On another node.
    Elsewhere,
    /// No record in view places the whole group as its members declare it.
    Unplaced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssignedTo {
    /// No assignment. `decided` is whether the scheduler has read past the
    /// workload's spec record, so it left it unassigned on purpose.
    Nobody { decided: bool },
    /// This node, by the `wl.scheduler.out` record at `assignment`, written
    /// by the scheduler holder at `scheduler_epoch`.
    ThisNode {
        assignment: u64,
        scheduler_epoch: u64,
    },
    /// Another node, and what its `nodes` entry says.
    Elsewhere { assignee: Assignee },
}

/// What a node reads of another node's `nodes` entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Assignee {
    /// Rewritten lately, and it can take the workload: it has room, is not
    /// draining and can claim.
    Coming,
    /// No entry, a stale one, or one that says it takes no work.
    Unavailable,
    /// The read failed, or the entry is in a format this build cannot read.
    Unreadable,
}

/// What a node does about a free workload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Turn {
    /// Claim it after this long, under this authority.
    Claim(Duration, Authority),
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

    fn turn(&self, workload: &str, _assigned: &Assigned, random: f64) -> Turn {
        let reason = if workload == names::SCHEDULER {
            Reason::Scheduler
        } else {
            Reason::NoScheduler
        };
        self.first_claim(reason, random)
    }
}

impl FirstClaimWins {
    fn first_claim(&self, reason: Reason, random: f64) -> Turn {
        Turn::Claim(self.claim_delay(random), Authority::FirstClaim { reason })
    }
}

/// The longest a free workload waits for an assignee that can take it,
/// unless told otherwise.
pub const DEFAULT_ASSIGNMENT_BOUND: Duration = Duration::from_secs(15);
/// The longest a free workload waits for a healthy scheduler to read its
/// spec record and assign it, unless told otherwise. A healthy scheduler
/// takes well under a second.
pub const DEFAULT_ASSIGNMENT_GRACE: Duration = Duration::from_secs(2);
/// The longest a group member is held for its group's record, unless told
/// otherwise. The scheduler does not move a group off a dead node's members
/// until it forgets the node (thirty fact intervals, 15 min at the defaults),
/// which is longer than this, so after a node death such a member is claimed
/// `group-bound` once this passes.
pub const DEFAULT_GROUP_BOUND: Duration = Duration::from_secs(180);

/// Follows the scheduler's assignments while it runs and its holder renews,
/// with first claim wins as the fallback. A node claims a workload assigned
/// to it at once. One assigned to a node that can take it, it leaves alone
/// until `bound` has passed since it first saw the workload free; one whose
/// spec record the scheduler has not read yet, until `grace` has. Anything
/// else is first claim wins at once. The scheduler itself is always placed
/// by first claim wins. A group member is claimed only where its group's
/// record places it, whatever the scheduler's health, until `group_bound`
/// has passed; then it is first claim wins with reason `group-bound`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scheduled {
    pub fallback: FirstClaimWins,
    pub bound: Duration,
    pub grace: Duration,
    pub group_bound: Duration,
}

impl Placement for Scheduled {
    fn has_room(&self, held: usize) -> bool {
        self.fallback.has_room(held)
    }

    /// The reasons for a first claim are checked in the order of
    /// [`Reason`]'s variants, and the first that holds is the one recorded.
    fn turn(&self, workload: &str, assigned: &Assigned, random: f64) -> Turn {
        let first = |reason| self.fallback.first_claim(reason, random);
        if workload == names::SCHEDULER {
            return first(Reason::Scheduler);
        }
        if let Some(group) = assigned.group {
            return match group.placed {
                GroupPlaced::ThisNode {
                    assignment,
                    scheduler_epoch,
                } => Turn::Claim(
                    Duration::ZERO,
                    Authority::Assigned {
                        assignment,
                        scheduler_epoch,
                    },
                ),
                _ if group.held < self.group_bound => Turn::Defer(self.group_bound - group.held),
                _ => first(Reason::GroupBound),
            };
        }
        if !assigned.scheduler {
            return first(Reason::NoScheduler);
        }
        match assigned.to {
            AssignedTo::ThisNode {
                assignment,
                scheduler_epoch,
            } => Turn::Claim(
                Duration::ZERO,
                Authority::Assigned {
                    assignment,
                    scheduler_epoch,
                },
            ),
            AssignedTo::Elsewhere {
                assignee: Assignee::Unreadable,
            } => first(Reason::Unreadable),
            AssignedTo::Nobody { decided: true } => first(Reason::Unassigned),
            AssignedTo::Elsewhere {
                assignee: Assignee::Unavailable,
            } => first(Reason::AssigneeUnavailable),
            AssignedTo::Nobody { decided: false } if assigned.waited < self.grace => {
                Turn::Defer(self.grace - assigned.waited)
            }
            AssignedTo::Nobody { decided: false } => first(Reason::Grace),
            AssignedTo::Elsewhere {
                assignee: Assignee::Coming,
            } if assigned.waited < self.bound => Turn::Defer(self.bound - assigned.waited),
            AssignedTo::Elsewhere {
                assignee: Assignee::Coming,
            } => first(Reason::Bound),
        }
    }
}

/// Who placed a claim: the scheduler's assignment in force, or first claim
/// wins. One claim has exactly one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "placed", rename_all = "kebab-case")]
pub enum Authority {
    /// The scheduler put the workload on the claiming node. `assignment` is
    /// the `wl.scheduler.out` offset of the latest record in the claimer's
    /// view that did (an `assign`, or the `table` it read from), and
    /// `scheduler_epoch` the epoch of the scheduler holder that wrote it.
    Assigned {
        assignment: u64,
        scheduler_epoch: u64,
    },
    /// First claim wins, for `reason`.
    FirstClaim { reason: Reason },
    /// A kind of authority a newer build wrote. Never produced by this build.
    #[serde(other)]
    Unknown,
}

/// Why a claim was first claim wins. The order of the variants is the order
/// the agent checks them in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reason {
    /// The workload is the scheduler, which is never assigned.
    Scheduler,
    /// A group member held past the group bound with no `group` record
    /// placing it on this node. The group runs split.
    GroupBound,
    /// The scheduler does not run, or its holder has not renewed lately.
    NoScheduler,
    /// The assignee's `nodes` entry could not be read.
    Unreadable,
    /// The scheduler read the spec record and left the workload unassigned.
    Unassigned,
    /// The assignee is down, silent, full, draining or blocked.
    AssigneeUnavailable,
    /// The scheduler had not read the spec record within the grace.
    Grace,
    /// The assignee looked able but did not claim within the bound.
    Bound,
    /// A reason a newer build wrote, kept as written. Never produced by this
    /// build.
    Other(String),
}

impl Reason {
    /// The name in records and in `felixd ps`.
    pub fn name(&self) -> &str {
        match self {
            Reason::Scheduler => "scheduler",
            Reason::GroupBound => "group-bound",
            Reason::NoScheduler => "no-scheduler",
            Reason::Unreadable => "unreadable",
            Reason::Unassigned => "unassigned",
            Reason::AssigneeUnavailable => "assignee-unavailable",
            Reason::Grace => "grace",
            Reason::Bound => "bound",
            Reason::Other(name) => name,
        }
    }
}

impl Serialize for Reason {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for Reason {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let name = String::deserialize(d)?;
        Ok(match name.as_str() {
            "scheduler" => Reason::Scheduler,
            "group-bound" => Reason::GroupBound,
            "no-scheduler" => Reason::NoScheduler,
            "unreadable" => Reason::Unreadable,
            "unassigned" => Reason::Unassigned,
            "assignee-unavailable" => Reason::AssigneeUnavailable,
            "grace" => Reason::Grace,
            "bound" => Reason::Bound,
            _ => Reason::Other(name),
        })
    }
}

impl std::fmt::Display for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Authority::Assigned {
                assignment,
                scheduler_epoch,
            } => write!(
                f,
                "assigned (assignment {assignment}, scheduler epoch {scheduler_epoch})"
            ),
            Authority::FirstClaim { reason } => write!(f, "first-claim ({})", reason.name()),
            Authority::Unknown => write!(f, "unknown (written by a newer build)"),
        }
    }
}

/// A record on `wl.<id>.placed`: the authority the claim at `epoch` on
/// `wl.<id>.out` was placed by. The claim record's format is frozen, and an
/// older node would fail the workload on a new kind of record there, so the
/// authority lives on a stream of its own that no older build reads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementRecord {
    /// The epoch of the claim it belongs to.
    pub epoch: u64,
    #[serde(flatten)]
    pub authority: Authority,
}

impl PlacementRecord {
    pub fn encode(&self) -> Vec<u8> {
        codec::encode_json(PLACEMENT_FORMAT, self)
    }

    pub fn decode(bytes: &[u8]) -> Result<PlacementRecord, DecodeError> {
        codec::decode_json(PLACEMENT_FORMAT, bytes)
    }
}

#[cfg(test)]
mod tests;
