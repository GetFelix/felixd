//! The scheduler: a workload that reads spec records and node facts and
//! writes which node each workload should run on (#60).
//!
//! It is an ordinary felixd workload with two inputs, [`names::SPECS`] and
//! [`names::FACTS`], so its assignments fail over, are written exactly once
//! and replay like any output. Its clocks are the append times of the inputs
//! it has stepped, never a wall clock, so a replay assigns the same way.
//! Agents claim what is assigned to them, still through the lease, and fall
//! back to first claim wins whenever the scheduler or the assignee is not
//! able to act ([`crate::placement`]). An assignment can delay work but
//! never decides whether output is correct.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::spec::{Group, SpecRecord, WorkloadState};
use crate::{DecodeError, codec, names};

/// The `sys.facts` record format this build writes and reads.
const FACT_FORMAT: u32 = 1;
/// The scheduler's output format.
const RECORD_FORMAT: u32 = 1;

/// How many of its own fact intervals a node may stay silent before the
/// scheduler counts it as gone.
pub const SILENT_INTERVALS: u64 = 3;
/// How many of its own fact intervals a node may stay silent before the
/// scheduler forgets it.
pub const FORGET_INTERVALS: u64 = 30;
/// The tick period: the scheduler writes its whole table again at the first
/// `sys.facts` record past each multiple of it, so a reader can start from
/// the latest table and older output can be trimmed.
pub const TABLE_EVERY_MICROS: u64 = 10 * 60 * 1_000_000;

/// A record on `sys.facts`: what a node says about itself. Agents append one
/// every `every_ms` while a scheduler runs, and soon after a field changes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeFact {
    pub node: String,
    /// How often the node appends a fact when nothing changes, in
    /// milliseconds. After [`SILENT_INTERVALS`] of these with none, the
    /// scheduler counts it as gone.
    pub every_ms: u64,
    /// The most workloads it holds.
    pub max_workloads: usize,
    /// How many it holds, however it claimed them.
    #[serde(default)]
    pub held: usize,
    /// The ids of the workloads it holds, all of them. A node claims only
    /// within `max_workloads`, so there are at most that many.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub holding: BTreeSet<String>,
    /// It is draining and takes no work.
    #[serde(default)]
    pub draining: bool,
    /// It cannot claim now, such as when it cannot read its drain mark.
    #[serde(default)]
    pub blocked: bool,
}

/// The most bytes the fact of node `node` can take while it holds
/// `max_workloads` workloads with the longest ids. Ids are `a-z`, `0-9` and
/// `-`, so each costs its length, two quotes and a comma.
pub fn worst_fact_bytes(node: &str, max_workloads: usize) -> usize {
    let bare = NodeFact {
        node: node.to_owned(),
        every_ms: u64::MAX,
        max_workloads,
        held: max_workloads,
        holding: BTreeSet::new(),
        draining: true,
        blocked: true,
    };
    (bare.encode().len() + r#","holding":[]"#.len())
        .saturating_add(max_workloads.saturating_mul(names::MAX_ID_LEN + 3))
}

/// Checks that a node's fact always fits in one record, so a node is never
/// left unheard because of what it holds.
pub fn check_fact_fits(node: &str, max_workloads: usize) -> Result<(), FactTooLarge> {
    let worst = worst_fact_bytes(node, max_workloads);
    if worst > crate::MAX_WRITE_BYTES {
        return Err(FactTooLarge { worst });
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "a node holding this many workloads would write facts of up to {worst} bytes, over the \
     {max} a record may take; lower --max-workloads",
    max = crate::MAX_WRITE_BYTES
)]
pub struct FactTooLarge {
    pub worst: usize,
}

impl NodeFact {
    pub fn encode(&self) -> Vec<u8> {
        codec::encode_json(FACT_FORMAT, self)
    }

    pub fn decode(bytes: &[u8]) -> Result<NodeFact, DecodeError> {
        codec::decode_json(FACT_FORMAT, bytes)
    }
}

/// One output of the scheduler.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// The next `sys.specs` offset the scheduler has not stepped. A running
    /// workload whose spec record is below it and has no assignment was left
    /// unassigned on purpose.
    pub specs: u64,
    #[serde(flatten)]
    pub change: Change,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Change {
    /// The node `workload` should run on, or none once it no longer runs.
    Assign {
        workload: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        node: Option<String>,
    },
    /// Every assignment in force. It replaces everything before it.
    Table {
        assignments: BTreeMap<String, String>,
        /// Every group placement in force, by group name.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        groups: BTreeMap<String, BTreeMap<String, String>>,
    },
    /// The node of every member of the group `group`, all in one record so
    /// no reader sees part of a group. No members withdraws the group's
    /// placement. An older agent cannot decode it and passes over it.
    Group {
        group: String,
        #[serde(default)]
        members: BTreeMap<String, String>,
    },
    /// A spec record that changed no assignment.
    Progress,
}

impl Record {
    /// The key the record is written under: the workload, for an assignment.
    pub fn key(&self) -> Option<&str> {
        match &self.change {
            Change::Assign { workload, .. } => Some(workload),
            _ => None,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        codec::encode_json(RECORD_FORMAT, self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Record, DecodeError> {
        codec::decode_json(RECORD_FORMAT, bytes)
    }
}

/// An input the scheduler cannot use. It rejects it and carries on.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refused {
    #[error("not a spec record this build reads: {0}")]
    Spec(DecodeError),
    #[error("not a node fact this build reads: {0}")]
    Fact(DecodeError),
    #[error("the scheduler does not read {0}")]
    Stream(String),
}

/// The scheduler's state: the nodes it has heard from, the workloads that
/// should run and where each is assigned. Every field defaults, so a snapshot
/// from a build with fewer fields restores.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Scheduler {
    /// The latest append time stepped from `sys.facts`. Silence is measured
    /// on it alone, so all of it is timed by the one shard's leader. It is
    /// also the tick clock ([`Scheduler::ticks`]).
    facts_micros: u64,
    /// Whether any fact has been stepped, so the first one ticks. A snapshot
    /// from a build before ticks restores as `false` and ticks once more.
    facts_seen: bool,
    /// The next `sys.specs` offset not stepped.
    specs: u64,
    nodes: BTreeMap<String, Heard>,
    /// Workloads whose last spec record says `running`.
    wanted: BTreeSet<String>,
    assigned: BTreeMap<String, String>,
    /// The group each wanted group member's spec declares. Members are
    /// placed only through [`Change::Group`], never one by one.
    declared: BTreeMap<String, Group>,
    /// The group placements in force, by group name: member to node.
    groups: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
struct Heard {
    at_micros: u64,
    every_ms: u64,
    max_workloads: usize,
    holding: BTreeSet<String>,
    draining: bool,
    blocked: bool,
}

/// A node that takes work, how much, and what it holds already.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Taker {
    max_workloads: usize,
    holding: BTreeSet<String>,
}

impl Scheduler {
    /// Steps the record `payload` at `offset` on `stream`, appended at
    /// `timestamp_micros`, and returns what to output.
    pub fn step(
        &mut self,
        stream: &str,
        offset: u64,
        timestamp_micros: u64,
        payload: &[u8],
    ) -> Result<Vec<Record>, Refused> {
        // Before the clock moves: time passing alone can make a node silent.
        let before = self.takers();
        let running_before = self.groups_running();
        let mut wanted_changed = false;
        let mut spec_step = false;
        let mut tick = false;
        match stream {
            names::SPECS => {
                self.specs = self.specs.max(offset.saturating_add(1));
                spec_step = true;
                let record = SpecRecord::decode(payload).map_err(Refused::Spec)?;
                // The scheduler is placed by first claim wins, never by itself.
                // A skip record is not a spec.
                if record.id != names::SCHEDULER && !record.skipped {
                    let running = record.spec.state == WorkloadState::Running;
                    let group = record.spec.group.filter(|_| running);
                    let declared = match group {
                        Some(group) => self.declared.insert(record.id.clone(), group.clone()),
                        None => self.declared.remove(&record.id),
                    };
                    let now = self.declared.get(&record.id);
                    wanted_changed = declared.as_ref() != now;
                    wanted_changed |= if running {
                        self.wanted.insert(record.id)
                    } else {
                        self.wanted.remove(&record.id)
                    };
                }
            }
            names::FACTS => {
                let fact = NodeFact::decode(payload).map_err(Refused::Fact)?;
                tick = self.ticks(timestamp_micros);
                self.facts_seen = true;
                self.facts_micros = self.facts_micros.max(timestamp_micros);
                let heard = Heard {
                    at_micros: timestamp_micros,
                    every_ms: fact.every_ms,
                    max_workloads: fact.max_workloads,
                    holding: fact.holding,
                    draining: fact.draining,
                    blocked: fact.blocked,
                };
                self.nodes.insert(fact.node, heard);
                self.forget_silent();
            }
            other => return Err(Refused::Stream(other.to_owned())),
        }
        let takers = self.takers();
        let changes =
            if wanted_changed || takers != before || self.groups_running() != running_before {
                self.reconcile(&takers)
            } else {
                Vec::new()
            };
        let specs = self.specs;
        let record = |change| Record { specs, change };
        if tick {
            // The table holds every change of this step too.
            let assignments = self.assigned.clone();
            let groups = self.groups.clone();
            return Ok(vec![record(Change::Table {
                assignments,
                groups,
            })]);
        }
        if changes.is_empty() && spec_step {
            return Ok(vec![record(Change::Progress)]);
        }
        Ok(changes.into_iter().map(record).collect())
    }

    /// Whether a fact appended at `at_micros` is a tick: the first fact, or
    /// one past a multiple of [`TABLE_EVERY_MICROS`] that the largest fact
    /// time so far is not. That time only grows, so a broker clock that
    /// steps back does not tick twice, and it is in the snapshot, so a
    /// replay ticks at the same records. `sys.specs` never ticks.
    fn ticks(&self, at_micros: u64) -> bool {
        !self.facts_seen || at_micros / TABLE_EVERY_MICROS > self.facts_micros / TABLE_EVERY_MICROS
    }

    /// The node each workload is assigned to.
    pub fn assignments(&self) -> &BTreeMap<String, String> {
        &self.assigned
    }

    /// The group placements in force: group name to member to node.
    pub fn groups(&self) -> &BTreeMap<String, BTreeMap<String, String>> {
        &self.groups
    }

    /// Whether the scheduler still remembers `node`.
    pub fn knows(&self, node: &str) -> bool {
        self.nodes.contains_key(node)
    }

    fn after(every_ms: u64, intervals: u64) -> u64 {
        every_ms.saturating_mul(intervals.saturating_mul(1000))
    }

    /// Forgets nodes silent for [`FORGET_INTERVALS`], so the map does not
    /// keep every node id ever seen.
    fn forget_silent(&mut self) {
        let now = self.facts_micros;
        self.nodes.retain(|_, h| {
            now.saturating_sub(h.at_micros) <= Self::after(h.every_ms, FORGET_INTERVALS)
        });
    }

    /// Whether `heard` is past [`SILENT_INTERVALS`] of its interval, by
    /// `sys.facts` time.
    fn silent(&self, heard: &Heard) -> bool {
        self.facts_micros.saturating_sub(heard.at_micros)
            > Self::after(heard.every_ms, SILENT_INTERVALS)
    }

    /// The nodes that take work now: heard from within [`SILENT_INTERVALS`] of
    /// their interval, by `sys.facts` time, and neither draining nor blocked.
    fn takers(&self) -> BTreeMap<String, Taker> {
        self.nodes
            .iter()
            .filter(|(_, h)| !h.draining && !h.blocked && !self.silent(h))
            .map(|(node, h)| {
                let taker = Taker {
                    max_workloads: h.max_workloads,
                    holding: h.holding.clone(),
                };
                (node.clone(), taker)
            })
            .collect()
    }

    /// Drops the assignments of workloads that no longer run, and assigns each
    /// workload with none, or with a node that takes no work, to the least
    /// loaded taker with room, the lowest id first. A node's load is every
    /// workload assigned to it or held by it, so workloads it took by first
    /// claim wins count. A node over its room loses assignments it does not
    /// hold, the highest ids first, until it fits. Nothing else is moved off a
    /// node that still takes work. A workload with nowhere to go keeps its
    /// assignment if its node is gone, which agents pass over, and is
    /// unassigned if it was moved off a full node.
    fn reconcile(&mut self, takers: &BTreeMap<String, Taker>) -> Vec<Change> {
        let mut changed = Vec::new();
        let wanted = &self.wanted;
        let declared = &self.declared;
        // A group member is placed with its group, never on its own.
        self.assigned.retain(|workload, _| {
            let keep = wanted.contains(workload) && !declared.contains_key(workload);
            if !keep {
                changed.push((workload.clone(), None));
            }
            keep
        });
        let mut load: BTreeMap<&str, usize> = takers
            .iter()
            .map(|(node, t)| (node.as_str(), t.holding.len()))
            .collect();
        for members in self.groups.values() {
            add_load(&mut load, takers, members, 1);
        }
        let mut over: Vec<String> = Vec::new();
        for (node, taker) in takers {
            let extra: Vec<&String> = self
                .assigned
                .iter()
                .filter(|(w, n)| *n == node && !taker.holding.contains(*w))
                .map(|(w, _)| w)
                .collect();
            let room = taker.max_workloads.saturating_sub(taker.holding.len());
            over.extend(extra.iter().skip(room).map(|w| (*w).clone()));
            *load.get_mut(node.as_str()).expect("every taker") += extra.len().min(room);
        }
        for workload in &over {
            self.assigned.remove(workload);
        }
        for workload in &self.wanted {
            if self.declared.contains_key(workload) {
                continue;
            }
            if self
                .assigned
                .get(workload)
                .is_some_and(|node| takers.contains_key(node))
            {
                continue;
            }
            let pick = load
                .iter()
                .filter(|&(&node, &l)| l < takers[node].max_workloads)
                .min_by_key(|&(&node, &l)| (l, node))
                .map(|(&node, _)| node);
            let Some(node) = pick else {
                if over.contains(workload) {
                    changed.push((workload.clone(), None));
                }
                continue;
            };
            if !takers[node].holding.contains(workload) {
                *load.get_mut(node).expect("picked from load") += 1;
            }
            self.assigned.insert(workload.clone(), node.to_owned());
            changed.push((workload.clone(), Some(node.to_owned())));
        }
        let mut changes: Vec<Change> = changed
            .into_iter()
            .map(|(workload, node)| Change::Assign { workload, node })
            .collect();
        let mut load: BTreeMap<String, usize> =
            load.into_iter().map(|(n, l)| (n.to_owned(), l)).collect();
        changes.extend(self.place_groups(takers, &mut load));
        changes
    }

    /// The groups every one of whose members is wanted and declares it. Two
    /// such groups under one name are both left out, so neither is placed.
    fn complete_groups(&self) -> BTreeMap<&str, &Group> {
        let mut complete: BTreeMap<&str, Option<&Group>> = BTreeMap::new();
        for group in self.declared.values() {
            let whole = group
                .members
                .iter()
                .all(|m| self.declared.get(m) == Some(group));
            if !whole {
                continue;
            }
            complete
                .entry(&group.name)
                .and_modify(|found| {
                    if *found != Some(group) {
                        *found = None;
                    }
                })
                .or_insert(Some(group));
        }
        complete
            .into_iter()
            .filter_map(|(name, group)| Some((name, group?)))
            .collect()
    }

    /// The node each member of `group` may run on, so no group move takes a
    /// member off a node that may still run it, or be about to (#247).
    ///
    /// A member runs where a node's latest fact says it is held; if two
    /// say so, the newer fact wins, then the lowest id. Silence does not end
    /// a lease, so a silent node's last fact still counts. A member the
    /// group's record places on a node also counts as running there while
    /// that node is silent or blocked, since a claim under way shows in no
    /// fact and such a node may still land one. A draining fact from a node
    /// not silent ends that: an agent says it is draining only once no
    /// claim is under way. A taker needs no hold, as the record stands while
    /// its nodes take work. So a node that takes work and then goes silent
    /// again is held again.
    fn running(&self, group: &Group) -> BTreeMap<String, String> {
        let mut running: BTreeMap<String, (u64, String)> = BTreeMap::new();
        for (node, heard) in &self.nodes {
            for member in group.members.iter().filter(|m| heard.holding.contains(*m)) {
                // Nodes come in id order, so a tie keeps the lowest id.
                let best = running
                    .entry(member.clone())
                    .or_insert_with(|| (heard.at_micros, node.clone()));
                if heard.at_micros > best.0 {
                    *best = (heard.at_micros, node.clone());
                }
            }
        }
        let record = self
            .groups
            .get(&group.name)
            .filter(|r| group.members.iter().eq(r.keys()));
        for (member, node) in record.into_iter().flatten() {
            // Not silent and not blocked means a taker or a settled drain.
            if self
                .nodes
                .get(node)
                .is_some_and(|h| h.blocked || self.silent(h))
            {
                running
                    .entry(member.clone())
                    .or_insert_with(|| (0, node.clone()));
            }
        }
        running
            .into_iter()
            .map(|(member, (_, node))| (member, node))
            .collect()
    }

    /// Where the members of every complete group run, so a member stopping
    /// on a node that takes no work still reconsiders its group.
    fn groups_running(&self) -> Vec<BTreeMap<String, String>> {
        if self.declared.is_empty() {
            return Vec::new();
        }
        self.complete_groups()
            .values()
            .map(|g| self.running(g))
            .collect()
    }

    /// Places each complete group with one record, or not at all. A group
    /// whose members all stopped running, or never ran, is placed on the
    /// least loaded takers with room, the lowest id first, and only if every
    /// member fits. While members run, a new placement keeps each of them on
    /// its node, which is how a member freed elsewhere is placed again. A
    /// member on a dead node is held there until the node is forgotten (see
    /// [`Self::running`]). A group with a member running off its placement
    /// is split and is not placed again until no member runs. A placement
    /// whose members all run where it says or sit on takers is left alone.
    /// The placement of a group no longer complete is withdrawn.
    fn place_groups(
        &mut self,
        takers: &BTreeMap<String, Taker>,
        load: &mut BTreeMap<String, usize>,
    ) -> Vec<Change> {
        let mut changes = Vec::new();
        let complete: BTreeMap<String, Group> = self
            .complete_groups()
            .into_iter()
            .map(|(n, g)| (n.to_owned(), g.clone()))
            .collect();
        let gone: Vec<String> = self
            .groups
            .keys()
            .filter(|name| {
                complete.get(*name).is_none_or(|g| {
                    // A placement for another member list is not this group's.
                    !g.members.iter().eq(self.groups[*name].keys())
                })
            })
            .cloned()
            .collect();
        for name in gone {
            if let Some(members) = self.groups.remove(&name) {
                add_load_owned(load, takers, &members, -1);
            }
            changes.push(Change::Group {
                group: name,
                members: BTreeMap::new(),
            });
        }
        for (name, group) in complete {
            let running = self.running(&group);
            let current = self.groups.get(&name);
            // With no record, running members count as matching it: they stay
            // where they are and the rest is placed beside them.
            if current.is_some_and(|c| {
                !running.is_empty() && !running.iter().all(|(m, n)| c.get(m) == Some(n))
            }) {
                continue;
            }
            if let Some(current) = current
                && current
                    .iter()
                    .all(|(m, n)| running.get(m) == Some(n) || takers.contains_key(n))
            {
                continue;
            }
            let mut trial = load.clone();
            if let Some(current) = current {
                add_load_owned(&mut trial, takers, current, -1);
            }
            let mut placed = running.clone();
            let mut fits = true;
            for member in group.members.iter().filter(|m| !running.contains_key(*m)) {
                let pick = trial
                    .iter()
                    .filter(|&(node, &l)| l < takers[node].max_workloads)
                    .min_by_key(|&(node, &l)| (l, node))
                    .map(|(node, _)| node.clone());
                let Some(node) = pick else {
                    fits = false;
                    break;
                };
                *trial.get_mut(&node).expect("picked from the load") += 1;
                placed.insert(member.clone(), node);
            }
            if !fits || current == Some(&placed) {
                continue;
            }
            *load = trial;
            self.groups.insert(name.clone(), placed.clone());
            changes.push(Change::Group {
                group: name,
                members: placed,
            });
        }
        changes
    }

    /// The state as JSON, for snapshots and `inspect`.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("scheduler state is always valid JSON")
    }

    pub fn decode(bytes: &[u8]) -> Result<Scheduler, DecodeError> {
        serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))
    }
}

/// Adds `by` to the load of each taker that `members` places a member on
/// and that does not hold it already.
fn add_load(
    load: &mut BTreeMap<&str, usize>,
    takers: &BTreeMap<String, Taker>,
    members: &BTreeMap<String, String>,
    by: isize,
) {
    for (member, node) in members {
        if let Some((taker, t)) = takers.get_key_value(node)
            && !t.holding.contains(member)
            && let Some(l) = load.get_mut(taker.as_str())
        {
            *l = l.saturating_add_signed(by);
        }
    }
}

fn add_load_owned(
    load: &mut BTreeMap<String, usize>,
    takers: &BTreeMap<String, Taker>,
    members: &BTreeMap<String, String>,
    by: isize,
) {
    for (member, node) in members {
        if takers
            .get(node)
            .is_some_and(|t| !t.holding.contains(member))
            && let Some(l) = load.get_mut(node)
        {
            *l = l.saturating_add_signed(by);
        }
    }
}

#[cfg(test)]
mod tests;
