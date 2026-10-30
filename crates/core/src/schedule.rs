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

use crate::spec::{SpecRecord, WorkloadState};
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
/// How often, in input time, the scheduler writes its whole table again, so
/// a reader can start from the latest table and older output can be trimmed.
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
    /// The ids of the workloads it holds, at most `max_workloads` of them.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub holding: BTreeSet<String>,
    /// It is draining and takes no work.
    #[serde(default)]
    pub draining: bool,
    /// It cannot claim now, such as when it cannot read its drain mark.
    #[serde(default)]
    pub blocked: bool,
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
    /// The latest append time stepped from either input.
    now_micros: u64,
    /// The latest append time stepped from `sys.facts`. Silence is measured
    /// on it alone, so all of it is timed by the one shard's leader.
    facts_micros: u64,
    /// The next `sys.specs` offset not stepped.
    specs: u64,
    /// When the last table was written, by `now_micros`.
    table_micros: Option<u64>,
    nodes: BTreeMap<String, Heard>,
    /// Workloads whose last spec record says `running`.
    wanted: BTreeSet<String>,
    assigned: BTreeMap<String, String>,
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
        self.now_micros = self.now_micros.max(timestamp_micros);
        let mut wanted_changed = false;
        let mut spec_step = false;
        match stream {
            names::SPECS => {
                self.specs = self.specs.max(offset.saturating_add(1));
                spec_step = true;
                let record = SpecRecord::decode(payload).map_err(Refused::Spec)?;
                // The scheduler is placed by first claim wins, never by itself.
                if record.id != names::SCHEDULER {
                    wanted_changed = if record.spec.state == WorkloadState::Running {
                        self.wanted.insert(record.id)
                    } else {
                        self.wanted.remove(&record.id)
                    };
                }
            }
            names::FACTS => {
                let fact = NodeFact::decode(payload).map_err(Refused::Fact)?;
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
        let changes = if wanted_changed || takers != before {
            self.reconcile(&takers)
        } else {
            Vec::new()
        };
        let specs = self.specs;
        let record = |change| Record { specs, change };
        let table_due = self
            .table_micros
            .is_none_or(|at| self.now_micros.saturating_sub(at) >= TABLE_EVERY_MICROS);
        if table_due {
            // The table holds every change of this step too.
            self.table_micros = Some(self.now_micros);
            let assignments = self.assigned.clone();
            return Ok(vec![record(Change::Table { assignments })]);
        }
        if changes.is_empty() && spec_step {
            return Ok(vec![record(Change::Progress)]);
        }
        Ok(changes
            .into_iter()
            .map(|(workload, node)| record(Change::Assign { workload, node }))
            .collect())
    }

    /// The node each workload is assigned to.
    pub fn assignments(&self) -> &BTreeMap<String, String> {
        &self.assigned
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

    /// The nodes that take work now: heard from within [`SILENT_INTERVALS`] of
    /// their interval, by `sys.facts` time, and neither draining nor blocked.
    fn takers(&self) -> BTreeMap<String, Taker> {
        self.nodes
            .iter()
            .filter(|(_, h)| {
                let silent = Self::after(h.every_ms, SILENT_INTERVALS);
                !h.draining && !h.blocked && self.facts_micros.saturating_sub(h.at_micros) <= silent
            })
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
    fn reconcile(&mut self, takers: &BTreeMap<String, Taker>) -> Vec<(String, Option<String>)> {
        let mut changed = Vec::new();
        let wanted = &self.wanted;
        self.assigned.retain(|workload, _| {
            let keep = wanted.contains(workload);
            if !keep {
                changed.push((workload.clone(), None));
            }
            keep
        });
        let mut load: BTreeMap<&str, usize> = takers
            .iter()
            .map(|(node, t)| (node.as_str(), t.holding.len()))
            .collect();
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
        changed
    }

    /// The state as JSON, for snapshots and `inspect`.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("scheduler state is always valid JSON")
    }

    pub fn decode(bytes: &[u8]) -> Result<Scheduler, DecodeError> {
        serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))
    }
}

#[cfg(test)]
mod tests;
