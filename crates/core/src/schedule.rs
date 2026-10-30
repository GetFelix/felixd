//! The scheduler: a workload that reads spec records and node facts and
//! writes which node each workload should run on (#60).
//!
//! It is an ordinary felixd workload with two inputs, [`names::SPECS`] and
//! [`names::FACTS`], so its assignments fail over, are written exactly once
//! and replay like any output. Its clock is the append time of the inputs it
//! has stepped, never a wall clock, so a replay assigns the same way.
//! Agents claim what is assigned to them, still through the lease, and fall
//! back to first claim wins when an assignment is missing or stale
//! ([`crate::placement`]). An assignment can delay work but never decides
//! whether output is correct.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::spec::{SpecRecord, WorkloadState};
use crate::{DecodeError, codec, names};

/// The `sys.facts` record format this build writes and reads.
const FACT_FORMAT: u32 = 1;
/// The assignment format.
const ASSIGNMENT_FORMAT: u32 = 1;

/// How many of its own fact intervals a node may stay silent before the
/// scheduler counts it as gone.
pub const SILENT_INTERVALS: u64 = 3;

/// A record on `sys.facts`: what a node says about itself. Agents append one
/// every `every_ms` while a scheduler runs, and at once when they start or
/// stop draining.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeFact {
    pub node: String,
    /// How often the node appends a fact, in milliseconds. After
    /// [`SILENT_INTERVALS`] of these with none, the scheduler counts it as
    /// gone.
    pub every_ms: u64,
    /// The most workloads it holds.
    pub max_workloads: usize,
    /// It is draining and takes no work.
    #[serde(default)]
    pub draining: bool,
}

impl NodeFact {
    pub fn encode(&self) -> Vec<u8> {
        codec::encode_json(FACT_FORMAT, self)
    }

    pub fn decode(bytes: &[u8]) -> Result<NodeFact, DecodeError> {
        codec::decode_json(FACT_FORMAT, bytes)
    }
}

/// One output of the scheduler, under the workload's id as its key: the node
/// the workload should run on, or none once it no longer runs. The latest
/// one per workload on the scheduler's output stream is in force.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub workload: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

impl Assignment {
    pub fn encode(&self) -> Vec<u8> {
        codec::encode_json(ASSIGNMENT_FORMAT, self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Assignment, DecodeError> {
        codec::decode_json(ASSIGNMENT_FORMAT, bytes)
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
/// should run and where each is assigned.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scheduler {
    /// The latest append time stepped, in microseconds since the Unix epoch.
    now_micros: u64,
    nodes: BTreeMap<String, Heard>,
    /// Workloads whose last spec record says `running`.
    wanted: BTreeSet<String>,
    assigned: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Heard {
    at_micros: u64,
    every_ms: u64,
    max_workloads: usize,
    draining: bool,
}

impl Scheduler {
    /// Steps the record `payload`, appended to `stream` at `timestamp_micros`,
    /// and returns the assignments that changed.
    pub fn step(
        &mut self,
        stream: &str,
        timestamp_micros: u64,
        payload: &[u8],
    ) -> Result<Vec<Assignment>, Refused> {
        // Before the clock moves: time passing alone can make a node silent.
        let before = self.takers();
        self.now_micros = self.now_micros.max(timestamp_micros);
        let mut wanted_changed = false;
        match stream {
            names::SPECS => {
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
                let heard = Heard {
                    at_micros: timestamp_micros,
                    every_ms: fact.every_ms,
                    max_workloads: fact.max_workloads,
                    draining: fact.draining,
                };
                self.nodes.insert(fact.node, heard);
            }
            other => return Err(Refused::Stream(other.to_owned())),
        }
        let takers = self.takers();
        if !wanted_changed && takers == before {
            return Ok(Vec::new());
        }
        Ok(self.reconcile(&takers))
    }

    /// The node each workload is assigned to.
    pub fn assignments(&self) -> &BTreeMap<String, String> {
        &self.assigned
    }

    /// The nodes that take work now, with how many workloads each takes: heard
    /// from within [`SILENT_INTERVALS`] of their interval, and not draining.
    fn takers(&self) -> BTreeMap<String, usize> {
        self.nodes
            .iter()
            .filter(|(_, h)| {
                let silent = h.every_ms.saturating_mul(SILENT_INTERVALS * 1000);
                !h.draining && self.now_micros.saturating_sub(h.at_micros) <= silent
            })
            .map(|(node, h)| (node.clone(), h.max_workloads))
            .collect()
    }

    /// Drops the assignments of workloads that no longer run, and assigns
    /// each workload with none, or with a node that takes no work, to the
    /// taker with the fewest assignments and room for one more, the lowest
    /// id first. Nothing is moved off a node that still takes work. A
    /// workload with nowhere to go keeps its assignment, which agents then
    /// find stale.
    fn reconcile(&mut self, takers: &BTreeMap<String, usize>) -> Vec<Assignment> {
        let mut changed = Vec::new();
        let wanted = &self.wanted;
        self.assigned.retain(|workload, _| {
            let keep = wanted.contains(workload);
            if !keep {
                changed.push(Assignment {
                    workload: workload.clone(),
                    node: None,
                });
            }
            keep
        });
        let mut load: BTreeMap<&str, usize> = takers.keys().map(|n| (n.as_str(), 0)).collect();
        for node in self.assigned.values() {
            if let Some(count) = load.get_mut(node.as_str()) {
                *count += 1;
            }
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
                .filter(|&(&node, &count)| count < takers[node])
                .min_by_key(|&(&node, &count)| (count, node))
                .map(|(&node, _)| node);
            let Some(node) = pick else {
                continue;
            };
            *load.get_mut(node).expect("picked from load") += 1;
            self.assigned.insert(workload.clone(), node.to_owned());
            changed.push(Assignment {
                workload: workload.clone(),
                node: Some(node.to_owned()),
            });
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
