//! The scheduler's assignments, as a node follows them, and the node facts it
//! appends to `sys.facts` while a scheduler runs. Neither touches the lease
//! or the fence: an assignment only says which node should try a claim.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use felixd_core::schedule::{Assignment, NodeFact};
use felixd_core::{names, output};
use tokio::sync::watch;

use crate::felix::{Felix, Trimmed};

/// How often a node reads new assignments.
const READ_EVERY: Duration = Duration::from_millis(500);
/// How long one read or append may take.
const WITHIN: Duration = Duration::from_secs(5);

/// The node a workload is assigned to, and the scheduler output offset that
/// says so.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placed {
    pub node: String,
    pub offset: u64,
}

/// What a node knows of the scheduler: whether one runs and its latest
/// assignment for each workload.
#[derive(Debug)]
pub(crate) struct Assignments {
    table: Mutex<HashMap<String, Placed>>,
    /// Bumped on every change, so a task leaving a workload to its assignee
    /// looks again.
    changed: watch::Sender<u64>,
    /// Whether the `workloads` cache says the scheduler runs.
    scheduler: watch::Sender<bool>,
}

impl Assignments {
    pub(crate) fn new() -> Assignments {
        Assignments {
            table: Mutex::new(HashMap::new()),
            changed: watch::channel(0).0,
            scheduler: watch::channel(false).0,
        }
    }

    pub(crate) fn set_scheduler(&self, runs: bool) {
        self.scheduler.send_if_modified(|now| {
            let changed = *now != runs;
            *now = runs;
            changed
        });
    }

    pub(crate) fn scheduler(&self) -> bool {
        *self.scheduler.borrow()
    }

    pub(crate) fn scheduler_changes(&self) -> watch::Receiver<bool> {
        self.scheduler.subscribe()
    }

    pub(crate) fn get(&self, workload: &str) -> Option<Placed> {
        self.lock().get(workload).cloned()
    }

    pub(crate) fn changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Placed>> {
        self.table.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn replace(&self, table: HashMap<String, Placed>) {
        *self.lock() = table;
        self.changed.send_modify(|n| *n += 1);
    }
}

/// Folds the scheduler output record at `offset` into `table`. Records that
/// are not assignments, such as claims, are passed over.
pub(crate) fn fold(table: &mut HashMap<String, Placed>, offset: u64, record: &[u8]) {
    let Ok(output::Record::Output(_, addressed)) = output::decode(record) else {
        return;
    };
    let Ok(assignment) = Assignment::decode(addressed.payload) else {
        return;
    };
    match assignment.node {
        Some(node) => {
            table.insert(assignment.workload, Placed { node, offset });
        }
        None => {
            table.remove(&assignment.workload);
        }
    }
}

/// Every assignment in force, read from the scheduler's output stream. A
/// stream trimmed below its start is read from where it begins, so an
/// assignment written before that is missing, which agents treat like no
/// assignment.
pub async fn read(felix: &Felix) -> Result<HashMap<String, Placed>> {
    let mut table = HashMap::new();
    read_from(felix, &mut table, 0).await?;
    Ok(table)
}

/// Reads the scheduler's output from `from` into `table` and returns the
/// next offset to read.
async fn read_from(felix: &Felix, table: &mut HashMap<String, Placed>, from: u64) -> Result<u64> {
    let stream = names::output_stream(names::SCHEDULER);
    let tail = felix.read_tail(&stream).await?;
    if tail <= from {
        return Ok(from);
    }
    let records = match felix.read_range(&stream, from, tail).await {
        Ok(records) => records,
        Err(error) => match Trimmed::of(&error).map(|t| t.oldest) {
            Some(oldest) if oldest > from => felix.read_range(&stream, oldest, tail).await?,
            _ => return Err(error),
        },
    };
    for record in records {
        fold(table, record.offset, &record.payload);
    }
    Ok(tail)
}

/// Follows the scheduler's output while the scheduler runs. When it stops,
/// the assignments are dropped, so every workload is first claim wins again.
pub(crate) async fn follow(felix: &Felix, assignments: &Assignments) {
    let mut scheduler = assignments.scheduler_changes();
    loop {
        if scheduler.wait_for(|runs| *runs).await.is_err() {
            return;
        }
        let mut table = HashMap::new();
        let mut next = 0;
        let mut failing = false;
        while *scheduler.borrow_and_update() {
            match tokio::time::timeout(WITHIN, read_from(felix, &mut table, next)).await {
                Ok(Ok(read)) => {
                    failing = false;
                    if read != next {
                        next = read;
                        assignments.replace(table.clone());
                    }
                }
                // Keeps what it has. Logged once per run of failures.
                Ok(Err(error)) if !std::mem::replace(&mut failing, true) => {
                    tracing::warn!(error = %format!("{error:#}"), "could not read the scheduler's assignments");
                }
                _ => {}
            }
            tokio::select! {
                () = tokio::time::sleep(READ_EVERY) => {}
                _ = scheduler.changed() => {}
            }
        }
        assignments.replace(HashMap::new());
    }
}

/// Appends `fact()` to `sys.facts` every `every` while the scheduler runs,
/// and at once when it starts to run or `draining` changes.
pub(crate) async fn write_facts(
    felix: &Felix,
    assignments: &Assignments,
    mut draining: watch::Receiver<bool>,
    every: Duration,
    fact: impl Fn(bool) -> NodeFact,
) {
    let mut scheduler = assignments.scheduler_changes();
    let mut failing = false;
    loop {
        if scheduler.wait_for(|runs| *runs).await.is_err() {
            return;
        }
        let record = fact(*draining.borrow_and_update()).encode();
        match tokio::time::timeout(WITHIN, felix.publish(names::FACTS, record)).await {
            Ok(Ok(())) => failing = false,
            Ok(Err(error)) if !std::mem::replace(&mut failing, true) => {
                tracing::warn!(error = %format!("{error:#}"), "could not append this node's facts to {}", names::FACTS);
            }
            _ => {}
        }
        tokio::select! {
            () = tokio::time::sleep(every) => {}
            _ = draining.changed() => {}
            _ = scheduler.changed() => {}
        }
    }
}

#[cfg(test)]
mod tests;
