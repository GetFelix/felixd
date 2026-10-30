//! The scheduler's assignments, as a node follows them, and the node facts it
//! appends to `sys.facts` while a scheduler runs. Neither touches the lease
//! or the fence: an assignment only says which node should try a claim.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Result, anyhow};
use felix_client::StreamRecord;
use felixd_core::schedule::{Change, NodeFact, Record};
use felixd_core::{names, output};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::felix::{Felix, Trimmed};

/// How often a node reads new assignments, and looks whether its facts
/// changed.
const READ_EVERY: Duration = Duration::from_millis(500);
/// How long one read of a page, or one append, may take. A read that takes
/// longer is tried again from where it was.
const WITHIN: Duration = Duration::from_secs(10);
/// How many offsets a reader looks back at once for the latest table.
const BACK_WINDOW: u64 = 256;

/// The node a workload is assigned to, and the scheduler output offset that
/// says so.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placed {
    pub node: String,
    pub offset: u64,
}

/// What a reader has folded from the scheduler's output so far.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Followed {
    pub table: HashMap<String, Placed>,
    /// The next `sys.specs` offset the scheduler had not stepped, as of the
    /// last record read.
    pub specs: u64,
    /// Where the next read starts, once the latest table is found.
    next: Option<u64>,
    /// While looking back for the latest table: where the next window ends.
    back: Option<u64>,
}

impl Followed {
    /// Folds the record at `offset`. Records that are not the scheduler's
    /// outputs, such as claims, are passed over.
    pub(crate) fn fold(&mut self, offset: u64, record: &[u8]) {
        let Some(found) = decode(record) else {
            return;
        };
        self.specs = self.specs.max(found.specs);
        match found.change {
            Change::Assign {
                workload,
                node: Some(node),
            } => {
                self.table.insert(workload, Placed { node, offset });
            }
            Change::Assign {
                workload,
                node: None,
            } => {
                self.table.remove(&workload);
            }
            Change::Table { assignments } => {
                self.table = assignments
                    .into_iter()
                    .map(|(workload, node)| (workload, Placed { node, offset }))
                    .collect();
            }
            Change::Progress => {}
        }
    }
}

fn decode(record: &[u8]) -> Option<Record> {
    let Ok(output::Record::Output(_, addressed)) = output::decode(record) else {
        return None;
    };
    Record::decode(addressed.payload).ok()
}

fn is_table(record: &StreamRecord) -> bool {
    decode(&record.payload).is_some_and(|r| matches!(r.change, Change::Table { .. }))
}

/// Where a reader gets the scheduler's output. The agent reads Felix; the
/// unit tests a stream in memory.
pub(crate) trait Source {
    fn tail(&self) -> impl Future<Output = Result<u64>> + Send;
    /// The records at `from..end`, and whether nothing is held below them.
    fn back(
        &self,
        from: u64,
        end: u64,
    ) -> impl Future<Output = Result<(Vec<StreamRecord>, bool)>> + Send;
    /// One page from `from`, and the offset after it.
    fn page(
        &self,
        from: u64,
        end: u64,
    ) -> impl Future<Output = Result<(Vec<StreamRecord>, u64)>> + Send;
}

struct Stream<'a>(&'a Felix, String);

impl Source for Stream<'_> {
    async fn tail(&self) -> Result<u64> {
        self.0.read_tail(&self.1).await
    }

    async fn back(&self, from: u64, end: u64) -> Result<(Vec<StreamRecord>, bool)> {
        self.0.read_back(&self.1, from, end).await
    }

    async fn page(&self, from: u64, end: u64) -> Result<(Vec<StreamRecord>, u64)> {
        self.0.read_page(&self.1, from, end).await
    }
}

async fn within<T>(call: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(WITHIN, call)
        .await
        .map_err(|_| anyhow!("a read of the scheduler's output took over {WITHIN:?}"))?
}

/// Reads what is new into `followed`, a page at a time. The first read looks
/// back from the tail for the latest table and starts there, so nothing
/// before it is read. Progress is kept in `followed` whenever a read fails,
/// so the next call carries on from there; `changed` runs after each page.
pub(crate) async fn read_new(
    source: &impl Source,
    followed: &mut Followed,
    mut changed: impl FnMut(&Followed),
) -> Result<()> {
    let tail = within(source.tail()).await?;
    while followed.next.is_none() {
        let end = followed.back.unwrap_or(tail);
        let from = end.saturating_sub(BACK_WINDOW);
        if end == 0 {
            followed.next = Some(0);
            break;
        }
        let (records, ended) = within(source.back(from, end)).await?;
        if let Some(table) = records.iter().rev().find(|r| is_table(r)) {
            followed.next = Some(table.offset);
        } else if ended {
            followed.next = Some(records.first().map_or(from, |r| r.offset));
        } else {
            followed.back = Some(from);
        }
    }
    let mut next = followed.next.unwrap_or(0);
    while next < tail {
        let (page, after) = match within(source.page(next, tail)).await {
            Ok(read) => read,
            Err(error) => match Trimmed::of(&error).map(|t| t.oldest) {
                Some(oldest) if oldest > next => {
                    next = oldest;
                    followed.next = Some(next);
                    continue;
                }
                _ => return Err(error),
            },
        };
        for record in &page {
            followed.fold(record.offset, &record.payload);
        }
        next = after;
        followed.next = Some(next);
        changed(followed);
    }
    Ok(())
}

/// Every assignment in force, read from the scheduler's output from its
/// latest table on.
pub async fn read(felix: &Felix) -> Result<Followed> {
    let mut followed = Followed::default();
    let source = Stream(felix, names::output_stream(names::SCHEDULER));
    read_new(&source, &mut followed, |_| {}).await?;
    Ok(followed)
}

/// What a node knows of the scheduler: whether it runs, when its lease
/// expires, and what it has assigned.
#[derive(Debug)]
pub(crate) struct Assignments {
    followed: Mutex<Followed>,
    /// Bumped on every change, so a task leaving a workload to its assignee
    /// looks again.
    changed: watch::Sender<u64>,
    /// Whether the `workloads` cache says the scheduler runs.
    scheduler: watch::Sender<bool>,
    /// When the scheduler's lease expires, by the broker's clock in
    /// milliseconds (0 never), or `None` when it has no lease.
    holder: Mutex<Option<u64>>,
}

impl Assignments {
    pub(crate) fn new() -> Assignments {
        Assignments {
            followed: Mutex::new(Followed::default()),
            changed: watch::channel(0).0,
            scheduler: watch::channel(false).0,
            holder: Mutex::new(None),
        }
    }

    pub(crate) fn set_scheduler(&self, runs: bool) {
        if self
            .scheduler
            .send_if_modified(|now| std::mem::replace(now, runs) != runs)
        {
            self.bump();
        }
    }

    pub(crate) fn set_holder(&self, expires_at_millis: Option<u64>) {
        *self.holder.lock().unwrap_or_else(|e| e.into_inner()) = expires_at_millis;
        self.bump();
    }

    /// Whether the scheduler runs and its lease has a live holder at
    /// `now_millis`. Only then does a node wait on its assignments.
    pub(crate) fn healthy(&self, now_millis: u64) -> bool {
        let holder = *self.holder.lock().unwrap_or_else(|e| e.into_inner());
        *self.scheduler.borrow() && holder.is_some_and(|at| at == 0 || at > now_millis)
    }

    pub(crate) fn scheduler_changes(&self) -> watch::Receiver<bool> {
        self.scheduler.subscribe()
    }

    /// `workload`'s assignment, and how far the scheduler has read
    /// `sys.specs`.
    pub(crate) fn get(&self, workload: &str) -> (Option<Placed>, u64) {
        let followed = self.lock();
        (followed.table.get(workload).cloned(), followed.specs)
    }

    pub(crate) fn changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Followed> {
        self.followed.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn bump(&self) {
        self.changed.send_modify(|n| *n += 1);
    }
}

/// Follows the scheduler's output while the scheduler runs, from where it
/// left off: neither a failed read nor the scheduler stopping and starting
/// sends it back to the start.
pub(crate) async fn follow(felix: &Felix, assignments: &Assignments) {
    let source = Stream(felix, names::output_stream(names::SCHEDULER));
    let mut scheduler = assignments.scheduler_changes();
    let mut followed = assignments.lock().clone();
    let mut failing = false;
    loop {
        if scheduler.wait_for(|runs| *runs).await.is_err() {
            return;
        }
        let read = read_new(&source, &mut followed, |now| {
            *assignments.lock() = now.clone();
            assignments.bump();
        })
        .await;
        match read {
            Ok(()) => failing = false,
            // Keeps what it has. Logged once per run of failures.
            Err(error) if !std::mem::replace(&mut failing, true) => {
                tracing::warn!(error = %format!("{error:#}"), "could not read the scheduler's assignments");
            }
            Err(_) => {}
        }
        tokio::select! {
            () = tokio::time::sleep(READ_EVERY) => {}
            _ = scheduler.changed() => {}
        }
    }
}

/// Appends `fact()` to `sys.facts` while the scheduler runs: when it starts
/// to run, within [`READ_EVERY`] of the fact changing, and every `every`
/// otherwise.
pub(crate) async fn write_facts(
    felix: &Felix,
    assignments: &Assignments,
    every: Duration,
    fact: impl Fn() -> NodeFact,
) {
    let mut scheduler = assignments.scheduler_changes();
    let mut failing = false;
    loop {
        if scheduler.wait_for(|runs| *runs).await.is_err() {
            return;
        }
        let mut last: Option<(NodeFact, Instant)> = None;
        while *scheduler.borrow_and_update() {
            let now = fact();
            if due(last.as_ref(), &now, every) {
                let record = now.encode();
                match tokio::time::timeout(WITHIN, felix.publish(names::FACTS, record)).await {
                    Ok(Ok(())) => {
                        failing = false;
                        last = Some((now, Instant::now()));
                    }
                    Ok(Err(error)) if !std::mem::replace(&mut failing, true) => {
                        tracing::warn!(error = %format!("{error:#}"), "could not append this node's facts to {}", names::FACTS);
                    }
                    _ => {}
                }
            }
            tokio::select! {
                () = tokio::time::sleep(READ_EVERY) => {}
                _ = scheduler.changed() => {}
            }
        }
    }
}

/// Whether a fact is to be written: none yet, it changed, or `every` has
/// passed since the last.
fn due(last: Option<&(NodeFact, Instant)>, now: &NodeFact, every: Duration) -> bool {
    last.is_none_or(|(fact, at)| fact != now || at.elapsed() >= every)
}

#[cfg(test)]
mod tests;
