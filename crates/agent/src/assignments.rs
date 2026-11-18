//! The scheduler's assignments, as a node follows them, and the node facts it
//! appends to `sys.facts` while a scheduler runs. Neither touches the lease
//! or the fence: an assignment only says which node should try a claim.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Result, anyhow};
use felix_client::StreamRecord;
use felixd_core::lease::LeaseTiming;
use felixd_core::schedule::{Change, NodeFact, Record};
use felixd_core::spec::Group;
use felixd_core::{names, output};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::felix::{Felix, Trimmed};

/// How often a node reads new assignments, and looks whether its facts
/// changed.
const READ_EVERY: Duration = Duration::from_millis(500);
/// How long one read of a page, or one append, may take. A read that takes
/// longer is tried again from where it was.
pub(crate) const WITHIN: Duration = Duration::from_secs(10);
/// How many offsets a reader looks back at once for the latest table.
const BACK_WINDOW: u64 = 256;

/// The node a workload is assigned to, the scheduler output offset that
/// says so, and the epoch of the scheduler holder that wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placed {
    pub node: String,
    pub offset: u64,
    pub epoch: u64,
}

/// One `group` record: the node of each member, the scheduler output
/// offset that says so, and the epoch of its writer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupPlacement {
    pub members: BTreeMap<String, String>,
    pub offset: u64,
    pub epoch: u64,
}

/// What a reader has folded from the scheduler's output so far.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Followed {
    pub table: HashMap<String, Placed>,
    /// The group placements in force, by group name.
    pub groups: HashMap<String, GroupPlacement>,
    /// The latest relist nonce the scheduler answered, by node.
    pub relisted: HashMap<String, u64>,
    /// The next `sys.specs` offset the scheduler had not stepped, as of the
    /// last record read.
    pub specs: u64,
    /// Where the next read starts, once the latest table is found.
    next: Option<u64>,
    /// While looking back for the latest table: where the next window ends.
    back: Option<u64>,
    /// Pages were folded that `changed` has not been given yet, kept across
    /// a failed read.
    unseen: bool,
    /// The tail a read is heading for. Kept until a read reaches it, so a
    /// log that keeps growing cannot move the target out from under retries.
    target: Option<u64>,
}

impl Followed {
    /// Folds the record at `offset`. Records that are not the scheduler's
    /// outputs, such as claims, are passed over.
    pub(crate) fn fold(&mut self, offset: u64, record: &[u8]) {
        let Some((epoch, found)) = decode(record) else {
            return;
        };
        self.specs = self.specs.max(found.specs);
        match found.change {
            Change::Assign {
                workload,
                node: Some(node),
            } => {
                self.table.insert(
                    workload,
                    Placed {
                        node,
                        offset,
                        epoch,
                    },
                );
            }
            Change::Assign {
                workload,
                node: None,
            } => {
                self.table.remove(&workload);
            }
            Change::Group { group, members } if members.is_empty() => {
                self.groups.remove(&group);
            }
            Change::Group { group, members } => {
                let placed = GroupPlacement {
                    members,
                    offset,
                    epoch,
                };
                self.groups.insert(group, placed);
            }
            Change::Table {
                assignments,
                groups,
            } => {
                self.groups = groups
                    .into_iter()
                    .map(|(group, members)| {
                        let placed = GroupPlacement {
                            members,
                            offset,
                            epoch,
                        };
                        (group, placed)
                    })
                    .collect();
                self.table = assignments
                    .into_iter()
                    .map(|(workload, node)| {
                        let placed = Placed {
                            node,
                            offset,
                            epoch,
                        };
                        (workload, placed)
                    })
                    .collect();
            }
            Change::Relisted { node, relist } => {
                self.relisted.insert(node, relist);
            }
            Change::Progress => {}
        }
    }

    /// The record in force for `group`, if it lists exactly its members.
    pub fn group_placement(&self, group: &Group) -> Option<&GroupPlacement> {
        let placement = self.groups.get(&group.name)?;
        placement
            .members
            .keys()
            .eq(group.members.iter())
            .then_some(placement)
    }

    /// The node of each member in the record in force for `group`.
    pub fn group_record(&self, group: &Group) -> Option<&BTreeMap<String, String>> {
        self.group_placement(group).map(|p| &p.members)
    }

    /// Where the record in force for `group` places `member`. A record for
    /// another member list under the same name places nobody.
    pub fn group_placed(&self, member: &str, group: &Group) -> Option<Placed> {
        let placement = self.group_placement(group)?;
        Some(Placed {
            node: placement.members.get(member)?.clone(),
            offset: placement.offset,
            epoch: placement.epoch,
        })
    }
}

/// A scheduler output and the epoch of the holder that wrote it.
fn decode(record: &[u8]) -> Option<(u64, Record)> {
    let Ok(output::Record::Output(written, addressed)) = output::decode(record) else {
        return None;
    };
    Some((written.epoch, Record::decode(addressed.payload).ok()?))
}

fn is_table(record: &StreamRecord) -> bool {
    decode(&record.payload).is_some_and(|(_, r)| matches!(r.change, Change::Table { .. }))
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
/// so the next call carries on from there. `changed` runs only once the
/// read reaches the tail it saw when it started, and that tail is kept across
/// failed reads, so nothing acts on a view that holds a record a later one,
/// already written, replaces, and a growing log cannot keep the read short.
pub(crate) async fn read_new(
    source: &impl Source,
    followed: &mut Followed,
    mut changed: impl FnMut(&Followed),
) -> Result<()> {
    let tail = match followed.target {
        Some(tail) => tail,
        None => {
            let tail = within(source.tail()).await?;
            followed.target = Some(tail);
            tail
        }
    };
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
        followed.unseen = true;
    }
    followed.target = None;
    if std::mem::take(&mut followed.unseen) {
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
    /// The group each running workload's spec declares, from the
    /// `workloads` cache, so a node can tell when a whole group is admitted.
    declared: Mutex<HashMap<String, Group>>,
    /// Replaces the view at the given later lock, as [`follow`] may.
    #[cfg(test)]
    swap_at_lock: Mutex<Option<(usize, Followed)>>,
}

impl Assignments {
    pub(crate) fn new() -> Assignments {
        Assignments {
            followed: Mutex::new(Followed::default()),
            changed: watch::channel(0).0,
            scheduler: watch::channel(false).0,
            holder: Mutex::new(None),
            declared: Mutex::new(HashMap::new()),
            #[cfg(test)]
            swap_at_lock: Mutex::new(None),
        }
    }

    /// Records the group `workload`'s running spec declares, or none.
    pub(crate) fn set_declared(&self, workload: &str, group: Option<Group>) {
        let mut declared = self.declared.lock().unwrap_or_else(|e| e.into_inner());
        let before = match group {
            Some(group) => declared
                .insert(workload.to_owned(), group.clone())
                .filter(|b| *b == group)
                .is_some(),
            None => declared.remove(workload).is_none(),
        };
        drop(declared);
        if !before {
            self.bump();
        }
    }

    /// Whether every member of `group` runs and declares it.
    pub(crate) fn complete(&self, group: &Group) -> bool {
        let declared = self.declared.lock().unwrap_or_else(|e| e.into_inner());
        group.members.iter().all(|m| declared.get(m) == Some(group))
    }

    /// Where the `group` record in force places `member`, and, when it is
    /// on `node`, `relisted`'s answer to whether the same view holds the
    /// scheduler's answer to a relist nonce. Both come from one lock, since
    /// [`follow`] can swap in a view that both moves the member and answers
    /// the nonce (#256, #257).
    pub(crate) fn group_placed(
        &self,
        member: &str,
        group: &Group,
        node: &str,
        relisted: impl FnOnce(&dyn Fn(u64) -> bool) -> bool,
    ) -> (Option<Placed>, bool) {
        let followed = self.lock();
        let placed = followed.group_placed(member, group);
        let here = placed.as_ref().is_some_and(|p| p.node == node);
        let answered = here && relisted(&|relist| followed.relisted.get(node) == Some(&relist));
        (placed, answered)
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

    /// Whether the scheduler runs and its holder renewed its lease lately,
    /// as of `now_millis`. Only then does a node wait on its assignments. A
    /// lease that has merely not expired is not enough: its holder may have
    /// died up to a TTL ago.
    pub(crate) fn healthy(&self, timing: &LeaseTiming, now_millis: u64) -> bool {
        let holder = *self.holder.lock().unwrap_or_else(|e| e.into_inner());
        *self.scheduler.borrow() && holder.is_some_and(|at| timing.renewed_lately(at, now_millis))
    }

    pub(crate) fn scheduler_changes(&self) -> watch::Receiver<bool> {
        self.scheduler.subscribe()
    }

    /// Whether this node's view holds the scheduler's answer to `relist`,
    /// and so every output of the steps up to the fact that carried it.
    pub(crate) fn relisted(&self, node: &str, relist: u64) -> bool {
        self.lock().relisted.get(node) == Some(&relist)
    }

    #[cfg(test)]
    pub(crate) fn fold(&self, offset: u64, record: &[u8]) {
        self.lock().fold(offset, record);
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
        #[cfg_attr(not(test), allow(unused_mut))]
        let mut followed = self.followed.lock().unwrap_or_else(|e| e.into_inner());
        #[cfg(test)]
        {
            let mut swap = self.swap_at_lock.lock().unwrap_or_else(|e| e.into_inner());
            match swap.as_mut() {
                Some((0, _)) => *followed = swap.take().map(|(_, view)| view).unwrap_or_default(),
                Some((n, _)) => *n -= 1,
                None => {}
            }
        }
        followed
    }

    /// Makes the lock taken after `locks` more replace the view with `view`.
    #[cfg(test)]
    pub(crate) fn swap_view_at_lock(&self, locks: usize, view: Followed) {
        *self.swap_at_lock.lock().unwrap_or_else(|e| e.into_inner()) = Some((locks, view));
    }

    /// A copy of the view, to build a test's next one from.
    #[cfg(test)]
    pub(crate) fn view(&self) -> Followed {
        self.followed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
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

/// Appends `fact()` to `sys.facts` while the scheduler runs or `members`
/// (group members held) is not 0: when that starts, within [`READ_EVERY`]
/// of the fact changing, and every `every` otherwise.
///
/// A node holding a member writes through a scheduler stop because the
/// `scheduler` watch can be stale: a node that still sees it wanted keeps
/// appending, so on restart the scheduler finds no gap to pause over, and a
/// holder that went quiet would be forgotten while it runs its members
/// (#271). Its own facts are the only evidence that it is alive.
pub(crate) async fn write_facts(
    felix: &Felix,
    assignments: &Assignments,
    mut members: watch::Receiver<usize>,
    every: Duration,
    fact: impl Fn() -> NodeFact,
    landed: impl Fn(),
) {
    let mut scheduler = assignments.scheduler_changes();
    let mut failing = false;
    loop {
        tokio::select! {
            r = scheduler.wait_for(|runs| *runs) => if r.is_err() { return },
            r = members.wait_for(|n| *n > 0) => if r.is_err() { return },
        }
        let mut last: Option<(NodeFact, Instant)> = None;
        while writes_facts(*scheduler.borrow_and_update(), *members.borrow_and_update()) {
            let now = fact();
            // A node the scheduler cannot see, for the tests.
            if due(last.as_ref(), &now, every) && !crate::faults::no_facts() {
                let record = now.encode();
                let sent = async {
                    if crate::faults::facts_stopped() {
                        anyhow::bail!("injected: facts stopped");
                    }
                    felix.publish(names::FACTS, record).await
                };
                match tokio::time::timeout(WITHIN, sent).await {
                    Ok(Ok(())) => {
                        landed();
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
                _ = members.changed() => {}
            }
        }
    }
}

/// Whether a node appends facts: while a scheduler is wanted, or while it
/// holds a group member, whatever it thinks of the scheduler.
fn writes_facts(scheduler: bool, members: usize) -> bool {
    scheduler || members > 0
}

/// Whether a fact is to be written: none yet, it changed, or `every` has
/// passed since the last.
fn due(last: Option<&(NodeFact, Instant)>, now: &NodeFact, every: Duration) -> bool {
    last.is_none_or(|(fact, at)| fact != now || at.elapsed() >= every)
}

#[cfg(test)]
mod tests;
