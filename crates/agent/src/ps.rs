//! `felixd ps`: each workload's state, holder and positions, and each node,
//! read from the caches and streams. Needs read grants only.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use felixd_core::lease::Lease;
use felixd_core::node::{self, Drain, NodeInfo};
use felixd_core::placement::{Authority, PlacementRecord};
use felixd_core::spec::{Group, WorkloadSpec, WorkloadState};
use felixd_core::status::{self, Found, State};
use felixd_core::{merge, names, output};

use crate::assignments::{self, Placed};
use crate::drain;
use crate::felix::Felix;
use crate::runner;
use crate::settings::Settings;
use crate::snapshots;

/// How many records back from the tail `ps` looks for the divergence record
/// a failure follows.
const DIVERGENCE_LOOK: u64 = 8;
/// How many records back from the tail of `wl.<id>.placed` `ps` looks for
/// the holder's placement record. A node writing records without claims
/// could push it further back; `felixd check` reports those.
const PLACED_LOOK: u64 = 8;

/// What `felixd ps` shows.
#[derive(Debug)]
pub struct Report {
    pub workloads: Vec<Workload>,
    pub nodes: Vec<Node>,
    /// Whether the scheduler workload is running, so assignments are in force.
    pub scheduled: bool,
    /// Each group the running specs declare.
    pub groups: Vec<GroupReport>,
}

/// A group of workloads placed together, by name.
#[derive(Debug, PartialEq, Eq)]
pub struct GroupReport {
    pub name: String,
    /// Every member the group lists, with the node holding it.
    pub members: Vec<(String, Option<String>)>,
    pub status: GroupStatus,
}

/// Where a group stands, by the leases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupStatus {
    /// No member is held yet.
    Waiting,
    /// Every member is held, each where the group's record in force places
    /// it.
    Together,
    /// Every member is held but no record in force matches the group: the
    /// group bound released them, or no scheduler runs.
    Released,
    /// Some member is held and some is not, or a member is held off the
    /// group's record: the group bound passed, or a node died.
    Split,
}

impl GroupStatus {
    pub fn name(self) -> &'static str {
        match self {
            GroupStatus::Waiting => "waiting",
            GroupStatus::Together => "together",
            GroupStatus::Released => "released",
            GroupStatus::Split => "split",
        }
    }
}

/// The status of a group whose members are held by `holders`, against the
/// node each member has in the group's record in force, if there is one.
/// Without a record nothing is together.
pub(crate) fn group_status(
    members: &[(String, Option<String>)],
    record: Option<&BTreeMap<String, String>>,
) -> GroupStatus {
    let held = members
        .iter()
        .filter(|(_, holder)| holder.is_some())
        .count();
    if held == 0 {
        return GroupStatus::Waiting;
    }
    let Some(record) = record else {
        return if held < members.len() {
            GroupStatus::Split
        } else {
            GroupStatus::Released
        };
    };
    let off_record = members
        .iter()
        .any(|(m, holder)| holder.is_some() && holder.as_ref() != record.get(m));
    if held < members.len() || off_record {
        GroupStatus::Split
    } else {
        GroupStatus::Together
    }
}

#[derive(Debug)]
pub struct Workload {
    pub id: String,
    /// `None` when the spec does not decode; `unread` says why.
    pub state: Option<State>,
    /// The live lease.
    pub holder: Option<Lease>,
    /// Whether the holder is marked as draining.
    pub holder_draining: bool,
    /// The scheduler's assignment in force, while a scheduler runs.
    pub assigned: Option<Placed>,
    /// Who placed the holder's claim, while a scheduler runs and the
    /// workload is held.
    pub placed: Option<PlacedBy>,
    /// The output stream's next offset.
    pub outputs: u64,
    /// The epoch of the last record on the output stream.
    pub last_epoch: Option<u64>,
    /// The divergence record the output stream ends on, behind any failure
    /// or other note after it, when it names the spec record the workload
    /// runs.
    pub divergence: Option<status::Diverged>,
    /// Where the workload is on each input. For a workload with several
    /// inputs, how far each has been merged.
    pub inputs: Vec<Position>,
    /// For a workload with several inputs, the next step on its merge log.
    pub steps: Option<Position>,
    /// Why some of the above could not be read.
    pub unread: Option<String>,
}

/// What `wl.<id>.placed` says about the holder's claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacedBy {
    Recorded(Authority),
    /// No placement record for the claim: an older holder, a workload
    /// admitted before placement records, a crash between the claim and the
    /// record, or one still being retried.
    Unknown,
}

/// How far a workload is along a stream.
#[derive(Debug, PartialEq, Eq)]
pub struct Position {
    pub stream: String,
    /// The next offset it takes, as far as the records show.
    pub next: u64,
    /// The stream's next offset.
    pub tail: u64,
}

impl Position {
    /// Records on the stream past `next`. Claims and other records that are
    /// not inputs count too.
    pub fn lag(&self) -> u64 {
        self.tail.saturating_sub(self.next)
    }
}

#[derive(Debug)]
pub struct Node {
    pub id: String,
    /// Whether it has an entry in `nodes`, which it keeps while it runs.
    pub up: bool,
    /// That entry, `None` if there is none or this build cannot read it.
    pub info: Option<NodeInfo>,
    /// Whether it is marked in `drains`.
    pub draining: bool,
    /// The mark, if this build reads it.
    pub mark: Option<Drain>,
    /// How many live leases name it.
    pub leases: usize,
}

impl Node {
    /// Whether the node has said that it read its mark and has no claim
    /// under way. A node of an older build, or one that cannot read
    /// `drains`, never does.
    pub fn acknowledged(&self) -> bool {
        node::acknowledges(self.info.as_ref(), self.mark.as_ref())
    }
}

/// Reports on the workloads in `ids`, or every workload when it is empty,
/// and on every node that is up, holds a lease or is marked as draining.
pub async fn ps(settings: &Settings, ids: &[String]) -> Result<Report> {
    let felix = Felix::connect(settings).await?;
    let leases: HashMap<String, Lease> = drain::leases(&felix).await?.into_iter().collect();
    let marks = drain::draining(&felix).await?;
    let draining: BTreeSet<String> = marks.iter().map(|(n, _)| n.clone()).collect();
    let specs = felix.entries(names::WORKLOADS).await?;
    let scheduled = specs.iter().any(|(id, value)| {
        id == names::SCHEDULER
            && WorkloadSpec::decode(value).is_ok_and(|s| s.state == WorkloadState::Running)
    });
    let followed = if scheduled {
        assignments::read(&felix).await?
    } else {
        assignments::Followed::default()
    };
    let mut placed = followed.table.clone();
    let declared: BTreeMap<String, Group> = specs
        .iter()
        .filter_map(|(id, value)| {
            let spec = WorkloadSpec::decode(value).ok()?;
            Some((
                id.clone(),
                spec.group
                    .filter(|_| spec.state == WorkloadState::Running)?,
            ))
        })
        .collect();
    let mut workloads = Vec::new();
    for (id, value) in specs {
        if !ids.is_empty() && !ids.contains(&id) {
            continue;
        }
        let holder = leases.get(&id).cloned();
        let assigned = placed.remove(&id).or_else(|| {
            let group = declared.get(&id)?;
            followed.group_placed(&id, group)
        });
        let holder_draining = holder.as_ref().is_some_and(|l| draining.contains(&l.owner));
        let mut workload = Workload {
            id,
            state: None,
            assigned,
            placed: None,
            holder,
            holder_draining,
            outputs: 0,
            last_epoch: None,
            divergence: None,
            inputs: Vec::new(),
            steps: None,
            unread: None,
        };
        match WorkloadSpec::decode(&value) {
            Ok(spec) => {
                if let Err(error) = read_streams(&felix, &spec, &mut workload).await {
                    workload.unread = Some(format!("{error:#}"));
                }
                let epoch = workload.holder.as_ref().map_or(0, |l| l.epoch);
                if scheduled && epoch > 0 {
                    match placement(&felix, &workload.id, epoch).await {
                        Ok(placed) => workload.placed = Some(placed),
                        Err(error) if workload.unread.is_none() => {
                            workload.unread = Some(format!("{error:#}"));
                        }
                        Err(_) => {}
                    }
                }
            }
            Err(error) => workload.unread = Some(format!("the spec does not decode: {error}")),
        }
        workloads.push(workload);
    }
    for id in ids {
        anyhow::ensure!(workloads.iter().any(|w| &w.id == id), "no workload {id}");
    }

    let mut nodes: BTreeMap<String, Node> = BTreeMap::new();
    for (id, value) in felix.entries(names::NODES).await? {
        let entry = node_entry(&mut nodes, &id);
        entry.up = true;
        entry.info = NodeInfo::decode(&value).ok();
    }
    for (id, mark) in marks {
        let entry = node_entry(&mut nodes, &id);
        entry.draining = true;
        entry.mark = mark;
    }
    for lease in leases.values() {
        node_entry(&mut nodes, &lease.owner).leases += 1;
    }
    let groups = group_reports(&declared, &leases, &followed);
    Ok(Report {
        workloads,
        nodes: nodes.into_values().collect(),
        scheduled,
        groups,
    })
}

/// A report for each group name whose members all declare the same group.
/// Members that disagree on a name are not reported.
fn group_reports(
    declared: &BTreeMap<String, Group>,
    leases: &HashMap<String, Lease>,
    followed: &assignments::Followed,
) -> Vec<GroupReport> {
    let mut by_name: BTreeMap<&str, Option<&Group>> = BTreeMap::new();
    for group in declared.values() {
        by_name
            .entry(&group.name)
            .and_modify(|found| {
                if *found != Some(group) {
                    *found = None;
                }
            })
            .or_insert(Some(group));
    }
    by_name
        .into_values()
        .flatten()
        .map(|group| {
            let members: Vec<(String, Option<String>)> = group
                .members
                .iter()
                .map(|m| (m.clone(), leases.get(m).map(|l| l.owner.clone())))
                .collect();
            let record = followed.group_record(group);
            GroupReport {
                name: group.name.clone(),
                status: group_status(&members, record),
                members,
            }
        })
        .collect()
}

fn node_entry<'a>(nodes: &'a mut BTreeMap<String, Node>, id: &str) -> &'a mut Node {
    nodes.entry(id.to_owned()).or_insert_with(|| Node {
        id: id.to_owned(),
        up: false,
        info: None,
        draining: false,
        mark: None,
        leases: 0,
    })
}

/// The placement record of the claim at `epoch`. It is near the tail: one
/// record per claim, and a claim after it would have a later epoch.
async fn placement(felix: &Felix, id: &str, epoch: u64) -> Result<PlacedBy> {
    let stream = names::placed_stream(id);
    let Some(tail) = felix.read_tail_if_exists(&stream).await? else {
        return Ok(PlacedBy::Unknown);
    };
    let (recent, _) = felix
        .read_back(&stream, tail.saturating_sub(PLACED_LOOK), tail)
        .await?;
    Ok(placed_by(recent.iter().map(|r| &r.payload[..]), epoch))
}

/// The newest record for `epoch` among `records`, oldest first. A retried
/// append can leave two for one epoch.
fn placed_by<'a>(records: impl DoubleEndedIterator<Item = &'a [u8]>, epoch: u64) -> PlacedBy {
    records
        .rev()
        .filter_map(|r| PlacementRecord::decode(r).ok())
        .find(|p| p.epoch == epoch)
        .map_or(PlacedBy::Unknown, |p| PlacedBy::Recorded(p.authority))
}

/// Reads the state and positions from the workload's streams into
/// `workload`.
async fn read_streams(felix: &Felix, spec: &WorkloadSpec, workload: &mut Workload) -> Result<()> {
    let outputs = felix.read_tail(&spec.output).await?;
    workload.outputs = outputs;
    let last = felix.last_record(&spec.output, outputs).await?;
    let last = last.map(|r| r.payload);
    workload.last_epoch = last
        .as_deref()
        .and_then(|r| output::decode(r).ok())
        .map(|r| r.epoch());
    workload.state = Some(status::state(Found {
        spec,
        lease: workload.holder.as_ref(),
        last_record: last.as_deref(),
    }));
    if let (Some(record), Some(last)) = (spec.record, last.as_deref())
        && status::may_follow_divergence(last)
    {
        let (recent, _) = felix
            .read_back(
                &spec.output,
                outputs.saturating_sub(DIVERGENCE_LOOK),
                outputs,
            )
            .await?;
        workload.divergence = status::divergence(recent.iter().map(|r| r.payload.as_ref()), record);
    }

    let written = runner::last_output(felix, &spec.output, outputs)
        .await?
        .map(|(_, w)| w.header);
    let snap_stream = names::snapshot_stream(&workload.id);
    let snap_tail = felix.read_tail(&snap_stream).await?;
    let snapshot = snapshots::latest(felix, &snap_stream, snap_tail)
        .await
        .map_err(snapshots::ReadError::into_anyhow)?
        .snapshot
        .map(|s| s.taken().header);
    let next = status::next_step(written, snapshot);
    if !spec.merged() {
        workload.inputs.push(Position {
            stream: spec.inputs[0].clone(),
            next,
            tail: felix.read_tail(&spec.inputs[0]).await?,
        });
        return Ok(());
    }
    let merge_stream = names::merge_stream(&workload.id);
    let merge_tail = felix.read_tail(&merge_stream).await?;
    let last_merged = felix.last_record(&merge_stream, merge_tail).await?;
    let decoded = last_merged
        .as_ref()
        .map(|r| merge::decode(&r.payload))
        .transpose()?;
    let cursor = merge::cursor(decoded.as_ref(), spec.inputs.len())?;
    for (input, merged) in spec.inputs.iter().zip(cursor) {
        workload.inputs.push(Position {
            stream: input.clone(),
            next: merged,
            tail: felix.read_tail(input).await?,
        });
    }
    workload.steps = Some(Position {
        stream: merge_stream,
        next,
        tail: merge_tail,
    });
    Ok(())
}

#[cfg(test)]
mod tests;
