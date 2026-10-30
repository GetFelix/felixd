//! `felixd ps`: each workload's state, holder and positions, and each node,
//! read from the caches and streams. Needs read grants only.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use felixd_core::lease::Lease;
use felixd_core::node::{self, Drain, NodeInfo};
use felixd_core::spec::{WorkloadSpec, WorkloadState};
use felixd_core::status::{self, Found, State};
use felixd_core::{merge, names, output};

use crate::assignments::{self, Placed};
use crate::drain;
use crate::felix::Felix;
use crate::runner;
use crate::settings::Settings;
use crate::snapshots;

/// What `felixd ps` shows.
#[derive(Debug)]
pub struct Report {
    pub workloads: Vec<Workload>,
    pub nodes: Vec<Node>,
    /// Whether the scheduler workload is running, so assignments are in force.
    pub scheduled: bool,
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
    /// The output stream's next offset.
    pub outputs: u64,
    /// The epoch of the last record on the output stream.
    pub last_epoch: Option<u64>,
    /// Where the workload is on each input. For a workload with several
    /// inputs, how far each has been merged.
    pub inputs: Vec<Position>,
    /// For a workload with several inputs, the next step on its merge log.
    pub steps: Option<Position>,
    /// Why some of the above could not be read.
    pub unread: Option<String>,
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
    let mut placed = if scheduled {
        assignments::read(&felix).await?
    } else {
        HashMap::new()
    };
    let mut workloads = Vec::new();
    for (id, value) in specs {
        if !ids.is_empty() && !ids.contains(&id) {
            continue;
        }
        let holder = leases.get(&id).cloned();
        let assigned = placed.remove(&id);
        let holder_draining = holder.as_ref().is_some_and(|l| draining.contains(&l.owner));
        let mut workload = Workload {
            id,
            state: None,
            assigned,
            holder,
            holder_draining,
            outputs: 0,
            last_epoch: None,
            inputs: Vec::new(),
            steps: None,
            unread: None,
        };
        match WorkloadSpec::decode(&value) {
            Ok(spec) => {
                if let Err(error) = read_streams(&felix, &spec, &mut workload).await {
                    workload.unread = Some(format!("{error:#}"));
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
    Ok(Report {
        workloads,
        nodes: nodes.into_values().collect(),
        scheduled,
    })
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
