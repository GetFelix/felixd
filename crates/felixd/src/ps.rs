//! How `felixd ps` prints its report.

use std::fmt::Write;

use felixd_agent::ps::Report;
use felixd_core::status::State;

/// The report as two tables, workloads then nodes, each followed by the
/// details that do not fit in a column.
pub(crate) fn render(report: &Report) -> String {
    let mut out = String::new();
    let mut rows = vec![row(&[
        "WORKLOAD", "STATE", "HOLDER", "EPOCH", "OUTPUTS", "INPUT", "NEXT", "TAIL", "LAG",
    ])];
    let mut details = Vec::new();
    for w in &report.workloads {
        let holder = match &w.holder {
            Some(lease) if w.holder_draining => format!("{} (draining)", lease.owner),
            Some(lease) => lease.owner.clone(),
            None => "-".to_owned(),
        };
        let epoch = match (&w.holder, w.last_epoch) {
            (Some(lease), _) if lease.epoch > 0 => lease.epoch.to_string(),
            (None, Some(epoch)) => format!("({epoch})"),
            _ => "-".to_owned(),
        };
        let mut lines = w.inputs.iter().chain(&w.steps).map(|p| {
            [
                p.stream.clone(),
                p.next.to_string(),
                p.tail.to_string(),
                p.lag().to_string(),
            ]
        });
        let first = lines
            .next()
            .unwrap_or_else(|| ["-".into(), "-".into(), "-".into(), "-".into()]);
        let mut cells = vec![
            w.id.clone(),
            state_word(w.state.as_ref()).to_owned(),
            holder,
            epoch,
            w.outputs.to_string(),
        ];
        cells.extend(first);
        rows.push(cells);
        for more in lines {
            let mut cells = vec![String::new(); 5];
            cells.extend(more);
            rows.push(cells);
        }
        if let Some(state @ (State::Failed(_) | State::NeedsOperator(_))) = &w.state {
            details.push(format!("{}: {state}", w.id));
        }
        if let Some(unread) = &w.unread {
            details.push(format!("{}: could not read: {unread}", w.id));
        }
    }
    table(&mut out, &rows);
    for line in details {
        let _ = writeln!(out, "{line}");
    }

    let _ = writeln!(out);
    let mut rows = vec![row(&["NODE", "UP", "LEASES", "MAX", "VERSION", "DRAINING"])];
    let mut pending = false;
    for n in &report.nodes {
        let (up, max, version) = match (&n.info, n.up) {
            (Some(info), _) => ("yes", info.max_workloads.to_string(), info.version.clone()),
            // An entry this build cannot read, from a newer one.
            (None, true) => ("yes", "?".to_owned(), "?".to_owned()),
            (None, false) => ("no", "-".to_owned(), "-".to_owned()),
        };
        let draining = match (n.draining, n.acknowledged()) {
            (true, true) => "yes",
            (true, false) if n.up => {
                pending = true;
                "pending"
            }
            (true, false) => "yes",
            (false, _) => "no",
        };
        rows.push(vec![
            n.id.clone(),
            up.to_owned(),
            n.leases.to_string(),
            max,
            version,
            draining.to_owned(),
        ]);
    }
    table(&mut out, &rows);
    let waiting = report
        .workloads
        .iter()
        .any(|w| w.state == Some(State::Waiting));
    let takers = report.nodes.iter().any(|n| n.up && !n.draining);
    if waiting && !takers {
        let _ = writeln!(
            out,
            "no node is up and not draining: waiting workloads wait until one is"
        );
    }
    if pending {
        let _ = writeln!(
            out,
            "pending: the node has not said it read its drain mark; a node of an older build, \
             or one without read on the drains cache, never does"
        );
    }
    out
}

fn state_word(state: Option<&State>) -> &'static str {
    match state {
        None => "unreadable",
        Some(State::Stopped) => "stopped",
        Some(State::Failed(_)) => "failed",
        Some(State::NeedsOperator(_)) => "needs-operator",
        Some(State::Running) => "running",
        Some(State::Claiming) => "claiming",
        Some(State::Waiting) => "waiting",
    }
}

fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|&c| c.to_owned()).collect()
}

/// Writes `rows` with each column as wide as its widest cell.
fn table(out: &mut String, rows: &[Vec<String>]) {
    let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
    let widths: Vec<usize> = (0..columns)
        .map(|c| {
            rows.iter()
                .filter_map(|r| r.get(c))
                .map(|cell| cell.chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect();
    for r in rows {
        let mut line = String::new();
        for (cell, width) in r.iter().zip(&widths) {
            let _ = write!(line, "{cell:width$}  ");
        }
        let _ = writeln!(out, "{}", line.trim_end());
    }
}

#[cfg(test)]
mod tests;
