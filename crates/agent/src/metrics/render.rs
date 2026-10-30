//! The Prometheus text format, version 0.0.4.

use std::fmt::Write;
use std::sync::atomic::Ordering;

use super::{Histogram, Metrics, REASONS, reason_label};

impl Metrics {
    /// Every series, as a scrape returns them.
    pub(crate) fn render(&self) -> String {
        let mut out = String::new();
        let node = escape(&self.node);
        family(
            &mut out,
            "felixd_node_info",
            "gauge",
            "This node and its version.",
        );
        let _ = writeln!(
            out,
            "felixd_node_info{{node=\"{node}\",version=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION")
        );
        family(
            &mut out,
            "felixd_node_draining",
            "gauge",
            "1 while this node is marked draining and claims nothing.",
        );
        let _ = writeln!(
            out,
            "felixd_node_draining {}",
            u8::from(self.draining.load(Ordering::Relaxed))
        );
        family(
            &mut out,
            "felixd_leases_held",
            "gauge",
            "Workloads this node holds the lease of and runs.",
        );
        let _ = writeln!(
            out,
            "felixd_leases_held {}",
            self.leases_held.load(Ordering::Relaxed).max(0)
        );
        family(
            &mut out,
            "felixd_lease_claims_total",
            "counter",
            "Lease claims, by result: won, contended (another node held it) or error.",
        );
        for (result, count) in ["won", "contended", "error"].iter().zip(&self.claims) {
            let _ = writeln!(
                out,
                "felixd_lease_claims_total{{result=\"{result}\"}} {}",
                count.load(Ordering::Relaxed)
            );
        }
        family(
            &mut out,
            "felixd_lease_handoffs_total",
            "counter",
            "Leases this node let go of for another node to take, by reason: drained or lost.",
        );
        for (reason, count) in ["drained", "lost"].iter().zip(&self.handoffs) {
            let _ = writeln!(
                out,
                "felixd_lease_handoffs_total{{reason=\"{reason}\"}} {}",
                count.load(Ordering::Relaxed)
            );
        }
        family(
            &mut out,
            "felixd_needs_operator_total",
            "counter",
            "Workloads this node stopped because they need an operator, by reason.",
        );
        for (reason, count) in REASONS.iter().zip(&self.needs_operator) {
            let _ = writeln!(
                out,
                "felixd_needs_operator_total{{reason=\"{}\"}} {}",
                reason_label(*reason),
                count.load(Ordering::Relaxed)
            );
        }
        family(
            &mut out,
            "felixd_workload_failures_total",
            "counter",
            "Workloads this node marked failed.",
        );
        let _ = writeln!(
            out,
            "felixd_workload_failures_total {}",
            self.failures.load(Ordering::Relaxed)
        );
        family(
            &mut out,
            "felixd_snapshots_total",
            "counter",
            "Snapshots, by result: written, or skipped because they ran out of snapshot_fuel.",
        );
        let _ = writeln!(
            out,
            "felixd_snapshots_total{{result=\"written\"}} {}",
            self.snapshots_written.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "felixd_snapshots_total{{result=\"skipped\"}} {}",
            self.snapshots_skipped.load(Ordering::Relaxed)
        );
        family(
            &mut out,
            "felixd_snapshot_duration_seconds",
            "histogram",
            "Time to take and write a snapshot, over all workloads.",
        );
        histogram(
            &mut out,
            "felixd_snapshot_duration_seconds",
            "",
            &self.snapshot_seconds,
        );

        let workloads: Vec<_> = self
            .workloads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(id, w)| (escape(id), w.clone()))
            .collect();
        let each = |out: &mut String,
                    name: &str,
                    kind: &str,
                    help: &str,
                    value: &dyn Fn(&super::WorkloadMetrics) -> u64| {
            family(out, name, kind, help);
            for (id, w) in &workloads {
                let _ = writeln!(out, "{name}{{workload=\"{id}\"}} {}", value(w));
            }
        };
        each(
            &mut out,
            "felixd_workload_held",
            "gauge",
            "1 while this node holds the workload's lease.",
            &|w| w.held.load(Ordering::Relaxed).max(0) as u64,
        );
        each(
            &mut out,
            "felixd_steps_total",
            "counter",
            "Inputs stepped through the workload on this node.",
            &|w| w.steps.load(Ordering::Relaxed),
        );
        family(
            &mut out,
            "felixd_step_duration_seconds",
            "histogram",
            "Time in the workload's step call.",
        );
        for (id, w) in &workloads {
            histogram(
                &mut out,
                "felixd_step_duration_seconds",
                &format!("workload=\"{id}\""),
                &w.step_seconds,
            );
        }
        each(
            &mut out,
            "felixd_output_records_total",
            "counter",
            "Output records this node appended.",
            &|w| w.output_records.load(Ordering::Relaxed),
        );
        family(
            &mut out,
            "felixd_output_refused_total",
            "counter",
            "Output appends the stream refused because its tail had moved, by result: fenced (a later holder wrote, so this one stops) or reconciled (its own earlier write or a race, settled by reading the stream).",
        );
        for (id, w) in &workloads {
            for (result, count) in ["reconciled", "fenced"].iter().zip(&w.output_refused) {
                let _ = writeln!(
                    out,
                    "felixd_output_refused_total{{workload=\"{id}\",result=\"{result}\"}} {}",
                    count.load(Ordering::Relaxed)
                );
            }
        }
        each(
            &mut out,
            "felixd_merge_log_appends_total",
            "counter",
            "Entries this node appended to the workload's merge log.",
            &|w| w.merge_entries.load(Ordering::Relaxed),
        );
        each(
            &mut out,
            "felixd_workload_restarts_total",
            "counter",
            "Times the workload's run was restarted after an error talking to Felix.",
            &|w| w.restarts.load(Ordering::Relaxed),
        );
        // Not for `_other`, where workloads would overwrite each other.
        family(
            &mut out,
            "felixd_workload_snapshot_bytes",
            "gauge",
            "Size of the workload's last snapshot, chunks together.",
        );
        for (id, w) in workloads.iter().filter(|(id, _)| id != super::OTHER) {
            let _ = writeln!(
                out,
                "felixd_workload_snapshot_bytes{{workload=\"{id}\"}} {}",
                w.snapshot_bytes.load(Ordering::Relaxed)
            );
        }
        family(
            &mut out,
            "felixd_input_lag_records",
            "gauge",
            "Records on an input past the next one the workload steps, as of the last tail read (every 5 s).",
        );
        for (id, w) in &workloads {
            let inputs = w.inputs.lock().unwrap_or_else(|e| e.into_inner());
            for input in inputs.iter() {
                if let Some(lag) = input.lag() {
                    let _ = writeln!(
                        out,
                        "felixd_input_lag_records{{workload=\"{id}\",input=\"{}\"}} {lag}",
                        escape(&input.stream)
                    );
                }
            }
        }
        out
    }
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}

fn histogram(out: &mut String, name: &str, labels: &str, h: &Histogram) {
    let lead = if labels.is_empty() {
        String::new()
    } else {
        format!("{labels},")
    };
    let mut total = 0;
    for (bound, bucket) in h.bounds.iter().zip(&h.buckets) {
        total += bucket.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{{lead}le=\"{bound}\"}} {total}");
    }
    total += h.buckets[h.bounds.len()].load(Ordering::Relaxed);
    let _ = writeln!(out, "{name}_bucket{{{lead}le=\"+Inf\"}} {total}");
    let braces = if labels.is_empty() {
        String::new()
    } else {
        format!("{{{labels}}}")
    };
    let sum = h.sum_nanos.load(Ordering::Relaxed) as f64 / 1e9;
    let _ = writeln!(out, "{name}_sum{braces} {sum}");
    let _ = writeln!(out, "{name}_count{braces} {total}");
}

/// A label value: backslash, quote and newline escaped.
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
