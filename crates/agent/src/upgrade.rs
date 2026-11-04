//! Module upgrades for replays: the instance changes modules at the input a
//! spec says, carrying its state across with a snapshot and a restore.

use anyhow::{Context, Result};
use felixd_core::spec::WorkloadSpec;
use felixd_runtime::{Runtime, Workload};

use crate::check::module_bytes;
use crate::felix::Felix;

/// The upgrades a replay has yet to reach.
pub(crate) struct Upgrades {
    runtime: Runtime,
    pending: Vec<(u64, Vec<u8>)>,
}

impl Upgrades {
    /// The upgrades of `spec` after a state taken at input `taken`, or the
    /// initial state for `None`, with their modules read from the cache.
    pub(crate) async fn after(
        runtime: &Runtime,
        felix: &Felix,
        spec: &WorkloadSpec,
        taken: Option<u64>,
    ) -> Result<Upgrades> {
        let mut pending = Vec::new();
        for upgrade in spec.upgrades_after(taken) {
            pending.push((upgrade.at, module_bytes(felix, &upgrade.module).await?));
        }
        pending.reverse();
        Ok(Upgrades {
            runtime: runtime.clone(),
            pending,
        })
    }

    /// No upgrades: one module runs every input.
    pub(crate) fn none(runtime: &Runtime) -> Upgrades {
        Upgrades {
            runtime: runtime.clone(),
            pending: Vec::new(),
        }
    }

    /// Switches `workload` to the module of the latest upgrade at or below
    /// input `at`, if one is due.
    pub(crate) fn reach(&mut self, workload: &mut Workload, at: u64) -> Result<()> {
        let mut due = None;
        while self.pending.last().is_some_and(|(from, _)| *from <= at) {
            due = self.pending.pop();
        }
        let Some((from, bytes)) = due else {
            return Ok(());
        };
        let chunks = workload
            .snapshot()
            .with_context(|| format!("snapshot the state to hand to the module of input {from}"))?;
        let mut next = self
            .runtime
            .load(&bytes, workload.limits())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let chunks: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        next.restore(&chunks)
            .with_context(|| format!("the module of input {from} could not restore the state"))?;
        *workload = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
