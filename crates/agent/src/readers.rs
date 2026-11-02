//! The workloads that read a stream, found for deleting it
//! ([`felixd_core::readers`]).
//!
//! Every workload in the `workloads` cache counts, whatever its state. A
//! reader's floor is the last trim record on its `.out`, read the way a
//! resume reads it. Anything unknown about a reader is a [`Known`] that holds
//! the stream back.

use anyhow::Result;
use felixd_core::names;
use felixd_core::readers::Known;
use felixd_core::spec::WorkloadSpec;
use felixd_core::trim::Trim;

use crate::felix::Felix;
use crate::runner;

/// Every workload, as the deletion rule sees it. `own` stands in for the
/// workload `id` itself, which is deleting from what it holds in memory
/// rather than from its cache entry and `.out`. Only the trim records of
/// readers of one of `streams` are read.
pub(crate) async fn known(
    felix: &Felix,
    id: &str,
    own: Known,
    streams: &[&str],
) -> Result<Vec<Known>> {
    let mut found = vec![own];
    for (other, value) in felix.entries(names::WORKLOADS).await? {
        if other == id {
            continue;
        }
        let Ok(spec) = WorkloadSpec::decode(&value) else {
            found.push(Known {
                inputs: None,
                floors: None,
            });
            continue;
        };
        let reads = spec.inputs.iter().any(|i| streams.contains(&i.as_str()));
        let floors = match reads {
            true => floors_of(felix, &other, &spec).await,
            false => None,
        };
        found.push(Known {
            inputs: Some(spec.inputs),
            floors,
        });
    }
    Ok(found)
}

/// The input floors of the last trim record on `spec`'s `.out`. `None` when
/// there is none or it cannot be read, which holds the streams it reads back.
async fn floors_of(felix: &Felix, id: &str, spec: &WorkloadSpec) -> Option<Vec<u64>> {
    match last_trim(felix, &spec.output).await {
        Ok(trim) => trim.map(|t| t.input_floors),
        Err(error) => {
            tracing::warn!(workload = id, %error, "reading a reader's trim record failed, keeping what it reads");
            None
        }
    }
}

async fn last_trim(felix: &Felix, output: &str) -> Result<Option<Trim>> {
    // A workload that has not run has no stream yet.
    match felix.read_tail_if_exists(output).await? {
        Some(tail) => runner::last_trim(felix, output, tail).await,
        None => Ok(None),
    }
}
