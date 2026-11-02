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

use crate::felix::Felix;
use crate::runner;
use crate::specs;

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
        found.push(match reads {
            true => reader(felix, &other, spec).await,
            false => Known {
                inputs: Some(spec.inputs),
                floors: None,
            },
        });
    }
    Ok(found)
}

/// A workload that reads one of the streams. Its floors are by the inputs
/// its `.out` was written under, which a spec in the cache can differ from
/// (a conflicting spec parks the workload, and reverting it resumes under the
/// old inputs). Then it counts as needing all of both. Anything unreadable
/// holds back the streams it reads.
async fn reader(felix: &Felix, id: &str, spec: WorkloadSpec) -> Known {
    let held = |inputs, floors| Known {
        inputs: Some(inputs),
        floors,
    };
    let tail = match felix.read_tail_if_exists(&spec.output).await {
        // A workload that has not run has no stream yet.
        Ok(None) => return held(spec.inputs, None),
        Ok(Some(tail)) => tail,
        Err(error) => return unreadable(id, &error, spec.inputs),
    };
    let written = match specs::lineage(felix, id, spec.clone()).await {
        Ok(written) => written,
        Err(error) => return unreadable(id, &error, spec.inputs),
    };
    if written.inputs != spec.inputs {
        let mut inputs = spec.inputs;
        for input in written.inputs {
            if !inputs.contains(&input) {
                inputs.push(input);
            }
        }
        return held(inputs, None);
    }
    match runner::last_trim(felix, &spec.output, tail).await {
        Ok(trim) => held(spec.inputs, trim.map(|t| t.input_floors)),
        Err(error) => unreadable(id, &error, spec.inputs),
    }
}

fn unreadable(id: &str, error: &anyhow::Error, inputs: Vec<String>) -> Known {
    tracing::warn!(workload = id, %error, "reading a reader's trim record failed, keeping what it reads");
    Known {
        inputs: Some(inputs),
        floors: None,
    }
}
