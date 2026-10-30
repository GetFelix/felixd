//! Admission: requests on `sys.intake` and the decisions the admission
//! workload outputs for them (#114).
//!
//! Admission is an ordinary felixd workload reading [`names::INTAKE`]. For
//! each request it outputs exactly one [`Decision`], keyed by the workload id,
//! on `wl.admission.out`, so its decisions are written once, fail over and
//! replay like any output. [`decide`] is the whole policy and reads nothing
//! but the request, so a replay of the same module over the same requests
//! decides the same way, and a replay of another module shows which past
//! requests a policy change would decide differently.
//!
//! The node holding admission's lease then projects each accepted decision
//! to `sys.specs` as a spec record naming the decision. Agents run a spec
//! record written after admission's first one only if the decision it names
//! accepted exactly that spec ([`admits`]).

use serde::{Deserialize, Serialize};

use crate::spec::{WorkloadSpec, WorkloadState, validate_inputs};
use crate::{DecodeError, MAX_WRITE_BYTES, names};

/// The request format this build writes and reads.
const REQUEST_FORMAT: u32 = 1;
/// The decision format this build writes and reads.
const DECISION_FORMAT: u32 = 1;

/// A request on `sys.intake`: run, change or stop workload `id`. A stop is
/// the spec with state `stopped`. The requester is not in the payload, which
/// any publisher can fill with anything.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub format: u32,
    pub id: String,
    /// What the workload should run. Its `output` must be the workload's
    /// output stream, and `record` and `failure` must be unset.
    pub spec: WorkloadSpec,
    /// The module's length in bytes, which the projector checks against the
    /// `modules` cache before the spec is recorded.
    pub module_bytes: u64,
    /// Replicas of the workload's output and snapshot streams.
    pub replication_factor: u32,
}

impl Request {
    pub fn new(id: &str, spec: WorkloadSpec, module_bytes: u64, replication_factor: u32) -> Self {
        Request {
            format: REQUEST_FORMAT,
            id: id.to_owned(),
            spec,
            module_bytes,
            replication_factor,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a request is always valid JSON")
    }

    pub fn decode(bytes: &[u8]) -> Result<Request, DecodeError> {
        let request: Request =
            serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))?;
        if request.format != REQUEST_FORMAT {
            return Err(unknown(request.format));
        }
        Ok(request)
    }
}

/// Admission's answer to one request, an output on `wl.admission.out`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub format: u32,
    /// The `sys.intake` offset of the request it answers.
    pub request: u64,
    /// The workload the request names, or empty for one that did not decode.
    pub id: String,
    #[serde(flatten)]
    pub outcome: Outcome,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "lowercase")]
pub enum Outcome {
    Accepted {
        spec: WorkloadSpec,
        module_bytes: u64,
        replication_factor: u32,
    },
    Rejected {
        reason: Reason,
        /// For people. Programs compare `reason`.
        detail: String,
    },
}

/// Why a request was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// The request does not decode, or is of a format this build does not
    /// know.
    Unreadable,
    /// The id is not usable in a stream name.
    InvalidId,
    /// The id is one only the operator may write, such as admission's own.
    Reserved,
    /// The inputs are not 1 to the most a workload reads, all different and
    /// allowed for this workload.
    InvalidInputs,
    /// The spec is not one a submitter may write, such as a `failed` state or
    /// an output stream not derived from the id.
    InvalidSpec,
    /// A limit is over what the policy allows.
    OverLimit,
    /// The module is larger than the policy allows.
    ModuleTooLarge,
}

impl Reason {
    /// The rejection's code, stable across builds.
    pub fn code(self) -> u32 {
        match self {
            Reason::Unreadable => 1,
            Reason::InvalidId => 2,
            Reason::Reserved => 3,
            Reason::InvalidInputs => 4,
            Reason::InvalidSpec => 5,
            Reason::OverLimit => 6,
            Reason::ModuleTooLarge => 7,
        }
    }
}

impl std::fmt::Display for Reason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Reason::Unreadable => "unreadable",
            Reason::InvalidId => "invalid_id",
            Reason::Reserved => "reserved",
            Reason::InvalidInputs => "invalid_inputs",
            Reason::InvalidSpec => "invalid_spec",
            Reason::OverLimit => "over_limit",
            Reason::ModuleTooLarge => "module_too_large",
        })
    }
}

impl Decision {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a decision is always valid JSON")
    }

    pub fn decode(bytes: &[u8]) -> Result<Decision, DecodeError> {
        let decision: Decision =
            serde_json::from_slice(bytes).map_err(|e| DecodeError::Json(e.to_string()))?;
        if decision.format != DECISION_FORMAT {
            return Err(unknown(decision.format));
        }
        Ok(decision)
    }

    pub fn accepted(&self) -> Option<&WorkloadSpec> {
        match &self.outcome {
            Outcome::Accepted { spec, .. } => Some(spec),
            Outcome::Rejected { .. } => None,
        }
    }
}

fn unknown(format: u32) -> DecodeError {
    DecodeError::UnknownVersion(u8::try_from(format).unwrap_or(u8::MAX))
}

/// What admission allows. It is compiled into the admission module, so a
/// policy change is a module change, and `felixd replay --diff --module`
/// shows which recorded requests it would decide differently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    pub max_fuel_per_call: u64,
    pub max_memory_bytes: u64,
    pub max_snapshot_fuel: u64,
    pub max_module_bytes: u64,
    pub max_replication_factor: u32,
}

impl Default for Policy {
    fn default() -> Self {
        let limits = crate::spec::Limits::default();
        Policy {
            max_fuel_per_call: 10 * limits.fuel_per_call,
            max_memory_bytes: 1 << 30,
            max_snapshot_fuel: 10 * limits.snapshot_fuel,
            max_module_bytes: MAX_WRITE_BYTES as u64,
            max_replication_factor: 7,
        }
    }
}

/// Decides the request at `sys.intake` offset `offset`. Total and pure: every
/// request gets exactly one decision, from its bytes and `policy` alone.
pub fn decide(policy: &Policy, offset: u64, payload: &[u8]) -> Decision {
    let (id, outcome) = match Request::decode(payload) {
        Ok(request) => {
            let outcome = match check(policy, &request) {
                Ok(()) => Outcome::Accepted {
                    spec: request.spec,
                    module_bytes: request.module_bytes,
                    replication_factor: request.replication_factor,
                },
                Err((reason, detail)) => Outcome::Rejected { reason, detail },
            };
            (request.id, outcome)
        }
        Err(error) => (
            String::new(),
            Outcome::Rejected {
                reason: Reason::Unreadable,
                detail: error.to_string(),
            },
        ),
    };
    Decision {
        format: DECISION_FORMAT,
        request: offset,
        id,
        outcome,
    }
}

fn check(policy: &Policy, request: &Request) -> Result<(), (Reason, String)> {
    let spec = &request.spec;
    names::validate_id(&request.id).map_err(|e| (Reason::InvalidId, e.to_string()))?;
    if request.id == names::ADMISSION {
        return Err((
            Reason::Reserved,
            format!(
                "{} is a system workload, whose spec only an operator writes",
                request.id
            ),
        ));
    }
    validate_inputs(&spec.inputs).map_err(|e| (Reason::InvalidInputs, e.to_string()))?;
    for input in &spec.inputs {
        if !input_allowed(input) {
            return Err((
                Reason::InvalidInputs,
                format!("workload {} may not read {input}", request.id),
            ));
        }
    }
    let invalid = |detail: &str| Err((Reason::InvalidSpec, detail.to_owned()));
    if spec.output != names::output_stream(&request.id) {
        return invalid("the output stream is wl.<id>.out");
    }
    if spec.record.is_some() || spec.failure.is_some() {
        return invalid("a request names no spec record and no failure");
    }
    if spec.state == WorkloadState::Failed {
        return invalid("a request asks for running or stopped");
    }
    if spec.snapshot_every == 0
        || spec.snapshot_every_ms == Some(0)
        || spec.keep_snapshots == Some(0)
    {
        return invalid("snapshot intervals and counts are at least 1");
    }
    if !is_module_hash(&spec.module) {
        return invalid("the module is the hex sha256 of its bytes");
    }
    if request.replication_factor == 0 || request.replication_factor > policy.max_replication_factor
    {
        return Err((
            Reason::OverLimit,
            format!(
                "replication factor {} is not 1 to {}",
                request.replication_factor, policy.max_replication_factor
            ),
        ));
    }
    let over = |what: &str, asked: u64, most: u64| {
        Err((Reason::OverLimit, format!("{what} {asked} is over {most}")))
    };
    let limits = spec.limits;
    if limits.fuel_per_call > policy.max_fuel_per_call {
        return over(
            "fuel per call",
            limits.fuel_per_call,
            policy.max_fuel_per_call,
        );
    }
    if limits.max_memory_bytes > policy.max_memory_bytes {
        return over("memory", limits.max_memory_bytes, policy.max_memory_bytes);
    }
    if limits.snapshot_fuel > policy.max_snapshot_fuel {
        return over(
            "snapshot fuel",
            limits.snapshot_fuel,
            policy.max_snapshot_fuel,
        );
    }
    if request.module_bytes == 0 || request.module_bytes > policy.max_module_bytes {
        return Err((
            Reason::ModuleTooLarge,
            format!(
                "the module is {} bytes, not 1 to {}",
                request.module_bytes, policy.max_module_bytes
            ),
        ));
    }
    Ok(())
}

/// No admitted workload reads a system stream or one of admission's own.
/// Reading admission's streams would let its trim records mark decisions
/// trimmable, and a node needs every decision a live spec names.
fn input_allowed(input: &str) -> bool {
    let admissions = format!("wl.{}.", names::ADMISSION);
    !input.starts_with("sys.") && !input.starts_with(&admissions)
}

fn is_module_hash(module: &str) -> bool {
    module.len() == 64
        && module
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Whether the decision recorded on `wl.admission.out` as `output` admits a
/// spec record of workload `id` saying `spec`: it is an accepted decision for
/// that workload and exactly that spec. Anything else, including a record
/// that does not decode, does not.
pub fn admits(output: &[u8], id: &str, spec: &WorkloadSpec) -> Result<(), String> {
    let Ok(crate::output::Record::Output(_, addressed)) = crate::output::decode(output) else {
        return Err("it is not an output of admission".to_owned());
    };
    let decision = Decision::decode(addressed.payload).map_err(|e| e.to_string())?;
    if decision.id != id {
        return Err(format!(
            "it answers a request for workload {:?}",
            decision.id
        ));
    }
    match &decision.outcome {
        Outcome::Accepted { spec: accepted, .. } if accepted == spec => Ok(()),
        Outcome::Accepted { .. } => Err("it accepted another spec".to_owned()),
        Outcome::Rejected { reason, .. } => Err(format!("it rejected the request: {reason}")),
    }
}

/// Whether a spec record of workload `id` at `sys.specs` offset `at` must
/// name an accepting decision, given the offset of the first record that
/// could be admission's own (`admission_from`). System workloads' records are
/// written by an operator and need none; records before admission was
/// bootstrapped predate it.
pub fn needs_decision(id: &str, at: u64, admission_from: Option<u64>) -> bool {
    !names::is_system(id) && admission_from.is_some_and(|from| from < at)
}

/// The end of a one-record read of decision `decision` on
/// `wl.admission.out`, whose tail is `tail`. `None` for a decision at or past
/// the tail, or at `u64::MAX`, which no read reaches: such a decision is not
/// on the stream, and is never read or trusted.
pub fn decision_end(decision: u64, tail: u64) -> Option<u64> {
    decision.checked_add(1).filter(|_| decision < tail)
}

#[cfg(test)]
mod tests;
