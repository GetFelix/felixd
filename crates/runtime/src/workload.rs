use felixd_core::output::validate_name;
use felixd_core::snapshot::{MAX_CHUNK_BYTES, MAX_CHUNKS};
use thiserror::Error;
use wasmtime::{Store, StoreLimits, Trap};

use crate::Limits;
use crate::bindings::Workload as Bindings;
use crate::bindings::exports::felixd::workload::step as wit;

/// A loaded workload. Calls take `&mut self`, so they never overlap.
pub struct Workload {
    store: Store<StoreLimits>,
    bindings: Bindings,
    limits: Limits,
    failure: Option<Failure>,
}

/// One input record and where it sits in the input. Every field comes from
/// the input stream and the spec, so a replay passes the same values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Input<'a> {
    /// The input stream's name, as the spec records it.
    pub stream: &'a str,
    /// The incarnation the name resolved to. 0 until specs record it.
    pub incarnation: u64,
    pub offset: u64,
    /// The broker's append time, in microseconds since the Unix epoch.
    pub timestamp_micros: u64,
    /// The key the record was addressed with, when it is another workload's
    /// output.
    pub key: Option<&'a [u8]>,
    pub payload: &'a [u8],
}

/// A record for a named output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Output {
    /// Valid by [`validate_name`].
    pub name: String,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
}

impl Output {
    /// An output under the default name with no key.
    pub fn unnamed(payload: Vec<u8>) -> Output {
        Output {
            name: felixd_core::output::DEFAULT_NAME.to_owned(),
            key: None,
            payload,
        }
    }
}

/// What one step produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stepped {
    /// The outputs, in order.
    Outputs(Vec<Output>),
    /// The workload rejected the input and carries on.
    Rejected { code: u32, detail: String },
}

/// Why a workload was stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub kind: FailureKind,
    pub message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind {
    OutOfFuel,
    Trap,
    /// The workload returned `poison`.
    Poison,
    /// The workload returned something the contract does not allow, such
    /// as an invalid output name, an effect request or a chunk too large.
    Contract,
}

#[derive(Debug, Error)]
pub enum WorkloadError {
    /// This call failed the workload, or an earlier one did. The failure is
    /// deterministic, so running it again elsewhere would fail the same way.
    #[error("workload failed: {}", .0.message)]
    Failed(Failure),
    /// A snapshot ran out of `limits.snapshot_fuel`. The workload has not
    /// failed and nothing was taken, but the trap leaves this instance
    /// unusable: the caller drops it and resumes from the last snapshot.
    #[error("snapshot ran out of fuel")]
    SnapshotOutOfFuel,
}

/// What a call produced, when the guest did not trap.
type Called<T> = Result<T, Failure>;

fn broke(message: impl Into<String>) -> Failure {
    Failure {
        kind: FailureKind::Contract,
        message: message.into(),
    }
}

impl Workload {
    pub(crate) fn new(store: Store<StoreLimits>, bindings: Bindings, limits: Limits) -> Workload {
        Workload {
            store,
            bindings,
            limits,
            failure: None,
        }
    }

    /// Runs later calls under `limits`, for a replay that crosses a spec
    /// change. Memory already grown stays; only growth past it is refused.
    pub fn set_limits(&mut self, limits: Limits) {
        *self.store.data_mut() = crate::runtime::store_limits(limits);
        self.limits = limits;
    }

    /// The reason the workload was stopped, if it was.
    pub fn failure(&self) -> Option<&Failure> {
        self.failure.as_ref()
    }

    /// Handles one input record. A rejection leaves the workload running;
    /// `poison`, a trap, running out of fuel or a result that breaks the
    /// contract fails it.
    pub fn step(&mut self, input: Input<'_>) -> Result<Stepped, WorkloadError> {
        let input = wit::Input {
            source: wit::Source {
                stream: input.stream.to_owned(),
                incarnation: input.incarnation,
            },
            offset: input.offset,
            timestamp_micros: input.timestamp_micros,
            body: wit::InputBody::Record(wit::RecordIn {
                key: input.key.map(<[u8]>::to_vec),
                payload: input.payload.to_vec(),
            }),
        };
        let fuel = self.limits.fuel_per_call;
        self.call(fuel, |bindings, store| {
            let stepped = bindings.felixd_workload_step().call_step(store, &input)?;
            Ok(stepped_from(stepped))
        })
    }

    /// The state, as the chunks `snapshot` and `snapshot-chunk` return. The
    /// calls share one allowance of `limits.snapshot_fuel`. Running out of it
    /// is [`WorkloadError::SnapshotOutOfFuel`], not a failure of the workload.
    pub fn snapshot(&mut self) -> Result<Vec<Vec<u8>>, WorkloadError> {
        let fuel = self.limits.snapshot_fuel;
        let result = self.attempt(fuel, |bindings, store| {
            let state = bindings.felixd_workload_state();
            let count = state.call_snapshot(&mut *store)?;
            if count > MAX_CHUNKS {
                return Ok(Err(broke(format!(
                    "the snapshot has {count} chunks, more than the {MAX_CHUNKS} allowed"
                ))));
            }
            let mut chunks = Vec::with_capacity(count as usize);
            for index in 0..count {
                let chunk = state.call_snapshot_chunk(&mut *store, index)?;
                if chunk.len() > MAX_CHUNK_BYTES {
                    return Ok(Err(broke(format!(
                        "snapshot chunk {index} is {} bytes, more than the {MAX_CHUNK_BYTES} allowed",
                        chunk.len()
                    ))));
                }
                chunks.push(chunk);
            }
            Ok(Ok(chunks))
        });
        match result {
            Err(failure) if failure.kind == FailureKind::OutOfFuel => {
                Err(WorkloadError::SnapshotOutOfFuel)
            }
            result => result.map_err(|failure| self.fail(failure)),
        }
    }

    /// Restores `chunks`, from [`Workload::snapshot`], into this instance,
    /// which must be fresh. The calls share one allowance of
    /// `limits.snapshot_fuel`.
    pub fn restore(&mut self, chunks: &[&[u8]]) -> Result<(), WorkloadError> {
        let count = match u32::try_from(chunks.len()) {
            Ok(count) if count <= MAX_CHUNKS => count,
            _ => return Err(self.fail(broke("a snapshot has too many chunks to restore"))),
        };
        let fuel = self.limits.snapshot_fuel;
        self.call(fuel, |bindings, store| {
            let state = bindings.felixd_workload_state();
            state.call_restore(&mut *store, count)?;
            for (index, chunk) in (0..count).zip(chunks) {
                state.call_restore_chunk(&mut *store, index, chunk)?;
            }
            Ok(Ok(()))
        })
    }

    /// The state as JSON.
    pub fn inspect(&mut self) -> Result<String, WorkloadError> {
        let fuel = self.limits.fuel_per_call;
        self.call(fuel, |bindings, store| {
            Ok(Ok(bindings.felixd_workload_state().call_inspect(store)?))
        })
    }

    fn fail(&mut self, failure: Failure) -> WorkloadError {
        self.failure = Some(failure.clone());
        WorkloadError::Failed(failure)
    }

    fn call<T>(
        &mut self,
        fuel: u64,
        f: impl FnOnce(&Bindings, &mut Store<StoreLimits>) -> wasmtime::Result<Called<T>>,
    ) -> Result<T, WorkloadError> {
        self.attempt(fuel, f).map_err(|failure| self.fail(failure))
    }

    /// Runs one guest call and returns how it failed without recording it.
    fn attempt<T>(
        &mut self,
        fuel: u64,
        f: impl FnOnce(&Bindings, &mut Store<StoreLimits>) -> wasmtime::Result<Called<T>>,
    ) -> Called<T> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        self.store.set_fuel(fuel).expect("fuel is enabled");
        match f(&self.bindings, &mut self.store) {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(failure)) => Err(failure),
            Err(error) => {
                let trap = error.downcast_ref::<Trap>();
                let kind = match trap {
                    Some(Trap::OutOfFuel) => FailureKind::OutOfFuel,
                    _ => FailureKind::Trap,
                };
                // A trap's own text is one line; the full error adds a backtrace.
                let message = match trap {
                    Some(trap) => trap.to_string(),
                    None => format!("{error:#}"),
                };
                Err(Failure { kind, message })
            }
        }
    }
}

fn stepped_from(stepped: Result<Vec<wit::Output>, wit::StepError>) -> Called<Stepped> {
    let outputs = match stepped {
        Ok(outputs) => outputs,
        Err(wit::StepError::Reject(error)) => {
            return Ok(Stepped::Rejected {
                code: error.code,
                detail: error.detail,
            });
        }
        Err(wit::StepError::Poison(reason)) => {
            return Err(Failure {
                kind: FailureKind::Poison,
                message: format!("poison: {reason}"),
            });
        }
    };
    let mut checked = Vec::with_capacity(outputs.len());
    for (index, output) in outputs.into_iter().enumerate() {
        match output {
            wit::Output::Record(addressed) => {
                if validate_name(&addressed.name).is_err() {
                    return Err(broke(format!(
                        "output {index} has the name {:?}, which is not 1 to 64 characters of a-z, 0-9 and '-'",
                        addressed.name
                    )));
                }
                checked.push(Output {
                    name: addressed.name,
                    key: addressed.key,
                    payload: addressed.payload,
                });
            }
            wit::Output::Request(request) => {
                return Err(broke(format!(
                    "output {index} requests a {} effect, and this build has no effect executors",
                    request.kind
                )));
            }
        }
    }
    Ok(Stepped::Outputs(checked))
}
