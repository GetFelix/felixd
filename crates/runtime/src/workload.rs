use thiserror::Error;
use wasmtime::{Store, StoreLimits, Trap};

use crate::Limits;
use crate::bindings::Workload as Bindings;

/// A loaded workload. Calls take `&mut self`, so they never overlap.
pub struct Workload {
    store: Store<StoreLimits>,
    bindings: Bindings,
    limits: Limits,
    failure: Option<Failure>,
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
}

#[derive(Debug, Error)]
pub enum WorkloadError {
    /// This call failed the workload, or an earlier one did. The failure is
    /// deterministic, so running it again elsewhere would fail the same way.
    #[error("workload failed: {}", .0.message)]
    Failed(Failure),
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

    /// The reason the workload was stopped, if it was.
    pub fn failure(&self) -> Option<&Failure> {
        self.failure.as_ref()
    }

    /// Handles one input record and returns the outputs it produced.
    pub fn step(&mut self, input: &[u8]) -> Result<Vec<Vec<u8>>, WorkloadError> {
        self.call(|bindings, store| bindings.call_step(store, input))
    }

    /// The whole state, in a form [`Workload::restore`] accepts.
    pub fn snapshot(&mut self) -> Result<Vec<u8>, WorkloadError> {
        self.call(|bindings, store| bindings.call_snapshot(store))
    }

    /// Replaces the state with one [`Workload::snapshot`] returned.
    pub fn restore(&mut self, state: &[u8]) -> Result<(), WorkloadError> {
        self.call(|bindings, store| bindings.call_restore(store, state))
    }

    /// The state as JSON.
    pub fn inspect(&mut self) -> Result<String, WorkloadError> {
        self.call(|bindings, store| bindings.call_inspect(store))
    }

    fn call<T>(
        &mut self,
        f: impl FnOnce(&Bindings, &mut Store<StoreLimits>) -> wasmtime::Result<T>,
    ) -> Result<T, WorkloadError> {
        if let Some(failure) = &self.failure {
            return Err(WorkloadError::Failed(failure.clone()));
        }
        self.store
            .set_fuel(self.limits.fuel_per_call)
            .expect("fuel is enabled");
        f(&self.bindings, &mut self.store).map_err(|error| {
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
            let failure = Failure { kind, message };
            self.failure = Some(failure.clone());
            WorkloadError::Failed(failure)
        })
    }
}
