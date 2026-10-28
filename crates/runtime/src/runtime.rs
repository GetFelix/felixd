use thiserror::Error;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store, StoreLimitsBuilder};

use crate::bindings::Workload as Bindings;
use crate::limits::Limits;
use crate::workload::Workload;

/// Compiles and instantiates workloads under one set of [`Limits`].
pub struct Runtime {
    engine: Engine,
    limits: Limits,
}

/// Why a component could not be loaded.
#[derive(Debug, Error)]
pub enum LoadError {
    #[error("the module is not a valid component: {0}")]
    Invalid(String),
    /// A workload gets nothing from the host, not even a clock or randomness.
    #[error("the component imports {}, but workloads may import nothing", .0.join(", "))]
    Imports(Vec<String>),
    #[error("the component does not implement the workload world: {0}")]
    Instantiate(String),
}

impl Runtime {
    pub fn new(limits: Limits) -> wasmtime::Result<Runtime> {
        let mut config = Config::new();
        // NaN bit patterns are the one source of float nondeterminism in Wasm.
        config.cranelift_nan_canonicalization(true);
        config.consume_fuel(true);
        config.wasm_relaxed_simd(false);
        Ok(Runtime {
            engine: Engine::new(&config)?,
            limits,
        })
    }

    /// Loads `bytes` as a component and instantiates it. The state is the
    /// workload's initial state; call [`Workload::restore`] to resume.
    pub fn load(&self, bytes: &[u8]) -> Result<Workload, LoadError> {
        let component =
            Component::new(&self.engine, bytes).map_err(|e| LoadError::Invalid(e.to_string()))?;
        let imports: Vec<String> = component
            .component_type()
            .imports(&self.engine)
            .map(|(name, _)| name.to_string())
            .collect();
        if !imports.is_empty() {
            return Err(LoadError::Imports(imports));
        }
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(self.limits.max_memory_bytes)
            .build();
        let mut store = Store::new(&self.engine, store_limits);
        store.limiter(|limits| limits);
        store
            .set_fuel(self.limits.fuel_per_call)
            .map_err(|e| LoadError::Instantiate(e.to_string()))?;
        let bindings = Bindings::instantiate(&mut store, &component, &Linker::new(&self.engine))
            .map_err(|e| LoadError::Instantiate(e.to_string()))?;
        Ok(Workload::new(store, bindings, self.limits))
    }
}

#[cfg(test)]
mod tests;
