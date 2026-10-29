use thiserror::Error;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};

use crate::Limits;
use crate::bindings::Workload as Bindings;
use crate::workload::Workload;

/// The Wasm stack a call may use. It is fixed here, not left to wasmtime's
/// default, because where a deep recursion overflows decides which step traps.
pub(crate) const MAX_WASM_STACK: usize = 512 << 10;
/// Bounds on what a workload may create besides memory. Like the stack, they
/// decide where a workload fails, so they are fixed per felixd build.
pub(crate) const MAX_TABLE_ELEMENTS: usize = 100_000;
pub(crate) const MAX_INSTANCES: usize = 16;
pub(crate) const MAX_TABLES: usize = 16;
pub(crate) const MAX_MEMORIES: usize = 4;

/// Compiles and instantiates workloads. Cloning it shares the engine.
#[derive(Clone)]
pub struct Runtime {
    pub(crate) engine: Engine,
}

/// A compiled component, ready to instantiate any number of times. Cloning
/// it is cheap.
#[derive(Clone)]
pub struct Compiled(Component);

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
    pub fn new() -> wasmtime::Result<Runtime> {
        let mut config = Config::new();
        // NaN bit patterns are the one source of float nondeterminism in Wasm.
        config.cranelift_nan_canonicalization(true);
        config.consume_fuel(true);
        config.wasm_relaxed_simd(false);
        config.max_wasm_stack(MAX_WASM_STACK);
        Ok(Runtime {
            engine: Engine::new(&config)?,
        })
    }

    /// Loads `bytes` as a component and instantiates it under `limits`. The
    /// state is the workload's initial state; call [`Workload::restore`] to
    /// resume.
    pub fn load(&self, bytes: &[u8], limits: Limits) -> Result<Workload, LoadError> {
        self.instantiate(&self.compile(bytes)?, limits)
    }

    /// Compiles `bytes` as a component and checks it imports nothing. This is
    /// the slow part of [`Runtime::load`].
    pub fn compile(&self, bytes: &[u8]) -> Result<Compiled, LoadError> {
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
        Ok(Compiled(component))
    }

    /// Instantiates a compiled component under `limits`, in its initial
    /// state.
    pub fn instantiate(&self, compiled: &Compiled, limits: Limits) -> Result<Workload, LoadError> {
        let component = &compiled.0;
        let mut store = self.store(limits);
        store
            .set_fuel(limits.fuel_per_call)
            .map_err(|e| LoadError::Instantiate(e.to_string()))?;
        let bindings = Bindings::instantiate(&mut store, component, &Linker::new(&self.engine))
            .map_err(|e| LoadError::Instantiate(e.to_string()))?;
        Ok(Workload::new(store, bindings, limits))
    }

    pub(crate) fn store(&self, limits: Limits) -> Store<StoreLimits> {
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(usize::try_from(limits.max_memory_bytes).unwrap_or(usize::MAX))
            .table_elements(MAX_TABLE_ELEMENTS)
            .instances(MAX_INSTANCES)
            .tables(MAX_TABLES)
            .memories(MAX_MEMORIES)
            .build();
        let mut store = Store::new(&self.engine, store_limits);
        store.limiter(|limits| limits);
        store
    }
}

#[cfg(test)]
mod tests;
