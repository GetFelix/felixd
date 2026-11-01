use thiserror::Error;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};

use crate::Limits;
use crate::bindings::{Guest, Version};
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
pub struct Compiled(Component, Version);

/// Why a component could not be loaded.
#[derive(Debug, Error)]
pub enum LoadError {
    #[error("the module is not a valid component: {0}")]
    Invalid(String),
    /// A workload gets nothing from the host, not even a clock or randomness.
    #[error("the component imports {}, but workloads may import nothing", .0.join(", "))]
    Imports(Vec<String>),
    /// The component is a workload of the 0.1 contract, which this build
    /// does not run. It needs a rebuild, not a retry.
    #[error("the component is built against felixd:workload@0.1.0, rebuild it against 0.3")]
    Contract01,
    /// A workload of a contract version newer than this build knows. Another
    /// node may run it, so it needs a newer felixd, not a rebuild.
    #[error(
        "the component is built against a workload contract this build does not know ({}); \
         upgrade felixd, which runs 0.3 and 0.2",
        .0.join(", ")
    )]
    UnknownContract(Vec<String>),
    #[error(
        "the component does not export the step and state interfaces of felixd:workload@0.3.0 \
         or 0.2.0; it exports {}",
        found(.0)
    )]
    NotAWorkload(Vec<String>),
    #[error("the component does not implement the workload world: {0}")]
    Instantiate(String),
}

/// The top-level functions a 0.1 workload exports.
const CONTRACT_01: [&str; 4] = ["step", "snapshot", "restore", "inspect"];

fn found(exports: &[String]) -> String {
    if exports.is_empty() {
        "nothing".to_owned()
    } else {
        exports.join(", ")
    }
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

    /// Compiles `bytes` as a component and checks it imports nothing and
    /// exports the interfaces of contract 0.3 or 0.2. This is the slow part of [`Runtime::load`].
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
        let exports: Vec<String> = component
            .component_type()
            .exports(&self.engine)
            .map(|(name, _)| name.to_string())
            .collect();
        let has = |name: &str| exports.iter().any(|e| e == name);
        let version = [Version::V03, Version::V02].into_iter().find(|version| {
            let (step, state) = version.interfaces();
            has(step) && has(state)
        });
        // Interfaces of this package at a version this build does not know.
        let known = [Version::V03, Version::V02].map(Version::interfaces);
        let newer: Vec<String> = exports
            .iter()
            .filter(|e| e.starts_with("felixd:workload/"))
            .filter(|e| {
                !known
                    .iter()
                    .any(|(step, state)| e.as_str() == *step || e.as_str() == *state)
            })
            .cloned()
            .collect();
        if let Some(version) = version {
            Ok(Compiled(component, version))
        } else if !newer.is_empty() {
            Err(LoadError::UnknownContract(newer))
        } else if CONTRACT_01.iter().all(|name| has(name)) {
            Err(LoadError::Contract01)
        } else {
            Err(LoadError::NotAWorkload(exports))
        }
    }

    /// Instantiates a compiled component under `limits`, in its initial
    /// state.
    pub fn instantiate(&self, compiled: &Compiled, limits: Limits) -> Result<Workload, LoadError> {
        let Compiled(component, version) = compiled;
        let mut store = self.store(limits);
        store
            .set_fuel(limits.fuel_per_call)
            .map_err(|e| LoadError::Instantiate(e.to_string()))?;
        let guest = Guest::instantiate(*version, &mut store, component, &Linker::new(&self.engine))
            .map_err(|e| LoadError::Instantiate(e.to_string()))?;
        Ok(Workload::new(store, guest, limits))
    }

    pub(crate) fn store(&self, limits: Limits) -> Store<StoreLimits> {
        let mut store = Store::new(&self.engine, store_limits(limits));
        store.limiter(|limits| limits);
        store
    }
}

pub(crate) fn store_limits(limits: Limits) -> StoreLimits {
    StoreLimitsBuilder::new()
        .memory_size(usize::try_from(limits.max_memory_bytes).unwrap_or(usize::MAX))
        .table_elements(MAX_TABLE_ELEMENTS)
        .instances(MAX_INSTANCES)
        .tables(MAX_TABLES)
        .memories(MAX_MEMORIES)
        .build()
}

#[cfg(test)]
mod tests;
