/// What a workload may use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Fuel given to each call into the workload. Running out of it fails the
    /// workload.
    pub fuel_per_call: u64,
    /// The most linear memory the workload may grow to, in bytes.
    pub max_memory_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            fuel_per_call: 50_000_000,
            max_memory_bytes: 64 << 20,
        }
    }
}
