# felixd-runtime

The wasmtime host that runs one workload component. It loads a component of
[workload contract 0.2](../../docs/contract.md), steps it one input at a time, and takes and
restores its snapshots. It does no I/O and does not know about Felix.

It depends on `felixd-core` (for `Limits`) and wasmtime. `felixd-agent` and the `felixd` binary
depend on it. `felixd-testkit` is a dev-dependency, for building the example workloads.

## What it provides

- `Runtime::new()` makes an engine. `Runtime::load(bytes, limits)` compiles and instantiates a
  component; `compile` and `instantiate` do the two steps apart, so one `Compiled` serves many
  instances.
- `Workload` has `step(Input)`, `snapshot()`, `restore(&[&[u8]])` and `inspect()`. A step returns
  `Stepped::Outputs` or `Stepped::Rejected`.
- A call that cannot go on returns `WorkloadError::Failed(Failure)`, with a `FailureKind` of
  `OutOfFuel`, `Trap`, `Poison` or `Contract`. After that the workload is never called again.
- `WorkloadError::SnapshotOutOfFuel` is not a failure: the snapshot is abandoned and the caller
  drops the instance and resumes from the last snapshot.

## Determinism and limits

Every node must reach the same result for the same input, including where a workload fails. So:

- A component may import nothing. `LoadError::Imports` names what it asked for.
- NaN results are canonicalized, relaxed SIMD is off, and fuel metering is on.
- Each `step` and `inspect` call gets `limits.fuel_per_call` (50,000,000 by default). A whole
  snapshot or a whole restore shares `limits.snapshot_fuel` (500,000,000 by default).
- Linear memory is capped at `limits.max_memory_bytes` (64 MiB by default).
- The Wasm stack, table size, and the counts of instances, tables and memories are fixed in this
  crate, not left to wasmtime's defaults, because they decide where a workload fails.
- The host refuses a chunk over 4 MiB, more than 65,536 chunks, an invalid output name and an
  effect request, each as a contract failure.
- A component of the old contract, 0.1, is refused.

The limits come from the workload's spec, so every node applies the same ones.

## Build and test

All tests run offline. They build the example workloads from `workloads/` on demand, which needs
the `wasm32-unknown-unknown` target; `rust-toolchain.toml` installs it.

```
cargo build -p felixd-runtime
cargo test -p felixd-runtime
cargo test -p felixd-runtime --test example_workloads   # counter and window
cargo test -p felixd-runtime --test limits              # fuel, traps, memory (faulty)
cargo test -p felixd-runtime --test contract            # the contract's rules (faulty)
```

See [docs/contract.md](../../docs/contract.md) for the interface and what each side guarantees.
