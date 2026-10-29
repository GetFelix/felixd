# window

An example workload of [contract 0.2](../../docs/contract.md). It aggregates `(timestamp_ms,
value)` records into ten second windows.

It is not part of the main cargo workspace; see [../README.md](../README.md). It imports nothing and
depends only on `wit-bindgen`.

## What it does

- **Input:** 16 bytes, a little-endian `u64` timestamp in milliseconds followed by a little-endian
  `i64` value. A window is the timestamp rounded down to a multiple of 10,000.
- **Output:** the record that opens a later window closes the open one and outputs its summary
  under the name `out`, with no key, as JSON:
  `{"window_start":0,"count":3,"sum":12,"min":2,"max":6}`. Every other step outputs nothing. A
  record with an earlier timestamp than the open window is counted in the open one.
- **Rejection:** a record that is not 16 bytes is rejected with code 1, and the workload carries on.
- **State:** the open window and two counters, as one 57-byte snapshot chunk. `inspect` returns the
  open window, and how many windows were emitted and records rejected. Poison is returned if an
  input is an effect response.

## Build

Run these from the repository root. The workloads are a separate cargo workspace for
`wasm32-unknown-unknown`, which `rust-toolchain.toml` installs:

```
cargo build --manifest-path workloads/Cargo.toml --release --target wasm32-unknown-unknown --package window
```

That produces a core module at `workloads/target/wasm32-unknown-unknown/release/window.wasm`, unless
`CARGO_TARGET_DIR` is set. `felixd run` takes a component, so wrap the module with
[wasm-tools](https://github.com/bytecodealliance/wasm-tools) (`cargo install wasm-tools`):

```
wasm-tools component new workloads/target/wasm32-unknown-unknown/release/window.wasm -o window.wasm
```

The tests do the same through `felixd_testkit::workload("window")`, so they need no manual step.

## Tests that use it

- `crates/runtime/tests/example_workloads.rs`: a window closes with the right summary, the same
  outputs on every run, and a restore from a snapshot carries on identically.
  Offline: `cargo test -p felixd-runtime --test example_workloads`.
- `crates/felixd/tests/restart.rs` and `specs.rs` run it under an agent that is killed and
  restarted. These need a Felix broker; see [crates/felixd](../../crates/felixd/README.md).
