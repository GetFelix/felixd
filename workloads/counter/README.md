# counter

An example workload of [contract 0.2](../../docs/contract.md). It counts its input records.

It is not part of the main cargo workspace; see [../README.md](../README.md). It imports nothing and
depends only on `wit-bindgen`.

## What it does

- **Input:** any record. The payload is ignored.
- **Output:** one record per input, under the name `out` with no key. The payload is the new total
  as eight big-endian bytes (a `u64`, wrapping).
- **State:** the total, as one eight-byte snapshot chunk. `inspect` returns `{"count":<total>}`.
- Never rejects an input.

## Build

Run these from the repository root. The workloads are a separate cargo workspace for
`wasm32-unknown-unknown`, which `rust-toolchain.toml` installs:

```
cargo build --manifest-path workloads/Cargo.toml --release --target wasm32-unknown-unknown --package counter
```

That produces a core module at `wasm32-unknown-unknown/release/counter.wasm` in the target directory.
`felixd run` takes a component, so the module must be wrapped with `wit-component`, as
`felixd_testkit::workload("counter")` does. The tests call that function and need no manual step.

## Tests that use it

- `crates/runtime/tests/example_workloads.rs`: the running total, the same outputs on every run.
  Offline: `cargo test -p felixd-runtime --test example_workloads`.
- `crates/felixd/tests/` (`restart`, `answers`, `check`, `cluster`, `replay` and others) submit it
  as the standard workload. These need a Felix broker; see [crates/felixd](../../crates/felixd/README.md).
