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
- **Snapshot layout:** it declares `{"format":"counter","reads":[]}` in its `felixd.snapshot` custom
  section ([contract.md](../../docs/contract.md#snapshot-layouts)), so another module that reads
  `counter` can restore its snapshots.

## Variants

Cargo features build variants from the same source, for the tests that check an upgrade with
`felixd replay --diff --module`. They are not meant to run:

| Feature | Behaviour | Declares |
|---|---|---|
| `layout-v2` | Identical to the default. | `{"format":"counter.2","reads":["counter"]}` |
| `changed` | Counts by two once the total reaches ten. Includes `layout-v2`. | As `layout-v2` |
| `unreadable` | Expects a sixteen-byte chunk in `restore`, so restoring a counter snapshot traps. Includes `layout-v2`. | As `layout-v2` |
| `foreign-layout` | Identical to the default. | `{"format":"tally","reads":[]}` |
| `malformed-layout` | Identical to the default. | JSON that is not a declaration, so reading it is an error |
| `undeclared` | Identical to the default. | Nothing |

`felixd_testkit::workload_with("counter", &["changed"])` builds one. Use one of `layout-v2`,
`foreign-layout`, `malformed-layout` and `undeclared` at a time; `changed` and `unreadable` already
include `layout-v2`.

## Build

Run these from the repository root. The workloads are a separate cargo workspace for
`wasm32-unknown-unknown`, which `rust-toolchain.toml` installs:

```
cargo build --manifest-path workloads/Cargo.toml --release --target wasm32-unknown-unknown --package counter
```

Add `--features changed` (or another feature from [Variants](#variants)) to build a variant. The
build produces a core module at `workloads/target/wasm32-unknown-unknown/release/counter.wasm`,
unless `CARGO_TARGET_DIR` is set. `felixd run` takes a component, so wrap the module with
[wasm-tools](https://github.com/bytecodealliance/wasm-tools) (`cargo install wasm-tools`):

```
wasm-tools component new workloads/target/wasm32-unknown-unknown/release/counter.wasm -o counter.wasm
```

The tests do the same through `felixd_testkit::workload("counter")`, so they need no manual step.

## Tests that use it

- `crates/runtime/tests/example_workloads.rs`: the running total, the same outputs on every run.
  Offline: `cargo test -p felixd-runtime --test example_workloads`.
- `crates/runtime/tests/snapshot_layouts.rs`: the declared layouts and restoring a snapshot into each
  variant. Offline: `cargo test -p felixd-runtime --test snapshot_layouts`.
- `crates/felixd/tests/upgrade.rs`: `felixd replay --diff --module` with each variant, from a snapshot
  and from the initial state.
- `crates/felixd/tests/` (`restart`, `answers`, `check`, `cluster`, `replay` and others) submit it
  as the standard workload. These need a Felix broker; see [crates/felixd](../../crates/felixd/README.md).
