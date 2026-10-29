# felixd-testkit

Helpers shared by felixd's tests. Nothing in the shipped binary depends on it: it is a
dev-dependency of `felixd-runtime` and `felixd`. It depends on no other felixd crate.

## What it provides

- `workload(name)` builds the example workload `name` (a directory under `workloads/`: `counter`,
  `window` or `faulty`) with `cargo build --release --target wasm32-unknown-unknown` and wraps the
  core module into a component with `wit-component`. It returns the component's bytes and panics
  if the build fails. It builds into `$CARGO_TARGET_DIR/workloads`, or `target/workloads` when that
  is unset, and takes a lock so tests in one process do not build at once.
- `contract_01_component()` returns a small component of the old contract, 0.1, which the host
  must refuse.

A test runs the real workload instead of a stand-in:

```rust
use felixd_runtime::{Limits, Runtime};

fn counter() -> felixd_runtime::Workload {
    Runtime::new()
        .expect("create the runtime")
        .load(&felixd_testkit::workload("counter"), Limits::default())
        .expect("load the counter")
}
```

## What tests need

`workload` needs the `wasm32-unknown-unknown` target (`rust-toolchain.toml` lists it) and a
first build of the workloads' dependencies. It needs no broker. Tests that use it fall into two
groups:

- `felixd-runtime` tests run offline.
- `felixd` integration tests also need a Felix broker and control plane. They read `FELIX_BROKERS`
  and the other `FELIX_*` variables, and skip with a message when `FELIX_BROKERS` is unset, unless
  `FELIXD_REQUIRE_FELIX` is set, which CI does. The README at the top level shows how to start the
  test stack in `tests/felix/`.

## Build and test

```
cargo build -p felixd-testkit
cargo test -p felixd-testkit    # no tests of its own; the crates above exercise it
```
