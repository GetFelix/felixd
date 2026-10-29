# faulty

A test fixture, not an example. It misbehaves on request, so tests can check what the host and the
agent do about it. Do not run it as a real workload.

It is not part of the main cargo workspace; see [../README.md](../README.md). It imports nothing and
depends only on `wit-bindgen`.

## What it does

The input payload picks the behaviour. Any payload not listed here is echoed under the name `out`
with no key. The list is also at the top of `src/lib.rs`.

| Payload | Fault |
|---|---|
| `spin` | loops forever, so it runs out of fuel |
| `trap` | panics, which traps |
| `grow` | allocates 64 MiB, over the default memory cap |
| `huge` | outputs one 17 MiB record, more than one write to Felix holds |
| `trap-snapshot` | outputs nothing; every later `snapshot` traps |
| `split:<bytes>` | outputs each byte after the colon as its own record, so one step writes a batch |
| `reject:<code>` | rejects the input with that code |
| `poison` | returns `poison` |
| `request` | returns an effect request, which this build refuses |
| `named:<name>:<key>:<payload>` | one output under that name and key (no key when empty); the name is not checked, so an invalid one tests the host |
| `meta` | outputs the input's metadata as text: stream, incarnation, offset, timestamp, key in hex (`-` for none) and payload in hex |
| `chunks:<n>` | the state becomes `n` counters, each its own snapshot chunk |
| `touch:<i>` | adds one to counter `i`, changing only its chunk, and outputs the new value in decimal |
| `big-chunk` | outputs nothing; every later snapshot has a chunk over the 4 MiB limit |

An effect response as input fails the workload with `poison`.

## Build

Run from the repository root. `rust-toolchain.toml` installs the `wasm32-unknown-unknown` target:

```
cargo build --manifest-path workloads/Cargo.toml --release --target wasm32-unknown-unknown --package faulty
```

That produces a core module at `wasm32-unknown-unknown/release/faulty.wasm` in the target directory. The
host needs a component, which `felixd_testkit::workload("faulty")` builds and wraps with
`wit-component`. The tests call it, so there is no manual step.

## Tests that use it

- `crates/runtime/tests/limits.rs` (`spin`, `trap`, `grow`) and `contract.rs` (rejections, poison,
  names and keys, metadata, many-chunk snapshots, oversize chunks, snapshot fuel). Offline:
  `cargo test -p felixd-runtime --test limits --test contract`.
- `crates/felixd/tests/contract.rs`, `failures.rs` and `replay.rs` submit it to an agent to check
  failure records, rejections and replay. These need a Felix broker; see
  [crates/felixd](../../crates/felixd/README.md).
