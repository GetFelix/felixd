# Example workloads

Two example workloads that implement workload contract 0.2, the `workload` world in
[`../wit/workload.wit`](../wit/workload.wit), described in
[`../docs/contract.md`](../docs/contract.md). Both write every output under the name `out` with no
key, and snapshot their state as one chunk.

| Workload | What it does |
|---|---|
| `counter` | Counts its input records. Each step outputs the new total as eight big-endian bytes. |
| `window` | Groups `(timestamp_ms, value)` records into ten second windows. The record that opens a later window closes the open one and outputs its count, sum, min and max as JSON. Every other step outputs nothing. A record that is not 16 bytes is rejected with code 1. |

`scheduler` is the placement scheduler, which felixd runs as an ordinary workload. It reads spec
records and node facts and writes assignments; see [scheduler/README.md](scheduler/README.md).

They are a separate cargo workspace because they build for `wasm32-unknown-unknown`. The tests in
`felixd-runtime` build them on demand with `felixd-testkit` and run them in wasmtime:

```
cargo test -p felixd-runtime --test example_workloads
```

`faulty` is a test fixture, not an example. Its input picks what it does: loop, trap, allocate too
much, output a record too large to write, trap in `snapshot`, reject the input, return `poison`,
return an effect request, output under a given name and key, output its input's metadata, keep its
state in many snapshot chunks and change one of them, or return a chunk over the 4 MiB limit. The
list is at the top of `faulty/src/lib.rs`. Anything else it echoes under `out`.

`felixd-testkit` also has a small component of the old contract, 0.1, which the host must refuse.
