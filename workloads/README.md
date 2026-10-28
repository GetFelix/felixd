# Example workloads

Two workloads that implement the `workload` world in [`../wit/workload.wit`](../wit/workload.wit).

| Workload | What it does |
|---|---|
| `counter` | Counts its input records. Each step outputs the new total as eight big-endian bytes. |
| `window` | Groups `(timestamp_ms, value)` records into ten second windows. The record that opens a later window closes the open one and outputs its count, sum, min and max as JSON. Every other step outputs nothing. |

They are a separate cargo workspace because they build for `wasm32-unknown-unknown`. The tests in
`felixd-runtime` build them on demand with `felixd-testkit` and run them in wasmtime:

```
cargo test -p felixd-runtime --test example_workloads
```
