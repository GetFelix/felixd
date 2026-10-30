# admission

Admission (#114). It is a felixd workload like any other, so its decisions are written exactly
once, fail over and replay. The policy is `felixd_core::admission`; this crate wraps it as a
[contract 0.2](../../docs/contract.md) component. Its spec is the one spec not submitted through
it: `felixd init` writes it.

## What it does

- **Input:** `sys.intake`, where `felixd run` publishes requests: a workload id, the spec it
  asks for (state `running` or `stopped`), the module's length and the replication factor.
- **Output:** one decision per request under the name `out`, keyed by the workload id, accepting
  the spec or rejecting it with a reason code. Formats are in
  [docs/records.md](../../docs/records.md#requests-and-decisions).
- **Policy:** the id is valid and not `admission`; the inputs are 1 to 64 different streams, and
  only the scheduler may read `sys.specs` and `sys.facts`; the output stream is `wl.<id>.out`; the
  state is `running` or `stopped`; snapshot settings are at least 1; fuel per call is at most ten
  times the default, memory at most 1 GiB, snapshot fuel at most ten times the default, the module
  at most 8 MiB and the replication factor 1 to 7. A decision reads nothing but its request, so
  the same requests always get the same decisions.
- **State:** none. A snapshot is empty.

The node holding admission's lease records each accepted decision on `sys.specs`, after checking
the module in `modules` has the hash and length the request gave and creating the workload's
streams. A node runs a spec record written after admission's own only if it names an accepting
decision for exactly that spec.

## Changing the policy

A policy change is a new module. Before switching, replay it over the recorded requests:

```
felixd replay --id admission --diff --all --module <hash of the new module in modules>
```

It prints each request the new policy decides otherwise. `admission-strict` is such a module,
allowing no more than the default fuel per call; the tests use it.

## Build

```
cargo build --release --target wasm32-unknown-unknown --package admission
wasm-tools component new target/wasm32-unknown-unknown/release/admission.wasm -o admission.wasm
```
