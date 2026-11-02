# admission

Admission (#114). It is a felixd workload like any other, so its decisions are written exactly
once, fail over and replay. The policy is `felixd_core::admission`; this crate wraps it as a
[contract 0.3](../../docs/contract.md) component, which is what lets it see who asked. Its spec is the one spec not submitted through
it: `felixd init` writes it.

## What it does

- **Input:** `sys.intake`, where `felixd run` publishes requests: a workload id, the spec it
  asks for (state `running` or `stopped`), the module's length and the replication factor.
- **Requester:** the principal that published the request, as the broker authenticated and stored
  it. Admission reads it from the record's publisher, never from the payload, which anyone who can
  publish can fill with anything. It needs a broker that records publishers: `FELIX_RECORD_PUBLISHERS=true`
  on a single broker, or the `publisher_principal` fleet feature finalized in a cluster. Without
  that, every request is rejected as `no_requester`.
- **Output:** one decision per request under the name `out`, keyed by the workload id, accepting
  the spec or rejecting it with a reason code, and recording the requester. Formats are in
  [docs/records.md](../../docs/records.md#requests-and-decisions).
- **Policy:** the request has a recorded requester (a request with none is rejected as
  `no_requester`, so admission fails closed; an empty name counts as none), and the requester is on the policy's list when it has
  one (`Policy::requesters`, empty by default, so any recorded requester; another is rejected as
  `not_permitted`). The id is valid and not a system workload's (`admission`, `scheduler`), whose specs
  only an operator writes; the inputs are 1 to 64 different streams, none of them a `sys.` stream
  or one of admission's own; the output stream is `wl.<id>.out`; the
  state is `running` or `stopped`; snapshot settings are at least 1; fuel per call is at most ten
  times the default, memory at most 1 GiB, snapshot fuel at most ten times the default, the module
  at most 8 MiB and the replication factor 1 to 7. A decision reads nothing but its request and
  its publisher, so the same requests always get the same decisions, live and on replay.
- **State:** none. A snapshot is empty.

The node holding admission's lease records each accepted decision on `sys.specs`, after checking
the module in `modules` has the hash and length the request gave and creating the workload's
streams. A node runs a spec record written after admission's own only if it names an accepting
decision for exactly that spec.

## Allowing only some requesters

To let only certain principals ask, build a module whose `policy()` lists them, the way
`admission-strict` lowers the fuel limit. `admission-requesters` is such a module, listing
`p:alice` and `p:bob`:

```rust
use felixd_core::admission::Policy;

fn policy() -> Policy {
    Policy {
        requesters: &["p:alice", "p:bob"],
        ..Policy::default()
    }
}

include!("../../admission/src/component.rs");
```

The module also needs `felixd-core` and `wit-bindgen` as dependencies, like the crates beside it.

Names are matched exactly as the broker records them, which is the subject of the requester's
token. A request from anyone else is rejected as `not_permitted`, and the decision still records who
asked. A new deployment can start from this module. An existing deployment keeps the module it
has: a workload's module cannot change once it has written output, and admission's id is fixed, so
switching waits for module upgrades.

## Changing the policy

A policy change is a new module. Before switching, replay it over the recorded requests. Replaying
this 0.3 module over decisions a 0.2 admission wrote differs on every line, because those decisions
have no requester and the 0.3 module rejects them as `no_requester`:

```
felixd replay --id admission --diff --all --module <hash of the new module in modules>
```

It prints each request the new policy decides otherwise. `admission-strict` is such a module,
allowing no more than the default fuel per call; the tests use it. Admission keeps no state, so its
snapshots are empty, and it declares no snapshot layout (see
[contract.md](../../docs/contract.md#snapshot-layouts)). A replay with another module therefore
always starts from the initial state, which costs nothing here.

## Build

```
cargo build --release --target wasm32-unknown-unknown --package admission
wasm-tools component new target/wasm32-unknown-unknown/release/admission.wasm -o admission.wasm
```
