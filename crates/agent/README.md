# felixd-agent

The node loop. It reads workloads from Felix, claims and holds their leases, feeds each one its
input, writes its output and snapshots back, and carries a workload on from the right input after
a crash or a takeover. The `felixd` command line is a thin wrapper over this crate.

It depends on `felixd-core` and `felixd-runtime`, and on `felix-client` for Felix. The `felixd`
binary depends on it.

## What it provides

| Module | Does |
|---|---|
| `agent` | `run` watches the `workloads` and `leases` caches, writes this node to `nodes`, and runs each workload it manages to claim while the lease holds. `AgentConfig` sets the node id, lease timing and placement. |
| `submit` | What `felixd run` does: stores the module in `modules`, creates the `wl.<id>.out` and `wl.<id>.snap` streams, appends the spec record to `sys.specs` and points `workloads` at it. |
| `check` | What `felixd check` does: replays each workload and reports gaps, duplicates, records out of order, epoch regressions and outputs that differ. |
| `replay` | What `felixd replay` does: `replay` rebuilds a workload's state as of an input offset or time, and `diff` compares a replay with the recorded outputs. Neither writes anything. |
| `settings`, `admin`, `felix` | How to reach Felix, the control plane's REST calls, and the Felix calls felixd makes. |

Behaviour that matters:

- **Claim.** A claim is a conditional cache write that succeeds only once the broker counts the
  previous lease as expired or deleted. The winner takes the next epoch, above every earlier
  claim's and every epoch already on the output stream, before it runs anything.
- **Renew.** The holder renews every TTL/3. A refused renewal means the lease is gone and it stops
  at once. A holder that cannot reach Felix stops by its own clock at two thirds of the TTL, before
  another node can claim.
- **Spec records.** An agent runs a `workloads` value only when the `sys.specs` record it names
  says the same, and refuses a spec whose module or input differ from the one the output stream's
  first claim names, before its own claim and again after it. Either refusal leaves the workload to
  an operator.
- **Fence.** A new owner appends a claim record, naming its spec record, to the output stream before it resumes. Every
  later write is `publish_if` at the tail, so a node frozen past its lease is refused when it
  wakes, whatever its clock says.
- **Resume.** The owner restores the newest snapshot, steps the inputs the output stream already
  covers and compares each result with the record in its place (`felixd_core::resume`), then
  carries on. Each output lands once through lost answers, partial batches and crashes.
- **Failure.** A trap, running out of fuel or `poison` writes a failure record, then `failed` into
  the spec, and the workload stops. A rejection does not fail it. A component of the old contract
  is left unclaimed rather than failed.

The `faults` feature lets `FELIXD_FAULTS` inject crashes and dropped answers, such as
`crash-after-append-sent=3`. Only the `felixd` integration tests enable it; a normal build
ignores the variable. The list is at the top of `src/faults.rs`.

## Build and test

```
cargo build -p felixd-agent
cargo test -p felixd-agent --lib    # offline: lease, runner and agent logic against fakes
```

The behaviour above is exercised end to end by the `felixd` integration tests, which need a Felix
broker; see [crates/felixd](../felixd/README.md).

For depth, see [docs/design.md](../../docs/design.md), [docs/records.md](../../docs/records.md)
and [docs/formal.md](../../docs/formal.md).
