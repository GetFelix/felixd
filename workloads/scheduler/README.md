# scheduler

The placement scheduler (#60). It is a felixd workload like any other, so its assignments fail
over, are written exactly once and replay. The logic is `felixd_core::schedule::Scheduler`; this
crate wraps it as a [contract 0.2](../../docs/contract.md) component.

Unlike the other workloads here it depends on `felixd-core`, for the record formats it reads and
writes.

## What it does

- **Inputs:** `sys.specs` (spec records) and `sys.facts` (node facts), merged through its merge
  log. Its clock is the append time of the inputs it has stepped, so a replay assigns the same way.
- **Output:** an assignment each time where a workload should run changes, under the name `out`
  with the workload id as key. The payload is `{"format":1,"value":{"workload":"w","node":"n"}}`,
  without `node` once the workload no longer runs.
- **Policy:** a node takes work while it has written a fact within three of its own intervals and
  is not draining. A running workload with no assignment, or one assigned to a node that no longer
  takes work, goes to the node with the fewest assignments and room under its `max_workloads`, the
  lowest id first. Nothing is moved off a node that still takes work. The scheduler never assigns
  itself.
- **State:** the nodes, the running workloads and the assignments, as JSON in 1 MiB snapshot
  chunks. `inspect` returns the same JSON.
- An input it cannot read is rejected with code 1.

## Run it

```
felixd run --id scheduler --input sys.specs --input sys.facts scheduler.wasm
```

The id must be `scheduler`: nodes follow `wl.scheduler.out`. Build it as the counter's README
shows, with `--package scheduler`.

## Tests that use it

- `crates/core/src/schedule/tests.rs`: the fold. Offline.
- `crates/runtime/tests/example_workloads.rs`: the component assigns as the fold does and carries
  on the same after a restore. Offline.
- `crates/felixd/tests/scheduler.rs`: takeover of the scheduler's node, an assignment to a dead
  node and to one that never claims, and `felixd check` and `replay --diff` on its output. These
  need a Felix broker.
