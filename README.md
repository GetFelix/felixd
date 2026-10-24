# felixd

A node agent that runs programs across a cluster with
[Felix](https://github.com/GetFelix/felix) as its only backend.

> **Status:** design stage. Nothing runs yet. The plan is in
> [docs/design.md](docs/design.md) and the work is tracked in
> [milestones](https://github.com/GetFelix/felixd/milestones).

## What we are trying to achieve

The long-term goal is a distributed operating system built on one idea:
every change to the system is a durable, ordered record in a log. That covers
what should run, where it runs, what each program received and what it
produced. If the log is the system's memory, a program's state at any moment
can be rebuilt from it. That one property gives you:

- **Failover without lost or repeated work.** When a machine dies, its programs
  continue on another machine from the last record they handled.
- **Migration.** Moving a program is the same operation as failover, done on
  purpose.
- **Time travel.** You can rebuild any program as it was at any past moment,
  inspect it, and step forward record by record.
- **An audit trail.** Every decision the system made is on the record, in
  order.

Today's cluster managers keep a snapshot of current state, so they can
restart programs but can't do any of the above. Felix already provides the
log, replication, conditional writes and per-client resume that the idea
needs. felixd is the first, deliberately small step: a node agent that proves
the idea on real machines.

## The first step

felixd runs on every node. It runs WebAssembly programs ("workloads") whose
only inputs and outputs are Felix streams. The first version has to show
three things:

1. **Kill a node mid-run.** Each of its workloads resumes on another node, and
   its output has no gaps and no duplicates.
2. **Freeze a node and let it come back late.** The stale copy of a workload
   cannot write anything once another node has taken over.
3. **Replay a workload to any past offset or time** and inspect its state.

Scheduling policy, resource limits, networking and native processes come
after that works. [docs/design.md](docs/design.md) explains the path from
here to an operating system.

## What you will be able to do

- Start felixd on a few machines pointed at one Felix cluster
- Submit a Wasm workload and the stream it reads from
- Kill or freeze any node and watch its workloads move
- Chain workloads, so one's output is the next one's input
- Replay a workload to an offset or a time and print its state

## Felix features it uses

| Need | Felix feature |
|---|---|
| What should run | A cache, watched with retained values |
| Which node runs a workload | A cache entry with a TTL, taken and renewed with `cache_put_if` |
| Which nodes are alive | A cache with a TTL |
| A workload's input | A durable stream, resumed with `subscribe_from` |
| Output that a stale owner cannot write | `publish_if` against the output stream's tail |
| Surviving a broker's loss | `Quorum` streams and caches |
| Snapshots | A durable stream per workload |
| Replay to a time | `offset_for_time` and range reads |
| What each node may write | RBAC grants on each node's token |

## How it works

A workload is a Wasm component with no access to clocks, randomness, files or
the network. It sees only the records on its input stream, so given the same
records it always reaches the same state. That determinism is what makes
failover and replay safe.

Each felixd watches the list of workloads and claims unowned ones by writing a
lease with a conditional put. While it holds the lease it feeds the workload
input records, appends its output with a write that only succeeds at the
output stream's current tail, and writes a snapshot every so often. If the
node stops renewing, the lease expires and another felixd claims the workload,
loads the latest snapshot and resumes from the input offset recorded in it.
Each output record carries the input offset that produced it, so the new
owner knows which outputs already exist and does not write them again.
[docs/design.md](docs/design.md) has the details.

## Status

| M | Milestone | What it proves | State |
|---|---|---|---|
| 0 | One node, one workload | The workload contract works; a restart resumes where it stopped | Planned |
| 1 | Leases and failover | Kill a node and its workloads continue elsewhere | Planned |
| 2 | Exactly-once output | A frozen node that comes back cannot write | Planned |
| 3 | Replay | Rebuild any workload at any offset or time | Planned |
| 4 | Pipelines and operations | Chained workloads, drain, status, metrics | Planned |
| 5 | Self-hosting | Anyone can run a cluster from the docs | Planned |

## Documentation

- [docs/design.md](docs/design.md): goals, design, data model and build order
- [Felix docs](https://docs.getfelix.dev/)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT. See [LICENSE](LICENSE).
