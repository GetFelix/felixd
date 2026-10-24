# felixd design

felixd is the first piece of a distributed operating system whose state is a
log. This document covers where that is going, what felixd has to prove first,
and how it maps onto Felix. Felix references are to Felix 0.6 (`c3ec6f00e`).

## Goal

The operating system we want has one source of truth: an ordered, replicated
log of everything that happened. That includes what should run, which machine
runs it, every input a program received and every output it produced. All
other state, from the list of running programs to a program's memory, is
derived from that log and can be rebuilt from it.

That gives an operating system properties that cluster managers built on a
key-value snapshot (Kubernetes on etcd, Nomad on Raft state) don't have:

- A program survives the loss of its machine with no lost or repeated work.
  Restarting from scratch is no longer the only option.
- Moving a program between machines is the same operation as failover.
- Any program can be rebuilt as it was at any past moment.
- Every decision the system made is recorded, in order, with who made it.

Felix supplies the parts that are hard to build: durable streams, quorum
replication, conditional writes, leases with TTLs, resume from an offset, and
per-tenant grants. What it doesn't do is run programs. felixd fills that gap.

### Where Felix fits, and where it doesn't

Felix is the system bus and the system journal. It isn't the kernel. A round
trip through a broker costs far more than local IPC, so Felix carries
coordination and program-level input and output, never syscalls or memory
traffic. Each machine still runs an ordinary host OS. Felix itself is the base
layer and is not scheduled by felixd, much as etcd is not scheduled by
Kubernetes.

### The path

1. **felixd (this repo, milestones M0 to M5).** Deterministic Wasm workloads
   with failover, exactly-once output and replay.
2. **Placement.** Resource-aware scheduling, node labels, spreading and
   draining, all decided through records on the log.
3. **Services.** Workloads that answer requests, with request and reply over
   Felix and name-based discovery.
4. **Effects.** Controlled access to the outside world (HTTP, files, clocks),
   with every response recorded as an input so replay stays exact.
5. **Native processes.** Programs that can't be made deterministic, run with
   checkpoint-and-restart semantics instead of replay.

Each step is a separate design. The rest of this document covers step 1.

### The long-term picture

This is where the path leads. It isn't a commitment, but it's the test
each step should pass: does it move toward this?

Every machine runs an ordinary host kernel and felixd. A few machines also run
Felix, which is the one layer the system doesn't schedule. Everything else is
a program on the log, including the scheduler, the name service and the shell's
history.

| OS concept | In an OS built on Felix |
|---|---|
| Process | A workload: a module plus its input streams. Its state is a fold over its inputs, so the record is the process. |
| PID | A workload id, independent of the machine running it |
| Address space | A local cache of the fold, rebuilt anywhere from a snapshot and the input |
| Pipes and IPC | Streams. `a \| b` makes b's input a's output, durable and across machines. |
| Signals | Records on a control stream, ordered with the workload's other input |
| Files | Streams are append-only files. A mutable file is a workload folding edits into contents, so old versions come for free. |
| Namespace | A tree of stream and cache names, with per-user views as in Plan 9 |
| Scheduler | A workload that reads heartbeats and demand and writes assignments, so scheduling decisions are recorded and replayable |
| System calls | Requests on a stream, carried out by a host service whose response comes back as input. Replay reuses the recorded response. |
| Time and randomness | Inputs like any other |
| Users and permissions | Felix tenants and grants. A program's token is its capability. |
| Boot | Reading the desired state and claiming leases. A cold start of the cluster is a replay. |
| Logs, `ps`, debugging | The log already is the history. Debugging is replay. |
| Upgrades | Run the new module over recorded inputs, compare, then switch at an offset |

Programs come in three kinds, matching steps 1, 4 and 5 of the path:

1. **Deterministic** (pure Wasm): full replay, migration and time travel.
2. **Effectful:** replayable, because every effect is recorded, at the cost of
   a round trip through the log per effect.
3. **Native** (existing binaries, databases, GPUs): not replayable.
   Checkpoint-and-restart, with their I/O recorded.

A session might look like this. The shell is a later step and the names are
placeholders:

```
$ fx run stats.wasm --in /sensors/temp --as /proc/temp-stats
$ fx at "2026-10-24 14:02" /proc/temp-stats inspect
$ fx why /proc/temp-alerts@9412      # which inputs produced this record
```

The hard parts, besides determinism at the effect boundary:

- **Latency.** Every effect and IPC hop goes through a replicated log, so the
  cost is milliseconds, not microseconds. This is an OS for services, data
  processing, fleets and simulations, not desktops or tight loops.
- **History grows.** Snapshots, compaction and a policy for how far back time
  travel reaches are part of the design, not an afterthought.
- **The log as a bottleneck.** Shard along the namespace so unrelated subtrees
  never coordinate. Programs that span shards need a recorded merge order
  (#25).
- **Secrets on the record.** A replicated history of everything includes
  every secret that passed through it, so records need encryption, and
  history needs redaction.

Plan 9 is the closest ancestor: one protocol for everything, per-process
namespaces, and a file system you could browse as it was on any past day. How
much of it to borrow is discussed in
[discussion #38](https://github.com/GetFelix/felixd/discussions/38).

## What felixd has to demonstrate

- **Failover:** kill a node mid-run, and each of its workloads continues on
  another node. Its output stream has no gaps and no duplicates.
- **Fencing:** freeze a node (`SIGSTOP`) past its lease and resume it. Its
  stale workloads write nothing.
- **Replay:** rebuild any workload at any input offset or time, and print its
  state.
- **Pipelines:** one workload's output is another's input, and failover in the
  middle of the chain loses nothing.

Out of scope for felixd: placement policy beyond "first claim wins", CPU and
memory limits beyond Wasm fuel and memory caps, networking, and native
processes.

## Architecture

One Rust workspace and one binary.

```
felixd agent      run on every node
felixd run        submit a workload
felixd ps         list workloads, owners and positions
felixd drain      release a node's workloads
felixd replay     rebuild a workload at an offset or time
```

| Crate | Owns |
|---|---|
| `core` | Record formats, key and stream names, lease rules, the resume fold. No I/O. |
| `runtime` | The wasmtime host: loading, fuel and memory caps, `step`, `snapshot`, `restore`, `inspect` |
| `agent` | The node loop: watching workloads, leases, feeding input, writing output and snapshots |
| `felixd` | The CLI |

Felix is the only backend. felixd keeps no local state that matters: a node
can lose its disk and rejoin.

## The workload contract

A workload is a Wasm component that implements one WIT world:

```wit
world workload {
  // Handle one input record. Returns the output records it produced.
  export step: func(input: list<u8>) -> list<list<u8>>;
  export snapshot: func() -> list<u8>;
  export restore: func(state: list<u8>);
  // Current state as JSON, for replay and inspection.
  export inspect: func() -> string;
}
```

The component gets no WASI imports: no clock, no randomness, no files, no
network. Anything it needs from the world, including the current time, arrives
as an input record. With the same inputs, a workload therefore always produces
the same outputs and the same state, which is what lets a second node take
over and a replay land on the state the original had. Wasm floating point is
deterministic apart from NaN bit patterns, and wasmtime's NaN
canonicalization covers that.

Each `step` runs with a fuel limit. A workload that runs out of fuel or traps
is stopped and marked failed. It isn't retried, because a deterministic
failure would fail again on any node.

## Data model

Everything lives in one Felix tenant, in a namespace per cluster.

| Resource | Kind | Key or name | Value | Written by |
|---|---|---|---|---|
| `workloads` | cache | workload id | spec: module hash, input stream, output stream, snapshot interval, state (`running`, `stopped`, `failed`) | `felixd run`, the owner on failure |
| `leases` | cache, TTL | workload id | owner node id, lease epoch | agents, with `cache_put_if` |
| `nodes` | cache, TTL | node id | address, capacity, version | each agent |
| `modules` | cache | sha256 of the module | the module bytes | `felixd run` |
| `wl.<id>.out` | stream, 1 shard, `Quorum` | | output records | the lease holder, with `publish_if` |
| `wl.<id>.snap` | stream, 1 shard, `Quorum` | | snapshots | the lease holder |

A workload's input is any stream: one written by an outside producer, or
another workload's `.out`. v0 reads one shard of one stream per workload.

Each output record carries a small header: the input offset that produced it
and its index among that step's outputs. Each snapshot carries the input
offset it covers and the output tail at that point.

`felixd run` creates the workload's streams through the control plane's REST
API (`/v1/tenants/{tenant}/namespaces/{namespace}/streams`). Agents never
create streams.

## Leases and failure detection

A lease is an entry in `leases` with a TTL (default 10 s). An agent claims a
workload with `cache_put_if` on "absent". It renews every TTL/3 with
`cache_put_if` at the version it last wrote. A refused renewal means the
lease is gone: the agent stops the workload at once.

An agent that cannot reach Felix stops its workloads once its own clock says
two thirds of the TTL have passed since the last successful renewal, which is
before any other node can claim. If the agent's clock is wrong or it is
frozen, fencing (below) still keeps the output correct. The lease only decides
who should run; it is not what keeps the output correct.

Agents watch `leases` with retained values. When an entry expires or is
deleted, any agent may claim it, after a random delay so that one node does
not grab everything. Takeover doesn't rely on the broker sending an
expiry event: each agent also knows `expires_at_millis` from the watch and
tries a claim after it passes.

v0 placement is "first claim wins" with a per-node cap (`max_workloads`).
`felixd drain` makes a node delete its own leases and stop claiming.

## Exactly-once output

The owner appends a step's outputs as one batch with `publish_if`, at the
offset it expects to be the output stream's tail. `publish_if` is a
compare-and-set on the whole shard (`crates/sdk/felix-client/src/client/commit.rs:162-181`
in Felix), and every `.out` stream has one shard and one writer at a time, so:

- If a stale owner wakes up and writes, the write is refused as soon as the
  new owner has appended anything, and the stale owner learns it has lost the
  workload.
- If the stale owner writes first, the new owner's write is refused, and it
  rereads the tail. Because the workload is deterministic, the stale owner
  wrote exactly what the new owner would have, so the new owner skips past it.

Either way each input offset's outputs appear once, in order. A lost answer to
`publish_if` is resolved by reading the stream at the expected offset, as the
Felix docs say to do.

A workload with no output for an input writes nothing, so the output stream
alone cannot tell the new owner how far its predecessor got. That is fine: the
new owner replays from the snapshot and only skips outputs that are already on
the stream. Workloads that produce no output are replayed a little further,
not wrongly.

## Resume

A new owner:

1. Reads the latest record on `wl.<id>.snap` and calls `restore`. With no
   snapshot it starts from the workload's initial state at input offset 0.
2. Reads the output tail. Its last record names the input offset `k` that
   produced it.
3. Subscribes to the input with `subscribe_from(snapshot offset + 1)`. For
   inputs up to `k` it runs `step` and drops the outputs, which are already on
   the stream. From `k + 1` on it writes them.

This is the fold in `core`, and the same fold drives replay.

Snapshots are written every N inputs (default 1000) or every T seconds of
activity. A stale owner's snapshot is harmless: determinism makes it identical
to the one the new owner would have written for that offset.

## Replay

`felixd replay <workload> --to <offset | time>`:

1. Turns a time into an input offset with `offset_for_time`.
2. Picks the latest snapshot at or before that offset and restores it in a
   local runtime.
3. Range-reads inputs up to the target, runs `step`, and prints `inspect()`.
   With `--diff`, it compares the outputs it produced with the ones on the
   stream, which checks determinism.

The same comparison with a different module (`--module <hash>`) checks an
upgrade before it goes live: run the new version over recorded inputs and
see where its outputs differ (#29).

Replay needs no lease and writes nothing, so it can run anywhere with read
grants.

## Grants

| Principal | May |
|---|---|
| Node | Read `workloads` and `modules`; write `leases` and `nodes`; read every input; publish to `wl.*.out` and `wl.*.snap` |
| Operator (`felixd run`, `drain`) | Write `workloads` and `modules`; create streams through the control plane |
| Viewer (`ps`, `replay`) | Read everything, write nothing |

In v0 any node may write any workload's output. Fencing keeps that correct,
but a compromised node could write garbage. Per-workload grants are a later
question.

## Retention

Inputs older than a workload's oldest kept snapshot can't be replayed, and
`subscribe_from` fails with `CursorTooOld` instead of silently skipping. v0
keeps everything. Trimming to "the last K snapshots" comes with M4.

## Build order

| M | Milestone | Done when |
|---|---|---|
| 0 | One node, one workload | A counter workload runs against a local Felix; killing and restarting felixd resumes at the right offset with no repeated output |
| 1 | Leases and failover | On 3 nodes, killing one moves its workloads within one TTL, verified by a checker that reads every `.out` |
| 2 | Exactly-once output | A node frozen past its lease and resumed writes nothing; a fault-injection test runs this in CI |
| 3 | Replay | `felixd replay --to` an offset or a time prints the state the live workload had there; `--diff` finds no differences |
| 4 | Pipelines and operations | A three-stage pipeline survives failover at each stage; `ps`, `drain`, metrics and snapshot trimming work |
| 5 | Self-hosting | A compose file and docs bring up Felix and three felixd nodes; release binaries are published |

## Risks, and what this surfaces in Felix

- **A stream per workload.** Conditional publish is per shard, so every
  workload needs its own single-shard output and snapshot streams, created
  through the control plane. That's fine for hundreds of workloads. At tens
  of thousands it depends on how Felix handles many small streams.
- **Module size.** Wasm modules can be several megabytes. Whether a cache
  value that large is reasonable, or modules should be chunked, needs
  measuring.
- **Batch atomicity.** Exactly-once output assumes a `publish_if` batch lands
  whole or not at all. That needs confirming against the broker before M2.
- **Determinism slips.** A host that hands the workload anything outside its
  inputs breaks replay quietly. The `--diff` check exists to catch it, and CI
  runs it on every example workload.

Anything Felix doesn't do yet that felixd needs gets an issue labelled
`felix-gap`, with the Felix code it refers to.

## Open questions

These are worth settling with collaborators before M1:

- Should a workload read several inputs, merged in a recorded order? This is
  needed for joins, and the merge order would itself be a record.
- Does placement belong in the agents (first claim wins, then rules) or in a
  scheduler workload that writes assignments? A scheduler that is itself a
  felixd workload would be replayable too.
- Which effect system for step 4 of the path: WASI with every response
  recorded, or a narrower set of host calls?
