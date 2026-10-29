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
  every secret that passed through it. Secrets stay out of the log
  ([Credentials](#credentials)), and records that are sensitive anyway need
  encryption and redaction.

### Lessons from Plan 9

Plan 9 is the closest ancestor, and it's a guide both to what to do and to
what to avoid. It's discussed in
[discussion #38](https://github.com/GetFelix/felixd/discussions/38).

What to do, as Plan 9 did:

- **One small protocol for everything.** Every resource is reached the same
  way. For us that's Felix's wire protocol. A read-only 9P or FUSE view for
  Linux tools is a later idea, kept off the data path (#78).
- **Names, not addresses.** A program names its inputs and outputs and doesn't
  know which machine serves them. That's what lets a workload move.
- **Per-program views of the namespace.** Isolation comes from what a program
  can see, backed by grants on its token.
- **History you can browse.** Plan 9's dump file system showed each past day.
  The log shows any past offset, and [history stays readable](#history-without-recomputation)
  without rerunning old code.
- **Programs never hold keys.** Plan 9's factotum ran authentication for
  programs. Here [credentials](#credentials) stay with executors and never
  enter the log.
- **Content addressing.** Venti stored blocks by the hash of their contents.
  Modules and snapshot chunks share [one such store](#content-addressed-storage).

What not to do, where Plan 9 went wrong:

- **Don't ask people to leave everything behind.** Plan 9 ran little existing
  software and lost to Unix being good enough. felixd runs beside an ordinary
  host OS, and the [native tier](#native-programs-and-the-bootstrap-boundary)
  runs existing programs unmodified.
- **Don't be distributed without being fault tolerant.** A dead Plan 9 file
  server hung every mount of it, and a lost root server meant a reboot. Here,
  a workload's state lives in the log, not on its server, and a node that
  loses Felix [stops and says so](#native-programs-and-the-bootstrap-boundary).
- **Don't make a remote effect look like a local call.** 9P made a remote read
  look like a local one, so a lost server had nowhere to show up but as a hang.
  [Effects](#effects) are requests with typed outcomes, and "unknown" is one of
  them.
- **Don't let interfaces become untyped strings.** Plan 9's `ctl` files each
  invented their own text commands and errors were strings. Records here have
  schemas and negotiated versions, and [errors have classes](#system-records-and-errors).
- **Don't hide latency behind a familiar interface.** 9P's request-reply
  style suffered over slow links. Every recorded effect costs a round trip
  through the log, so effects never block and that cost gets measured from
  the start.
- **Don't make a program's view of the world hard to see.** Per-process
  namespaces made "which `/net` is this?" a debugging question. A workload's
  bindings [name the stream incarnation](#names-and-incarnations) it reads,
  are on the record, and are shown by `ps` and replay.
- **Don't keep everything forever by accident.** Venti never forgot anything.
  Retention, compaction and redaction are designed in.
- **Don't fix one protocol version for good.** 9P forked into 9P2000.u and
  9P2000.L, and 9front had to replace Plan 9's DES tickets in place. Record
  formats, effect kinds and authentication are negotiated, as Felix's wire
  protocol is.
- **Don't make the system all-or-nothing.** Plan 9's ideas that survived (UTF-8,
  `/proc`, 9P in Linux, QEMU and WSL) could be adopted one at a time. Each part
  of felixd should be useful on its own.

Sources: Pike, Thompson and others, "Plan 9 from Bell Labs" (1995); Pike, TUHS
mailing list, 2023-01-31; Raymond, *The Art of Unix Programming*, ch. 20;
Guardiola, Cox and Van Hensbergen, "Persistent 9P Sessions for Plan 9" (2006);
Van Hensbergen and Minnich, "Grave Robbers from Outer Space" (2005); the 9front
dp9ik design notes. Each decision below cites its own.

### Lessons from Barrelfish

Barrelfish (ETH Zurich and Microsoft Research, 2009) treated one machine as a
distributed system: a kernel per core, no shared kernel state, and OS state
kept as replicas that change by agreement. It never caught on as an operating
system, but its authors wrote down what worked and what didn't, and the parts
that apply to felixd are these.

- **The scheduler's input is a recorded stream of node facts.** Barrelfish's
  System Knowledge Base held three kinds of fact: discovered (ACPI, PCI,
  CPUID), measured (message latency between core pairs), and asserted by hand,
  including quirks that correct wrong discovered data. Here the scheduler is a
  deterministic workload, so what it reads has to be on the record for its
  assignments to replay. Agents and operators append typed node facts:
  discovered ones (instruction set, determinism ABI, cores, memory, NUMA nodes,
  accelerators, effect kinds served), measured ones (broker round trip, step
  throughput, input lag) and asserted ones (failure domain, rack, drain intent,
  overrides). An asserted fact wins over a discovered one. `nodes` stays the
  liveness signal. There is no logic engine: the SKB's Prolog-based engine
  cost about 6,000 cycles for a trivial query and its authors saw "a tradeoff
  between expressiveness and performance", and our queries are filters plus
  a score (#60).
- **Policy stays off the mechanism path.** The SKB "is not directly involved
  in the underlying mechanisms", and the system ran without it, with a worse
  allocation. [Placement](#placement) already has this shape: first claim wins
  is the fallback. Barrelfish's device manager did end up booting devices
  through the SKB, which is the failure to avoid.
- **Every piece of state has a class.** Barrelfish chose a protocol per
  operation: one phase for unmap, two phases only for capability retype. It
  let closely coupled cores share a replica and centralised services that were
  not worth replicating. See [State classes](#state-classes) (#81).
- **Heterogeneity is a CI result.** The multikernel paper was evaluated only
  on x86, and its authors say reviewers caught that. Mixed-ISA boot stayed
  "in principle". Cross-ISA replay is checked in CI, not claimed
  ([Determinism](#determinism), #62).
- **Don't build a distributed capability system.** Barrelfish's capability
  operations needed a blocking two-phase protocol, about 8,000 lines, and one
  of its authors calls the capability model "by far the biggest distraction".
  See [Credentials](#credentials) (#74).
- **Queues decide performance.** Barrelfish found "extreme sensitivity to
  message queue lengths". Outstanding effect requests are bounded and queue
  depth is a metric ([Effects](#effects), #71).
- **A machine is one failure domain, and a move can be temporary.** Barrelfish
  said outright that "the complete system is a failure unit", and its later
  work parked a core's state elsewhere during an upgrade and moved it back. See
  [Placement](#placement) and [Upgrading felixd and scale](#upgrading-felixd-and-scale)
  (#82).

What this confirms: placement as policy with a fallback, pinning by instruction
set, per-lease credentials that expire instead of being revoked, effects as
asynchronous requests, and keeping memory and syscall traffic off the log in
the native tier (Arrakis made the same split, the OS as control plane and
applications on the data path). Barrelfish's agreement was cheap because it
assumed the whole machine fails together; felixd pays for fault-tolerant
agreement on every log write, so the classes below matter more here.

Sources: Baumann et al., "The Multikernel", SOSP 2009; Schüpbach et al.,
"Embracing diversity in the Barrelfish manycore operating system" (2008);
Baumann, "Fish in a Barrel", SIGOPS blog (2021); Zellweger, Octopus MSc thesis
(ETH, 2012); Gerber, PhD thesis (ETH, 2018); Zellweger et al., "Decoupling
Cores, Kernels, and Operating Systems", OSDI 2014; Bhardwaj et al., "NrOS",
OSDI 2021; Peter et al., "Arrakis", OSDI 2014 (abstract only).

## What felixd has to demonstrate

- **Failover:** kill a node mid-run, and each of its workloads continues on
  another node. Its output stream has no gaps and no duplicates.
- **Fencing:** freeze a node (`SIGSTOP`) past its lease and resume it. Its
  stale workloads write no output. A snapshot it took before the freeze can
  still land, and is of a correct state (see Resume).
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
felixd check      verify output streams against a replay of their inputs
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

Each call into the workload runs with a fuel limit and a memory cap. Both are
in the workload's spec, set by `felixd run --fuel --max-memory`, so every node
runs it under the same limits and fails it at the same step. The Wasm stack
size and the bounds on tables, table elements, instances and memories are
fixed in felixd itself. A workload that runs out of fuel or traps, in `step`,
`snapshot` or `restore`, is stopped and marked failed. It isn't retried,
because a deterministic failure would fail again on any node.

A call can run for as long as its fuel allows, so `step`, `snapshot` and
`restore` run on tokio's blocking threads, at most one per core at a time,
and each workload's lease renewals run in a task of their own. A long step
then delays only its own workload, never another workload's renewals or the
node's entry in `nodes`. A node compiles each module once: workloads that
need the same module at the same time wait for one compile, and at most one
compile per core runs at once.

Where fuel runs out and where the stack overflows depend on the code wasmtime
generates, so they can move between wasmtime versions. Every node must run the
same felixd build. Recording the build in the spec, so that a node running a
different one refuses the workload, is an open question.

## Data model

Everything lives in one Felix tenant, in a namespace per cluster. `felixd run`
creates the caches and streams below with three replicas and `Quorum`
consistency by default, so losing one broker loses no acknowledged write and
stops no claim. `--replication-factor 1` is for a single development broker.

| Resource | Kind | Key or name | Value | Written by |
|---|---|---|---|---|
| `workloads` | cache | workload id | spec: module hash, input stream, output stream, snapshot interval, fuel and memory limits, state (`running`, `stopped`, `failed`) | `felixd run`, the owner on failure |
| `leases` | cache, TTL | workload id | owner node id, lease epoch (0 until the claim has taken one) | agents, with `cache_put_if` |
| `epochs` | cache | workload id | the last lease epoch handed out | agents, with `cache_put_if` |
| `nodes` | cache, TTL | node id | felixd version, `max_workloads`, workloads held | each agent |
| `modules` | cache | sha256 of the module | the module bytes | `felixd run` |
| `wl.<id>.out` | stream, 1 shard, `Quorum` | | output records | the lease holder, with `publish_if` |
| `wl.<id>.snap` | stream, 1 shard, `Quorum` | | snapshots | the lease holder |

A workload's input is any stream: one written by an outside producer, or
another workload's `.out`. v0 reads one shard of one stream per workload.
Claim and failure records on another workload's `.out` are not inputs: the
owner and `felixd check` pass over them, so only outputs reach `step`.

Each output record carries a small header: the input offset that produced it,
its index among that step's outputs, and the lease epoch of the owner that
wrote it. The output stream also holds claim records, one per takeover, which
carry only the new owner's epoch, and a failure record when the workload
fails. Each snapshot carries the input offset it covers and the output tail
at that point.

`felixd run` creates the workload's streams through the control plane's REST
API (`/v1/tenants/{tenant}/namespaces/{namespace}/streams`). Agents never
create streams.

Running `felixd run` again for an existing id replaces the spec. A running
workload whose spec changes restarts under the new one: the agent stops the
task, releases the lease and claims it again, and the next owner resumes from
the streams. A new module or input stream would not reproduce what the
streams already hold, and the next resume would find them diverged, so
`felixd run` refuses that change once the output or snapshot stream has a
record. New code goes under a new id. A spec written to the cache by other
means is not checked, and a workload resumed under such a change fails with
the divergence.

## Leases and failure detection

A lease is an entry in `leases` with a TTL (default 10 s, at least 2 s,
`felixd agent --lease-ttl-ms`). An agent claims a workload with `cache_put_if`
on "absent", which the broker grants only once it counts the previous lease as
expired or deleted. The holder renews every TTL/3 with `cache_put_if` at the
version it last wrote. A refused renewal means the lease is gone: the agent
stops the workload at once. A renewal whose answer is lost is read back: the
lease value names the holder's epoch, so finding it at a newer version means
a renewal landed. It is counted from the send of the earliest renewal still
unanswered, since that may be the one that landed. A claim whose answer is
lost is not adopted; it expires unused.

A claim that wins then takes its epoch: one more than the larger of the last
epoch in `epochs` and the epochs on the last records of the output and
snapshot streams. It
writes that to `epochs` with `cache_put_if` at the version it read, then
rewrites the lease with the epoch, again with `cache_put_if`, and only then
runs anything. A claimer that was frozen after winning and lost its lease
meanwhile fails one of the two writes and gives up. The counter makes epochs
unique and growing; the output tail keeps a new epoch above every epoch
already written even if `leases` or `epochs` is recreated and starts again.
The one assumption is that `epochs` is not recreated while a holder that has
not written anything yet may still be running: its epoch is then on no
record, so the next claim could get the same one.

A holder that finds a record from a later epoch on the output stream renews
its lease at once, conditional on the version it holds. A read would not do,
because Felix does not yet define how fresh a cache read is after a leader
change, and a stale read can still show the holder's own lease. If the
renewal is refused, a later owner has taken over and the holder stops. If it
is applied, the lease is still its own: epochs went backwards, which nothing
can repair automatically, so it logs an error and marks the workload failed
with that reason instead of stopping and claiming again.

An agent that cannot reach Felix stops its workloads once its own clock says
two thirds of the TTL have passed since it sent the last successful renewal,
which is before any other node can claim. The clock is a monotonic one, so a
change to the wall clock doesn't move it. If the agent is frozen, its clock is
wrong, or the broker expires the lease early, fencing (below) still keeps the
output correct. The lease only decides who should run; it is not what keeps
the output correct.

Lease exclusivity rests on the clock of the broker leading the `leases`
shard. That leader decides when a TTL has passed and writes the expiry, and an
expiry is permanent once written: a leader whose clock jumps forward expires
leases early, and a later fix to the clock does not bring them back (Felix
`docs/cache-on-log.md`, the expiry pass under "TTL"). A leader change can move
the decision to a broker whose clock disagrees. So two agents can each believe
they hold the same workload for a while. That never makes the output wrong,
because output safety rests on the `publish_if` fence and the epochs below,
not on the lease. A wrong clock costs duplicate work and refused writes.

An agent never adopts a lease it finds under its own name. A restarted agent
waits for its old leases to expire, so two processes configured with the same
node id can't both believe they hold a workload. On a clean shutdown an agent
deletes its leases with `cache_delete_if`, so another node can claim at once.
Only a holder fails a workload, and the failure is fenced by epoch like an
output. The holder renews its lease the same way first, and gives up if that
renewal is refused. Then it appends a failure record to the output stream
with `publish_if` at the tail it just read, unless the last record there is
from a later epoch. Everything below a later owner's claim is from earlier
epochs, so the record lands only before any later owner has begun. A node
frozen after the renewal, while another took over, therefore can't fail the
workload the other node now runs: its append is refused and it finds the new
owner's claim. The record names the spec version the workload failed under
and the reason. `failed` in the spec follows from it, written with
`cache_put_if` on that version, so a later edit is not overwritten. An owner
that finds a failure record for its own spec version, while claiming or at
its expected tail, writes `failed` into the spec instead of running, which
covers a holder that crashed between the two writes. A failure record under
an older spec version is from a run before `felixd run` wrote the spec
again, and is passed over. The holder keeps renewing the lease until the
failure is recorded, so a slow write doesn't let the lease lapse with the
spec still `running`.

Agents watch `workloads` with retained values. When a watch ends they open
a new one, which redelivers every current spec first and says how many. Once
all of them are in, the agent stops the workloads that were not among them:
they were deleted or stopped while no watch was open.

Agents watch `leases` with retained values. When an entry expires or is
deleted, any agent may claim it, after a random delay so that one node does
not grab everything. Takeover doesn't rely on the broker sending an
expiry event: each agent also knows `expires_at_millis` from the watch and
tries a claim after it passes. It waits at most one TTL between tries, so a
local clock behind the broker's, or a delete the watch missed, costs at most
a TTL. A clock ahead of the broker's costs a refused claim, never a second
owner, since only the broker decides that a lease is gone.

Each agent writes itself to `nodes` every TTL/3 with a TTL, so a dead node's
entry goes away about when its leases do. Nothing reads `nodes` to make
decisions yet; it says which agents are up and how full they are.

v0 placement is "first claim wins" with a per-node cap: an agent holding
`--max-workloads` (default 64) claims nothing more until one of its
workloads ends, and every claim waits a random time of up to
`--claim-delay-ms` (default 1 s). The agent sees placement only through the
`Placement` trait in `core`, so the open question below (#26) can be settled
without touching the lease code.
`felixd drain` makes a node delete its own leases and stop claiming.

## Exactly-once output

Every write to the output stream is a `publish_if` at the offset the writer
expects to be the tail. `publish_if` is a compare-and-set on the whole shard
(`crates/sdk/felix-client/src/client/commit.rs:162-181` in Felix), and every
`.out` stream has one shard.

The claim fences the stream. Before a new owner reads anything to resume
from, it appends a claim record carrying its epoch at the output tail it read
along with the epoch floor when it took the lease, not at a fresh tail. A
claimer frozen between taking its epoch and writing the claim would otherwise
land its claim after a later owner's. Once the claim is on the stream, a
resume after an error does not claim again. A refused claim means someone
else appended first, and the records past that tail decide: one from a later epoch means this owner is
superseded and stops, its own claim means an earlier attempt whose answer was
lost landed, and anything else is claimed after. Then:

- If a stale owner wakes up and writes, the write is refused, because the
  claim moved the tail past what the stale owner expects. The records it finds
  at its expected tail include the claim, from a later epoch, so it stops
  without writing anything more. This holds when the new owner has written no
  output, for example because its input is idle.
- If the stale owner writes before the claim lands, the claim is refused and
  lands after it. The new owner then resumes past those outputs like any
  others, and because the workload is deterministic they are what it would
  have written.

The safety argument has two layers. The fence alone guarantees that nothing
from a lower epoch lands after a claim, so the epochs on the stream never
decrease and two owners never take turns. Determinism adds that what a stale
owner wrote before the claim equals what the new owner would have written.
Anything recorded that is not a deterministic function of the input, such as
a merge order (#58) or an effect (#27), may rely only on the first.

Each input offset's outputs appear once, in order. Without the epoch, a stale owner that finds the new owner's
identical records would skip them and write after them, and the two would take
turns on the stream. A new owner that finds a later epoch already on the
output stream when it resumes stops too.

A refused or lost answer to `publish_if` is never retried blindly. The
writer reads the stream from the offset it expected, as the Felix docs say to
do, and decides on what is there:

- Its own records: the write landed. An output batch skips them and writes
  the rest, a claim counts as made, a failure record as written.
- A record from a later epoch: someone else owns the workload. The writer
  stops without writing anything more.
- Nothing: the write did not land, or has not yet. It is sent again at the
  same offset, so if the first one lands after all, one of the two is
  refused and the next read finds it.

`tests/answers.rs` covers each with faults that drop the request before it
lands (`drop-append`, `drop-claim`, `drop-failure`) or lose the answer after
it lands (`lose-append-answer` and the like), and with an owner frozen after
a lost answer while another takes over. That owner has a long TTL, so its
own lease check does not stop it: the later owner's records do.

A batch is not all or nothing. Felix ships a `Quorum` stream's records to
followers by bytes, so a batch of several records can reach a follower in
part, and a follower promoted with part of a batch keeps that part (Felix
`docs/atomic-commit.md`, What atomic covers). The writer gets an error, so
after a failed append any prefix of the batch may be on the stream. felixd
relies on two things here:

- An owner whose append fails reads the stream at the tail it expected, and
  `output::reconcile` skips the records of its batch it finds there. It
  writes only the rest.
- An owner that dies instead leaves the prefix to the next one. The fold
  skips outputs by `(input offset, index)`, not by input, so the next owner
  runs that step again, drops the outputs already on the stream and writes
  the rest. A resume can start in the middle of a step.

`tests/batches.rs` leaves a prefix with the `append-prefix` fault and checks
both cases with `felixd check --complete`, on one broker and on the
three-broker cluster. The model lands part of a batch with `DeliverPrefix`
(`FelixdPartial`). Whether Felix should make a conditional batch all or
nothing, or document the prefix as part of its contract, is GetFelix/felix#1630.

A workload with no output for an input writes nothing, so the output stream
alone cannot tell the new owner how far its predecessor got. That is fine: the
new owner replays from the snapshot and only skips outputs that are already on
the stream. Workloads that produce no output are replayed a little further,
not wrongly.

## Resume

A new owner:

1. Appends its claim to `wl.<id>.out`, as described above.
2. Reads the latest record on `wl.<id>.snap` and calls `restore`. With no
   snapshot it starts from the workload's initial state at input offset 0.
3. Reads the last output record, passing over claims. It names the input
   offset `k` that produced it.
4. Subscribes to the input with `subscribe_from(snapshot offset + 1)`. For
   inputs up to `k` it runs `step` and drops the outputs, which are already on
   the stream, after checking each against the record in its place. From
   `k + 1` on it writes them.

This is the fold in `core`, and the same fold drives replay.

While it is replaying inputs before `k`, the owner takes no snapshot. The
output stream's tail is ahead of the workload's state then, so a snapshot
would claim outputs it doesn't cover, and an owner that crashed after writing
it would resume past them and write them again.
`a_snapshot_is_not_taken_while_replaying` in `tests/restart.rs` covers it: a
first owner writes 100 outputs and no snapshot (`skip-snapshots`), a second
replays them all and crashes after its first snapshot, and a third must
produce the rest exactly once.

Felix drops records for a slow subscriber by default, and a jump in offsets is
how that shows. Offsets that hold no event also jump, so when the next input
offset is past the expected one the owner reads the range in between and runs
whatever records it finds first. It never steps past a missing record. A
read stops short at what the broker has committed, which can lag the tail
just after a leader change, so a page that makes no progress is read again
and, after 20 s, is an error rather than an empty range.

A stream's tail is not where its last output is. When a Felix broker starts
leading a shard at a new generation (a promotion after a broker is lost, either
end of a shard move), it appends a generation-start record. The record takes
an offset and is never read back, so `.out` and `.snap` can end in offsets
that hold no record. The fold compares the last output's offset with the
snapshot's output tail rather than the stream's tail, and a stream with no
output records is one with no outputs, whatever its tail. A conditional write
still expects the raw tail, so a refused write that finds no records in
between just retries at the new tail. `generation_starts_after_failover` in
`tests/cluster.rs` kills the broker that leads `.out` on a three-broker
cluster and checks the output with `felixd check`.

Some states can't be resumed from on any node, and the design is to refuse
them rather than guess: a snapshot or output record that doesn't decode, a
fold that finds outputs missing or out of place, a replayed output that
differs from the stream, a record at the tail that differs from the owner's,
or a step whose output (or a snapshot) is larger than one write to Felix may
hold (8 MiB, against Felix's 16 MiB frame limit). The owner logs an error and
marks the workload failed with the reason. Only Felix errors are retried.

Snapshots are written every N inputs (default 1000). Each carries the
writer's epoch and is appended with `publish_if` at the snapshot stream's
tail as the owner last saw it, so a stale owner cannot write a snapshot after
a later owner's. A refused write rereads the records past that tail: a
snapshot from a later epoch means this owner is superseded and stops; ones
from its own epoch or earlier are a stale owner's that landed first, which
determinism makes correct, so it writes after them. A new owner that finds a
snapshot from an epoch above its own stops at once, as it does for an output
record, rather than ignoring it: a later owner exists, and that owner's claim
on the output stream would stop this one at its next output anyway. The
snapshot stream has no claim of its own, so a stale owner can still append a
snapshot after the new owner's claim until the new owner writes one. That
snapshot is of a correct state, by determinism.

`tests/fence.rs` freezes nodes past their leases: a holder of an idle
workload, one frozen before its claim, two frozen in turn across two
takeovers, and one frozen with a snapshot taken, whose snapshot lands after
the takeover and which a third owner restores and resumes past. Most have a
long TTL and their lease ended early, so their own lease check passes when
they wake and only the streams can stop them. The cluster CI job runs them
on three brokers.

## Checking output

`felixd check` runs each workload's module over its whole input stream, from
its initial state, and lines the outputs it produces up with the records on
the output stream by their headers. Each record should match the next
replayed output. It reports:

- a gap: a replayed output missing from the stream while a later one is there;
- a duplicate: a second record for the same input offset and index;
- a record out of order: one for an output before one already on the stream;
- an epoch regression: a record, output or claim, whose epoch is below an
  earlier record's;
- a record that differs from the replay, or that the replay did not produce,
  or that does not decode;
- a snapshot whose output tail does not split the output stream at its input
  offset X: every record below the tail must be an output of an input up to
  X, and every record from it on an output of a later input. A resume from
  such a snapshot would skip or repeat outputs.

Claims are not outputs, so they are matched to nothing; only their epochs
are checked. With `--complete` it also reports outputs missing from the end of
the stream.
Without it those are not a finding, since a running workload may not have
caught up. The snapshot stream is read first, then the output stream, then
the input, so every output a snapshot counts is on the stream read and every
record checked was produced from inputs the replay sees. A step that fails ends the
replay there, as it ends the workload on every node. Every failover test ends
with `felixd check --complete`.

The M1 failover test (`crates/felixd/tests/failover.rs`) runs Felix, three
agents and six workloads on one input with a steady producer. Every 3 s it
SIGKILLs the live agent holding the most leases and starts it again 1.5 s
later under the same node id, which then waits for its old leases to expire
like any other node. A lease can still name a node that already died, so
only a live holder counts as a kill; with none, the loop looks again 250 ms
later. Every other restart sets `crash-after-append-sent`, so that node also
aborts by itself between sending an append and acting on its answer, and the
loop starts it again. Pull requests run the loop for 30 s and a nightly job
for 10 minutes. At the end every output stream must equal an uninterrupted run
and pass `felixd check --complete`.

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
| Viewer (`ps`, `replay`, `check`) | Read everything, write nothing |

In v0 any node may write any workload's output. Fencing keeps that correct,
but a compromised node could write garbage. Per-lease tokens close that
([Credentials](#credentials)).

## Retention

Inputs older than a workload's oldest kept snapshot can't be replayed, and
`subscribe_from` fails with `CursorTooOld` instead of silently skipping. v0
keeps everything. Trimming to "the last K snapshots" comes with M4.

## Build order

| M | Milestone | Done when |
|---|---|---|
| 0 | One node, one workload | A counter workload runs against a local Felix; killing and restarting felixd resumes at the right offset with no repeated output |
| 1 | Leases and failover | On 3 nodes, killing one moves its workloads within one TTL, verified by a checker that reads every `.out` |
| 2 | Exactly-once output | A node frozen past its lease and resumed writes nothing once the new owner has claimed, even with no new input; a fault-injection test runs this in CI |
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
- **Batch atomicity.** A `publish_if` batch can land in part on a `Quorum`
  stream when its leader changes, and the writer gets an error. felixd copes
  (see Exactly-once output), but anything that cannot re-read and compare
  what landed needs an all-or-nothing batch (GetFelix/felix#1630).
- **Determinism slips.** A host that hands the workload anything outside its
  inputs breaks replay quietly. The `--diff` check exists to catch it, and CI
  runs it on every example workload.

Anything Felix doesn't do yet that felixd needs gets an issue labelled
`felix-gap`, with the Felix code it refers to.

## Determinism

A workload must produce the same outputs on any node, after any history of
failovers. Three rules hold that:

- **Snapshot points are a function of input offsets**, never of when a node
  happened to start. After a snapshot point, execution always continues from a
  fresh instance restored from that snapshot, so an instance's age can't change
  where a memory or stack limit trips. `restore(snapshot())` must behave as the
  identity, and `snapshot` must not change state.
- **A determinism ABI** names everything that affects results: the felixd
  runtime version, the wasmtime version and its settings, limits, and the
  instruction set while stack depth still depends on it. It is recorded in the
  spec, in each snapshot header and in `nodes`. A node never claims a workload
  pinned to an ABI it doesn't implement. Until stack depth is deterministic,
  mixed x86_64 and aarch64 clusters pin workloads by instruction set. The pin
  comes off only when CI replays the same recorded inputs on x86_64 and aarch64
  and the output and snapshot digests match; native workloads stay pinned
  regardless (#62).
- **Trap classes.** A trap that depends only on the inputs (a guest panic,
  running out of fuel) is a poison input and fails the workload. A trap that
  can depend on the host (stack overflow, a host resource failure) is retried
  once on a fresh instance before it counts as poison.

## State size

State is not capped by the size of one record. Snapshots are written in chunks,
and a snapshot writes only the chunks that changed since the last one, with
separate fuel from `step`. Snapshots are taken by input count and by time, old
snapshots and inputs are trimmed by a stated retention, and each workload has a
recovery time budget that bounds how far a resume may replay.

## The workload contract, version 0.2

Before anything is built on it, the contract moves to a versioned interface:
exports carry their version, `step` receives the input's offset and time,
outputs are addressed (a key and a named output) rather than a single stream,
a workload can `reject` one input without failing, and snapshots are exported
in chunks. Upgrading a running workload's module is a recorded change that
takes effect at a stated input offset, after `felixd replay --module` shows the
new module agrees on the inputs it has already seen.

## Upgrading felixd and scale

A node that meets a record version it doesn't understand stops claiming that
workload; it never marks it failed. New record formats are written only after
every node advertises that it reads them. Nodes advertise their versions and
labels (failure domain, instruction set) in `nodes`.

The control plane must not grow as nodes times workloads. Nodes hold one
session lease each, and assignments (from the scheduler in #60) replace every
agent watching every workload lease.

### State classes

Agreement costs a quorum round trip, so only state that needs it pays for it.
Each piece of system state has one class (#81):

| State | Class | Why |
|---|---|---|
| Workload inputs, outputs, snapshots | Log record | Replay and exactly-once output depend on one order |
| Epochs, claims, fences, grants | Log record (conditional write) | Two nodes must not both believe they hold a lease |
| Placement assignments, node facts | Log record | The scheduler is a workload and has to replay |
| Spec history, stream incarnations | Log record | Inputs bind to them |
| Node presence and versions in `nodes` | TTL cache entry | Stale means absent; nothing replays from it |
| Per-workload lease renewals beyond the node session | Avoid | Replaced by one session lease per node |
| Instance pools, compiled modules, local metrics | Node-local, never agreed | Derived from the log and rebuilt after a restart |
| Credentials | Node-local, outside the log | Secrets never enter the log |

A new feature picks a row before it picks a mechanism. The default is not the
log.

### One machine, one failure domain

A machine runs one agent and holds one session lease. Several agents on a
machine would look like independent failure domains and are not. NUMA nodes and
accelerators are node facts for the scheduler and for the agent's instance
pools, which pin an instance to a NUMA node over local shared memory, never
over Felix. A GPU or DPU host is an effect executor that advertises its effect
kinds like any other.

A planned upgrade records a drain with a return intent as an asserted node
fact. When the node rejoins, the scheduler sends its workloads back instead of
leaving them where first claim put them. This is a scheduler input, not lease
machinery (#82).

## Several inputs

A workload reads one input stream. Replay is deterministic because the input's
offsets fix the order. With two streams, arrival order depends on timing, so a
replay could see a different interleaving.

Joins are two steps for now: a merge workload writes one combined stream, and
the join reads it. Later (#58), felixd will record the merge order itself. The
lease holder appends the interleaving it chose to a merge stream, fenced like
outputs, and resume, replay and `felixd check` read that log. A deterministic
merge by timestamp is not planned: one idle input would stall the workload, and
it depends on clocks.

## Placement

Agents place workloads by first claim wins: when a lease is free or expired,
an agent with room claims it after a random delay, and the broker's
conditional write picks one winner. It sits behind the `Placement` seam.

When that limits us (balancing, labels, graceful drain), placement moves to a
scheduler that is itself a felixd workload (#60). It writes assignments as
records, so placement is replayable and survives failover like any workload.
Agents still claim through the lease, so the scheduler decides where work runs,
never whether output is correct. First claim wins stays as the fallback when an
assignment is missing or stale. A scheduler outside felixd is not planned.

The scheduler reads a recorded stream of node facts (discovered, measured and
operator-asserted, with asserted winning), not the `nodes` cache, so its view
replays. See [Lessons from Barrelfish](#lessons-from-barrelfish).

## Effects

A workload reaches the outside world by asking, never by calling. These rules
settle #27 and are tracked in #71:

- **An effect is a request written as an output, and its response is an
  input.** There are no blocking host calls inside `step`. The response comes
  back on the workload's response stream and enters the fold through the
  recorded merge order (#58), so replay reads the recorded response and never
  performs the effect again.
- **The effect id is the output header**: workload, input offset, output index
  and epoch. It is unique and deterministic, so executors pass it on as the
  idempotency key.
- **Every response has a typed outcome**: `ok`, `error` with a code,
  `timeout`, or `unknown` when the executor was lost after it may have acted.
  The workload decides what `unknown` means for it. The runtime never guesses.
- **Executors check epochs.** An executor refuses a request whose epoch is below
  the highest it has seen for that workload. That only holds if a claim fences
  the workload's streams before the new owner runs anything, so effects wait
  for that fence.
- **Guarantees are per effect kind**, and the kind's WIT type states which one:
  idempotent (the target dedupes on the effect id), fenced (the target refuses
  a lower epoch), at-most-once or at-least-once. Nothing promises "exactly
  once" with a target that doesn't help.
- **WASI only as a shim inside the guest.** A ported program that makes WASI
  calls links a guest-side layer that turns each call into a request and
  suspends the step until the response arrives. When WASI's async support is
  stable, the shim uses it rather than a hand-made blocking bridge.
- A workload may have many requests outstanding at once, and executors
  advertise the effect kinds and versions they serve, so a node never claims a
  workload whose effects nobody can execute. Per-effect latency is a metric
  from the first version.
- **Outstanding requests are bounded.** The spec sets a limit per workload,
  which is a determinism input because hitting it changes when requests are
  issued. Each executor has its own limit and answers an excess request with a
  typed backpressure outcome. Queue depth is a metric per workload and per
  executor. Barrelfish found its performance anomalies in queue lengths.

Plan 9 made a remote file look like a local one, and most of its clients did
not plan for a lost server: programs had to be restarted. Octopus replaced 9P
with Op because 9P took too many round trips for interactive use at WAN
latency.

Sources: Waldo, Wyant, Wollrath and Kendall, "A Note on Distributed Computing"
(1994); Ballesteros et al., Octopus Op protocol, `O/0intro`; Guardiola, Cox and
Van Hensbergen, "Persistent 9P Sessions for Plan 9" (2006).

## Names and incarnations

A name is resolved once, when a spec is written, and the result is recorded
(#72):

- A spec stores each input stream's name and the Felix incarnation it resolved
  to. A stream deleted and created again under the same name is a different
  stream. A node refuses to resume a workload whose input's incarnation has
  changed and records it as needing an operator.
- The output stream is derived from the workload id (`wl.<id>.out`), so the
  spec's `output` field goes away.
- Every spec change that can affect results (module, inputs, limits,
  determinism ABI) is a versioned record that takes effect at a stated input
  offset. Spec history is kept as those records, not as the latest cache
  value, so `check` and replay judge each output under the spec that applied.
  `felixd ps` shows the bindings.

Plan 9 resolved a name at every `open`, so a rebind silently changed what a
program read. Felix fixed the same class of bug for forwards in #1324.

Sources: Pike, Presotto, Thompson, Trickey and Winterbottom, "The Use of Name
Spaces in Plan 9" (1993); Pike, "Lexical File Names in Plan 9" (2000).

## System records and errors

Payloads stay opaque bytes. The system's own records do not (#73):

- Specs, leases, failures, effect requests and effect responses are versioned
  typed records, defined once in WIT with the workload contract and generated
  into `core`. Today's JSON cache values and the free-text `failure` field are
  replaced.
- Every error has a class: **transient** (retried while the lease is held),
  **needs operator** (a missing module, an input past retention, a changed
  incarnation) or **poison** (an input the workload deterministically fails on).
  The [trap classes](#determinism) map onto these. The text is detail, never
  what a program compares.
- A workload stuck on a transient error past a bound releases its lease and is
  recorded `degraded` with its class and reason, so it is visible instead of
  retrying silently on one node.
- A divergence record carries the node, its determinism ABI, the input offset,
  the snapshot it started from, and the digests of both the expected and the
  actual output, so `felixd replay` can reproduce it.
- Metrics and traces use OpenTelemetry: input lag, last output offset, epoch,
  takeovers, replay time and per-effect latency per workload.

Plan 9's `ctl` files each had their own text commands and `errstr` returned a
string that callers matched by text. Linux sysfs had to impose one value per
file to keep the same idea readable.

Sources: Plan 9 `errstr(2)`; Linux `Documentation/filesystems/sysfs.rst`.

## Credentials

Tracked in #74.

- **Secrets never enter the log.** A workload names a credential in an effect
  request; the executor resolves the name through a credential agent on its
  own node and runs the authentication itself. Response records exclude
  credentials by construction.
- **Tokens are per lease.** A claim gets a short-lived token scoped to that
  workload's streams and epoch, so a node can write only the outputs it holds.
  This closes the v0 grant that lets any node write any output, and needs
  scoped token minting from Felix.
- **Workload identity in the SPIFFE style.** Nodes and workloads have
  identities that executors and grants check, issued by the cluster rather than
  configured by hand. Authentication stays negotiable, so a weak scheme can be
  replaced without a flag day.
- **Tokens are not delegable.** A token is never copied or handed to another
  workload. A stale one is refused by expiry and by the epoch fence, so there
  is no revocation protocol. Delegation is a grant record written at the
  control plane, the one authority. Barrelfish's distributed capabilities
  needed agreement for every copy and revocation.

Plan 9's factotum held each user's keys and ran authentication protocols on
behalf of programs, so programs never saw secrets. Its original DES tickets
later had to be swapped out in place.

Sources: Cox, Grosse, Pike, Presotto and Quinlan, "Security in Plan 9" (2002);
the 9front dp9ik design notes and `authsrv(6)`; the SPIFFE specification;
Gerber, PhD thesis (ETH, 2018), ch. 4; Baumann, "Fish in a Barrel" (2021).

## Native programs and the bootstrap boundary

Deterministic Wasm is not the only tier (#75). Programs that can't be made
deterministic run as **supervised native processes** beside Wasm workloads.
They fail over through the same lease and fence, but felixd makes no replay
promise for them: a new owner restarts the program, or restores a checkpoint
where the host supports one, and their I/O is recorded at the stream boundary.
The first native goal is running an unmodified existing program, such as a
database, from an OCI image through an OCI runtime (runc or crun, for example).
Images are referenced by digest and their signatures are checked before they
run.

Some things are not felixd workloads, because felixd depends on them: the Felix
brokers and control plane, felixd itself, each node's credentials and the base
image they come in. They are installed and upgraded with the image. felixd
release binaries are built reproducibly and signed (#23).

When a node can't reach Felix, it stops stepping every workload, native ones
included, once two thirds of the lease TTL have passed (as in
[Leases](#leases-and-failure-detection)). It reports itself `degraded` locally,
marks nothing failed, and claims again when Felix is back. Recovery is resume
from an offset, which is the normal path. If the whole Felix quorum is gone,
every workload stalls until it returns, and that is visible on every node.

Plan 9 lost to Unix being good enough. Its POSIX layer, APE, could not emulate
much of what existing programs needed, and losing the root file server meant
rebooting the machine.

Sources: Pike, TUHS mailing list, 2023-01-31; Raymond, *The Art of Unix
Programming*, ch. 20; Trickey, "APE: The ANSI/POSIX Environment"; the OCI
runtime and image specifications.

## History without recomputation

Tracked in #76.

- **Each snapshot carries a plain rendering beside it**, the output of
  `inspect()` at that point, which can be read without the module or the
  runtime.
- **Two tiers.** Frequent ephemeral snapshots exist for resume and are trimmed
  quickly. A sparser archive tier is kept for time travel. Each tier states its
  own retention (#63).
- **Runtimes are kept per determinism ABI** (#62), in the
  [content-addressed store](#content-addressed-storage), for as long as
  retained inputs need them, so old history stays replayable after felixd
  moves on.
- Looking at a past state should be an everyday command (`fx at`), not a
  debugging session.

Plan 9's dump showed each past day as a directory that `cat` could read years
later, without the file server that wrote it. Fossil kept frequent local
snapshots and archived a subset to Venti.

Sources: Quinlan, McKie and Cox, "Fossil, an Archival File Server"; Plan 9
`yesterday(1)`.

## Content-addressed storage

Modules, snapshot chunks and archived runtimes live in one content-addressed
chunk store (#77):

- **Retention is by reference count.** A chunk is kept while a module, a
  snapshot manifest or a runtime still in retention refers to it, so trimming
  and redaction actually free data.
- **Addresses are keyed per tenant**: a keyed hash of the contents with a
  per-tenant secret. Two tenants storing the same bytes get different
  addresses, so the store leaks no equality across tenants, and deleting a
  tenant's key and chunks removes its data.
- Large modules are chunked the same way, which answers the module-size
  question under [Risks](#risks-and-what-this-surfaces-in-felix) (#21).

Venti never deleted a block, which made it simple and made deletion
impossible. Plain content hashes also let anyone who can probe the store
confirm that someone else stored a known file.

Sources: Quinlan and Dorward, "Venti: a new approach to archival storage"
(FAST 2002).

## Open questions

- A read-only 9P or FUSE view of the namespace, so `ls`, `cat` and `tail -f`
  can browse streams (#78). For people and tools, not a data path.
