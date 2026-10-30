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
  are on the record, and are shown by replay and, once it exists, `ps`.
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

Out of scope for felixd: placement policy beyond "first claim wins" and the
simple scheduler below, CPU and memory limits beyond Wasm fuel and memory
caps, networking, and native processes.

## Architecture

One Rust workspace and one binary.

```
felixd agent      run on every node
felixd run        submit a workload
felixd check      verify output streams against a replay of their inputs
felixd replay     rebuild a workload at an offset or time
felixd ps         show workloads, holders, positions and nodes
felixd drain      move a node's workloads off it; felixd undrain lets it claim again
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

A workload is a Wasm component that implements the WIT world in
`wit/workload.wit`, workload contract 0.2. [contract.md](contract.md) gives the
interface and what each side guarantees. In short, `step` gets one input
record with its stream, offset, append time and key, and returns outputs
addressed by name and key, or rejects the input, or poisons the workload. The
state is snapshotted and restored in chunks, and `inspect` returns it as JSON.
A component of the 0.1 contract is refused.

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
fixed in felixd itself. A whole snapshot or restore, every chunk call
together, runs under its own `snapshot_fuel`. A snapshot that runs out of it
is abandoned, not a failure: nothing is written, the previous snapshot stays
current, and the workload resumes from it. A restore that runs out leaves the
workload to an operator, who can raise the limit. A workload that runs out of
fuel in `step` or traps, in `step`, `snapshot` or `restore`, returns `poison`, or returns
something the contract forbids, is stopped and marked failed. It isn't retried,
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

Everything lives in one Felix tenant, in a namespace per cluster. The
namespace must already exist: `felixd run` does not create it, and fails with
the control plane's HTTP error if it is missing. `felixd run` creates the
caches and streams below with three replicas and `Quorum`
consistency by default, so losing one broker loses no acknowledged write and
stops no claim. `--replication-factor 1` is for a single development broker.

| Resource | Kind | Key or name | Value | Written by |
|---|---|---|---|---|
| `sys.specs` | stream, 1 shard, `Quorum` | | spec records: workload id and spec. A record's offset is its version, so the stream is every workload's spec history | today `felixd run`; end state: admission ([Submitting work](#submitting-work)) |
| `workloads` | cache | workload id | the latest spec record's spec and offset, and the state (`running`, `stopped`, `failed`): module hash, input stream, output stream, snapshot interval, fuel and memory limits | today `felixd run`, the owner on failure; end state: admission writes specs, the owner writes state |
| `leases` | cache, TTL | workload id | owner node id, lease epoch (0 until the claim has taken one) | agents, with `cache_put_if` |
| `epochs` | cache | workload id | the last lease epoch handed out | agents, with `cache_put_if` |
| `nodes` | cache, TTL | node id | felixd version, `max_workloads`, workloads held | each agent |
| `modules` | cache | sha256 of the module | the module bytes | submitters (`felixd run`) |
| `wl.<id>.out` | stream, 1 shard, `Quorum` | | output records | the lease holder, with `publish_if` |
| `wl.<id>.snap` | stream, 1 shard, `Quorum` | | snapshots | the lease holder |

A workload's input is any stream: one written by an outside producer, or
another workload's `.out`. v0 reads one shard of one stream per workload.
Claim, failure and needs-operator records on another workload's `.out` are
not inputs, and neither are rejections or outputs under another name than
`out`: the owner, `felixd check` and replay pass over them, so only outputs
addressed to `out` reach `step` ([Pipelines](#pipelines)).

Each output record carries a small header: the input offset that produced it,
its index among that step's outputs, and the lease epoch of the owner that
wrote it. The output stream also holds claim records, one per takeover, which
carry the new owner's epoch and the `sys.specs` offset of the spec it runs,
and a failure record when the workload fails. Each snapshot carries the input offset it covers and the output tail
at that point.

Today `felixd run` creates the workload's streams through the control plane's
REST API (`/v1/tenants/{tenant}/namespaces/{namespace}/streams`). Agents never
create streams. The end state moves that to admission ([Submitting
work](#submitting-work)).

Running `felixd run` again for an existing id appends a new spec record and
points `workloads` at it. The cache only moves forward: a write naming an
older record than the one there is dropped. Agents run a spec only if its
record says the same thing as the cache value, so everything that ran is in
the history. A running workload whose spec changes restarts under the new
one: the agent stops the task, releases the lease and claims it again, and
the next owner's claim names the new record. A new module or input stream
would not reproduce what the streams already hold, so `felixd run` refuses
that change once the output or snapshot stream has a record, and an agent
refuses to run a spec whose module or input differ from the spec the output
stream's first claim names. Claims only move forward too: each claim carries
the highest spec record claimed so far, like the epoch floor, and an agent
refuses a spec record below the last claim's floor, which is what a node with
a stale watch holds. A floor is permanent: moving `workloads` back to an
older record below it is refused, and recovery is a new record above it. A
claim from an older build carries no floor and resets it to zero, which is
safe only because every node must run the same felixd build. `felixd check`
applies the same rule, so it judges a claim the agent refused as not run.
Finding the last claim pages back from the tail a few
records at a time. It checks both before its lease claim, so a
refused spec takes no epoch, and again once its own claim is on the stream,
when no other claim can still land first. Either way the workload is left to
an operator. `felixd run` refuses to point `workloads` at its record if the
entry already names a later one, which happens when another submit won or
`sys.specs` was recreated. New code goes under a new id.

`felixd check` and `felixd replay --diff` run the module and input of the
first claim's spec, and judge each input under the limits of the spec whose
claim covers it: a claim's spec applies from the input after the last output
before it. A claim its owner refused, under another module or an older
record, is passed over.

## Pipelines

A pipeline is workloads that read each other's `.out`. Nothing about a stage
is special: a downstream is an ordinary workload whose input stream happens to
be fenced by someone else. Its exactly-once output and resume rest on one
property of that stream, which the upstream's fencing gives: a record on it is
never rewritten or removed, so every reader at an offset sees the same record.

An upstream takeover leaves its marks on the stream, and a downstream reads
through them:

- A claim lands between two outputs, possibly in the middle of one step's
  batch when the previous owner left a prefix of it (Felix#1630). The
  downstream passes over the claim. Its inputs keep their offsets, gaps and
  all, so its own headers and the fold do not move.
- Outputs written by a stale owner before the claim are on the stream once,
  in order, by the upstream's fence. The downstream sees each once, wherever
  the claim fell.
- Generation-start records take offsets and are never read back, as on any
  input.

So a downstream's step sees the same inputs, with the same metadata, on every
node and in `felixd check` and replay, and its own fence makes its output
exactly once. `crates/agent/src/input.rs` is the one place that decides what a
record on another workload's `.out` gives a step.

Spec history applies to a downstream like any workload: its input is the
upstream's stream name, so pointing it at another upstream once it has output
is refused by `felixd run` and left to an operator by an agent. Recreating an
upstream under the same id is the incarnation problem ([Names and
incarnations](#names-and-incarnations)), solved only in part: a node that
finds its input ending before the inputs its output or snapshot shows it
stepped, when it resumes or when its subscription goes back below its
position, records needs-operator. A recreated input that has already grown
past that offset is not detected and
is read as if it continued the old one, until Felix exposes incarnations to
clients (#133).

`felixd check` judges each stage against its input as it stands, and checks
stages upstream first. Each stage's input is then read after the stage before
it was checked, so `--complete` on every stage says that every input on the
first stage's input when it was checked went through the last.
`felixd check --id <last> --upstream` checks a whole pipeline from its last
stage. `felixd replay` works stage by stage.

`tests/pipeline.rs` runs three stages, each on its own node with a spare,
and loses the holder of each in turn: stage one's new holder dies after half
a batch lands, stage two's is frozen past its lease and woken after a third
node took over, and stage three's is killed. Every stage must then hold what
an uninterrupted pipeline writes and pass `felixd check --complete` and
`replay --diff`. CI runs it on one broker and on three.

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

A workload that needs an operator is recorded the same way, with a
needs-operator record instead of a failure, and the spec is left `running`
([System records and errors](#system-records-and-errors)). The record lands
only before any later owner has begun, and a claimer that finds it last
before its claim, or past the tail when its claim is refused, stops instead
of claiming after it.

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
entry goes away about when its leases do. It says which agents are up and
how full they are, and `felixd ps` shows it. The only decision that reads it
is whether to wait for a workload's assignee ([Placement](#placement)).

Without a scheduler, placement is "first claim wins" with a per-node cap: an
agent holding `--max-workloads` (default 64) claims nothing more until one of
its workloads ends, and every claim waits a random time of up to
`--claim-delay-ms` (default 1 s). The agent sees placement only through the
`Placement` trait in `core`, so the scheduler ([Placement](#placement)) came
in without touching the lease code.

`felixd drain <node>` writes a mark under the node's id in the `drains` cache,
with no TTL; `felixd undrain` deletes it. Each agent reads its own mark every
second. Once it finds one, every workload it holds stops the way a clean
shutdown stops it: the work is dropped, renewals end, and only then is the
lease deleted with `cache_delete_if` at the held version, so another node can
claim at once instead of after the TTL. The mark is read again right before
every claim, since the last regular read can be a second old, or much older
in a process that was stopped, so a drained node takes no new work. A read
that fails means no claim, so a node needs read on `drains`. A broker that
answers that it does not know the cache counts as "not draining" only until
the node has once found the cache: after that it is a broker restarting or not
serving it yet, and the answer is unknown. That is kept per process, so a node
that restarts while the broker does not serve `drains` yet reads "not
draining" until it finds the cache. Tasks for new specs still start on a
drained node, but they wait until the mark is gone.

A claim that read the mark just before it was written can still land. So a
node counts its claims between that read and the end of the claim, and puts
the mark's nonce in its `nodes` entry as `draining` only once it has read the
mark and none are under way. Any claim it starts after that finds the mark.
`felixd drain` picks a new nonce for each mark, so an acknowledgement left
from an earlier drain, before an undrain, does not count for this one.

A drain needs nothing new for safety. Releasing is what a clean shutdown
already does, and a write the old holder still has in flight meets the new
owner's claim like any stale write
([Exactly-once output](#exactly-once-output)). In the TLA model it is a crash
followed at once by the lease expiring, which the model already explores. Two
things are not guaranteed: a node of an older build ignores the mark, and a
node that cannot read the mark keeps what it holds while its renewals still
succeed. `felixd drain --wait` returns once no live lease names the node and
the node acknowledges this mark, or it is down, since its leases expire. A node that
is stopped with a claim under way is not up and can land the claim when it
wakes; it releases it at its next read. `--wait` gives up after
`--timeout-secs` (300) with exit code 1, which is what a node of an older build
or one that cannot read `drains` gets, since it never says `draining`.
`felixd ps` shows such a mark as `pending`. Draining the last
node that is up leaves its workloads with no holder; they stay `running` and
`felixd ps` shows them waiting until a node can take them.

While a scheduler runs, a node that reads its mark appends a node fact saying
it is draining within half a second, and its `nodes` entry says so too, so no
node waits for it. The scheduler moves the node's assignments to other
nodes, so the workloads it releases go where the scheduler puts them instead
of to whichever node claims first. A node's assignments are left alone after
an undrain; nothing is moved back.

## Operating a cluster

`felixd ps` reads everything it shows from the caches and streams, so any
machine with read grants sees the same thing and no node's own view goes into
it. For each workload:

- **State.** `stopped` and `failed`, with the class and reason, come from the
  spec. `needs-operator`, with the reason and detail, when the last record on
  the output stream is a needs-operator record naming the spec record the spec
  comes from. Otherwise `running` when a lease with an epoch exists, `claiming`
  when the lease has no epoch yet, and `waiting` when there is none.
- **Holder and epoch.** The lease's owner and epoch, and whether that node is
  draining. With no holder, the epoch of the last record on the output stream.
- **Outputs.** The output stream's tail.
- **Positions.** For one input, the next input the workload steps: past the
  input of the last output or rejection, or of the latest snapshot, whichever
  is later. Lag is the input's tail minus that. A step that wrote no output
  and is in no snapshot does not show, so the position can be behind the
  holder by the steps since; it is never ahead. For several inputs, each
  input's position is how far it is merged (the last merge log record's
  `next`), and a separate line gives the next step on the merge log against
  its tail. Lag counts records on the stream, claims included when the input
  is another workload's output.

- **Assigned.** While a scheduler runs, an `ASSIGNED` column gives the node the
  scheduler assigned the workload to and the offset of that assignment on
  `wl.scheduler.out`, such as `node-2@912`. A holder that differs from it took
  the workload by first claim wins, or before the assignment changed.

For each node: whether it has an entry in `nodes`, how many live leases name
it, its `--max-workloads`, its version and whether it is draining: `yes`, or
`pending` while it is up and has not said it read its mark. An entry this build
cannot read shows as up with its version unknown. Drains are
in [Leases and failure detection](#leases-and-failure-detection).

Divergence records, which `felixd ps` would also show, are not built yet
([#73](https://github.com/GetFelix/felixd/issues/73)).

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

Snapshots are written every N inputs (default 1000) and, with
`snapshot_every_ms`, at the first input of each interval of that length,
counted from the Unix epoch: an input whose append time falls in a later
interval than the previous input's. The append time is the broker's stamp on
the input record, so every node picks the same inputs, and a slow input still
gets snapshots. Either trigger restarts the count. Each carries the
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

`felixd replay --id <workload> --to <offset | time>`:

1. Turns a time into an input offset with `offset_for_time`: the state as of a
   time includes every input appended before it. An offset includes that
   input.
2. Picks the snapshot with the highest input offset at or before that point
   and restores it on a fresh instance, under the limits in the spec. Without
   one it starts from the initial state.
3. Range-reads the inputs after the snapshot up to the target, runs `step`, and
   prints `inspect()`.

`felixd replay --id <workload> --diff [--from <offset>] [--to <offset | time>]`
replays the range the same way, from the nearest snapshot before `--from`, and
compares each output it produces with the one on the stream by input offset,
index and the sha256 of the payload. It stops at the last input with a
recorded output, since the workload may not have reached later ones. The first
divergence is reported with the input offset, the epoch of the record, the node
its epoch's claim names and both digests, and the command exits with 1. That
checks determinism; CI runs it on every example workload after a takeover.

The same comparison with a different module (`--module <hash>`, a module in
the `modules` cache) checks an upgrade before it goes live: the new version
runs over the recorded inputs from its initial state, since the snapshots hold
the old version's state, and the diff shows where its outputs differ (#29).
`--module` without `--diff` prints the new version's state at `--to`.

Replay needs no lease and writes nothing. It should run anywhere with read
grants, but it reads the input and snapshot streams' tails with a refused
conditional write, so for now it needs publish grants on those streams too
(Felix #1621). `felixd check` has the same gap. A node reads its input's tail
with a read past the end instead, which Felix refuses with the tail, so a node
needs only read on its inputs.

## Grants

| Principal | May |
|---|---|
| Node | Read `workloads`, `sys.specs`, `modules`, `epochs` and `drains`; write `leases`, `epochs` and `nodes`; write `workloads` to mark a workload failed; read every input; publish to `wl.*.out`, `wl.*.snap` and `wl.*.merge` |
| Operator (`felixd run`) | Read and write `workloads`; read `sys.specs` and append to it (a conditional write); write `modules`; read the tails of the output and snapshot streams (a conditional write, so publish) to refuse a spec change once records exist; create the caches and streams through the control plane. Today's grant, replaced by the submitter and admission rows below |
| Submitter (`felixd run`) | Put on `modules`; publish to `sys.intake`; read `wl.admission.out` and `sys.specs`. Nothing else. Until admission knows the requester, any submitter can change or stop any workload that is not a system workload (`admission`, `scheduler`), whoever submitted it |
| Admission | Read `sys.intake` and `modules`; write `sys.specs` and `workloads`; hold the stream-creation credential. Today its holder's node token and control plane token, so any node with both can write `sys.specs` |
| Operator (`felixd init`) | Write admission's own spec directly. The one direct write left |
| Viewer (`replay`, `check`, `ps`) | Read everything, including `sys.specs`, write nothing |
| Operator (`felixd drain`, `felixd undrain`) | Write `drains`; create it through the control plane |

In v0 any node may write any workload's output. Fencing keeps that correct,
but a compromised node could write garbage. Per-lease tokens close that
([Credentials](#credentials)).

## Retention

`keep_snapshots` (`felixd run --keep-snapshots K`) keeps a workload's last K
snapshots. After each snapshot the holder writes, it appends a trim record to
`wl.<id>.out` ([records.md](records.md#output-stream-records)) naming the
floors below which this workload's resume reads nothing:

- `.snap` below the lowest offset any kept snapshot reads, its manifest or a
  chunk it shares with an older snapshot;
- the merge log below the oldest kept step, whose entry is kept for its
  `next` and its append time;
- each input below the record that step read, with one input, or below the
  entry's `next`, with several, the entry's own record kept.

A resume reads back the last stepped record for its append time
(`snapshot_every_ms`), so that record stays above the floor. Every read that
walks back from a tail stops at the oldest offset Felix holds rather than
asking below it. That holds for Felix's own age and size retention too: a walk
that used to fail on `CursorTooOld` now ends quietly. When retention has
removed the last stepped record, the resume has no append time for it, so the
next `snapshot_every_ms` snapshot can fall on a different input than an
uninterrupted run's. Outputs are unaffected.

A new owner reads the last trim record on `.out` when it resumes and never
writes floors below it. After `keep_snapshots` is raised the last trim stands
until the kept snapshots pass it.

The oldest kept snapshot is the one with the lowest step among the last K on
the stream, not the first of them: a stale holder's snapshot can land after a
newer holder's and be behind it. A holder trims only once the newest snapshot
on `.snap` is its own. Until then an earlier holder can still land one, of a
lower step, that the next resume would restore. Once it is, every snapshot
written later is from this epoch or a later one and at or past a kept step, so
the floors never go back, and `trim::plan` refuses one that would.

The record goes on `.out` with `publish_if` at the tail like an output, so a
stale holder's is refused by the next claim. A crash before it lands changes
nothing, and one after leaves a record the next holder passes over, like a
claim. Resume, reconcile, `felixd check` and downstream readers pass over it.

Nothing is deleted yet. Felix trims only by age or size, which knows nothing
of what a resume needs, and has no call to trim a stream before an offset
([Felix #1662](https://github.com/GetFelix/felix/issues/1662)). When it does,
the floors decide different things per stream:

- `.snap` and the merge log are the workload's own. A holder may delete them
  up to its last trim record's floors, after the record has landed, so a crash
  at any point leaves less deleted than recorded.
- An input may be another workload's `.out` or a stream other workloads and
  programs read. One workload's floor says only what that workload's resume
  needs. An input is deleted only below the lowest floor of all its readers
  ([#150](https://github.com/GetFelix/felixd/issues/150)), never on one
  workload's floor.
- `.out` is not trimmed. It is every downstream's input, it holds the first
  claim that spec lineage reads, and `felixd check` lines records up with
  claims from the start of the stream (#150).

`felixd check` and `felixd replay` read from the start of each stream while
the records are there, trim records or not. Once something they need is
deleted they stop and report the stream and the oldest offset Felix holds;
checking from the oldest kept snapshot instead comes with #150.

A workload whose input Felix has trimmed past what it still needs, through
retention or a later trim, is recorded as needing an operator (reason 9)
rather than skipping records, as `subscribe_from` refuses with `CursorTooOld`.

`tests/trim.rs` and `tests/merge.rs` resume with every read below the floors
refused (the `enforce-trim-floors` fault), as if they were deleted, and the
workload carries on.

## Build order

| M | Milestone | Done when |
|---|---|---|
| 0 | One node, one workload | A counter workload runs against a local Felix; killing and restarting felixd resumes at the right offset with no repeated output |
| 1 | Leases and failover | On 3 nodes, killing one moves its workloads within one TTL, verified by a checker that reads every `.out` |
| 2 | Exactly-once output | A node frozen past its lease and resumed writes nothing once the new owner has claimed, even with no new input; a fault-injection test runs this in CI |
| 3 | Replay | `felixd replay --to` an offset or a time prints the state the live workload had there; `--diff` finds no differences |
| 4 | Pipelines and operations | A three-stage pipeline survives failover at each stage; `ps`, `drain`, metrics and snapshot trimming work (pipelines, workloads with several inputs, `ps` and `drain` are built, [Pipelines](#pipelines), [Several inputs](#several-inputs) and [Operating a cluster](#operating-a-cluster); metrics are built, [Metrics](metrics.md); snapshot trimming is recorded but deletes nothing until Felix can trim at an offset, [Retention](#retention)) |
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
- **A determinism ABI** (not built yet,
  [#62](https://github.com/GetFelix/felixd/issues/62)) names everything that affects results: the felixd
  runtime version, the wasmtime version and its settings, limits, and the
  instruction set while stack depth still depends on it. It is recorded in the
  spec, in each snapshot header and in `nodes`. A node never claims a workload
  pinned to an ABI it doesn't implement. Until stack depth is deterministic,
  mixed x86_64 and aarch64 clusters pin workloads by instruction set. The pin
  comes off only when CI replays the same recorded inputs on x86_64 and aarch64
  and the output and snapshot digests match; native workloads stay pinned
  regardless.
- **Trap classes** (not built yet,
  [#98](https://github.com/GetFelix/felixd/issues/98)). A trap that depends only on the inputs (a guest panic,
  running out of fuel) is a poison input and fails the workload. A trap that
  can depend on the host (stack overflow, a host resource failure) is retried
  once, on another node. The agent releases the lease and records the trap
  class, so the second try runs in a different environment and the count is on
  the log rather than in one node's memory. A poison input is never retried
  (#98).

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

The interface, the guarantees and the record formats are in
[contract.md](contract.md).

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
| Drain marks in `drains` | Cache entry, no TTL | Operator intent; each node reads its own and nothing replays from it |
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

With one input, replay is deterministic because the input's offsets fix the
order. With two, arrival order depends on timing, so a replay could see
another interleaving. felixd records the order instead (#58). A spec may name
up to 64 inputs (`felixd run --input a --input b`), and such a workload gets a
merge log, `wl.<id>.merge`.

The lease holder subscribes to every input, and as records arrive it appends
an entry for each to the merge log, naming the input and the record's offset
and append time, then steps them. The merge order is a record like any other:
the entry's offset on the merge log is the step's offset, which outputs and
snapshots name. Each entry also carries every input's next offset, so the
last record alone says where each input carries on. The layout is in
[records.md](records.md#merge-log-records).

The merge log is fenced like the output stream. After its claim on `.out`
lands, a holder appends a claim to the merge log at the tail it reads, having
checked that the last record there is from an epoch no later than its own,
and merges only after that. Every append is a `publish_if` at the tail it
expects, so once the claim is on, a stale holder's append is refused and it
finds the later epoch and stops. Entries a stale holder appended before the
claim stay: they are the order it chose, and the new holder steps them as its
own. Since entries land before their steps, a holder that dies between the two
leaves merged records without outputs. The next holder resumes as usual from
the snapshot and the last output, steps every entry the log holds from there,
and only then subscribes to the inputs, each from its next offset.

Replay, the resume fold and `felixd check` read the merge log, not the raw
inputs, and step the records it names in its order. `felixd check` also checks
the log itself: every record of every input merged once and in its input's
order, `next` offsets that follow from the entries, and epochs that never go
backwards; with `--complete`, that no record is left unmerged. `felixd replay
--to` takes a merge log offset for such a workload, and a time there covers
the entries merged before it, not the records appended to their inputs before
it: entries are not in their records' time order.

This rests on the first layer of the safety argument ([Exactly-once
output](#exactly-once-output)) only: the merge order is not a function of the
inputs, so a stale holder's entries are kept, not recomputed.
`specs/FelixdMerge.tla` models the merge log ([formal.md](formal.md#several-inputs)).

A deterministic merge by timestamp is not planned: one idle input would stall
the workload, and it depends on clocks. A workload with one input has no merge
log and runs as before.

## Placement

Without a scheduler, agents place workloads by first claim wins: when a lease
is free or expired, an agent with room claims it after a random delay, and the
broker's conditional write picks one winner. It sits behind the `Placement`
seam.

Built (#60): placement can come from a scheduler that is itself a felixd
workload, `scheduler`, submitted with `felixd run --id scheduler --input
sys.specs --input sys.facts scheduler.wasm`. It writes assignments as
records, so placement is replayable and survives failover like any workload.
Agents still claim through the lease, so the scheduler decides where work runs,
never whether output is correct. A scheduler outside felixd is not planned.

- **Inputs.** Spec records on `sys.specs`, and node facts on `sys.facts`.
  While the `scheduler` workload runs, each agent appends a fact: its id,
  how often it appends, its `--max-workloads`, the ids of the workloads it
  holds however it claimed them, and whether it is draining or cannot claim
  (it cannot read its drain mark). It appends one every
  `--facts-every-ms` (three lease TTLs by default) and within half a second
  of a change. The scheduler's clocks are the append times of the inputs it
  steps, never a wall clock, so a replay assigns the same way. A node takes
  work while its last fact is within three of its intervals of the latest
  `sys.facts` append time and says it is neither draining nor blocked.
  Silence is measured on `sys.facts` alone, which one shard leader stamps,
  so a broker clock that runs ahead on `sys.specs` cannot make every node
  look silent. The skew left is a leader change on `sys.facts` to a broker
  whose clock is behind or ahead, which can shift one node's silence by that
  much. Nodes silent for thirty intervals are forgotten.
- **Policy.** A running workload with no assignment, or one assigned to a node
  that no longer takes work, goes to the node with the lowest load and room,
  the lowest id first. A node's load is every workload assigned to it or held
  by it, counted once, so workloads it took by first claim wins, the
  scheduler included, count. A node over its room loses the assignments it
  does not hold, highest ids first, to nodes with room or to none. Nothing
  else is moved off a node that still takes work, and a returning node gets
  nothing back. A stopped workload's assignment is dropped. The scheduler
  never assigns itself; it is always placed by first claim wins.
- **Output.** An assignment per change, keyed by workload. Each output also
  says how far the scheduler has read `sys.specs`, and a spec record that
  changes nothing still gets a progress record, so a node can tell "not yet
  assigned" from "left unassigned". The whole table is written again on the
  first step and then every ten minutes of input time. A takeover is not an
  input, so a table written on takeover would not replay; the timed table is
  the deterministic way to the same end. Readers start from the latest table,
  so `wl.scheduler.out` can be trimmed below it (#19).
- **Agents.** Each agent reads `wl.scheduler.out` a page at a time every half
  second, starting from the latest table, and keeps its place across failed
  reads and across the scheduler stopping and starting. A node follows
  assignments only while the scheduler is healthy: its spec says `running`
  and its holder renewed the lease lately: within the freshness window W, one
  renewal interval (TTL/3) plus 1 s for the renewal's round trip, watch delay
  and skew, at least TTL/2 and at most five sixths of a TTL. That is 5 s at
  the default 10 s TTL. A lease that has only not yet expired is not enough,
  since its holder may have died up to a TTL ago. Then a node claims a free
  workload assigned to it at once. It leaves one to another node only while
  that node's `nodes` entry says it takes work (under `--max-workloads`, not
  draining, not blocked) and was rewritten lately: a read gives no expiry, so
  an entry whose version has not changed for W since this node first read it
  counts as gone. That wait lasts at most `--assignment-bound-ms`
  (15 s). It leaves a workload with no assignment only while the scheduler
  has not read its spec record, for at most `--assignment-grace-ms` (2 s): a
  healthy scheduler assigns a new spec in well under a second. Both are
  counted from when the node first saw the workload free, whatever the
  assignment did meanwhile.
- **Fallback.** Everything else is first claim wins at once: no healthy
  scheduler, an assignee that is down, silent, full, draining or blocked, an
  entry that cannot be read, a workload the scheduler read and left
  unassigned, and a wait past the bound or the grace. An assignment cannot
  make two owners, since the lease and the `publish_if` fence still decide.

What this adds to a failover or a new workload, compared with first claim
wins:

| Case | Added wait |
|---|---|
| No scheduler, or its holder not renewing | None once its lease has gone W without renewal |
| Assignee down, full, draining or blocked | None (one `nodes` read) |
| Assignee died but its `nodes` entry has not expired | At most W from this node's first read, plus a second between reads |
| Assigned to this node | Less: it claims with no random delay |
| Scheduler read the spec and left it unassigned | None |
| Healthy scheduler not yet at the spec record | Until it is, typically under a second, at most the grace (2 s) |
| Assignee healthy, with room, but does not claim | The bound (15 s) |

When the scheduler's own node dies, nodes stop counting on it once its lease
has gone W without renewal, W after its last renewal. Until then a new workload waits at most the grace, and a workload
on a dead node is first claim wins at once as before.

The scheduler's own view of a dead node is slower: three fact intervals,
90 s by default. That is deliberate. It only decides where new assignments
go, while each node's check of the assignee's `nodes` entry decides whether
anyone waits, so a dead assignee costs a claim at most W whatever
the scheduler thinks. Matching the scheduler's view to the lease TTL would
mean a fact per node per TTL, three times the growth below.

Growth. `sys.facts` gets one record per node per `--facts-every-ms`, about
2,900 a day per node at the default 30 s, plus one per claim or release, at
most two a second per node. A fact is about 150 bytes plus each held
workload's id and three bytes: about 1.6 KB for a node holding 64 workloads
with 20-character ids, and at most about 4.5 KB with 64-character ones. A
full node at the defaults so writes about 4.6 MB a day. The agent refuses a
`--max-workloads` whose largest possible fact would not fit in one record
(about 125,000). `wl.scheduler.out` gets one
record per spec record, one per assignment change and a table every ten
minutes of input time. Neither is trimmed yet (#164).

Mixed versions. A node from before the scheduler ignores assignments and
claims by first claim wins. That only takes work an assignment meant for
another node; the lease still picks one owner. It writes no facts, so the
scheduler never assigns to it.

What is not built yet: moving a running workload to its assignee (the
scheduler only places free workloads), labels and the other node facts
below, and replacing per-workload lease watching with assignments
([Upgrading felixd and scale](#upgrading-felixd-and-scale)).

Rules for the scheduler and the records it writes (#100):

- **Keyed per workload.** Assignments and leases are per workload, never one
  versioned record per node. Per-node sequence numbers made Omega's
  schedulers conflict two to three times as often.
- **Incremental by default.** An assignment batch applies per workload.
  All-or-nothing placement is an opt-in for a group that needs it.
- **One placement authority per workload at a time:** the scheduler, or first
  claim when its assignment is missing or stale.
- **Reconsideration is a recorded tick input**, like time in the workload
  contract, so the scheduler stays a replayable fold over recorded events.
- **Status names what it acts on.** Agent and workload status records carry the
  spec version and assignment offset they act on, so `ps` can show "running
  spec 7, assignment 912" and a lagging node is visible.
- **One writer kind per system stream**, enforced by grants. Readers keep
  refusing unknown versions. `sys.specs` is written only by admission
  ([Submitting work](#submitting-work)).

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
- **Effect kinds and credentials are bound in the spec.** The spec records the
  kinds (with versions) and credential names the component's WIT declares.
  An executor refuses a request outside that set and resolves only bound
  credentials, so its authority is never ambient from the workload's point of
  view (#97).
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

Built: spec records on `sys.specs`, claims that name the record they run,
and `check` and `replay --diff` judging each input under the limits that
applied ([Data model](#data-model)). The offset a spec takes effect at is
where its owner's claim lands, not one the submitter states. Not built:
incarnations, which Felix does not expose to clients yet (#133), dropping `output`
from the spec, and module changes at an offset (#64).

Plan 9 resolved a name at every `open`, so a rebind silently changed what a
program read. Felix fixed the same class of bug for forwards in #1324.

Sources: Pike, Presotto, Thompson, Trickey and Winterbottom, "The Use of Name
Spaces in Plan 9" (1993); Pike, "Lexical File Names in Plan 9" (2000).

## System records and errors

Payloads stay opaque bytes. The system's own records do not (#73):

- Specs, leases, failures, effect requests and effect responses are versioned
  typed records. Today they are defined once, in `core`, and every write goes
  through them: the cache values carry a format number under which the value
  sits, so an older build refuses them instead of reading what it can, and a
  failure carries its class ([records.md](records.md)). Defining them in WIT
  with the workload contract waits for the effect records, which are not
  built.
- Every error has a class: **transient** (retried while the lease is held),
  **needs operator** (a missing module, an input past retention, a changed
  incarnation) or **poison** (an input the workload deterministically fails on).
  The [trap classes](#determinism) map onto these. The text is detail, never
  what a program compares. A needs-operator error also has a reason code.
- **Needs operator is recorded** (#112). It is an observation, not spec
  state: a record on `wl.<id>.out` naming the spec record, the reason and
  detail, appended by a holder and fenced by epoch like the failure record
  ([Leases and failure detection](#leases-and-failure-detection)). A node that
  finds the problem before claiming claims only to write it. Every node whose
  spec names that record finds it last on the stream and leaves the workload
  alone, so it is written once per workload, spec record and reason. It is
  not a takeover, so it does not count toward restart intensity. Only a spec
  change, which is a new spec record, or removing the workload clears it. A
  module still missing after 30 s, a module this build refuses, a spec that
  is not in the history or conflicts with the output, and a restore out of
  fuel are recorded. A stale spec is not, since only the node holding it sees
  it. The record raises the spec floor like a claim and is passed over when
  finding the first claim, so it neither hides the history nor lets a stale
  node claim after it. A spec record still read as past the tail of
  `sys.specs` after 30 s is recorded, so a lagging spec-history read stops the
  workload cluster-wide until the spec is submitted again. `felixd ps` shows it
  as `needs-operator` with the reason and detail.
- A workload stuck on a transient error past a bound releases its lease and is
  recorded `degraded` with its class and reason, so it is visible instead of
  retrying silently on one node. `degraded` is not built yet
  ([#99](https://github.com/GetFelix/felixd/issues/99)).
- **Restart intensity is counted cluster-wide.** The release above is per
  holder, so a workload that fails everywhere would bounce between nodes, each
  spending its own budget. The spec sets MaxR takeovers per window. Any
  claimer counts them from the claim records on `wl.<id>.out`; past the limit
  it records `degraded` with class needs operator, fenced by epoch like the
  `failed` write (#70), and nothing claims the workload until an operator acts.
  There is no escalation tree. The TLA model gets the counter under
  `NoStaleFailure` (#104). Implementation is #99.
- **Poison has a recorded way out.** The failing input is in the log, so every
  replay reaches it. An operator resolves it with a spec change at offset N
  that either skips input N, copying it to a dead-letter stream, or switches
  the module from N. Both take effect at a stated offset ([Names and
  incarnations](#names-and-incarnations)), so `check` and replay judge outputs
  under the spec that applied and the skip replays. `failed` and `degraded`
  stay observations; the spec change is the only action (#98).
- **Failure modes are versioned.** The set of outcomes an effect kind can
  return is part of that kind's WIT version and is negotiated like any record
  format. Adding an outcome is a new version. Contract 0.2 already treats
  running out of snapshot fuel as an abandoned snapshot and out of restore fuel
  as needs operator (`docs/contract.md`); this section does not change that.
- A divergence record carries the node, its determinism ABI, the input offset,
  the snapshot it started from, and the digests of both the expected and the
  actual output, so `felixd replay` can reproduce it. Not built yet.
- Metrics are Prometheus text per node, which an OpenTelemetry Collector
  scrapes with its Prometheus receiver; [Metrics](metrics.md) lists them.
  Input lag, claims, handoffs, steps, step latency, outputs, refusals and
  snapshots are built ([#18](https://github.com/GetFelix/felixd/issues/18)).
  Pushing OTLP from felixd, traces, and replay time and per-effect latency
  (effects are not built) are not.

Plan 9's `ctl` files each had their own text commands and `errstr` returned a
string that callers matched by text. Linux sysfs had to impose one value per
file to keep the same idea readable.

Sources: Plan 9 `errstr(2)`; Linux `Documentation/filesystems/sysfs.rst`.

## Credentials

Tracked in #74. Built so far: felixd reads its tokens only from files, never
from arguments or the log, and holds them in a type that formats as a
placeholder, so no log line, error or panic message carries one. An
integration test (`crates/felixd/tests/secrets.rs`) runs a workload through a
failover with every log turned up and scans every record, cache entry, metric
and log line for the token bytes. Per-lease tokens are not built; see below.

- **Secrets never enter the log.** A workload names a credential in an effect
  request; the executor resolves the name through a credential agent on its
  own node and runs the authentication itself. Response records exclude
  credentials by construction.
- **Names are bound in the spec.** A workload can name only the credentials its
  spec binds ([Effects](#effects), #97); the executor resolves nothing else.
- **Tokens are per lease.** A claim gets a short-lived token scoped to that
  workload's streams and epoch, so a node can write only the outputs it holds.
  This closes the v0 grant that lets any node write any output, and needs
  scoped token minting from Felix
  ([GetFelix/felix#1625](https://github.com/GetFelix/felix/issues/1625)).
  Felix's token exchange can narrow a token to named streams, but only from an
  upstream identity token whose grants already cover every output, so a node
  minting its own per-lease token would still hold the authority to mint one
  for any workload, and every claim would wait on the control plane, which
  failover does not today. The mint has to check the lease itself, and refuse
  a holder whose epoch is no longer the latest. Until Felix has that, output
  stays protected by the `publish_if` fence alone.
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

## Submitting work

Decided. Today `felixd run` uploads the module, creates the streams with the
control plane token and writes the spec into `workloads` last, so anyone who
can submit work holds admin rights. Who may change system state is the part
hardest to retrofit, and admission is where placement constraints, guarded
operator actions (#101) and staged rollouts have to live, so the path is fixed
now:

1. **Spec history is records first** (#72). Versioned spec records take effect
   at a stated input offset ([Names and incarnations](#names-and-incarnations)).
   Agents act on spec records, not on the latest `workloads` value. Built:
   agents run only what a record says; `workloads` is still how they learn of
   a change.
2. **Requests go to an intake stream, `sys.intake`.** A request carries the
   module hash, the spec and the requester, taken from the authenticated
   publisher and never from the payload. Changes, stops and removals of
   workloads are requests on the same stream.
3. **Admission is an ordinary deterministic felixd workload**, like the
   scheduler (#60). It reads `sys.intake`, checks the module is present and
   within size and quota, and checks limits, grants, quotas and names. It
   writes an accepted spec record or a rejection record with a reason code to
   `sys.specs`, each naming the request it answers, so the submitter reads the
   outcome back. Admission is the only writer of `sys.specs`, enforced by
   grants ([Placement](#placement)). Because it replays, a policy change can be
   checked against past requests.
4. **Admission holds the stream-creation credential**, bound in its spec
   ([Credentials](#credentials), #97). Agents never hold control-plane rights.
5. **Modules stay in the `modules` cache**, uploaded by hash. The store is
   content-addressed, so a submitter cannot overwrite another's module, and
   binaries stay off the log. Admission verifies presence and size.
6. **`felixd run` is a thin client:** it uploads the module, publishes the
   request, waits for the accepted or rejected record and prints it.
7. **Bootstrap.** `felixd init` writes admission's own spec directly with
   operator credentials. That is the only direct write left and it is the
   [bootstrap boundary](#native-programs-and-the-bootstrap-boundary) for
   workload submission.

Built (#114): `sys.intake`, the admission workload and `felixd init`, and
`felixd run` submitting through intake wherever admission runs. Admission
outputs one decision per request on `wl.admission.out`, written once under
its lease and fence; the node holding its lease records each accepted one on
`sys.specs` as a spec record naming the decision, after checking the module's
hash and length in `modules` and creating the streams. Rejections stay on
`wl.admission.out` rather than `sys.specs`, so every reader of the spec
history, the scheduler included, reads only specs. Once admission's own
record is on `sys.specs`, a node runs a later record only if the decision it
names accepted that workload and exactly that spec, so a record written around
admission, by a stale tool or a submitter with direct write rights, never
runs or claims, and a record naming a decision no later than one an earlier
record of the workload named is refused, so an old acceptance cannot be
replayed over a later change or stop. A record whose decision Felix no longer
holds needs an operator; no admitted workload may read admission's streams,
so no trim record marks a decision trimmable, and `felixd init` sets no
`keep_snapshots` on admission. System workloads (`admission`, `scheduler`)
are refused through intake: an operator writes their specs directly, and the
gate lets those through by id alone, so whoever can write `sys.specs` can run
any module with any inputs as the scheduler, as before admission. Only records
whose decision accepted exactly their spec count as earlier decisions, so a
forged record naming a huge or absent decision blocks nothing. The gate and
the projector read `sys.specs` from its first record, so it must never be
trimmed. The scheduler reads every spec record, admitted or
not, so it can assign one admission did not accept; the assignee refuses it
at the gate, and the assignment only delays others until the bound.
`felixd replay --id admission --diff --all --module <hash>` lists the recorded
requests a new policy decides otherwise.

Not built: the requester. Felix records a record's publisher only when the
broker is configured to and a reader asks, and the workload contract has no
field for it, so admission decides from the payload alone and policy cannot
depend on who asked. Grants (#117) are not configured by felixd: until they
are, a principal that can write `sys.specs` can still write admission's own
spec, and any node can write `wl.admission.out`, as it can any output, until
per-lease tokens (#74, GetFelix/felix#1625). The stream-creation credential is
the holder's control plane token rather than one bound in admission's spec
(#97), so only nodes with one hold admission.

Open: the quota model (per tenant, per submitter, or per namespace, and what
it counts: workloads, module bytes, fuel and memory limits).

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
image they come in. Admission's own spec, written by `felixd init`, is the one
workload that is not submitted through admission ([Submitting
work](#submitting-work)). They are installed and upgraded with the image. felixd
release binaries are built reproducibly and signed (#23).

When a node can't reach Felix, it stops stepping every workload, native ones
included, once two thirds of the lease TTL have passed (as in
[Leases](#leases-and-failure-detection)). It will report itself `degraded` locally
(not built yet, [#99](https://github.com/GetFelix/felixd/issues/99)),
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

## Later

Submission through intake and admission is decided and is built in the order
of the issues under [Submitting work](#submitting-work): spec history (#72),
intake and admission (#114), the thin `felixd run` (#115), `felixd init` (#116)
and the grants wiring (#117). The items below are not designed yet. Each has an
issue and a rough shape.

- **Guarded operator actions and a small operator surface** (#101). Operator
  actions become typed records with a precondition and a dry-run, and are
  refused when they would break a running workload. Fleet changes go one
  failure domain at a time and halt when `degraded` or `failed` counts rise.
  System records share one metadata, spec and status shape, and nothing
  extends the claim or fence path.
- **Provisional module upgrades with forward-only rollback** (#102, #65). The
  new module shadow-replays live inputs for a stated window before the switch
  record is written. Rollback writes the old module at a later offset from a
  state it can restore; it never replays over outputs already written.
- **Scheduler replay as a simulator, and a measured envelope** (#103).
  `felixd replay --module` on the scheduler tests a placement policy over
  recorded node facts, and CI measures how far workloads, nodes, streams and
  spec sizes scale.

Requirements recorded for the native tier and NineLives, whose kernel, init and
images are undecided:

- A process boundary between tenants on a node, so a sandbox escape in one
  tenant's workload does not reach the tokens of another's.
- A restart policy for native workloads in the spec: restart type and
  intensity, the first restart in place and the next on another node, counted
  on the log like the Wasm limit above.

Declined: the runtime does not snapshot a guest's linear memory by default.
Linear memory changes almost everywhere between snapshots, which defeats the
chunk-hash skipping that contract 0.2 relies on. It could return as an opt-in.

## Open questions

- A read-only 9P or FUSE view of the namespace, so `ls`, `cat` and `tail -f`
  can browse streams (#78). For people and tools, not a data path.
