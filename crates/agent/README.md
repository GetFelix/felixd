# felixd-agent

The node loop. It reads workloads from Felix, claims and holds their leases, feeds each one its
input, writes its output and snapshots back, and carries a workload on from the right input after
a crash or a takeover. The `felixd` command line is a thin wrapper over this crate.

It depends on `felixd-core` and `felixd-runtime`, and on `felix-client` for Felix. The `felixd`
binary depends on it.

## What it provides

| Module | Does |
|---|---|
| `agent` | `run` watches the `workloads` and `leases` caches, writes this node to `nodes`, and runs each workload it manages to claim while the lease holds. `AgentConfig` sets the node id, lease timing, placement and the assignment bound. |
| `assignments` | While a scheduler runs: follows its output on `wl.scheduler.out` a page at a time from the latest table, and appends this node's facts to `sys.facts`. `read` returns the assignments in force and how far the scheduler has read `sys.specs`, for `ps`. |
| `submit` | What `felixd run` does: stores the module in `modules`, creates the `wl.<id>.out` and `wl.<id>.snap` streams, and `wl.<id>.merge` for several inputs, appends the spec record to `sys.specs` and points `workloads` at it. |
| `check` | What `felixd check` does: replays each workload and reports gaps, duplicates, records out of order, epoch regressions and outputs that differ, and checks a merge log against its inputs. |
| `replay` | What `felixd replay` does: `replay` rebuilds a workload's state as of an input offset or time, and `diff` compares a replay with the recorded outputs. Neither writes anything. |
| `ps` | What `felixd ps` does: each workload's state, holder, assignment and positions, and each node, read from the caches and streams. |
| `drain` | Drain marks in the `drains` cache: `mark`, `lift` and `held_by`. |
| `settings`, `admin`, `felix` | How to reach Felix, the control plane's REST calls, and the Felix calls felixd makes. Tokens are a `Secret`, which formats as a placeholder; `expose` is called only where a token is handed to Felix. |

Behaviour that matters:

- **Claim.** A claim is a conditional cache write that succeeds only once the broker counts the
  previous lease as expired or deleted. The winner takes the next epoch, above every earlier
  claim's and every epoch already on the output stream, before it runs anything.
- **Renew.** The holder renews every TTL/3. A refused renewal means the lease is gone and it stops
  at once. A holder that cannot reach Felix stops by its own clock at two thirds of the TTL, before
  another node can claim.
- **Spec records.** An agent runs a `workloads` value only when the `sys.specs` record it names
  says the same. It refuses a spec whose module or input differ from the one the output stream's
  first claim names, or whose record is below the last claim's floor (the highest spec record claimed so far, carried by every claim), before its lease claim
  and again after its own claim is on the stream. Either refusal leaves the workload to an
  operator. A record past the tail of `sys.specs` is waited for 30 s, then left to an operator too.
- **Fence.** A new owner appends a claim record, naming its spec record, to the output stream before it resumes. Every
  later write is `publish_if` at the tail, so a node frozen past its lease is refused when it
  wakes, whatever its clock says.
- **Several inputs.** The holder appends a claim to the merge log after its claim on the output
  stream, steps any entries already there past its last output, then merges the inputs as their
  records arrive: each batch is appended to the merge log with `publish_if` before it is stepped.
  Check, replay and resume read the merge log, never the raw inputs.
- **Resume.** The owner restores the newest snapshot, steps the inputs the output stream already
  covers and compares each result with the record in its place (`felixd_core::resume`), then
  carries on. Each output lands once through lost answers, partial batches and crashes.
- **Drain.** Each agent reads its own drain mark every second and again right before each claim.
  While marked it stops what it holds, deletes those leases so another node can claim at once, and
  claims nothing; once no claim is under way it says `draining` in `nodes`. A failed read means no
  claim, and a broker that does not know the `drains` cache counts as no mark only until the node
  has once found it. The output stays fenced as in any takeover.
- **Placement.** With no scheduler, first claim wins: every node with room claims a free workload
  after a random delay of up to `--claim-delay-ms`. While the `scheduler` workload runs and its
  holder renews its lease, a node claims what is assigned to it at once. It leaves a workload to
  another node only while that node's `nodes` entry says it takes work (up, with room, not
  draining, able to read its drain mark) and was rewritten lately, for at most
  `--assignment-bound-ms`, and an unassigned one only while the scheduler has not read its spec
  record, for at most `--assignment-grace-ms`, both from when it first saw it free. The
  scheduler counts as running only while its holder renewed its lease lately.
  Anything else is first claim wins at once. Every claim still goes through the lease and the
  fence, so an assignment can delay work but never makes two owners. Each node appends a fact (its
  id, interval, `--max-workloads`, the workloads it holds, and whether it is draining or blocked) to
  `sys.facts` every `--facts-every-ms` and within half a second of a change, and says in `nodes`
  when it cannot read its drain mark (`blocked`).
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
