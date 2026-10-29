# The formal model

`specs/Felixd.tla` models one workload: nodes that claim it through the `leases` cache, take an
epoch from `epochs`, and fold its input into `wl.<id>.out` and `wl.<id>.snap` with
`publish_if`. TLC checks it under each `.cfg` beside it. `scripts/check_tla.sh` runs them all
and holds each to its registered outcome; the `tla` job in CI runs the script.

```bash
scripts/check_tla.sh                 # every configuration (needs Java 11+)
scripts/check_tla.sh FelixdNoMarker  # one
TLC_SIMULATE=100000 scripts/check_tla.sh FelixdFull  # random behaviours, not every state
```

## What it checks

| Invariant | Says |
|---|---|
| `OutputsExact` | The output records, in stream order, are a prefix of the deterministic outputs of the inputs in order: no duplicate, no gap. |
| `EpochMonotone` | Epochs on `.out` and on `.snap` never decrease (generation-start records excepted). |
| `NoInterleave` | Once an owner has begun, nothing from an earlier epoch lands after that point on `.out`. An owner begins when its claim record lands, or without the marker, when it reads the tail to resume. |
| `NoStaleFailure` | `failed` in the spec follows a failure record on `.out`, that record landed before any later owner began, and only generation-start records land after it (the model has one spec version; a resubmit's run lands after it in the code). The same holds for a needs-operator record, so it is written once and no claim lands after it. Without `FailRecord`: no node writes `failed` while the lease is not its own. |
| `NoPoison` | No node finds the streams impossible to resume from. The model is deterministic, so every such finding would fail a healthy workload. |
| `UniqueEpoch` | No two claims that run, or have records or requests in flight, share an epoch. |
| `SnapshotsSplit` | For each snapshot (X, T), the output records below offset T are of inputs up to X and the rest are of later inputs. |

Under weak fairness of every node's own steps, of delivery and of expiry, `FelixdLive` checks
`FreeIsClaimed` (a free, running workload is claimed) and `EventuallyDone` (every output is
written, or the workload failed). Both allow for a workload left to an operator: once a
needs-operator record is on `.out`, no node claims it again. Its local faults cover a failing node that crashes between the
failure record and the spec write: the next owner finds the record and writes the spec.

## Configurations

No single configuration turns every mechanism on at full size: with two inputs, three claims, a
crash and a generation-start record together, the state space is too large to check in CI. The passing
configurations split those across smaller models, each checked exhaustively with node symmetry.
Each twin turns one mechanism off, or drops one assumption, and must find the named violation.
Every configuration has two nodes.

Distinct states and times are from TLC 1.7.4 with `-workers 4` on an Apple M-series laptop. The
whole script takes about six minutes there. A twin's count varies a little between runs,
since TLC stops at the first violation a worker finds.

| Configuration | Expected | Model | Distinct states | Time |
|---|---|---|---|---|
| `Felixd` | pass | two inputs, two claims, a crash, a clock fault, a snapshot | 3,636,093 | 50 s |
| `FelixdGen` | pass | one input, two claims, a generation-start record, a clock fault, a snapshot | 4,628,610 | 57 s |
| `FelixdThree` | pass | one input, three claims, a clock fault, a snapshot | 7,194,107 | 91 s |
| `FelixdIdle` | pass | an input with no output, then one with two | 1,153,049 | 15 s |
| `FelixdRecreate` | pass | `epochs` recreated once, while every live epoch is on a record | 1,084,222 | 14 s |
| `FelixdFailWindow` | pass | one input, two claims, a clock fault anywhere in the failure path | 142,402 | 3 s |
| `FelixdLive` | pass | no timing faults, local faults; liveness | 153,378 | 5 s |
| `FelixdPartial` | pass | an input with no output, then a batch of two that may land in part, a crash, a clock fault | 5,391,648 | 75 s |
| `FelixdNoMarker` | `NoInterleave` | the claim record off | about 5,000 | under 1 s |
| `FelixdNoFloor` | `EpochMonotone` | the epoch floor off | about 64,000 | under 1 s |
| `FelixdNoConfirm` | `NoStaleFailure` | the `Confirmer` and the failure record off | about 2,000 | under 1 s |
| `FelixdNoFailRecord` | `NoStaleFailure` | the failure record off, with a clock fault allowed between an applied confirm and the spec write | about 3,000 | under 1 s |
| `FelixdNoReplayGuard` | `OutputsExact` | the `replaying` guard on snapshots off | about 59,000 | under 1 s |
| `FelixdNoReplayGuardSplit` | `SnapshotsSplit` | the same | about 24,000 | under 1 s |
| `FelixdRecreateAnytime` | `UniqueEpoch` | `epochs` recreated while an epoch is on no record | about 1,500 | under 1 s |
| `FelixdTailFold` | `NoPoison` | the fold on event offsets off (#67) | 20 | under 1 s |
| `FelixdNoLaterEpochWins` | `EpochMonotone` | reconcile's later-epoch rule off | about 20,000 | under 1 s |
| `FelixdPartialNoIndex` | `NoPoison` | the fold skipping whole inputs, not outputs by index | about 300,000 | 4 s |

`FelixdFull` has everything at once, with a second input whose batch of two may land in part. The `TLA nightly` workflow walks three million random
behaviours of it with `TLC_SIMULATE`, which takes about five minutes on the laptop above. A
simulation can miss a violation that exhaustive checking would find, so it backs the passing
configurations rather than replacing them.

`FelixdNoMarker` is the fence before #80, and its trace is the gap the claim record closes: a stale
owner's append lands after the new owner has resumed, when the new owner has not written yet. Determinism makes
the content right, so `OutputsExact` still holds there; `NoInterleave` is what a merge log or an
effect stream would need.

`FelixdNoFailRecord` is the failure path before #70, and its trace is the gap PR #68 names: the
confirm and the spec write are two calls, and a suspend or a broker clock jump between them lets
a node fail a workload another node now holds. The failure record closes it (`FailRecord`): it is
appended with `publish_if` at the tail just read, unless the last record there is from a later
epoch, so it lands only before any later owner has begun, and the spec's `failed` follows from
it. With the record, the passing configurations no longer need the `FaultsInFailWindow`
assumption, and `FelixdFailWindow` checks that window on its own. The `Confirmer` is then no
longer needed for `NoStaleFailure` (TLC passes with it off and the record on); the code keeps
it so a node that has lost its lease does not try.

`FelixdRecreateAnytime` pins the one assumption design.md states for epochs.

## How actions map to the code

Paths are from the repository root, as of workload contract 0.2.

| Action | Code |
|---|---|
| `Claim` | `cache_put_if` on absent with the TTL: `crates/agent/src/lease.rs:180-193`; the agent's retry loop: `crates/agent/src/agent.rs:405-500` |
| `TakeEpoch` | the floor, `written_epoch` over the last records of `.out` and `.snap`: `crates/agent/src/agent.rs:698-719`; the counter read and `next_epoch`: `crates/agent/src/lease.rs:212-220`, `crates/core/src/lease.rs:53-55` |
| `PutEpoch` | `put_epoch_if` at the version read: `crates/agent/src/lease.rs:221-226`; a refusal releases the claim: `crates/agent/src/lease.rs:194-198` |
| `PutLease` | the lease rewritten with the epoch at the claim's version: `crates/agent/src/lease.rs:227-248` |
| `Marker`, `MarkerAck`, `MarkerRetry` | `OutputLog::claim` with the tail read with the floor, and `check_found` on the last record and the records past the tail: `crates/agent/src/output.rs:249-379`, `crates/agent/src/agent.rs:464-472`; the record is `encode_claim_under`: `crates/core/src/output.rs:260-270` |
| `Probe` | `OutputLog::claim` once the claim has landed: the tail and `check_found` on the last record: `crates/agent/src/output.rs:256-265` |
| `ReadSnap` | the last snapshot, passing over chunk records, and the superseded check on the last record of any kind: `crates/agent/src/runner.rs:295-304`, `crates/agent/src/snapshots.rs:55-83` |
| `ReadOut` | the last output passing over claims, the superseded check and `resume::fold`: `crates/agent/src/runner.rs:305-323`, `crates/agent/src/runner.rs:618-638`, `crates/core/src/resume.rs:104-137` |
| `Step` | `Steps::step`, with `already_written` and `Written::check`: `crates/agent/src/runner.rs:420-457`, `crates/core/src/resume.rs:70-77`, `crates/agent/src/runner.rs:651-670`. A rejection is the step's one output, at index 0 |
| `OSend`, `OAck` | `OutputLog::append`, with the fence check before each write: `crates/agent/src/output.rs:393-436` |
| `Reconcile` | `skip_written` and `output::reconcile`: `crates/agent/src/output.rs:438-489`, `crates/core/src/output.rs:496-545` |
| `Continue`, `SnapSend` | the snapshot cadence and the `replaying` guard: `crates/agent/src/runner.rs:459-464`, `crates/core/src/resume.rs:82-85` |
| `SSend`, `SAck`, `SRefused`, `SLost` | `snapshot` and `write_snapshot`: `crates/agent/src/runner.rs:468-577`. `SSend` is the write that holds the manifest; chunk records written before it are inert until a manifest names them, so the model leaves them out |
| `Sup` | `Outcome::Superseded` and the confirm: `crates/agent/src/agent.rs:814-832` |
| `LocalFault` | the `fail-workload` test fault, or any trap only this node hits: `crates/agent/src/agent.rs:765-767` |
| `ConfirmFail` | `mark_failed` up to the confirm: `crates/agent/src/agent.rs:969-1023`; the `Confirmer`: `crates/agent/src/lease.rs:258-271` |
| `FailSend`, `FailAck`, `FailRetry` | `record_failure` and `record_needs_operator` over `append_last`: `crates/agent/src/output.rs:56-197`; the records are `encode_failure` and `encode_needs_operator`: `crates/core/src/output.rs:272-297`. A retry reads the tail again in `append_last`'s loop, or confirms again in `mark_failed`'s or `record_needs_operator`'s once that loop gives up |
| `NeedsOperator` | a problem found before the claim, which the node then claims only to record (`before_claim` and `own`: `crates/agent/src/agent.rs:405-571`), or once the claim is on the stream, when the spec is checked again or a restore runs out of fuel (`Outcome::NeedsOperator`); recorded by `record_needs_operator`: `crates/agent/src/agent.rs:895-960` |
| `Park` | the lease released and the task left waiting for a spec change: `own`, `Ended::NeedsOperator`. A needs-operator record found by `check_found` (`crates/agent/src/output.rs:345`) or before the claim (`recorded_needs_operator`: `crates/agent/src/agent.rs:573`) leads here |
| `Derive` | `write_failed`: `crates/agent/src/agent.rs:1053-1089`; after a record found by `check_found` or `reconcile`, `Outcome::FailedOnStream`: `crates/agent/src/agent.rs:803-813` |
| `WriteFail` | the spec written straight after the confirm, as before #70; only with `FailRecord` off |
| `Renew` | `hold` and `renew`: `crates/agent/src/lease.rs:276-356` |
| `LocalDeadline`, `StopLocal` | `Fence::holds` and the 2/3 TTL stop: `crates/agent/src/lease.rs:125-159`, `crates/core/src/lease.rs:87-93` |
| `ClockFault` | a suspend (the monotonic clock stops) or the `leases` leader's clock jumping: design.md, Leases and failure detection |
| `Expire` | the broker's TTL expiry (Felix `docs/cache-on-log.md`, TTL) |
| `Deliver`, `Timeout` | `publish_if` landing at any later time, and an answer that never comes; the `drop-*` and `lose-*-answer` test faults: `crates/agent/src/faults.rs` |
| `DeliverPrefix` | a `Quorum` leader change that keeps part of a batch and answers with an error (Felix `docs/atomic-commit.md`, What atomic covers); the `append-prefix` test fault: `crates/agent/src/faults.rs` |
| `GenStart` | Felix's generation-start record on a new shard leader |
| `Crash`, `Restart` | an agent killed and started again under the same node id |
| `Recreate` | the `epochs` cache recreated |

### The claim record, as modelled

The model follows `OutputLog::claim` as merged in #80:

- The claim record is written at the `.out` tail read with the epoch floor, not at a fresh tail.
  A fresh tail would let a node frozen between taking its epoch and writing its claim land the
  claim after a later owner's.
- On refusal or a lost answer, the owner reads past its expected tail: a record from a later
  epoch means superseded, its own claim means an earlier attempt landed, and anything else means
  it claims again at the new tail.
- The claim comes before the snapshot is read, and the resume works below the tail after the
  claim.
- A resume after an error does not claim again (`Probe`). It reads the tail and stops if the last
  record of any kind, claims included, is from a later epoch.
- The resume's own superseded check and fold use the last output, passing over claims, and
  `Written::check` passes over claims when comparing skipped outputs.
- A failure record found last before the claim's tail, among the records past it, or at an
  owner's expected tail ends the run, and the owner writes `failed` into the spec (`Derive`).
- `.snap` gets no claim record. A stale owner's snapshot can still land after a takeover; the
  model shows that is safe under determinism (`SnapshotsSplit`, `EpochMonotone`).

The floor read and the counter read are one atomic step in the model (`TakeEpoch`); in the code
they are several reads after the claim has won.

## Abstractions

- **Time** is not a number. A holder's deadline passing is an action. The broker expires a lease
  only once its holder has stopped by its own clock, which is what the 1/3 TTL margin buys, unless a
  `ClockFault` has put them out of step. An applied renewal puts them back in step. Clock rate
  drift and node clock skew are inside that margin: no node compares its clock with another's.
- **Determinism** is assumed: an input's outputs are a fixed function of its offset. Payloads are
  the `(input offset, index)` pair. A rejection is an output, and an output's name and key are part
  of its payload.
- **Snapshot chunks** are not modelled. A snapshot is its manifest record; the chunk records written
  before it are read only through a manifest that names them.
- **Snapshots** are taken whenever an owner chooses, which covers `snapshot_every` and
  `snapshot_every_ms`.
- **Cache calls** are atomic and answered. Only stream appends are delayed, reordered, or left
  unanswered.

## Not modelled

- Lost answers to cache calls, and the renewal read-back (`crates/agent/src/lease.rs:341-355`).
- Cache reads after a cache leader change (Felix leaves them undefined), and `Leader` caches that
  lose acknowledged writes on failover.
- Spec versions: every failure record is taken to be for the spec that runs. The code passes
  over a failure record from an older spec version, which only a resubmit makes.
- The later epoch a holder that finds epochs went backwards puts on its failure record: in the
  model a holder never finds a later epoch while it still holds the lease, so that record is never
  written. The epoch floor reads it like any other record, so later claims stay above it.
- Stale cache reads of the spec version. The code reads again when it sees a version below the
  record's.
- Spec edits and spec records: the agent's refusal of a spec whose module or input differ from
  the first claim's, and of a spec record below the last claim's floor, the highest spec record
  claimed so far, which every claim carries forward, a refused one included. Each is checked before the
  lease claim, where it writes nothing, and again after the owner's claim is on the stream, where
  that claim stays and the lease is released, like the 0.1-module check after a claim. Tracked in
  [#121](https://github.com/GetFelix/felixd/issues/121).
- Several inputs, pipelines, `felixd run`'s history check, `felixd check` itself, and drain.
- Placement, `max_workloads` and the claim delay: any idle node may claim.
- The retry limit in `OutputLog::append`.
- Nondeterministic workloads, and the instance-history effects of design review finding 1.
