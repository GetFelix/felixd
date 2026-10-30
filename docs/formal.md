# The formal model

`specs/Felixd.tla` models one workload: nodes that claim it through the `leases` cache, take an
epoch from `epochs`, and fold its input into `wl.<id>.out` and `wl.<id>.snap` with
`publish_if`. TLC checks it under each `.cfg` beside it. `scripts/check_tla.sh` runs them all
and holds each to its registered outcome; the `tla` job in CI runs the script.
`specs/FelixdSpecs.tla` models spec history on the same stream, and the `tla-specs` job checks
it ([Spec history](#spec-history)).
`specs/FelixdMerge.tla` models the merge log of a workload with several inputs, and the
`tla-merge` job checks it ([Several inputs](#several-inputs)).
`specs/FelixdAdmission.tla` models admission's projection and the gate a spec record passes
before it runs, and the `tla-admission` job checks it ([Admission](#admission)).

```bash
scripts/check_tla.sh                 # every configuration (needs Java 11+)
scripts/check_tla.sh FelixdNoMarker  # one
TLC_SIMULATE=100000 scripts/check_tla.sh FelixdFull  # random behaviours, not every state
TLA_MODULE=FelixdSpecs scripts/check_tla.sh          # the spec history model
TLA_MODULE=FelixdMerge scripts/check_tla.sh          # the merge log model
TLA_MODULE=FelixdAdmission scripts/check_tla.sh      # the admission model
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

Distinct states and times are from TLC 1.7.4 with `-workers 4` on the CI runner (`ubuntu-latest`),
where the whole script takes about 21 minutes. A twin's count varies a little between runs,
since TLC stops at the first violation a worker finds.

| Configuration | Expected | Model | Distinct states | Time |
|---|---|---|---|---|
| `Felixd` | pass | two inputs, two claims, a crash, a clock fault, a snapshot | 6,446,504 | 2 min 48 s |
| `FelixdGen` | pass | one input, two claims, a generation-start record, a clock fault, a snapshot | 9,807,531 | 3 min 46 s |
| `FelixdThree` | pass | one input, three claims, a clock fault, a snapshot | 20,207,128 | 7 min 47 s |
| `FelixdIdle` | pass | an input with no output, then one with two | 2,161,306 | 52 s |
| `FelixdRecreate` | pass | `epochs` recreated once, while every live epoch is on a record | 2,356,734 | 56 s |
| `FelixdFailWindow` | pass | one input, two claims, a clock fault anywhere in the failure path | 395,543 | 10 s |
| `FelixdLive` | pass | no timing faults, local faults; liveness | 318,874 | 47 s |
| `FelixdPartial` | pass | an input with no output, then a batch of two that may land in part, a crash, a clock fault | 9,489,712 | 4 min 5 s |
| `FelixdNoMarker` | `NoInterleave` | the claim record off | about 9,000 | 1 s |
| `FelixdNoFloor` | `EpochMonotone` | the epoch floor off | about 43,000 | 2 s |
| `FelixdNoConfirm` | `NoStaleFailure` | the `Confirmer` and the failure record off | about 2,000 | under 1 s |
| `FelixdNoFailRecord` | `NoStaleFailure` | the failure record off, with a clock fault allowed between an applied confirm and the spec write | about 3,000 | under 1 s |
| `FelixdNoReplayGuard` | `OutputsExact` | the `replaying` guard on snapshots off | about 60,000 | 2 s |
| `FelixdNoReplayGuardSplit` | `SnapshotsSplit` | the same | about 26,000 | 2 s |
| `FelixdRecreateAnytime` | `UniqueEpoch` | `epochs` recreated while an epoch is on no record | about 1,500 | under 1 s |
| `FelixdTailFold` | `NoPoison` | the fold on event offsets off (#67) | 20 | under 1 s |
| `FelixdNoLaterEpochWins` | `EpochMonotone` | reconcile's later-epoch rule off | about 21,000 | 2 s |
| `FelixdPartialNoIndex` | `NoPoison` | the fold skipping whole inputs, not outputs by index | about 700,000 | 14 s |

`FelixdFull` has everything at once, with a second input whose batch of two may land in part. The `TLA nightly` workflow walks three million random
behaviours of it with `TLC_SIMULATE`, which takes about five minutes on an Apple M-series laptop. A
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
| `Reconcile` | `skip_written` and `output::reconcile`: `crates/agent/src/output.rs:438-489`, `crates/core/src/output.rs:520-569` |
| `Continue`, `SnapSend` | the snapshot cadence and the `replaying` guard: `crates/agent/src/runner.rs:459-464`, `crates/core/src/resume.rs:82-85` |
| `SSend`, `SAck`, `SRefused`, `SLost` | `snapshot` and `write_snapshot`: `crates/agent/src/runner.rs:468-577`. `SSend` is the write that holds the manifest; chunk records written before it are inert until a manifest names them, so the model leaves them out |
| `Sup` | `Outcome::Superseded` and the confirm: `crates/agent/src/agent.rs:814-832` |
| `LocalFault` | the `fail-workload` test fault, or any trap only this node hits: `crates/agent/src/agent.rs:765-767` |
| `ConfirmFail` | `mark_failed` up to the confirm: `crates/agent/src/agent.rs:969-1023`; the `Confirmer`: `crates/agent/src/lease.rs:258-271` |
| `FailSend`, `FailAck`, `FailRetry` | `record_failure` and `record_needs_operator` over `append_last`: `crates/agent/src/output.rs:56-197`; the records are `encode_failure` and `encode_needs_operator`: `crates/core/src/output.rs:272-297`. A retry reads the tail again in `append_last`'s loop, or confirms again in `mark_failed`'s or `record_needs_operator`'s once that loop gives up |
| `NeedsOperator` | a problem found before the claim, which the node then claims only to record (`before_claim` and `own`: `crates/agent/src/agent.rs:405-571`), or once the claim is on the stream, when the spec is checked again, a restore runs out of fuel, or the input holds a record kind this build does not read or ends before the inputs already stepped (`Outcome::NeedsOperator`); recorded by `record_needs_operator`: `crates/agent/src/agent.rs:895-960` |
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

## Spec history

`specs/FelixdSpecs.tla` models which spec each owner runs. Spec records are numbered in
`sys.specs` order, and each has a module and input. A node reads the spec through its own
`workloads` view, which moves only forward and may lag, so a stale node holds an older spec.
Three kinds of record land on `.out`: claims, which carry the spec their owner runs and the
floor, the highest spec claimed so far including its own; needs-operator records, which name
their writer's spec; and outputs. Leases and epochs are reduced to the claim's fence, which
`Felixd.tla` checks: a claim lands only at the tail its owner read, and an owner writes only
while its claim is the last claim or needs-operator record. Any node may begin a claim at any
time, as if the lease had expired.

A node checks the spec before the lease claim (`refuse_stale_or_foreign`) and writes nothing
if it is stale. A spec whose module or input differ from the first claim's is recorded as
needing an operator, as is any other problem found before the claim. Otherwise it claims at the
tail it read, carrying the floor below that tail. A refused claim reads the records past the
tail, raises its floor from their claims and needs-operator records, and claims again. Once the
claim is on, the node checks again and either runs or leaves the claim there and stops. The
check passes over needs-operator records when it finds the first claim, and counts them toward
the floor.

| Invariant | Says |
|---|---|
| `OutputLineage` | Every output was written under the first claim's module and input. |
| `FloorMonotone` | Claim floors never decrease along the stream. |
| `RanMonotone` | The specs of claims whose owner ran them never decrease along the stream. |
| `NoRunBelowFloor` | No owner runs, and no output is written under, a spec below the highest spec claimed or recorded before its claim. |
| `CheckMatchesRun` | `felixd check` (`specs_applied` and `judged_under`) replays each output under the module, input and spec its writer ran, counts every claim that ran, and passes over every claim its owner refused for a reason on the stream at or before it. |

A claim refused only for a record that landed after it, or whose owner stopped before checking
it, is left out of `CheckMatchesRun`: the stream cannot say whether it ran, and such a claim
never governs an output, since the next claim that runs replaces it. It can still decide the
limits `felixd check` replays the inputs after the last output under.

| Configuration | Expected | Model | Distinct states | Time |
|---|---|---|---|---|
| `FelixdSpecs` | pass | two nodes, an edit and then a module change, three claims, a needs-operator record, two inputs | 2,450,721 | 53 s |
| `FelixdSpecsStale` | pass | three nodes, an edit and then an input change, three claims, one input | 7,286,048 | 2 min 50 s |
| `FelixdSpecsNoFloorCarry` | `RanMonotone` | a claim carries only its own spec | about 3,400 | under 3 s |
| `FelixdSpecsNoRetryFloor` | `FloorMonotone` | a refused claim does not raise its floor from the records past the tail | about 1,900 | under 3 s |
| `FelixdSpecsNoNopFloor` | `NoRunBelowFloor` | needs-operator records do not count toward the floor (#127, second defect) | about 1,300 | under 3 s |
| `FelixdSpecsNopFirst` | `OutputLineage` | the first claim is the first record, needs-operator records included (#127, first defect) | about 11,000 | under 3 s |
| `FelixdSpecsNoHistory` | `OutputLineage` | no refusal of a changed module or input | about 4,300 | under 3 s |
| `FelixdSpecsNoPostCheck` | `RanMonotone` | the refusals only before the lease claim | about 2,600 | under 3 s |
| `FelixdSpecsCheckNopFirst` | `CheckMatchesRun` | `felixd check` takes the first record as the first claim | about 1,800 | under 3 s |
| `FelixdSpecsCheckNoNopFloor` | `CheckMatchesRun` | `felixd check` does not raise the floor on needs-operator records | about 1,300 | under 3 s |
| `FelixdSpecsCheckNoRefusal` | `CheckMatchesRun` | `felixd check` counts a claim below the floor before it | about 1,300 | under 3 s |
| `FelixdSpecsCheckNoLineage` | `CheckMatchesRun` | `felixd check` counts a claim with another module or input | about 2,100 | under 3 s |

The twins have two nodes, two claims, a needs-operator record and two inputs. Counts and times
are from the `tla-specs` job on the CI runner. `FelixdSpecsFull` has three nodes, four claims,
a needs-operator record and two inputs; it passed 13.8 million distinct states in six minutes
without finishing, so the `TLA nightly` workflow walks it with `TLC_SIMULATE` instead.

| Action | Code |
|---|---|
| `Learn` | a node's `workloads` read moving forward: `specs::project` and `moves_forward` |
| `Begin` | `refuse_stale_or_foreign` before the lease claim, and the pending problem: `crates/agent/src/agent.rs` |
| `Start`, `ClaimLand` | `OutputLog::claim`: the floor from `specs::claim_floor` and the records past the tail: `crates/agent/src/output.rs`; `encode_claim_under`: `crates/core/src/output.rs` |
| `Recheck` | `refuse_stale_or_foreign` once the claim is on: `crates/agent/src/runner.rs` |
| `Record` | `record_needs_operator`: `crates/agent/src/agent.rs` |
| `Emit` | `OutputLog::append`, fenced by the claim |
| `Stop` | a crash, a lost lease, or a superseded owner |

Not in this model: failures, snapshots, epochs (`Felixd.tla`), inputs with no output, and
`felixd run`'s own history check, which the agent's refusal backs.

## Several inputs

`specs/FelixdMerge.tla` models a workload with several inputs: the merge log, `wl.<id>.merge`,
and the outputs stepped from it on `.out`. An input record is its input and offset, and its
output is a function of it. A step's number is its entry's offset on the merge log, and its
output names it.

A holder takes an epoch above the counter and the last `.out` record, and claims `.out` at the
tail it read with that floor. Only then does it claim the merge log, at a tail it reads then: it
stops if the last record there is from a later epoch, and does not claim again over a record of
its own epoch. It resumes from the last output. It steps the entries from there up to its claim,
which covers entries an earlier holder merged and never stepped, and then merges records from
the inputs in any order, up to `PerWrite` in one append, stepping each batch once it is on the
log. Every append to either stream is `publish_if` at the tail its holder expects, stamped with
its epoch. A refused append, or one whose answer was lost, reads the records past the tail: its
own are taken as written and a later epoch stops it. Anything else, or a lost answer after the
retries, ends the run, and the holder starts again under the same lease without claiming `.out`.

As in `FelixdSpecs.tla`, leases are reduced to the fence: any idle node may begin at any time, as
if the lease had expired, and a stale holder keeps running until a later record stops it.
Requests may land late, after their sender stopped waiting, or not at all, and a batch of
entries may land in part. A holder may crash at any point, including between a merge append and
its steps. The `epochs` cache may be recreated while every epoch a holder or a request in flight
could still write is on `.out`, the assumption design.md states.

| Invariant | Says |
|---|---|
| `MergeExact` | Every record of every input is on the merge log at most once and in its input's order, and each record's `next` follows from the entries up to it. |
| `MergeEpochMonotone` | Epochs on the merge log never decrease, and nothing from an earlier epoch lands after a later holder began there. |
| `EntriesClaimed` | Every entry follows a claim of its own epoch, with no other claim between (`felixd check`'s `MergeUnclaimed`). |
| `OutputsFenced` | The same as `MergeEpochMonotone`, for `.out`. |
| `OneWriter` | One holder writes under each epoch, on both streams. |
| `OutputsFollowMerge` | The outputs on `.out` are a prefix of the outputs of the merge log's entries, in log order: no gap, no duplicate. |
| `StepsFollowMerge` | Each output names the offset of the entry it was stepped from, in increasing order, so a replay of the log steps the same records in the same order. |

| Configuration | Expected | Model | Distinct states | Time |
|---|---|---|---|---|
| `FelixdMerge` | pass | two inputs (two records and one), three holders, a crash mid-merge, a lost answer, batches of up to two | 7,212,850 | 5 min 56 s |
| `FelixdMergeLost` | pass | two inputs (two records and one), two holders, two lost answers or timeouts, a batch of two that lands in part | 2,814,975 | 1 min 59 s |
| `FelixdMergeRecreate` | pass | one record on each input, three holders, a crash, `epochs` recreated once | 157,557 | 7 s |
| `FelixdMergeNoSkip` | pass | the same-epoch skip off, a crash and a lost answer | 91,744 | 4 s |
| `FelixdMergeNoClaimExact` | pass | no merge claim, a crash and a lost answer; checks only `MergeExact`, `OneWriter`, `OutputsFollowMerge` and `StepsFollowMerge` | 65,121 | 3 s |
| `FelixdMergeNoClaim` | `EntriesClaimed` | no merge claim: the first entry follows no claim of its epoch | about 80 | under 3 s |
| `FelixdMergeNoClaimFence` | `MergeEpochMonotone` | no merge claim: a stale holder appends after the new holder read the tail | about 250 | under 3 s |
| `FelixdMergeNoClaimCheck` | `MergeEpochMonotone` | the claim does not check the last record: a holder frozen after its `.out` claim claims the merge log after a later holder | about 350 | under 3 s |
| `FelixdMergeNoCatchUp` | `OutputsFollowMerge` | the entries before the claim not stepped: a holder that crashed between its merge append and its steps leaves a gap | about 2,300 | under 3 s |
| `FelixdMergeBeforeOut` | `OneWriter` | the merge log claimed before `.out`, with `epochs` recreated | about 250 | under 3 s |
| `FelixdMergeNoSameBytes` | `StepsFollowMerge` | reconcile takes any record of its own epoch past the tail as the entry it sent | about 240 | under 3 s |

Every configuration has two nodes. Counts and times are from the `tla-merge` job on the CI
runner, where the whole script takes about eight and a half minutes. `FelixdMergeFull` has two records on each input, four holders, two crashes, two lost
answers, a partial batch and a recreated `epochs`; the `TLA nightly` workflow walks it with
`TLC_SIMULATE`.

`FelixdMergeBeforeOut` is why the merge claim waits for the `.out` claim. The epoch floor reads
`.out`, not the merge log, so a holder that claims the merge log and crashes before its `.out`
claim leaves an epoch the floor does not see. Once `epochs` is recreated, the next holder takes
the same epoch, finds that claim last, takes it as its own, and writes under it too. Claiming
`.out` first keeps the merge log's highest epoch at or below `.out`'s.

`FelixdMergeNoSkip` passes: claiming again over a claim of the holder's own epoch only adds a
record, so the skip is not a guard.

The merge claim does not keep the log or the outputs exact: `FelixdMergeNoClaimExact` passes
without it, because a stale holder's entry that lands after the new holder read the tail makes
the new holder's next append fail as `Diverged`, and it reads the log again. What the claim buys
is that this never happens: from the claim on, an earlier holder's append is refused, so a
holder past its claim never retries for that reason, and `felixd check` can require every entry
to follow its own epoch's claim (`MergeUnclaimed`, `EntriesClaimed`). The two `NoClaim` twins
show those two effects.

A holder steps the entries it sent at the offsets it believes they landed at, not what it reads
back, so `StepsFollowMerge` checks reconcile. `FelixdMergeNoSameBytes` is a holder whose append
timed out and that ended the run, merged another record at the same tail, and found the old
append there first: comparing epochs alone, it steps the new record at the old one's offset.

| Action | Code |
|---|---|
| `Begin` | the lease claim, the epoch floor and the `.out` tail: `crates/agent/src/agent.rs`, `crates/agent/src/lease.rs` |
| `OClaim`, `OClaimAns` | `OutputLog::claim`: `crates/agent/src/output.rs` |
| `MRead`, `MClaim`, `MClaimAns` | `MergeLog::claim`: `crates/agent/src/merge.rs`; the record is `merge::encode_claim`: `crates/core/src/merge.rs` |
| `Probe` | a run that ended in an error starting again under the same lease: `crates/agent/src/agent.rs` |
| `Resume` | the last output, `resume::fold` and the catch-up loop in `merged`: `crates/agent/src/runner.rs` |
| `Merge`, `MAns` | `merged`, `merge_and_step` and `MergeLog::append` with `merge::reconcile`: `crates/agent/src/runner.rs`, `crates/agent/src/merge.rs`, `crates/core/src/merge.rs` |
| `OSend`, `OAns` | `Steps::step` and `OutputLog::append`: `crates/agent/src/runner.rs`, `crates/agent/src/output.rs` |
| `Deliver`, `LoseAnswer`, `Timeout`, `DeliverPrefix` | `publish_if` landing late or not at all, and the `drop-*`, `lose-*-answer` and `append-prefix` test faults: `crates/agent/src/faults.rs` |
| `Crash` | an agent killed, or `crash-after-merge` |
| `Recreate` | the `epochs` cache recreated |

Not in this model: snapshots, failure and needs-operator records, generation-start records,
records a step passes over (every entry has one output here), and the gap read-back, which the
model covers by letting a holder merge any input's next record.

## Admission

`specs/FelixdAdmission.tla` models admission (#114): the decisions on `wl.admission.out`, the
projector that copies the accepted ones to `sys.specs`, and the gate a node passes a spec record
through before it runs it. Admission's decisions are outputs of an ordinary workload, so their
exactly-once and fencing are `Felixd.tla`'s; here a decision lands in one step.

The rule it checks. Once `felixd init`'s marker exists, a spec record for a workload runs only
if the decision it names is on `wl.admission.out`, accepted that workload and exactly that spec,
and no verified record of the workload in the part of `sys.specs` the node read names a later
decision. A record is verified when its decision is all of those things. A request for a
reserved id (`admission`, `scheduler`) is rejected. The projector copies each accepted decision
with a conditional append at the tail it read, or appends a skip record for one whose module is
missing, and takes its progress from the latest decision any verified record names, so it keeps
nothing of its own across a restart or a failover.

The model starts after `felixd init`, with a record written before admission still on
`sys.specs` and `workloads` pointing at it. The marker is read fresh. An adversary can append
any spec record (any spec, no decision, any decision including one past the tail, or a copy of
an older record) and point `workloads` at any record. Projectors crash, and a stale one keeps
running beside a new one. Nodes crash, and read a prefix of `sys.specs` that only grows; a node
that read less after a restart is refused by the claim floor, which `FelixdSpecs.tla` checks.

| Invariant | Says |
|---|---|
| `OnlyAdmittedRuns` | After a check, a node runs the record `workloads` pointed at if it names the latest decision any verified record of the workload names in what the node read, and that decision is not a stop; otherwise it runs nothing. |
| `NoRollback` | A node never acts on a decision older than one it acted on, so a stop or a newer version is never undone. |
| `SkippedStaysSkipped` | No projector records a decision a projector passed over. |
| `ProjectedAtMostOnce` | Projectors record each decision at most once, in decision order. |
| `ReservedNeverViaIntake` | No request for a reserved id is accepted. |
| `LiveRuns` | With no adversary, every accepted request whose module is there is eventually acted on by a node, which runs it or something later (`FelixdAdmissionLive`). |

| Configuration | Expected | Model | Distinct states | Time |
|---|---|---|---|---|
| `FelixdAdmission` | pass | two projectors that crash and race, two decisions that may be rejected or stops, a forged record, two repoints | 6,827,780 | 1 min 26 s |
| `FelixdAdmissionSkip` | pass | missing modules uploaded late, a request for a reserved id, three decisions, two projectors that crash and race, a repoint | 4,277,794 | 55 s |
| `FelixdAdmissionForge` | pass | two forged records over three specs, a repoint, one projector, two decisions | 5,346,950 | 1 min 1 s |
| `FelixdAdmissionLive` | pass | `LiveRuns` with no adversary: two projectors, a crash, three decisions | 323,964 | 1 min 51 s |
| `FelixdAdmissionBeforeAt` | `OnlyAdmittedRuns` | the gate reads only the records before the one it checks, and is on only if admission's record is among them: the record written before admission runs | about 20 | under 3 s |
| `FelixdAdmissionForgedCount` | `OnlyAdmittedRuns` | every record naming a decision counts, verified or not: a forged record past the tail blocks the latest admitted record | about 650 | under 3 s |
| `FelixdAdmissionOldRecord` | `NoRollback` | a record is superseded by a later record, not a later decision: a record of an older decision that lands after a newer one runs after it | about 95,000 | under 3 s |
| `FelixdAdmissionRetrySkipped` | `SkippedStaysSkipped` | no skip records, and a projector walks the decisions from the first: a new projector records a decision an earlier one passed over, once its module is uploaded | about 150 | under 3 s |
| `FelixdAdmissionReservedIntake` | `ReservedNeverViaIntake` | admission accepts a reserved id | about 20 | under 3 s |
| `FelixdAdmissionNoCondWrite` | `ProjectedAtMostOnce` | projections are plain appends: two projectors record the same decision | about 70 | under 3 s |
| `FelixdAdmissionFoldMemo` | `NoRollback` | a node keeps its verdict on a record it read: a forged record naming a decision past the tail stays unverified after that decision lands, so after running it the node runs an older record | about 75,000 | under 3 s |

Every configuration has one node: nodes do not interact here, and each invariant is about one
node's checks. Counts and times are from the `tla-admission` job on the CI runner, where the whole script takes about six minutes.

`OnlyAdmittedRuns` restates the rule over the records and decisions the node read, so in the
passing configurations it checks that the gate is the rule and nothing else; what makes it hold
is the twins. The other invariants are about the projector and about time, which the rule alone
does not say.

The rule lets a copy of an accepted decision through wherever it lands, including one the
projector passed over: whoever can write `sys.specs` can record a decision whose module was
missing. It runs only if the module has since been uploaded under its hash, since nodes load
modules by hash.

| Action | Code |
|---|---|
| `Decide`, `DecideReserved` | `admission::decide`: `crates/core/src/admission.rs` |
| `Upload` | a module put in `modules` after its request was decided |
| `Start` | `Projector::start`: `crates/agent/src/admission.rs` |
| `Step` | `Projector::catch_up` and `record`, with `module_matches` |
| `Point` | `specs::project`, which only moves `workloads` forward: `crates/agent/src/specs.rs` |
| `CrashProjector`, `CrashNode` | a crash, a lost lease, or the `crash-after-projection-sent` fault |
| `Forge`, `Repoint` | a principal that can write `sys.specs` and `workloads` |
| `Check` | `check_admitted`, `Gate::fold` and `Gate::newest`: `crates/agent/src/admission.rs` |

Not in this model: the scheduler and other system workloads, which the gate lets through by id
alone; forged skip records, which only make the projector pass over decisions, a delay rather
than a safety problem; a writer of `wl.admission.out` other than admission (#74); trimmed
decisions; and the module check, reduced to whether the module is there.

## Pipelines

A pipeline needs no new model. `Felixd.tla` takes the input as a fixed sequence of records, each
read when an owner gets to it, so an input that arrives late looks like an owner that is slow. Every
invariant is about one workload's `.out` and `.snap` given that input. A downstream's input is an upstream's `.out`. Felix never rewrites a
record on it, and the upstream's fence makes its outputs land once and in order, which is what
`OutputsExact` and `EpochMonotone` check for the upstream. Which records a downstream steps is a
function of each record alone (`crates/agent/src/input.rs`), so a claim, even one that lands
between the halves of a batch, is an offset the downstream skips, like an offset that holds no
record. A two-stage model would be two copies of this one with the first's output fed to the
second, and would check nothing the one-stage model does not. `tests/pipeline.rs` tests the
composition.

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
- Spec edits and spec records in `Felixd.tla`. `FelixdSpecs.tla` models them
  ([Spec history](#spec-history)).
- Several inputs and the merge log in `Felixd.tla`. `FelixdMerge.tla` models them
  ([Several inputs](#several-inputs)).
- Admission in `Felixd.tla` and `FelixdSpecs.tla`, which take spec records as given.
  `FelixdAdmission.tla` models it ([Admission](#admission)).
- `felixd check` beyond which spec each input ran under.
- Drain as an action of its own. A drained holder stops and then deletes its lease, which the model
  already covers as `Crash` followed at once by `Expire`, with the holder's requests still in flight.
  Claiming nothing while drained is placement, which the model leaves out.
- Placement, `max_workloads`, the claim delay and the scheduler's assignments: any idle node may
  claim at any time. Assignments (#60) only narrow which node tries first and when, and every
  claim still goes through `Claim`, `TakeEpoch` and the marker, so they remove behaviours and add
  none, and the safety invariants carry over. For `FreeIsClaimed`, a node waits on an assignment
  only for at most `--assignment-bound-ms` from when it first saw the workload free, whatever the
  assignment does meanwhile, then claims as before, so a free workload is still claimed. The
  scheduler itself is a workload with two inputs, which `Felixd.tla` and `FelixdMerge.tla` already
  cover.
- The retry limit in `OutputLog::append`.
- Nondeterministic workloads, and the instance-history effects of design review finding 1.

No model has a trim action yet. A trim record is a fenced append that every reader passes over and
that deletes nothing, so it can break no property the models check; the action and its twins come
with deletion ([#151](https://github.com/GetFelix/felixd/issues/151)).
