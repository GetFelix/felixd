# The formal model

`specs/Felixd.tla` models one workload: nodes that claim it through the `leases` cache, take an
epoch from `epochs`, and fold its input into `wl.<id>.out` and `wl.<id>.snap` with
`publish_if`. TLC checks it under each `.cfg` beside it. `scripts/check_tla.sh` runs them all
and holds each to its registered outcome; the `tla` job in CI runs the script.

```bash
scripts/check_tla.sh                 # every configuration (needs Java 11+)
scripts/check_tla.sh FelixdNoMarker  # one
```

## What it checks

| Invariant | Says |
|---|---|
| `OutputsExact` | The output records, in stream order, are a prefix of the deterministic outputs of the inputs in order: no duplicate, no gap. |
| `EpochMonotone` | Epochs on `.out` and on `.snap` never decrease (generation-start records excepted). |
| `NoInterleave` | Once an owner has begun, nothing from an earlier epoch lands after that point on `.out`. An owner begins when its claim record lands, or without the marker, when it reads the tail to resume. |
| `NoStaleFailure` | No node writes `failed` into the spec while the lease is not its own. |
| `NoPoison` | No node finds the streams impossible to resume from. The model is deterministic, so every such finding would fail a healthy workload. |
| `UniqueEpoch` | No two claims that run, or have records or requests in flight, share an epoch. |
| `SnapshotsSplit` | For each snapshot (X, T), the output records below offset T are of inputs up to X and the rest are of later inputs. |

Under weak fairness of every node's own steps, of delivery and of expiry, `FelixdLive` checks
`FreeIsClaimed` (a free, running workload is claimed) and `EventuallyDone` (every output is
written, or the workload failed).

## Configurations

Each passing configuration is small enough to finish in minutes. Each twin turns one mechanism
off, or drops one assumption, and must find the named violation.

| Configuration | Expected | Turns off |
|---|---|---|
| `Felixd` | pass | nothing: two inputs, a crash, a clock fault, a generation-start record, a snapshot |
| `FelixdIdle` | pass | nothing: an input with no output, then one with two |
| `FelixdRecreate` | pass | nothing: `epochs` recreated once, while every live epoch is on a record |
| `FelixdLive` | pass | no timing faults; liveness |
| `FelixdNoMarker` | `NoInterleave` | the claim record |
| `FelixdNoFloor` | `EpochMonotone` | the epoch floor |
| `FelixdNoConfirm` | `NoStaleFailure` | the `Confirmer` |
| `FelixdFailWindow` | `NoStaleFailure` | the assumption that no clock fault falls between an applied confirm and the failure write |
| `FelixdNoReplayGuard` | `OutputsExact` | the `replaying` guard on snapshots |
| `FelixdNoReplayGuardSplit` | `SnapshotsSplit` | the same |
| `FelixdRecreateAnytime` | `UniqueEpoch` | the assumption that `epochs` is recreated only when every live epoch is on a record |
| `FelixdTailFold` | `NoPoison` | the fold on event offsets (#67) |
| `FelixdNoLaterEpochWins` | `EpochMonotone` | reconcile's later-epoch rule |

`FelixdNoMarker` is today's fence, and its trace is design.md's caveat: a stale owner's append
lands after the new owner has resumed, when the new owner has not written yet. Determinism makes
the content right, so `OutputsExact` still holds there; `NoInterleave` is what a merge log or an
effect stream would need.

`FelixdFailWindow` pins a gap PR #68 names: the confirm and the spec write are two calls, and a
suspend or a broker clock jump between them lets a node fail a workload another node now holds.
Closing it needs the failure write fenced by epoch (#70).

`FelixdRecreateAnytime` pins the one assumption design.md states for epochs.

## How actions map to the code

Paths are from the repository root, at the commit that added the model.

| Action | Code |
|---|---|
| `Claim` | `cache_put_if` on absent with the TTL: `crates/agent/src/lease.rs:179-192`; the agent's retry loop: `crates/agent/src/agent.rs:398-438` |
| `TakeEpoch` | the floor, `written_epoch` over the last records of `.out` and `.snap`: `crates/agent/src/agent.rs:532-546`; the counter read and `next_epoch`: `crates/agent/src/lease.rs:211-219`, `crates/core/src/lease.rs:53-55` |
| `PutEpoch` | `put_epoch_if` at the version read: `crates/agent/src/lease.rs:220-225`; a refusal releases the claim: `crates/agent/src/lease.rs:193-197` |
| `PutLease` | the lease rewritten with the epoch at the claim's version: `crates/agent/src/lease.rs:226-247` |
| `Marker`, `MarkerAck`, `MarkerRetry` | the claim record, being added in a parallel PR: `output::Record::Claim` and `encode_claim` in `crates/core/src/output.rs`; modelled as the intended design (below) |
| `ReadSnap` | the last snapshot and the superseded check: `crates/agent/src/runner.rs:228-245` |
| `ReadOut` | the last record, the superseded check and `resume::fold`: `crates/agent/src/runner.rs:246-275`, `crates/core/src/resume.rs:104-137` |
| `Step` | `Steps::step`, with `already_written` and `Written::check`: `crates/agent/src/runner.rs:348-374`, `crates/core/src/resume.rs:70-77`, `crates/agent/src/runner.rs:459-472` |
| `OSend`, `OAck` | `OutputLog::append`, with the fence check before each write: `crates/agent/src/output.rs:43-58` |
| `Reconcile` | `skip_written` and `output::reconcile`: `crates/agent/src/output.rs:81-120`, `crates/core/src/output.rs:92-116` |
| `Continue`, `SnapSend` | the snapshot cadence and the `replaying` guard: `crates/agent/src/runner.rs:376-380`, `crates/core/src/resume.rs:82-85` |
| `SSend`, `SAck`, `SRefused`, `SLost` | `snapshot` and `write_snapshot`: `crates/agent/src/runner.rs:384-445` |
| `Sup` | `Outcome::Superseded` and the confirm: `crates/agent/src/agent.rs:595-610` |
| `LocalFault` | the `fail-workload` test fault, or any trap only this node hits: `crates/agent/src/agent.rs:581-583` |
| `ConfirmFail`, `WriteFail` | `mark_failed`: `crates/agent/src/agent.rs:651-700`; the `Confirmer`: `crates/agent/src/lease.rs:257-270` |
| `Renew` | `hold` and `renew`: `crates/agent/src/lease.rs:275-355` |
| `LocalDeadline`, `StopLocal` | `Fence::holds` and the 2/3 TTL stop: `crates/agent/src/lease.rs:124-158`, `crates/core/src/lease.rs:87-93` |
| `ClockFault` | a suspend (the monotonic clock stops) or the `leases` leader's clock jumping: design.md, Leases and failure detection |
| `Expire` | the broker's TTL expiry (Felix `docs/cache-on-log.md`, TTL) |
| `Deliver`, `Timeout` | `publish_if` landing at any later time, and an answer that never comes |
| `GenStart` | Felix's generation-start record on a new shard leader |
| `Crash`, `Restart` | an agent killed and started again under the same node id |
| `Recreate` | the `epochs` cache recreated |

### The claim record, as modelled

The model takes these from the parallel PR's design, and they are worth checking against it:

- The claim record is written at the `.out` tail read with the epoch floor, not at a tail probed
  afresh. A probe would let a node frozen between taking its epoch and writing the marker land
  its marker after a later owner's.
- On refusal or a lost answer, the owner reads past its expected tail: a record from a later
  epoch means superseded; its own claim record means it landed; otherwise it writes again at the
  new tail.
- The resume's superseded check reads the last record of any kind, claims included.
- `Written::check` passes over claim records when comparing skipped outputs. As
  `output::same_output` treats a claim as nothing, a check that did not skip them would fail
  every resume past a claim record.
- `.snap` gets no claim record. A stale owner's snapshot can still land after a takeover; the
  model shows that is safe for snapshots under determinism (`SnapshotsSplit`, `EpochMonotone`).

## Abstractions

- **Time** is not a number. A holder's deadline passing is an action. The broker expires a lease
  only once its holder has stopped by its own clock, which is what the 1/3 TTL margin buys, unless a
  `ClockFault` has put them out of step. An applied renewal puts them back in step. Clock rate
  drift and node clock skew are inside that margin: no node compares its clock with another's.
- **Determinism** is assumed: an input's outputs are a fixed function of its offset. Payloads are
  the `(input offset, index)` pair.
- **Snapshots** are taken whenever an owner chooses, which covers every `snapshot_every`.
- **Cache calls** are atomic and answered. Only stream appends are delayed, reordered, or left
  unanswered.

## Not modelled

- Lost answers to cache calls, and the renewal read-back (`crates/agent/src/lease.rs:340-354`).
- Cache reads after a cache leader change (Felix leaves them undefined), and `Leader` caches that
  lose acknowledged writes on failover.
- Several inputs, pipelines, spec edits, `felixd run`'s history check, `felixd check` itself, and
  drain.
- Placement, `max_workloads` and the claim delay: any idle node may claim.
- Batches larger than one write, and the retry limit in `OutputLog::append`.
- Nondeterministic workloads, and the instance-history effects of design review finding 1.
