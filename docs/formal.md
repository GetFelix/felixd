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
| `NoStaleFailure` | No node writes `failed` into the spec while the lease is not its own. |
| `NoPoison` | No node finds the streams impossible to resume from. The model is deterministic, so every such finding would fail a healthy workload. |
| `UniqueEpoch` | No two claims that run, or have records or requests in flight, share an epoch. |
| `SnapshotsSplit` | For each snapshot (X, T), the output records below offset T are of inputs up to X and the rest are of later inputs. |

Under weak fairness of every node's own steps, of delivery and of expiry, `FelixdLive` checks
`FreeIsClaimed` (a free, running workload is claimed) and `EventuallyDone` (every output is
written, or the workload failed).

## Configurations

No single configuration turns every mechanism on at full size: with two inputs, three claims, a
crash and a generation-start record together, the state space is too large to check in CI. The passing
configurations split those across smaller models, each checked exhaustively with node symmetry.
Each twin turns one mechanism off, or drops one assumption, and must find the named violation.
Every configuration has two nodes.

Distinct states and times are from TLC 1.7.4 with `-workers 4` on an Apple M-series laptop. The
whole script takes about a minute and a half there. A twin's count varies a little between runs,
since TLC stops at the first violation a worker finds.

| Configuration | Expected | Model | Distinct states | Time |
|---|---|---|---|---|
| `Felixd` | pass | two inputs, two claims, a crash, a clock fault, a snapshot | 1,498,109 | 19 s |
| `FelixdGen` | pass | one input, two claims, a generation-start record, a clock fault, a snapshot | 1,607,811 | 18 s |
| `FelixdThree` | pass | one input, three claims, a clock fault, a snapshot | 2,574,126 | 29 s |
| `FelixdIdle` | pass | an input with no output, then one with two | 429,893 | 5 s |
| `FelixdRecreate` | pass | `epochs` recreated once, while every live epoch is on a record | 402,510 | 5 s |
| `FelixdLive` | pass | no timing faults; liveness | 32,875 | 2 s |
| `FelixdNoMarker` | `NoInterleave` | the claim record off | about 5,000 | under 1 s |
| `FelixdNoFloor` | `EpochMonotone` | the epoch floor off | about 64,000 | under 1 s |
| `FelixdNoConfirm` | `NoStaleFailure` | the `Confirmer` off | about 2,000 | under 1 s |
| `FelixdFailWindow` | `NoStaleFailure` | a clock fault allowed between an applied confirm and the failure write | about 3,000 | under 1 s |
| `FelixdNoReplayGuard` | `OutputsExact` | the `replaying` guard on snapshots off | about 59,000 | under 1 s |
| `FelixdNoReplayGuardSplit` | `SnapshotsSplit` | the same | about 24,000 | under 1 s |
| `FelixdRecreateAnytime` | `UniqueEpoch` | `epochs` recreated while an epoch is on no record | about 1,500 | under 1 s |
| `FelixdTailFold` | `NoPoison` | the fold on event offsets off (#67) | 20 | under 1 s |
| `FelixdNoLaterEpochWins` | `EpochMonotone` | reconcile's later-epoch rule off | about 20,000 | under 1 s |

`FelixdFull` has everything at once. The `TLA nightly` workflow walks three million random
behaviours of it with `TLC_SIMULATE`, which takes about five minutes on the laptop above. A
simulation can miss a violation that exhaustive checking would find, so it backs the passing
configurations rather than replacing them.

`FelixdNoMarker` is the fence before #80, and its trace is the gap the claim record closes: a stale
owner's append lands after the new owner has resumed, when the new owner has not written yet. Determinism makes
the content right, so `OutputsExact` still holds there; `NoInterleave` is what a merge log or an
effect stream would need.

`FelixdFailWindow` pins a gap PR #68 names: the confirm and the spec write are two calls, and a
suspend or a broker clock jump between them lets a node fail a workload another node now holds.
Closing it needs the failure write fenced by epoch (#70).

`FelixdRecreateAnytime` pins the one assumption design.md states for epochs.

## How actions map to the code

Paths are from the repository root, as of the claim record (#80).

| Action | Code |
|---|---|
| `Claim` | `cache_put_if` on absent with the TTL: `crates/agent/src/lease.rs:180-193`; the agent's retry loop: `crates/agent/src/agent.rs:398-440` |
| `TakeEpoch` | the floor, `written_epoch` over the last records of `.out` and `.snap`: `crates/agent/src/agent.rs:536-557`; the counter read and `next_epoch`: `crates/agent/src/lease.rs:212-220`, `crates/core/src/lease.rs:53-55` |
| `PutEpoch` | `put_epoch_if` at the version read: `crates/agent/src/lease.rs:221-226`; a refusal releases the claim: `crates/agent/src/lease.rs:194-198` |
| `PutLease` | the lease rewritten with the epoch at the claim's version: `crates/agent/src/lease.rs:227-248` |
| `Marker`, `MarkerAck`, `MarkerRetry` | `OutputLog::claim` with the tail read with the floor: `crates/agent/src/output.rs:42-114`, `crates/agent/src/agent.rs:410-419`; the record is `encode_claim`: `crates/core/src/output.rs:82-87` |
| `Probe` | `OutputLog::claim` once the claim has landed: the tail and the last record's epoch: `crates/agent/src/output.rs:48-57` |
| `ReadSnap` | the last snapshot and the superseded check: `crates/agent/src/runner.rs:234-250` |
| `ReadOut` | the last output passing over claims, the superseded check and `resume::fold`: `crates/agent/src/runner.rs:252-270`, `crates/agent/src/runner.rs:451-473`, `crates/core/src/resume.rs:104-137` |
| `Step` | `Steps::step`, with `already_written` and `Written::check`: `crates/agent/src/runner.rs:347-378`, `crates/core/src/resume.rs:70-77`, `crates/agent/src/runner.rs:484-503` |
| `OSend`, `OAck` | `OutputLog::append`, with the fence check before each write: `crates/agent/src/output.rs:133-169` |
| `Reconcile` | `skip_written` and `output::reconcile`: `crates/agent/src/output.rs:171-210`, `crates/core/src/output.rs:199-233` |
| `Continue`, `SnapSend` | the snapshot cadence and the `replaying` guard: `crates/agent/src/runner.rs:379-384`, `crates/core/src/resume.rs:82-85` |
| `SSend`, `SAck`, `SRefused`, `SLost` | `snapshot` and `write_snapshot`: `crates/agent/src/runner.rs:386-447` |
| `Sup` | `Outcome::Superseded` and the confirm: `crates/agent/src/agent.rs:618-631` |
| `LocalFault` | the `fail-workload` test fault, or any trap only this node hits: `crates/agent/src/agent.rs:595-597` |
| `ConfirmFail`, `WriteFail` | `mark_failed`: `crates/agent/src/agent.rs:674-722`; the `Confirmer`: `crates/agent/src/lease.rs:258-271` |
| `Renew` | `hold` and `renew`: `crates/agent/src/lease.rs:276-356` |
| `LocalDeadline`, `StopLocal` | `Fence::holds` and the 2/3 TTL stop: `crates/agent/src/lease.rs:125-159`, `crates/core/src/lease.rs:87-93` |
| `ClockFault` | a suspend (the monotonic clock stops) or the `leases` leader's clock jumping: design.md, Leases and failure detection |
| `Expire` | the broker's TTL expiry (Felix `docs/cache-on-log.md`, TTL) |
| `Deliver`, `Timeout` | `publish_if` landing at any later time, and an answer that never comes |
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
  the `(input offset, index)` pair.
- **Snapshots** are taken whenever an owner chooses, which covers every `snapshot_every`.
- **Cache calls** are atomic and answered. Only stream appends are delayed, reordered, or left
  unanswered.

## Not modelled

- Lost answers to cache calls, and the renewal read-back (`crates/agent/src/lease.rs:341-355`).
- Cache reads after a cache leader change (Felix leaves them undefined), and `Leader` caches that
  lose acknowledged writes on failover.
- Several inputs, pipelines, spec edits, `felixd run`'s history check, `felixd check` itself, and
  drain.
- Placement, `max_workloads` and the claim delay: any idle node may claim.
- Batches larger than one write, and the retry limit in `OutputLog::append`.
- Nondeterministic workloads, and the instance-history effects of design review finding 1.
