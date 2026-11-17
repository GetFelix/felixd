# The formal model

`specs/Felixd.tla` models one workload: nodes that claim it through the `leases` cache, take an
epoch from `epochs`, and fold its input into `wl.<id>.out` and `wl.<id>.snap` with
`publish_if`. TLC checks it under each `.cfg` beside it. `scripts/check_tla.sh` runs them all
and holds each to its registered outcome; the `tla` job in CI runs the script, the `tla-trim` job its trimming configurations ([Trimming](#trimming)), and the `tla-intensity` job its restart intensity ones ([Restart intensity](#restart-intensity)).
`specs/FelixdSpecs.tla` models spec history on the same stream, and the `tla-specs` job checks
it ([Spec history](#spec-history)).
`specs/FelixdMerge.tla` models the merge log of a workload with several inputs, and the
`tla-merge` job checks it ([Several inputs](#several-inputs)).
`specs/FelixdAdmission.tla` models admission's projection and the gate a spec record passes
before it runs, and the `tla-admission` job checks it ([Admission](#admission)).
`specs/FelixdPlacement.tla` models placement groups, and the `tla-placement` job checks it
([Placement groups](#placement-groups)).
`Felixd.tla` and `FelixdMerge.tla` also model trimming and the deletion it allows
([Trimming](#trimming)).

```bash
scripts/check_tla.sh                 # every configuration (needs Java 11+)
scripts/check_tla.sh FelixdNoMarker  # one
TLC_SIMULATE=100000 scripts/check_tla.sh FelixdFull  # random behaviours, not every state
TLA_MODULE=FelixdSpecs scripts/check_tla.sh          # the spec history model
TLA_MODULE=FelixdMerge scripts/check_tla.sh          # the merge log model
TLA_MODULE=FelixdAdmission scripts/check_tla.sh      # the admission model
TLA_MODULE=FelixdPlacement scripts/check_tla.sh      # the placement groups model
```

## What it checks

Each invariant is stated over what can be observed (`wl.scheduler.out`, the input log, the leases
and which nodes are up), not over the guards that are meant to keep it true:

| Invariant | Says |
|---|---|
| `GroupWhole` | Every `group` record names every member, or none (a withdrawal). |
| `GroupTogether` | Before the bound a group never runs split: one whole `group` record places every member that runs on an up node and was claimed before its claimer's bound, on the node that runs it. |
| `SplitNeverKept` | A record that replaces a whole record never places a member on the node the facts it answers show running it off the record it replaces. |
| `ReplaySame` | Once a holder has caught up with what was written, from whichever snapshot it replayed, the record in force on `wl.scheduler.out` is its own placement. |
| `StaleNeverApplied` | No agent claims a member on a `group` record whose epoch is lower than a claim of `wl.scheduler.out` before it. |

| Configuration | Expected | Model | Distinct states | Time |
|---|---|---|---|---|
| `FelixdPlacement` | pass | two nodes of room two, a group of two, three facts or lapses, a takeover with replay, a node crash | STATES_CRASH | TIME_CRASH |
| `FelixdPlacementLease` | pass | `FelixdPlacement` with a lease lost by a live holder instead of the node crash | STATES_LEASE | TIME_LEASE |
| `FelixdPlacementCrashLease` | pass | a node crash and a lease lost by a live holder, no takeover | STATES_CL | TIME_CL |
| `FelixdPlacementFull` | `GroupTogether` (nightly, random behaviours; #247) | three nodes of room one, live nodes judged silent, two takeovers, a bound of two steps | FULL_RESULT | FULL_TIME |
| `FelixdPlacementSilentHolder` | `GroupTogether` (#247) | a live node judged silent keeps its member while the group moves | STATES_SH | under 5 s |
| `FelixdPlacementNoBarrier` | `GroupTogether` | the agent claims a member without looking for its record | STATES_NB | under 5 s |
| `FelixdPlacementNoSplitGuard` | `SplitNeverKept` | the split check in `place_groups` off | STATES_NSG | under 5 s |
| `FelixdPlacementPerMember` | `GroupWhole` | a group written one record per member | STATES_PM | under 5 s |
| `FelixdPlacementNoFence` | `StaleNeverApplied` | a deposed holder's append not fenced, and an agent claims on it | STATES_NF | under 5 s |
| `FelixdPlacementLiveTakers` | `ReplaySame` | takers read from which nodes are up when the scheduler steps, not from recorded facts | STATES_LT | under 5 s |

The pass configurations start with the whole group admitted and leave three inputs for facts and
lapses, enough for a second group record: re-placing a member a dead node held. The
`tla-placement` job checks each pass configuration in its own leg, and the twins in another.

`FelixdPlacementNoSplitGuard` does not break `GroupTogether`: the record it writes keeps every
member where the facts show it running, so the members claimed after it agree with one record.
What it breaks is that a split made past the bound becomes the placement.

A known gap (#247): a live node judged silent by `sys.facts` time keeps the member it holds, since
nothing stops its agent, while `running()` skips silent nodes and `place_groups` moves the group
to nodes that are takers. The rest of the group is claimed there before the bound, and the group
runs split. `FelixdPlacementSilentHolder` pins it, and the nightly `FelixdPlacementFull`, which
also judges live nodes silent, is expected to find it until the fix lands.

What the model leaves out: ungrouped workloads and the `assign`, grace and bound paths of `turn`;
`table` records, which hold the same group placements as the records before them; the skip past a
trimmed range in `read_new` (`crates/agent/src/assignments.rs`), which for one group leaves the
record in force at an earlier offset until a later record or the table is folded, the same as an
agent whose view lags, which the model has; a member's spec
stopping, so a group never becomes incomplete again and is never withdrawn; two groups under one
name; the claim delay, which only orders claims; and the epoch fence and exactly-once of
`wl.scheduler.out`, which `Felixd.tla` checks.

For the scheduler, "running" is as its facts say: a member on a node the facts call silent does
not count, so the scheduler can place it again elsewhere while its old holder still has the lease.
No second holder can claim that member until its lease lapses, but the rest of its group can move
(#247). design.md's "no assignment moves a running member" holds only in the facts' sense.

The other promises in the placement rules (one authority per claim, status offsets, liveness when
agents disagree about the scheduler) are not modelled yet.

## Not modelled

- Lost answers to cache calls, and the renewal read-back (`renew` in `crates/agent/src/lease.rs`).
- A paused holder resuming. A holder past its deadline pauses its writes, and resumes only when a
  renewal at the held version is applied, which means the lease never lapsed and no other node
  can have claimed. That run is the model's run with no `LocalDeadline`, the holder taking no
  steps for a while, so it adds no behaviour.
- The claim limits and the backoff after a lost lease, which only delay claims.
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
  cover. [Placement groups](#placement-groups) models groups.
- The retry limit in `OutputLog::append`.
- Nondeterministic workloads, and the instance-history effects of design review finding 1.

- The head trim of `.out`, with the first claim's spec record carried in the trim record so the
  spec lineage survives it. Both are built, but `FelixdSpecs.tla` numbers its offsets from the
  start of the stream, so modelling a cut head means rewriting its first-claim and check
  functions on a base offset. It is covered by unit and integration tests
  (`crates/agent/src/specs/tests.rs`, `crates/felixd/tests/retention.rs`).
- Deleting a stream shared with other readers: an input, or a workload's `.out`, which goes only
  below the lowest floor of every reader. The models check one workload's floors, and treat that
  workload as the only reader. Whether the lowest floor across readers is right is covered by
  `felixd_core::readers` unit tests and `crates/felixd/tests/retention.rs`, not by TLC.

A degraded record is a fenced append that every reader passes over. It stops no node and
raises no floor, and the release after it is a holder stopping and deleting its lease, which the
models already cover as `Crash` followed at once by `Expire`. None has an action for it.
