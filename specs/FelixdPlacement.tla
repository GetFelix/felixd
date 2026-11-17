-------------------------- MODULE FelixdPlacement --------------------------
(***************************************************************************)
(* Placement groups (#100, #235): the scheduler places a group with one   *)
(* `group` record on `wl.scheduler.out` or not at all, and an agent holds *)
(* a free member for that record until the group bound, then claims it    *)
(* first claim wins with reason `group-bound`.                            *)
(*                                                                         *)
(* One group of K members (1..K) and N nodes (1..N). The scheduler is the *)
(* deterministic fold of its input log `inputs` (spec records and node facts) *)
(* in `Scheduler::step` (crates/core/src/schedule.rs). Its holder may     *)
(* crash or be paused past its lease; a new holder claims `out` at the    *)
(* next epoch and replays from a snapshot. Fencing and exactly-once of    *)
(* `out` are Felixd.tla's: here the claim is one record and an append     *)
(* lands only behind its writer's own claim. Silence by `sys.facts` time  *)
(* is abstracted as a `lapse` input that marks a node silent until its    *)
(* next fact.                                                             *)
(*                                                                         *)
(* Agents follow `turn` in crates/core/src/placement.rs: claim a member   *)
(* the group record in their view places on them, or past the bound. The *)
(* bound is counted in a node's own steps: since first seen free with the *)
(* group complete, or since first seen free while it is not.              *)
(*                                                                         *)
(* Knobs, each turned off in a twin that TLC finds a violation in:        *)
(*   Barrier     the agent checks the group record before anything else. *)
(*   SplitGuard  the scheduler leaves a group with a member running off   *)
(*               its record alone.                                        *)
(*   OneRecord   a group is placed in one record, not one per member.    *)
(*   Fence       an append lands only behind its writer's own claim.     *)
(*   FactTakers  the scheduler's takers come from recorded facts, not     *)
(*               from which nodes are up at the moment it steps.          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    N,            \* nodes 1..N
    K,            \* group members 1..K
    Cap,          \* max_workloads of every node
    Bound,        \* the group bound, in a node's own steps
    InitAdmitted, \* members admitted at the start
    MaxIn,        \* inputs to the scheduler
    MaxEpoch,     \* scheduler epochs (takeovers + 1)
    MaxNodeCrash, \* node crashes
    MaxExpire,    \* leases lost by a live holder
    LapseLive,    \* whether a live node may be judged silent
    Barrier, SplitGuard, OneRecord, Fence, FactTakers

Nodes == 1..N
Members == 1..K
None == 0
NoMap == [m \in {} |-> 0]
NoStale == [e |-> 0, map |-> NoMap]

VARIABLES
    inputs, admitted,                         \* scheduler inputs, `workloads` cache
    st, pos, written, hist, stepOut,      \* scheduler state and replay history
    epoch, up, stale, out,                \* holder, a paused deposed holder, `out`
    lease, auth, nup, view, since, gsince,\* agents
    nodeCrashes, expires,
    replayOk, splitKept                   \* history for invariants

vars == <<inputs, admitted, st, pos, written, hist, stepOut, epoch, up, stale, out,
          lease, auth, nup, view, since, gsince, nodeCrashes, expires,
          replayOk, splitKept>>

Min(S) == CHOOSE x \in S : \A y \in S : x <= y

(***************************************************************************)
(* The scheduler, as Scheduler::step and place_groups.                    *)
(***************************************************************************)
SInit == [heard |-> [n \in Nodes |-> {}], live |-> {}, declared |-> {}, groups |-> NoMap]

Live(s) == IF FactTakers THEN s.live ELSE {n \in Nodes : nup[n]}

\* takers(): what a node holds is part of its entry, so a new fact changes it.
Takers(s) == [n \in Live(s) |-> s.heard[n]]

Complete(s) == s.declared = Members

\* running(): the node each member runs on, by facts of nodes not silent;
\* the lowest id wins.
Running(s) ==
    LET on(m) == {n \in s.live : m \in s.heard[n]}
    IN [m \in {m \in Members : on(m) # {}} |-> Min(on(m))]

GroupsRunning(s) == IF Complete(s) THEN {Running(s)} ELSE {}

\* The pick loop of place_groups: each member not running, in id order, on
\* the least loaded taker with room, the lowest id first. Load is what a
\* node holds; the record in force is taken off it (add_load_owned -1).
RECURSIVE Pick(_, _, _, _)
Pick(ms, trial, placed, s) ==
    IF ms = <<>> THEN placed
    ELSE LET cands == {n \in Live(s) : trial[n] < Cap}
         IN IF cands = {} THEN NoMap \* does not fit
            ELSE LET n == CHOOSE n \in cands :
                            \A o \in cands : trial[n] < trial[o] \/ (trial[n] = trial[o] /\ n <= o)
                 IN Pick(Tail(ms), [trial EXCEPT ![n] = @ + 1], placed @@ (Head(ms) :> n), s)

\* The members of S in id order.
RECURSIVE SeqOf(_)
SeqOf(S) == IF S = {} THEN <<>> ELSE <<Min(S)>> \o SeqOf(S \ {Min(S)})

\* Result: the new state, the change (NONE, or a member map; empty withdraws)
\* and whether the change keeps a split (for SplitNeverKept).
NoChange == [kind |-> "none", map |-> NoMap]
PlaceGroups(s) ==
    IF ~Complete(s)
    THEN IF s.groups # NoMap
         THEN [st |-> [s EXCEPT !.groups = NoMap], ch |-> [kind |-> "group", map |-> NoMap], kept |-> FALSE]
         ELSE [st |-> s, ch |-> NoChange, kept |-> FALSE]
    ELSE
      LET run == Running(s)
          cur == s.groups
          offRecord == cur # NoMap /\ \E m \in DOMAIN run : cur[m] # run[m]
      IN IF SplitGuard /\ offRecord
         THEN [st |-> s, ch |-> NoChange, kept |-> FALSE]
         ELSE IF cur # NoMap /\ \A m \in Members :
                   (m \in DOMAIN run /\ run[m] = cur[m]) \/ cur[m] \in Live(s)
         THEN [st |-> s, ch |-> NoChange, kept |-> FALSE]
         ELSE LET free == SeqOf(Members \ DOMAIN run)
                  trial == [n \in Live(s) |-> Cardinality(s.heard[n])]
                  placed == IF free = <<>> THEN run ELSE Pick(free, trial, run, s)
              IN IF placed = NoMap \/ DOMAIN placed # Members \/ placed = cur
                 THEN [st |-> s, ch |-> NoChange, kept |-> FALSE]
                 ELSE [st |-> [s EXCEPT !.groups = placed],
                       ch |-> [kind |-> "group", map |-> placed],
                       kept |-> offRecord]

StepIn(s, x) ==
    LET s1 == CASE x.k = "spec"  -> [s EXCEPT !.declared = @ \cup {x.m}]
                [] x.k = "fact"  -> [s EXCEPT !.heard[x.n] = x.hold, !.live = @ \cup {x.n}]
                [] x.k = "lapse" -> [s EXCEPT !.live = @ \ {x.n}]
        trigger == (x.k = "spec" /\ x.m \notin s.declared)
                   \/ Takers(s1) # Takers(s)
                   \/ GroupsRunning(s1) # GroupsRunning(s)
    IN IF trigger THEN PlaceGroups(s1) ELSE [st |-> s1, ch |-> NoChange, kept |-> FALSE]

GroupRec(map, e) == [k |-> "group", map |-> map, e |-> e]
ClaimRec(e) == [k |-> "claim", e |-> e]
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
LastClaim(o) == o[Max({j \in 1..Len(o) : o[j].k = "claim"})].e

\* What a change appends. Without OneRecord, a group goes as one record per
\* member, then the whole map.
Appends(ch, e) ==
    IF ch.kind = "none" THEN <<>>
    ELSE IF OneRecord \/ ch.map = NoMap THEN <<GroupRec(ch.map, e)>>
    ELSE <<GroupRec([m \in {Min(DOMAIN ch.map)} |-> ch.map[m]], e), GroupRec(ch.map, e)>>

SchedStep ==
    /\ up /\ pos < Len(inputs)
    /\ LET r == StepIn(st, inputs[pos + 1]) IN
       IF pos + 1 > written
       THEN /\ out' = out \o Appends(r.ch, epoch)
            /\ written' = pos + 1
            /\ hist' = Append(hist, r.st)
            /\ stepOut' = Append(stepOut, r.ch)
            /\ splitKept' = (splitKept \/ r.kept)
            /\ UNCHANGED replayOk
       ELSE \* Replay: the guard writes nothing; the outputs must match.
            /\ replayOk' = (replayOk /\ r.ch = stepOut[pos + 1])
            /\ UNCHANGED <<out, written, hist, stepOut, splitKept>>
    /\ st' = StepIn(st, inputs[pos + 1]).st
    /\ pos' = pos + 1
    /\ UNCHANGED <<inputs, admitted, epoch, up, stale, lease, auth, nup, view, since, gsince,
                   nodeCrashes, expires>>

SchedCrash ==
    /\ up /\ epoch < MaxEpoch
    /\ up' = FALSE
    /\ UNCHANGED <<inputs, admitted, st, pos, written, hist, stepOut, epoch, stale, out,
                   lease, auth, nup, view, since, gsince, nodeCrashes, expires,
                   replayOk, splitKept>>

\* The holder's lease expired: it crashed, or it is paused and keeps its
\* next output to append later. The new holder claims `out` and restores the
\* snapshot after any step it has.
SchedTakeover ==
    /\ epoch < MaxEpoch
    /\ \E k \in 0..written :
        /\ st' = IF k = 0 THEN SInit ELSE hist[k]
        /\ pos' = k
    /\ stale' = IF up /\ pos < Len(inputs)
                THEN LET ch == StepIn(st, inputs[pos + 1]).ch
                     IN IF ch.kind = "none" THEN NoStale ELSE [e |-> epoch, map |-> ch.map]
                ELSE NoStale
    /\ epoch' = epoch + 1
    /\ up' = TRUE
    /\ out' = Append(out, ClaimRec(epoch + 1))
    /\ UNCHANGED <<inputs, admitted, written, hist, stepOut, lease, auth, nup, view, since, gsince,
                   nodeCrashes, expires, replayOk, splitKept>>

\* The deposed holder's append: the fence refuses it behind a later claim.
StaleWrite ==
    /\ stale # NoStale
    /\ out' = IF ~Fence \/ LastClaim(out) = stale.e
              THEN Append(out, GroupRec(stale.map, stale.e)) ELSE out
    /\ stale' = NoStale
    /\ UNCHANGED <<inputs, admitted, st, pos, written, hist, stepOut, epoch, up,
                   lease, auth, nup, view, since, gsince, nodeCrashes, expires,
                   replayOk, splitKept>>

(***************************************************************************)
(* Inputs: admission and node facts.                                       *)
(***************************************************************************)
Holding(n) == {m \in Members : lease[m] = n}

InputUnchanged == UNCHANGED <<st, pos, written, hist, stepOut, epoch, up, stale, out,
                              lease, auth, nup, view, since, gsince, nodeCrashes, expires,
                              replayOk, splitKept>>

Admit(m) ==
    /\ m \notin admitted /\ Len(inputs) < MaxIn
    /\ admitted' = admitted \cup {m}
    /\ inputs' = Append(inputs, [k |-> "spec", m |-> m])
    /\ InputUnchanged

Fact(n) ==
    /\ nup[n] /\ Len(inputs) < MaxIn
    /\ inputs' = Append(inputs, [k |-> "fact", n |-> n, hold |-> Holding(n)])
    /\ UNCHANGED admitted /\ InputUnchanged

Lapse(n) ==
    /\ LapseLive \/ ~nup[n]
    /\ Len(inputs) < MaxIn
    /\ inputs' = Append(inputs, [k |-> "lapse", n |-> n])
    /\ UNCHANGED admitted /\ InputUnchanged

(***************************************************************************)
(* Agents: Followed::fold and group_placement, wait_for_chance and turn.  *)
(***************************************************************************)
\* The record in force in a prefix of `out`: the last group record, if it
\* lists exactly the members (a record for another list places nobody).
InForce(v) ==
    LET gs == {j \in 1..v : out[j].k = "group"}
    IN IF gs = {} THEN None
       ELSE LET j == CHOOSE j \in gs : \A l \in gs : l <= j
            IN IF DOMAIN out[j].map = Members THEN j ELSE None

PlacesHere(v, m, n) == LET j == InForce(v) IN j # None /\ out[j].map[m] = n

Held(n, m) == IF admitted = Members THEN gsince[n][m] ELSE since[n][m]

AgentUnchanged == UNCHANGED <<inputs, admitted, st, pos, written, hist, stepOut, epoch, up, stale,
                              out, replayOk, splitKept>>

Read(n) ==
    /\ nup[n] /\ view[n] < Len(out)
    /\ view' = [view EXCEPT ![n] = @ + 1]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, auth, nup, since, gsince, nodeCrashes, expires>>

\* One of the node's own steps while the member is free and admitted.
Tick(n, m) ==
    /\ nup[n] /\ lease[m] = None /\ m \in admitted
    /\ \/ since[n][m] < Bound
       \/ admitted = Members /\ gsince[n][m] < Bound
    /\ since' = [since EXCEPT ![n][m] = IF @ < Bound THEN @ + 1 ELSE @]
    /\ gsince' = [gsince EXCEPT ![n][m] = IF admitted = Members /\ @ < Bound THEN @ + 1 ELSE @]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, auth, nup, view, nodeCrashes, expires>>

Claim(n, m) ==
    /\ nup[n] /\ lease[m] = None /\ m \in admitted
    /\ Cardinality(Holding(n)) < Cap
    /\ \/ ~Barrier
       \/ PlacesHere(view[n], m, n)
       \/ Held(n, m) >= Bound
    /\ lease' = [lease EXCEPT ![m] = n]
    /\ auth' = [auth EXCEPT ![m] = [view |-> view[n], held |-> Held(n, m)]]
    \* While the lease is held, every node's clocks for it start over.
    /\ since' = [x \in Nodes |-> [since[x] EXCEPT ![m] = 0]]
    /\ gsince' = [x \in Nodes |-> [gsince[x] EXCEPT ![m] = 0]]
    /\ AgentUnchanged
    /\ UNCHANGED <<nup, view, nodeCrashes, expires>>

\* A lease lapses: its holder is down, or lost it while alive and stops.
Expire(m) ==
    /\ lease[m] # None
    /\ ~nup[lease[m]] \/ expires < MaxExpire
    /\ expires' = IF nup[lease[m]] THEN expires + 1 ELSE expires
    /\ lease' = [lease EXCEPT ![m] = None]
    /\ AgentUnchanged
    /\ UNCHANGED <<auth, nup, view, since, gsince, nodeCrashes>>

NodeCrash(n) ==
    /\ nup[n] /\ nodeCrashes < MaxNodeCrash
    /\ nup' = [nup EXCEPT ![n] = FALSE]
    /\ nodeCrashes' = nodeCrashes + 1
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, auth, view, since, gsince, expires>>

\* A restarted agent reads `out` again and its clocks start over.
NodeRestart(n) ==
    /\ ~nup[n] /\ Holding(n) = {}
    /\ nup' = [nup EXCEPT ![n] = TRUE]
    /\ view' = [view EXCEPT ![n] = 0]
    /\ since' = [since EXCEPT ![n] = [m \in Members |-> 0]]
    /\ gsince' = [gsince EXCEPT ![n] = [m \in Members |-> 0]]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, auth, nodeCrashes, expires>>

Init ==
    /\ inputs = [i \in 1..Cardinality(InitAdmitted) |-> [k |-> "spec", m |-> SeqOf(InitAdmitted)[i]]]
    /\ admitted = InitAdmitted
    /\ st = SInit /\ pos = 0 /\ written = 0 /\ hist = <<>> /\ stepOut = <<>>
    /\ epoch = 1 /\ up = TRUE /\ stale = NoStale /\ out = <<ClaimRec(1)>>
    /\ lease = [m \in Members |-> None]
    /\ auth = [m \in Members |-> [view |-> 0, held |-> 0]]
    /\ nup = [n \in Nodes |-> TRUE]
    /\ view = [n \in Nodes |-> 0]
    /\ since = [n \in Nodes |-> [m \in Members |-> 0]]
    /\ gsince = [n \in Nodes |-> [m \in Members |-> 0]]
    /\ nodeCrashes = 0 /\ expires = 0
    /\ replayOk = TRUE /\ splitKept = FALSE

Next ==
    \/ SchedStep \/ SchedCrash \/ SchedTakeover \/ StaleWrite
    \/ \E m \in Members : Admit(m) \/ Expire(m)
    \/ \E n \in Nodes : Fact(n) \/ Lapse(n) \/ Read(n) \/ NodeCrash(n) \/ NodeRestart(n)
    \/ \E n \in Nodes, m \in Members : Tick(n, m) \/ Claim(n, m)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants.                                                             *)
(***************************************************************************)
\* Every group record names every member, or none (a withdrawal): no reader
\* can see part of a group.
GroupWhole ==
    \A j \in 1..Len(out) :
        out[j].k = "group" => (DOMAIN out[j].map = Members \/ out[j].map = NoMap)

\* No partial group runs before the bound: a running member was claimed on
\* a record in its claimer's view that placed it there, or with the
\* claimer's clock past the bound.
GroupHeld ==
    \A m \in Members :
        lease[m] # None =>
            \/ PlacesHere(auth[m].view, m, lease[m])
            \/ auth[m].held >= Bound

\* A split is never kept: no record replaces one in force while a member,
\* by the scheduler's facts, runs off it.
SplitNeverKept == ~splitKept

\* Replay after a crash places the same set: re-stepping an input gives the
\* change first written for it.
ReplaySame == replayOk

\* Assignments only from the current epoch: nothing from an earlier epoch
\* lands after a later claim.
FencedOut ==
    \A i, j \in 1..Len(out) :
        (i < j /\ out[i].k = "claim" /\ out[j].k = "group") => out[j].e >= out[i].e

TypeOK ==
    /\ lease \in [Members -> Nodes \cup {None}]
    /\ pos <= Len(inputs) /\ written <= Len(inputs)
=============================================================================
