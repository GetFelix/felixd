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
(* group complete, or since first seen free while it is not. Like        *)
(* wait_for_chance, a node starts its clocks over only when its lease     *)
(* watch shows the member held, which it may skip.                        *)
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
NoStale == [e |-> 0, map |-> NoMap, p |-> 0]

VARIABLES
    inputs, admitted,                         \* scheduler inputs, `workloads` cache
    st, pos, written, hist,               \* scheduler state and its snapshots
    epoch, up, stale, out,                \* holder, a paused deposed holder, `out`
    lease, early, nup, view, since, gsince,\* agents
    nodeCrashes, expires,
    applied,                              \* history: records agents claimed on
    lagged, silentHeld, judged            \* ghosts for the two known splits

vars == <<inputs, admitted, st, pos, written, hist, epoch, up, stale, out,
          lease, early, nup, view, since, gsince, nodeCrashes, expires,
          applied, lagged, silentHeld, judged>>

\* The ghosts only Claim, Lapse and NodeCrash change.
ghosts == <<lagged, silentHeld, judged>>

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

\* Result: the new state and the change (NONE, or a member map; empty
\* withdraws).
NoChange == [kind |-> "none", map |-> NoMap]
PlaceGroups(s) ==
    IF ~Complete(s)
    THEN IF s.groups # NoMap
         THEN [st |-> [s EXCEPT !.groups = NoMap], ch |-> [kind |-> "group", map |-> NoMap]]
         ELSE [st |-> s, ch |-> NoChange]
    ELSE
      LET run == Running(s)
          cur == s.groups
          offRecord == cur # NoMap /\ \E m \in DOMAIN run : cur[m] # run[m]
      IN IF SplitGuard /\ offRecord
         THEN [st |-> s, ch |-> NoChange]
         ELSE IF cur # NoMap /\ \A m \in Members :
                   (m \in DOMAIN run /\ run[m] = cur[m]) \/ cur[m] \in Live(s)
         THEN [st |-> s, ch |-> NoChange]
         ELSE LET free == SeqOf(Members \ DOMAIN run)
                  trial == [n \in Live(s) |-> Cardinality(s.heard[n])]
                  placed == IF free = <<>> THEN run ELSE Pick(free, trial, run, s)
              IN IF placed = NoMap \/ DOMAIN placed # Members \/ placed = cur
                 THEN [st |-> s, ch |-> NoChange]
                 ELSE [st |-> [s EXCEPT !.groups = placed],
                       ch |-> [kind |-> "group", map |-> placed]]

StepIn(s, x) ==
    LET s1 == CASE x.k = "spec"  -> [s EXCEPT !.declared = @ \cup {x.m}]
                [] x.k = "fact"  -> [s EXCEPT !.heard[x.n] = x.hold, !.live = @ \cup {x.n}]
                [] x.k = "lapse" -> [s EXCEPT !.live = @ \ {x.n}]
        trigger == (x.k = "spec" /\ x.m \notin s.declared)
                   \/ Takers(s1) # Takers(s)
                   \/ GroupsRunning(s1) # GroupsRunning(s)
    IN IF trigger THEN PlaceGroups(s1) ELSE [st |-> s1, ch |-> NoChange]

\* `p` is the input the record answers; only SplitNeverKept reads it.
GroupRec(map, e, p) == [k |-> "group", map |-> map, e |-> e, p |-> p]
ClaimRec(e) == [k |-> "claim", e |-> e]
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
LastClaim(o) == o[Max({j \in 1..Len(o) : o[j].k = "claim"})].e

\* What a change appends. Without OneRecord, a group goes as one record per
\* member, then the whole map.
Appends(ch, e, p) ==
    IF ch.kind = "none" THEN <<>>
    ELSE IF OneRecord \/ ch.map = NoMap THEN <<GroupRec(ch.map, e, p)>>
    ELSE <<GroupRec([m \in {Min(DOMAIN ch.map)} |-> ch.map[m]], e, p), GroupRec(ch.map, e, p)>>

SchedStep ==
    /\ up /\ pos < Len(inputs)
    /\ LET r == StepIn(st, inputs[pos + 1]) IN
       IF pos + 1 > written
       THEN /\ out' = out \o Appends(r.ch, epoch, pos + 1)
            /\ written' = pos + 1
            /\ hist' = Append(hist, r.st)
       ELSE \* Replay: the guard writes nothing.
            UNCHANGED <<out, written, hist>>
    /\ st' = StepIn(st, inputs[pos + 1]).st
    /\ pos' = pos + 1
    /\ UNCHANGED <<inputs, admitted, epoch, up, stale, lease, early, nup, view, since, gsince,
                   nodeCrashes, expires, applied, ghosts>>

SchedCrash ==
    /\ up /\ epoch < MaxEpoch
    /\ up' = FALSE
    /\ UNCHANGED <<inputs, admitted, st, pos, written, hist, epoch, stale, out,
                   lease, early, nup, view, since, gsince, nodeCrashes, expires,
                   applied, ghosts>>

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
                     IN IF ch.kind = "none" THEN NoStale ELSE [e |-> epoch, map |-> ch.map, p |-> pos + 1]
                ELSE NoStale
    /\ epoch' = epoch + 1
    /\ up' = TRUE
    /\ out' = Append(out, ClaimRec(epoch + 1))
    /\ UNCHANGED <<inputs, admitted, written, hist, lease, early, nup, view, since, gsince,
                   nodeCrashes, expires, applied, ghosts>>

\* The deposed holder's append: the fence refuses it behind a later claim.
StaleWrite ==
    /\ stale # NoStale
    /\ out' = IF ~Fence \/ LastClaim(out) = stale.e
              THEN Append(out, GroupRec(stale.map, stale.e, stale.p)) ELSE out
    /\ stale' = NoStale
    /\ UNCHANGED <<inputs, admitted, st, pos, written, hist, epoch, up,
                   lease, early, nup, view, since, gsince, nodeCrashes, expires,
                   applied, ghosts>>

(***************************************************************************)
(* Inputs: admission and node facts.                                       *)
(***************************************************************************)
Holding(n) == {m \in Members : lease[m] = n}

InputUnchanged == UNCHANGED <<st, pos, written, hist, epoch, up, stale, out,
                              lease, early, nup, view, since, gsince, nodeCrashes, expires,
                              applied>>

Admit(m) ==
    /\ m \notin admitted /\ Len(inputs) < MaxIn
    /\ admitted' = admitted \cup {m}
    /\ inputs' = Append(inputs, [k |-> "spec", m |-> m])
    /\ InputUnchanged /\ UNCHANGED ghosts

Fact(n) ==
    /\ nup[n] /\ Len(inputs) < MaxIn
    /\ inputs' = Append(inputs, [k |-> "fact", n |-> n, hold |-> Holding(n)])
    /\ UNCHANGED <<admitted, ghosts>> /\ InputUnchanged

Lapse(n) ==
    /\ LapseLive \/ ~nup[n]
    /\ Len(inputs) < MaxIn
    /\ inputs' = Append(inputs, [k |-> "lapse", n |-> n])
    \* Ghosts for #247: a node judged silent while up, and what it holds.
    /\ judged' = IF nup[n] THEN [judged EXCEPT ![n] = TRUE] ELSE judged
    /\ silentHeld' = IF nup[n] THEN [m \in Members |-> silentHeld[m] \/ lease[m] = n] ELSE silentHeld
    /\ UNCHANGED <<admitted, lagged>> /\ InputUnchanged

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

AgentUnchanged == UNCHANGED <<inputs, admitted, st, pos, written, hist, epoch, up, stale,
                              out>>

Read(n) ==
    /\ nup[n] /\ view[n] < Len(out)
    /\ view' = [view EXCEPT ![n] = @ + 1]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, early, nup, since, gsince, nodeCrashes, expires, applied, ghosts>>

\* One of the node's own steps for an admitted member. A clock starts only
\* while the member is free, then runs until the node sees it held.
Tick(n, m) ==
    /\ nup[n] /\ m \in admitted
    /\ lease[m] = None \/ since[n][m] > 0 \/ gsince[n][m] > 0
    /\ \/ since[n][m] < Bound
       \/ admitted = Members /\ gsince[n][m] < Bound
    /\ since' = [since EXCEPT ![n][m] = IF @ < Bound THEN @ + 1 ELSE @]
    /\ gsince' = [gsince EXCEPT ![n][m] = IF admitted = Members /\ @ < Bound THEN @ + 1 ELSE @]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, early, nup, view, nodeCrashes, expires, applied, ghosts>>

\* The node's lease watch shows the member held with time left, and
\* wait_for_chance starts its clocks over. A watch may skip from one free
\* to the next without showing this.
SeeHeld(n, m) ==
    /\ nup[n] /\ lease[m] # None
    /\ since[n][m] > 0 \/ gsince[n][m] > 0
    /\ since' = [since EXCEPT ![n][m] = 0]
    /\ gsince' = [gsince EXCEPT ![n][m] = 0]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, early, nup, view, nodeCrashes, expires, applied, ghosts>>

Claim(n, m) ==
    /\ nup[n] /\ lease[m] = None /\ m \in admitted
    /\ Cardinality(Holding(n)) < Cap
    /\ \/ ~Barrier
       \/ PlacesHere(view[n], m, n)
       \/ Held(n, m) >= Bound
    /\ lease' = [lease EXCEPT ![m] = n]
    /\ early' = [early EXCEPT ![m] = Held(n, m) < Bound]
    /\ applied' = IF PlacesHere(view[n], m, n) THEN applied \cup {InForce(view[n])} ELSE applied
    \* Ghosts for the known splits: claimed under a record no longer in force
    \* (#248), or by a node judged silent while up (#247).
    /\ lagged' = [lagged EXCEPT ![m] = InForce(view[n]) # InForce(Len(out))]
    /\ silentHeld' = [silentHeld EXCEPT ![m] = judged[n]]
    \* The claimer waits afresh once it lets the member go.
    /\ since' = [since EXCEPT ![n][m] = 0]
    /\ gsince' = [gsince EXCEPT ![n][m] = 0]
    /\ AgentUnchanged
    /\ UNCHANGED <<nup, view, nodeCrashes, expires, judged>>

\* A lease lapses: its holder is down, or lost it while alive and stops.
Expire(m) ==
    /\ lease[m] # None
    /\ ~nup[lease[m]] \/ expires < MaxExpire
    /\ expires' = IF nup[lease[m]] THEN expires + 1 ELSE expires
    /\ lease' = [lease EXCEPT ![m] = None]
    /\ AgentUnchanged
    /\ UNCHANGED <<early, nup, view, since, gsince, nodeCrashes, applied, ghosts>>

NodeCrash(n) ==
    /\ nup[n] /\ nodeCrashes < MaxNodeCrash
    /\ nup' = [nup EXCEPT ![n] = FALSE]
    /\ nodeCrashes' = nodeCrashes + 1
    \* Down, it is judged silent rightly: a restart starts afresh.
    /\ judged' = [judged EXCEPT ![n] = FALSE]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, early, view, since, gsince, expires, applied, lagged, silentHeld>>

\* A restarted agent reads `out` again and its clocks start over.
NodeRestart(n) ==
    /\ ~nup[n] /\ Holding(n) = {}
    /\ nup' = [nup EXCEPT ![n] = TRUE]
    /\ view' = [view EXCEPT ![n] = 0]
    /\ since' = [since EXCEPT ![n] = [m \in Members |-> 0]]
    /\ gsince' = [gsince EXCEPT ![n] = [m \in Members |-> 0]]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, early, nodeCrashes, expires, applied, ghosts>>

Init ==
    /\ inputs = [i \in 1..Cardinality(InitAdmitted) |-> [k |-> "spec", m |-> SeqOf(InitAdmitted)[i]]]
    /\ admitted = InitAdmitted
    /\ st = SInit /\ pos = 0 /\ written = 0 /\ hist = <<>>
    /\ epoch = 1 /\ up = TRUE /\ stale = NoStale /\ out = <<ClaimRec(1)>>
    /\ lease = [m \in Members |-> None]
    /\ early = [m \in Members |-> FALSE]
    /\ nup = [n \in Nodes |-> TRUE]
    /\ view = [n \in Nodes |-> 0]
    /\ since = [n \in Nodes |-> [m \in Members |-> 0]]
    /\ gsince = [n \in Nodes |-> [m \in Members |-> 0]]
    /\ nodeCrashes = 0 /\ expires = 0
    /\ applied = {}
    /\ lagged = [m \in Members |-> FALSE]
    /\ silentHeld = [m \in Members |-> FALSE]
    /\ judged = [n \in Nodes |-> FALSE]

Next ==
    \/ SchedStep \/ SchedCrash \/ SchedTakeover \/ StaleWrite
    \/ \E m \in Members : Admit(m) \/ Expire(m)
    \/ \E n \in Nodes : Fact(n) \/ Lapse(n) \/ Read(n) \/ NodeCrash(n) \/ NodeRestart(n)
    \/ \E n \in Nodes, m \in Members : Tick(n, m) \/ SeeHeld(n, m) \/ Claim(n, m)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants.                                                             *)
(***************************************************************************)
\* Every group record names every member, or none (a withdrawal): no reader
\* can see part of a group.
GroupWhole ==
    \A j \in 1..Len(out) :
        out[j].k = "group" => (DOMAIN out[j].map = Members \/ out[j].map = NoMap)

\* A member runs while its lease holder is up. One claimed before the bound
\* is early: its claimer's clock had not reached the bound.
EarlyRunning == {m \in Members : lease[m] # None /\ nup[lease[m]] /\ early[m]}

\* One whole group record places every member of S on the node that holds it.
TogetherOn(S) ==
    S # {} =>
        \E j \in 1..Len(out) :
            /\ out[j].k = "group" /\ DOMAIN out[j].map = Members
            /\ \A m \in S : out[j].map[m] = lease[m]

\* No group runs split before the bound. It reads `early`, the claimer's
\* clock at claim time: a clock that never reached the bound would make it
\* hold by construction.
GroupTogether == TogetherOn(EarlyRunning)

\* GroupTogether for the members neither known split (#247, #248) marks, so
\* a new way to split is still found where those splits can happen.
GroupTogetherFresh ==
    TogetherOn({m \in EarlyRunning : ~lagged[m] /\ ~silentHeld[m]})

\* What `sys.facts` say after the first p inputs: each node's holdings and
\* which nodes are not silent.
RECURSIVE FactsAt(_)
FactsAt(p) ==
    IF p = 0 THEN [heard |-> [n \in Nodes |-> {}], live |-> {}]
    ELSE LET f == FactsAt(p - 1)
             x == inputs[p]
         IN CASE x.k = "fact"  -> [heard |-> [f.heard EXCEPT ![x.n] = x.hold], live |-> f.live \cup {x.n}]
              [] x.k = "lapse" -> [f EXCEPT !.live = @ \ {x.n}]
              [] OTHER         -> f

\* A split is never kept: a whole record that replaces a whole record never
\* places a member where the facts it answers show it running off the
\* record it replaces.
SplitNeverKept ==
    \A j \in 1..Len(out) :
        (out[j].k = "group" /\ DOMAIN out[j].map = Members) =>
            LET gs == {i \in 1..(j - 1) : out[i].k = "group"}
            IN \/ gs = {}
               \/ DOMAIN out[Max(gs)].map # Members
               \/ LET prev == out[Max(gs)].map
                      f == FactsAt(out[j].p)
                  IN \A m \in Members : \A n \in f.live :
                         (m \in f.heard[n] /\ prev[m] # n) => out[j].map[m] # n

\* Replay after a takeover places the same set: once the holder has caught
\* up with what was written, the record in force on `out` is its placement.
LastGroup == LET gs == {j \in 1..Len(out) : out[j].k = "group"}
             IN IF gs = {} THEN NoMap ELSE out[Max(gs)].map
ReplaySame == (up /\ pos >= written) => LastGroup = st.groups

\* No agent applies a stale record: a record a member was claimed on has an
\* epoch no lower than any claim on `out` before it.
StaleNeverApplied ==
    \A j \in applied : \A i \in 1..(j - 1) :
        out[i].k = "claim" => out[i].e <= out[j].e

TypeOK ==
    /\ lease \in [Members -> Nodes \cup {None}]
    /\ pos <= Len(inputs) /\ written <= Len(inputs)
=============================================================================
