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
(* next fact. Forgetting a node that is down rides on another node's fact,*)
(* as forget_silent runs only when a fact is folded.                      *)
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
(*   RecordHold  running() counts the record's members on a node that is  *)
(*               silent or blocked (#247).                                *)
(*   TailGate    the reader shows its view only once it reaches the tail  *)
(*               it saw when the read began (#248).                       *)
(* ForgetRace lets a forgotten node restart while the record in force    *)
(* may still name it: before its forget is folded, or after a forget     *)
(* that could not move the group. The time-only release of felixd #251, *)
(* pinned by its own config.                                              *)
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
    ForgetDown,   \* whether a node that is down may be forgotten
    ForgetRace,   \* whether it may restart before the scheduler folds that
    Blocking,     \* whether a fact may say its node is blocked
    MaxDrain,     \* drain marks set
    Barrier, SplitGuard, OneRecord, Fence, FactTakers, RecordHold, TailGate

Nodes == 1..N
Members == 1..K
None == 0
NoMap == [m \in {} |-> 0]
NoStale == [e |-> 0, map |-> NoMap, p |-> 0]

VARIABLES
    inputs, admitted,                         \* scheduler inputs, `workloads` cache
    st, pos, written, hist,               \* scheduler state and its snapshots
    epoch, up, stale, out,                \* holder, a paused deposed holder, `out`
    lease, early, nup, since, gsince,     \* agents
    view, rd, tgt,                        \* each agent's reader
    marked, drains,                       \* drain marks
    nodeCrashes, expires,
    applied                               \* history: records agents claimed on

vars == <<inputs, admitted, st, pos, written, hist, epoch, up, stale, out,
          lease, early, nup, since, gsince, view, rd, tgt, marked, drains,
          nodeCrashes, expires, applied>>

\* What only agents change.
agentVars == <<lease, early, nup, since, gsince, view, rd, tgt, marked, drains,
               nodeCrashes, expires, applied>>

Min(S) == CHOOSE x \in S : \A y \in S : x <= y

(***************************************************************************)
(* The scheduler, as Scheduler::step and place_groups.                    *)
(***************************************************************************)
\* `known` is the scheduler's `nodes`: heard from and not forgotten. `at` is
\* the input that carried a node's last fact, standing in for its time.
SInit == [heard |-> [n \in Nodes |-> {}], at |-> [n \in Nodes |-> 0], known |-> {},
          live |-> {}, blocked |-> {}, draining |-> {}, declared |-> {}, groups |-> NoMap]

\* takers(): heard from, not silent, neither draining nor blocked.
Live(s) == IF FactTakers THEN s.live \ (s.blocked \cup s.draining) ELSE {n \in Nodes : nup[n]}

\* What a node holds is part of its entry, so a new fact changes it.
Takers(s) == [n \in Live(s) |-> s.heard[n]]

Complete(s) == s.declared = Members

\* running(): where a known node's last fact holds a member, silent or not,
\* the newest fact winning. Then, with RecordHold, a member the record in
\* force places on a known node that is silent or blocked: it may land a
\* claim no fact shows. A taker, or a draining node not silent, releases it.
Running(s) ==
    LET on(m) == {n \in s.known : m \in s.heard[n]}
        newest(m) == CHOOSE n \in on(m) : \A o \in on(m) : s.at[o] <= s.at[n]
        held(m) == /\ RecordHold /\ s.groups # NoMap
                   /\ s.groups[m] \in s.known
                   /\ s.groups[m] \notin s.live \/ s.groups[m] \in s.blocked
    IN [m \in {m \in Members : on(m) # {} \/ held(m)} |->
            IF on(m) # {} THEN newest(m) ELSE s.groups[m]]

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

\* forget_silent drops node g in the same fold as the fact that dates it.
ForgetIn(s, g) ==
    IF g = None THEN s
    ELSE [s EXCEPT !.heard[g] = {}, !.at[g] = 0, !.known = @ \ {g}, !.live = @ \ {g},
                   !.blocked = @ \ {g}, !.draining = @ \ {g}]

\* Folds input p, which is x.
StepIn(s, x, p) ==
    LET s1 == CASE x.k = "spec"  -> [s EXCEPT !.declared = @ \cup {x.m}]
                [] x.k = "fact"  -> ForgetIn([s EXCEPT !.heard[x.n] = x.hold, !.at[x.n] = p,
                                       !.known = @ \cup {x.n}, !.live = @ \cup {x.n},
                                       !.blocked = IF x.blocked THEN @ \cup {x.n} ELSE @ \ {x.n},
                                       !.draining = IF x.draining THEN @ \cup {x.n} ELSE @ \ {x.n}],
                                       x.gone)
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
    /\ LET r == StepIn(st, inputs[pos + 1], pos + 1) IN
       IF pos + 1 > written
       THEN /\ out' = out \o Appends(r.ch, epoch, pos + 1)
            /\ written' = pos + 1
            /\ hist' = Append(hist, r.st)
       ELSE \* Replay: the guard writes nothing.
            UNCHANGED <<out, written, hist>>
    /\ st' = StepIn(st, inputs[pos + 1], pos + 1).st
    /\ pos' = pos + 1
    /\ UNCHANGED <<inputs, admitted, epoch, up, stale, agentVars>>

SchedCrash ==
    /\ up /\ epoch < MaxEpoch
    /\ up' = FALSE
    /\ UNCHANGED <<inputs, admitted, st, pos, written, hist, epoch, stale, out, agentVars>>

\* The holder's lease expired: it crashed, or it is paused and keeps its
\* next output to append later. The new holder claims `out` and restores the
\* snapshot after any step it has.
SchedTakeover ==
    /\ epoch < MaxEpoch
    /\ \E k \in 0..written :
        /\ st' = IF k = 0 THEN SInit ELSE hist[k]
        /\ pos' = k
    /\ stale' = IF up /\ pos < Len(inputs)
                THEN LET ch == StepIn(st, inputs[pos + 1], pos + 1).ch
                     IN IF ch.kind = "none" THEN NoStale ELSE [e |-> epoch, map |-> ch.map, p |-> pos + 1]
                ELSE NoStale
    /\ epoch' = epoch + 1
    /\ up' = TRUE
    /\ out' = Append(out, ClaimRec(epoch + 1))
    /\ UNCHANGED <<inputs, admitted, written, hist, agentVars>>

\* The deposed holder's append: the fence refuses it behind a later claim.
StaleWrite ==
    /\ stale # NoStale
    /\ out' = IF ~Fence \/ LastClaim(out) = stale.e
              THEN Append(out, GroupRec(stale.map, stale.e, stale.p)) ELSE out
    /\ stale' = NoStale
    /\ UNCHANGED <<inputs, admitted, st, pos, written, hist, epoch, up, agentVars>>

(***************************************************************************)
(* Inputs: admission and node facts.                                       *)
(***************************************************************************)
Holding(n) == {m \in Members : lease[m] = n}

InputUnchanged == UNCHANGED <<st, pos, written, hist, epoch, up, stale, out, agentVars>>

Admit(m) ==
    /\ m \notin admitted /\ Len(inputs) < MaxIn
    /\ admitted' = admitted \cup {m}
    /\ inputs' = Append(inputs, [k |-> "spec", m |-> m])
    /\ InputUnchanged

\* Standing::fact. `blocked` is follow_drain's last read of the drain mark
\* failing. The claim path reads the mark itself, so blocked stops no claim
\* and shows only in facts: the model picks it per fact. Draining is said
\* only with no claim under way and a won claim is named before its guard
\* drops, so with Claim one step a draining fact lists every lease held.
\* `gone` is a node the fold forgets after FORGET_INTERVALS of silence
\* (schedule.rs forget_silent): only one that is down, so forgetting a live
\* node whose facts fail is not modeled.
Fact(n) ==
    /\ nup[n] /\ Len(inputs) < MaxIn
    /\ \E b \in IF Blocking THEN BOOLEAN ELSE {FALSE},
          g \in {None} \cup (IF ForgetDown THEN {o \in Nodes \ {n} : ~nup[o]} ELSE {}) :
        inputs' = Append(inputs, [k |-> "fact", n |-> n, hold |-> Holding(n),
                                  blocked |-> b, draining |-> marked[n], gone |-> g])
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

\* The last group record on `out`.
LastGroup == LET gs == {j \in 1..Len(out) : out[j].k = "group"}
             IN IF gs = {} THEN NoMap ELSE out[CHOOSE j \in gs : \A l \in gs : l <= j].map

PlacesHere(v, m, n) == LET j == InForce(v) IN j # None /\ out[j].map[m] = n

Held(n, m) == IF admitted = Members THEN gsince[n][m] ELSE since[n][m]

AgentUnchanged == UNCHANGED <<inputs, admitted, st, pos, written, hist, epoch, up, stale,
                              out>>

\* read_new folds one record of `out`. A read takes the tail as its target
\* when it starts and keeps it across failed reads; with TailGate the view
\* the node acts on changes only once the read reaches that target.
Read(n) ==
    /\ nup[n] /\ rd[n] < Len(out)
    /\ LET t == IF tgt[n] = 0 THEN Len(out) ELSE tgt[n]
           done == ~TailGate \/ rd[n] + 1 = t
       IN /\ rd' = [rd EXCEPT ![n] = @ + 1]
          /\ view' = IF done THEN [view EXCEPT ![n] = rd[n] + 1] ELSE view
          /\ tgt' = [tgt EXCEPT ![n] = IF done THEN 0 ELSE t]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, early, nup, since, gsince, marked, drains, nodeCrashes, expires, applied>>

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
    /\ UNCHANGED <<lease, early, nup, view, rd, tgt, marked, drains, nodeCrashes, expires, applied>>

\* The node's lease watch shows the member held with time left, and
\* wait_for_chance starts its clocks over. A watch may skip from one free
\* to the next without showing this.
SeeHeld(n, m) ==
    /\ nup[n] /\ lease[m] # None
    /\ since[n][m] > 0 \/ gsince[n][m] > 0
    /\ since' = [since EXCEPT ![n][m] = 0]
    /\ gsince' = [gsince EXCEPT ![n][m] = 0]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, early, nup, view, rd, tgt, marked, drains, nodeCrashes, expires, applied>>

\* The claim path reads the drain mark and claims nothing while it is set.
Claim(n, m) ==
    /\ nup[n] /\ ~marked[n] /\ lease[m] = None /\ m \in admitted
    /\ Cardinality(Holding(n)) < Cap
    /\ \/ ~Barrier
       \/ PlacesHere(view[n], m, n)
       \/ Held(n, m) >= Bound
    /\ lease' = [lease EXCEPT ![m] = n]
    /\ early' = [early EXCEPT ![m] = Held(n, m) < Bound]
    /\ applied' = IF PlacesHere(view[n], m, n) THEN applied \cup {InForce(view[n])} ELSE applied
    \* The claimer waits afresh once it lets the member go.
    /\ since' = [since EXCEPT ![n][m] = 0]
    /\ gsince' = [gsince EXCEPT ![n][m] = 0]
    /\ AgentUnchanged
    /\ UNCHANGED <<nup, view, rd, tgt, marked, drains, nodeCrashes, expires>>

\* A lease lapses: its holder is down, or lost it while alive and stops.
Expire(m) ==
    /\ lease[m] # None
    /\ ~nup[lease[m]] \/ expires < MaxExpire
    /\ expires' = IF nup[lease[m]] THEN expires + 1 ELSE expires
    /\ lease' = [lease EXCEPT ![m] = None]
    /\ AgentUnchanged
    /\ UNCHANGED <<early, nup, since, gsince, view, rd, tgt, marked, drains, nodeCrashes, applied>>

\* The operator marks a node in `drains`.
Drain(n) ==
    /\ ~marked[n] /\ drains < MaxDrain
    /\ marked' = [marked EXCEPT ![n] = TRUE]
    /\ drains' = drains + 1
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, early, nup, since, gsince, view, rd, tgt, nodeCrashes, expires, applied>>

\* A drained node's task stops and releases its lease.
Release(m) ==
    /\ lease[m] # None /\ nup[lease[m]] /\ marked[lease[m]]
    /\ lease' = [lease EXCEPT ![m] = None]
    /\ AgentUnchanged
    /\ UNCHANGED <<early, nup, since, gsince, view, rd, tgt, marked, drains, nodeCrashes, expires, applied>>

NodeCrash(n) ==
    /\ nup[n] /\ nodeCrashes < MaxNodeCrash
    /\ nup' = [nup EXCEPT ![n] = FALSE]
    /\ nodeCrashes' = nodeCrashes + 1
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, early, view, rd, tgt, since, gsince, marked, drains, expires, applied>>

Forgets(j, n) == inputs[j].k = "fact" /\ inputs[j].gone = n

\* The scheduler has folded a forget of n and no fact of n since.
Forgotten(n) ==
    \E j \in 1..written :
        /\ Forgets(j, n)
        /\ \A l \in (j + 1)..written : ~(inputs[l].k = "fact" /\ inputs[l].n = n)

\* A restarted agent reads `out` again and its clocks start over. Without
\* ForgetRace it waits until every forget of it has been folded and, if it
\* is forgotten, the last group record no longer places a member on it.
\* Coming back while still named is the time-only release, felixd #251.
NodeRestart(n) ==
    /\ ~nup[n] /\ Holding(n) = {}
    /\ \/ ForgetRace
       \/ /\ \A j \in (written + 1)..Len(inputs) : ~Forgets(j, n)
          /\ Forgotten(n) => \A m \in DOMAIN LastGroup : LastGroup[m] # n
    /\ nup' = [nup EXCEPT ![n] = TRUE]
    /\ view' = [view EXCEPT ![n] = 0]
    /\ rd' = [rd EXCEPT ![n] = 0]
    /\ tgt' = [tgt EXCEPT ![n] = 0]
    /\ since' = [since EXCEPT ![n] = [m \in Members |-> 0]]
    /\ gsince' = [gsince EXCEPT ![n] = [m \in Members |-> 0]]
    /\ AgentUnchanged
    /\ UNCHANGED <<lease, early, marked, drains, nodeCrashes, expires, applied>>

Init ==
    /\ inputs = [i \in 1..Cardinality(InitAdmitted) |-> [k |-> "spec", m |-> SeqOf(InitAdmitted)[i]]]
    /\ admitted = InitAdmitted
    /\ st = SInit /\ pos = 0 /\ written = 0 /\ hist = <<>>
    /\ epoch = 1 /\ up = TRUE /\ stale = NoStale /\ out = <<ClaimRec(1)>>
    /\ lease = [m \in Members |-> None]
    /\ early = [m \in Members |-> FALSE]
    /\ nup = [n \in Nodes |-> TRUE]
    /\ view = [n \in Nodes |-> 0]
    /\ rd = [n \in Nodes |-> 0]
    /\ tgt = [n \in Nodes |-> 0]
    /\ marked = [n \in Nodes |-> FALSE]
    /\ drains = 0
    /\ since = [n \in Nodes |-> [m \in Members |-> 0]]
    /\ gsince = [n \in Nodes |-> [m \in Members |-> 0]]
    /\ nodeCrashes = 0 /\ expires = 0
    /\ applied = {}

Next ==
    \/ SchedStep \/ SchedCrash \/ SchedTakeover \/ StaleWrite
    \/ \E m \in Members : Admit(m) \/ Expire(m) \/ Release(m)
    \/ \E n \in Nodes : Fact(n) \/ Lapse(n) \/ Read(n) \/ Drain(n)
                        \/ NodeCrash(n) \/ NodeRestart(n)
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

\* What `sys.facts` say after the first p inputs: each node's holdings and
\* which nodes are not silent.
RECURSIVE FactsAt(_)
FactsAt(p) ==
    IF p = 0 THEN [heard |-> [n \in Nodes |-> {}], live |-> {}]
    ELSE LET f == FactsAt(p - 1)
             x == inputs[p]
             g == IF x.k = "fact" THEN x.gone ELSE None
             f1 == CASE x.k = "fact"  -> [heard |-> [f.heard EXCEPT ![x.n] = x.hold], live |-> f.live \cup {x.n}]
                     [] x.k = "lapse" -> [f EXCEPT !.live = @ \ {x.n}]
                     [] OTHER         -> f
         IN IF g = None THEN f1
            ELSE [heard |-> [f1.heard EXCEPT ![g] = {}], live |-> f1.live \ {g}]

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
