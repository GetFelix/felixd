---------------------------- MODULE FelixdSpecs ----------------------------
(***************************************************************************)
(* Spec history on one workload's output stream: which spec each owner    *)
(* runs, and what `felixd check` concludes ran.                           *)
(*                                                                         *)
(* Spec records are numbered 1..Len(Lineage) in `sys.specs` order, and     *)
(* Lineage[s] is spec s's <<module, input>>. Each node sees the           *)
(* `workloads` entry through its own cache view, which only moves         *)
(* forward and may lag: a stale node holds an older spec.                 *)
(*                                                                         *)
(* Three kinds of record land on the stream:                               *)
(*   claim  the spec its owner runs and the floor, the highest spec       *)
(*          claimed so far, its own included.                              *)
(*   nop    a needs-operator record, naming its writer's spec record.     *)
(*          Written by a holder with a problem to record, with no claim.  *)
(*   out    an output, one per input, with its writer's spec as a ghost.  *)
(* Only a claim names its node.                                            *)
(*                                                                         *)
(* Leases and epochs are abstracted to the claim record's fence, which    *)
(* Felixd.tla checks: a claim lands only at the tail its owner read, and  *)
(* an owner writes outputs only while its claim is the last claim or      *)
(* needs-operator record. Any node may start a claim at any time, as if   *)
(* the lease had expired. A refused claim that finds another node's       *)
(* claim past the tail may stop (a later epoch) or claim again (an        *)
(* earlier one).                                                           *)
(*                                                                         *)
(* Knobs, each turned off in a twin that TLC finds a violation in. The    *)
(* agent (crates/agent/src/specs.rs, output.rs):                           *)
(*   FloorCarry    a claim carries the floor, not only its own spec.      *)
(*   RetryFloor    a refused claim raises its floor from the records past *)
(*                 the tail.                                               *)
(*   NopFloor      needs-operator records count toward the floor.         *)
(*   SkipNopFirst  finding the first claim passes over needs-operator     *)
(*                 records.                                                *)
(*   HistoryGuard  a spec whose module or input differ from the first     *)
(*                 claim's is refused.                                     *)
(*   PostCheck     the refusals are made again once the claim is on.      *)
(* `felixd check` (crates/core/src/check.rs, judged_under):                *)
(*   CheckSkipNop  the lineage passes over needs-operator records.        *)
(*   CheckNopFloor needs-operator records raise the floor.                *)
(*   CheckRefuse   a claim below the floor before it did not run.         *)
(*   CheckLineage  a claim whose lineage differs from the first's did not *)
(*                 run.                                                    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,
    Lineage,           \* Lineage[s]: spec record s's <<module, input>>
    MaxClaims,
    MaxNops,
    MaxInputs,
    FloorCarry,
    RetryFloor,
    NopFloor,
    SkipNopFirst,
    HistoryGuard,
    PostCheck,
    CheckSkipNop,
    CheckNopFloor,
    CheckRefuse,
    CheckLineage

ASSUME \A b \in {FloorCarry, RetryFloor, NopFloor, SkipNopFirst, HistoryGuard,
                 PostCheck, CheckSkipNop, CheckNopFloor, CheckRefuse,
                 CheckLineage} : b \in BOOLEAN

\* Spec histories for the configurations: an edit that keeps the module and
\* input, then one that changes the module, or the input.
EditThenModule == <<<<"m1", "in1">>, <<"m1", "in1">>, <<"m2", "in1">>>>
EditThenInput == <<<<"m1", "in1">>, <<"m1", "in1">>, <<"m1", "in2">>>>

Specs == 1..Len(Lineage)

VARIABLES
    stream,  \* wl.<id>.out
    view,    \* view[n]: the spec record node n's `workloads` read names
    pc,      \* idle, leased, claiming, claimed, running, recording
    run,     \* the spec the current attempt runs
    start,   \* the tail the claim is written at
    fl,      \* the floor the claim will carry
    mine,    \* the offset (1-based) of this owner's claim
    ran,     \* ghost: claims whose owner passed the check after its claim
    early    \* ghost: claims whose owner refused them for a reason on the
             \* stream at or before the claim

vars == <<stream, view, pc, run, start, fl, mine, ran, early>>

Max(S) == CHOOSE x \in S \cup {0} : \A y \in S \cup {0} : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

Rec(k, s, f, n, i) == [kind |-> k, spec |-> s, floor |-> f, node |-> n, input |-> i]

Count(k) == Cardinality({i \in 1..Len(stream) : stream[i].kind = k})

\* The last offset in s whose record is of a kind in K, or 0.
LastOf(s, K) == Max({i \in 1..Len(s) : s[i].kind \in K})

\* The first offset in s whose record is of a kind in K, or 0.
FirstOf(s, K) ==
    LET idx == {i \in 1..Len(s) : s[i].kind \in K}
    IN IF idx = {} THEN 0 ELSE Min(idx)

\* The first claim's spec, or 0: `specs::written_under` and `judged_under`.
\* With skip, needs-operator records are passed over; without, a stream
\* that starts with one has no first claim.
FirstClaim(s, skip) ==
    LET i == FirstOf(s, IF skip THEN {"claim", "out"} ELSE {"claim", "out", "nop"})
    IN IF i # 0 /\ s[i].kind = "claim" THEN s[i].spec ELSE 0

\* `specs::claim_floor` over the whole stream: the last claim's floor, raised
\* by needs-operator records after it.
AgentFloor(s) ==
    LET c == LastOf(s, {"claim"})
        nops == IF NopFloor
                THEN {s[i].spec : i \in {j \in c + 1..Len(s) : s[j].kind = "nop"}}
                ELSE {}
    IN Max(nops \cup {IF c = 0 THEN 0 ELSE s[c].floor})

Conflict(s, v) ==
    LET w == FirstClaim(s, SkipNopFirst)
    IN HistoryGuard /\ w # 0 /\ Lineage[w] # Lineage[v]

Stale(s, v) == AgentFloor(s) > v

NextInput(s) == LET o == LastOf(s, {"out"}) IN IF o = 0 THEN 0 ELSE s[o].input + 1

Idle(n) ==
    /\ pc' = [pc EXCEPT ![n] = "idle"]
    /\ run' = [run EXCEPT ![n] = 0]
    /\ start' = [start EXCEPT ![n] = 0]
    /\ fl' = [fl EXCEPT ![n] = 0]
    /\ mine' = [mine EXCEPT ![n] = 0]

Init ==
    /\ stream = <<>>
    /\ view = [n \in Nodes |-> 1]
    /\ pc = [n \in Nodes |-> "idle"]
    /\ run = [n \in Nodes |-> 0]
    /\ start = [n \in Nodes |-> 0]
    /\ fl = [n \in Nodes |-> 0]
    /\ mine = [n \in Nodes |-> 0]
    /\ ran = {}
    /\ early = {}

\* A later spec record reaches node n's view.
Learn(n) ==
    /\ \E v \in Specs :
        /\ v > view[n]
        /\ view' = [view EXCEPT ![n] = v]
    /\ UNCHANGED <<stream, pc, run, start, fl, mine, ran, early>>

\* The check before the lease claim. A stale node writes nothing. A spec
\* that conflicts with the history is recorded, as is any other problem
\* found before the claim; otherwise the node takes the lease to claim.
Begin(n) ==
    /\ pc[n] = "idle"
    /\ ~Stale(stream, view[n])
    /\ \/ /\ ~Conflict(stream, view[n])
          /\ pc' = [pc EXCEPT ![n] = "leased"]
       \/ pc' = [pc EXCEPT ![n] = "recording"]
    /\ run' = [run EXCEPT ![n] = view[n]]
    /\ UNCHANGED <<stream, view, start, fl, mine, ran, early>>

\* The tail read with the epoch floor, and the claim floor below it.
Start(n) ==
    /\ pc[n] = "leased"
    /\ pc' = [pc EXCEPT ![n] = "claiming"]
    /\ start' = [start EXCEPT ![n] = Len(stream)]
    /\ fl' = [fl EXCEPT ![n] = AgentFloor(stream)]
    /\ UNCHANGED <<stream, view, run, mine, ran, early>>

\* `OutputLog::claim`: publish_if at the tail read. A refusal reads the
\* records past it.
ClaimLand(n) ==
    /\ pc[n] = "claiming"
    /\ IF Len(stream) = start[n]
       THEN /\ Count("claim") < MaxClaims
            /\ stream' = Append(stream,
                    Rec("claim", run[n],
                        IF FloorCarry THEN Max({fl[n], run[n]}) ELSE run[n], n, 0))
            /\ mine' = [mine EXCEPT ![n] = Len(stream) + 1]
            /\ pc' = [pc EXCEPT ![n] = "claimed"]
            /\ UNCHANGED <<view, run, start, fl, ran, early>>
       ELSE LET past == start[n] + 1..Len(stream)
                ownNop == \E i \in past : stream[i].kind = "nop" /\ stream[i].spec = run[n]
                other == \E i \in past : stream[i].kind = "claim" /\ stream[i].node # n
                floors == {stream[i].floor : i \in {j \in past : stream[j].kind = "claim"}}
                nops == IF NopFloor
                        THEN {stream[i].spec : i \in {j \in past : stream[j].kind = "nop"}}
                        ELSE {}
            IN \/ /\ (ownNop \/ other)
                  /\ Idle(n)
                  /\ UNCHANGED <<stream, view, ran, early>>
               \/ /\ ~ownNop
                  /\ start' = [start EXCEPT ![n] = Len(stream)]
                  /\ fl' = [fl EXCEPT ![n] =
                        IF RetryFloor THEN Max({fl[n]} \cup floors \cup nops) ELSE fl[n]]
                  /\ UNCHANGED <<stream, view, pc, run, mine, ran, early>>

\* `refuse_stale_or_foreign` again once the claim is on the stream. A
\* refused claim stays there.
Recheck(n) ==
    /\ pc[n] = "claimed"
    /\ LET v == run[n]
           conflict == Conflict(stream, v)
           stale == Stale(stream, v)
       IN IF PostCheck /\ (conflict \/ stale)
          THEN /\ Idle(n)
               /\ early' = IF conflict \/ stream[mine[n]].floor > v
                           THEN early \cup {mine[n]} ELSE early
               /\ UNCHANGED <<stream, view, ran>>
          ELSE /\ pc' = [pc EXCEPT ![n] = "running"]
               /\ ran' = ran \cup {mine[n]}
               /\ UNCHANGED <<stream, view, run, start, fl, mine, early>>

\* An output for the next input, fenced by the claim.
Emit(n) ==
    /\ pc[n] = "running"
    /\ LastOf(stream, {"claim", "nop"}) = mine[n]
    /\ NextInput(stream) < MaxInputs
    /\ stream' = Append(stream, Rec("out", run[n], 0, "none", NextInput(stream)))
    /\ UNCHANGED <<view, pc, run, start, fl, mine, ran, early>>

\* A holder records that the workload needs an operator, under its spec.
Record(n) ==
    /\ pc[n] = "recording"
    /\ Count("nop") < MaxNops
    /\ stream' = Append(stream, Rec("nop", run[n], 0, "none", 0))
    /\ Idle(n)
    /\ UNCHANGED <<view, ran, early>>

\* A crash, a lost lease, or being superseded.
Stop(n) ==
    /\ pc[n] # "idle"
    /\ Idle(n)
    /\ UNCHANGED <<stream, view, ran, early>>

Next == \E n \in Nodes :
    Learn(n) \/ Begin(n) \/ Start(n) \/ ClaimLand(n) \/ Recheck(n)
        \/ Emit(n) \/ Record(n) \/ Stop(n)

Spec == Init /\ [][Next]_vars

Symmetry == Permutations(Nodes)

-----------------------------------------------------------------------------
(* `felixd check`: crates/core/src/check.rs.                                *)

\* The workloads value, used when the stream names no first claim. A
\* node's view never runs ahead of it.
Cached == Max({view[n] : n \in Nodes})

CheckLineageSpec ==
    LET w == FirstClaim(stream, CheckSkipNop) IN IF w # 0 THEN w ELSE Cached

\* Whether the claim at k ran, as `specs_applied` and `judged_under` judge it
\* from the records before it.
CheckFloorBefore(k) ==
    LET s == SubSeq(stream, 1, k - 1)
        c == LastOf(s, {"claim"})
        nops == IF CheckNopFloor
                THEN {s[i].spec : i \in {j \in c + 1..Len(s) : s[j].kind = "nop"}}
                ELSE {}
    IN Max(nops \cup {IF c = 0 THEN 0 ELSE s[c].floor})

CheckRan(k) ==
    /\ ~(CheckRefuse /\ stream[k].spec < CheckFloorBefore(k))
    /\ ~(CheckLineage /\ Lineage[stream[k].spec] # Lineage[CheckLineageSpec])

\* `specs_applied` over the first k records: <<applied, next input, floor>>,
\* where applied is a sequence of <<first input, spec>>.
RECURSIVE Applied(_)
Applied(k) ==
    IF k = 0 THEN <<<<>>, 0, 0>>
    ELSE LET prev == Applied(k - 1)
             ap == prev[1]
             nx == prev[2]
             f == prev[3]
             r == stream[k]
         IN CASE r.kind = "claim" ->
                   LET refused == CheckRefuse /\ r.spec < f
                       lineage == CheckLineage
                                  /\ Lineage[r.spec] # Lineage[CheckLineageSpec]
                       skip == \/ refused
                               \/ lineage
                               \/ Len(ap) > 0 /\ ap[Len(ap)][2] = r.spec
                       popped == IF Len(ap) > 0 /\ ap[Len(ap)][1] = nx
                                 THEN SubSeq(ap, 1, Len(ap) - 1) ELSE ap
                   IN <<IF skip THEN ap ELSE Append(popped, <<nx, r.spec>>), nx, r.floor>>
              [] r.kind = "nop" ->
                   <<ap, nx, IF CheckNopFloor THEN Max({f, r.spec}) ELSE f>>
              [] OTHER -> <<ap, Max({nx, r.input + 1}), f>>

\* The spec whose limits the check replays input i under.
CheckSpecFor(ap, i) ==
    LET es == {j \in 1..Len(ap) : ap[j][1] <= i}
    IN IF es = {} THEN 0 ELSE ap[Max(es)][2]

-----------------------------------------------------------------------------
(* Invariants.                                                             *)

\* Every output was written under the first claim's module and input.
OutputLineage ==
    LET f == FirstOf(stream, {"claim"})
    IN \A i \in 1..Len(stream) :
        stream[i].kind = "out" =>
            /\ f # 0
            /\ Lineage[stream[i].spec] = Lineage[stream[f].spec]

\* Claim floors never decrease along the stream.
FloorMonotone ==
    \A i, j \in 1..Len(stream) :
        (i < j /\ stream[i].kind = "claim" /\ stream[j].kind = "claim")
            => stream[i].floor <= stream[j].floor

\* The specs of claims that ran never decrease along the stream.
RanMonotone ==
    \A i, j \in ran : i < j => stream[i].spec <= stream[j].spec

\* The highest spec claimed or recorded before offset k.
FloorBefore(k) ==
    Max({stream[j].spec : j \in {i \in 1..k - 1 : stream[i].kind \in {"claim", "nop"}}})

\* No owner runs, and no output is written under, a spec below the floor.
NoRunBelowFloor ==
    /\ \A i \in 1..Len(stream) :
        stream[i].kind = "out" => stream[i].spec >= FloorBefore(i)
    /\ \A n \in Nodes :
        pc[n] = "running" => run[n] >= FloorBefore(mine[n])

\* `felixd check` replays each output under the module, input and spec its
\* writer ran, counts every claim that ran, and passes over every claim its
\* owner refused for a reason on the stream at or before it. A claim refused
\* only for a record that landed after it, or never checked, is left out:
\* the stream does not say whether it ran.
CheckMatchesRun ==
    LET ap == Applied(Len(stream))[1]
    IN /\ \A i \in 1..Len(stream) :
            stream[i].kind = "out" =>
                /\ CheckSpecFor(ap, stream[i].input) = stream[i].spec
                /\ Lineage[CheckLineageSpec] = Lineage[stream[i].spec]
       /\ \A k \in ran : CheckRan(k)
       /\ \A k \in early : ~CheckRan(k)

=============================================================================
