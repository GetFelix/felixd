------------------------------- MODULE Felixd -------------------------------
(***************************************************************************)
(* One felixd workload: nodes that claim it through the `leases` cache,   *)
(* take an epoch from the `epochs` cache, and run it by folding its input *)
(* into the `.out` and `.snap` streams with conditional appends.          *)
(*                                                                         *)
(* Time is not modelled as numbers. A holder's local deadline passing is  *)
(* an action (LocalDeadline), and the broker may expire a lease only once *)
(* its holder has stopped by its own clock, unless a clock fault (a       *)
(* suspend of the holder, or the broker's clock jumping) has put the two  *)
(* out of step. An applied renewal puts them back in step.                *)
(*                                                                         *)
(* Requests to Felix's streams travel through `net` and may land at any   *)
(* later time, or after the sender has crashed, timed out or lost the     *)
(* workload. Cache calls are atomic.                                       *)
(*                                                                         *)
(* The claim record (ClaimMarker) is as built (#80): once the claim has   *)
(* its epoch, the owner appends a claim record to `.out` at the tail it   *)
(* read the epoch floor at, before it resumes. A later resume does not    *)
(* claim again; it only checks the last record (Probe). docs/formal.md     *)
(* maps each action to the code.                                           *)
(*                                                                         *)
(* Knobs, each turned off in a twin configuration that TLC finds a        *)
(* violation in:                                                          *)
(*   ClaimMarker     the claim record.                                     *)
(*   EpochFloor      a claim's epoch is above the last records' epochs.   *)
(*   Confirmer       a renewal at the held version right before failing.  *)
(*   LaterEpochWins  reconcile stops on any record from a later epoch.    *)
(*   ReplayGuard     no snapshot while replaying already written outputs. *)
(*   FoldOnEvents    the resume fold uses the last output's offset, not   *)
(*                   the raw tail.                                        *)
(*   FailRecord      a failure is a record on `.out`, appended with       *)
(*                   publish_if, and the spec's `failed` follows from it.  *)
(*   IndexFold       the fold skips outputs by (input, index), so a resume *)
(*                   can start in the middle of a step.                   *)
(* Assumptions, each dropped in a twin:                                    *)
(*   RecreateWhenWritten  `epochs` is recreated only while every live     *)
(*                        epoch is on a record (design.md, Leases).        *)
(*   FaultsInFailWindow   when FALSE, no clock fault falls between an     *)
(*                        applied confirm and the failure write.          *)
(*                        Only a twin without FailRecord needs it.        *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,
    NoNode,
    Outs,              \* Outs[i + 1]: how many outputs input i produces
    MaxEpoch,          \* claims the model makes
    MaxCrashes,
    MaxClockFaults,
    MaxGens,           \* Felix generation-start records
    MaxSnaps,
    MaxRecreates,
    MaxInflight,       \* requests one node may have in flight
    MaxPartials,       \* batches that land in part (DeliverPrefix)
    ClaimMarker,
    EpochFloor,
    Confirmer,
    LaterEpochWins,
    ReplayGuard,
    FoldOnEvents,
    FailRecord,
    IndexFold,
    LocalFaults,       \* a holder may fail the workload on its own
    TimingFaults,      \* local deadlines pass and clock faults happen
    FaultsInFailWindow,
    RecreateWhenWritten

ASSUME /\ NoNode \notin Nodes
       /\ \A b \in {ClaimMarker, EpochFloor, Confirmer, LaterEpochWins,
                    ReplayGuard, FoldOnEvents, FailRecord, IndexFold, LocalFaults, TimingFaults,
                    FaultsInFailWindow, RecreateWhenWritten} : b \in BOOLEAN

\* Workload shapes for the configurations.
One == <<1>>
OneEach == <<1, 1>>
IdleThenTwo == <<0, 2>>
OneThenTwo == <<1, 2>>

N == Len(Outs)

Max(S) == CHOOSE m \in S : \A y \in S : y <= m
Max2(a, b) == IF a > b THEN a ELSE b
Min2(a, b) == IF a < b THEN a ELSE b

\* `c` is the claim that wrote the record. Only the invariants read it.
Rec(k, e, c, i, x) == [k |-> k, e |-> e, c |-> c, i |-> i, x |-> x]
GenRec == Rec("gen", 0, 0, 0, 0)

\* What input i produces, stamped with epoch e by claim c.
StepOut(e, c, i) == [j \in 1..Outs[i + 1] |-> Rec("out", e, c, i, j - 1)]

RECURSIVE ExpectedFrom(_)
ExpectedFrom(i) ==
    IF i >= N THEN <<>>
    ELSE [j \in 1..Outs[i + 1] |-> [i |-> i, x |-> j - 1]] \o ExpectedFrom(i + 1)
Expected == ExpectedFrom(0)

NoLease == [owner |-> NoNode, cid |-> 0, e |-> 0]
NoSkip == [on |-> FALSE, i |-> 0, x |-> 0]
NoSnap == [on |-> FALSE, i |-> 0, t |-> 0]
NoMsg == [n |-> NoNode, s |-> "none", at |-> 0, recs |-> <<>>]

VARIABLES
    out,        \* the .out stream
    snap,       \* the .snap stream
    net,        \* conditional appends in flight
    lease,      \* the workload's entry in `leases`
    ctr,        \* the workload's entry in `epochs`
    spec,       \* the spec's state: "running" or "failed"
    nd,         \* each node's state
    bud,        \* fault and bound counters
    started,    \* ghost: [e, at] once epoch e began, with the .out tail then
    staleFail,  \* ghost: a failure was written by a node not holding the lease,
                \* or its record landed after a later owner began
    poisoned    \* ghost: some node found the streams impossible to resume

vars == <<out, snap, net, lease, ctr, spec, nd, bud, started, staleFail, poisoned>>
ghosts == <<started, staleFail, poisoned>>

NodeInit == [pc |-> "idle", ep |-> 0, cid |-> 0, ok |-> FALSE, desync |-> FALSE,
             up |-> TRUE, kt |-> 0, kst |-> 0, pos |-> 0, skip |-> NoSkip,
             wnext |-> 0, wend |-> 0, batch |-> <<>>, want |-> 0,
             cseen |-> [present |-> FALSE, ver |-> 0], sseen |-> NoSnap,
             await |-> NoMsg, rep |-> "none", nop |-> FALSE]

Init ==
    /\ out = <<>>
    /\ snap = <<>>
    /\ net = {}
    /\ lease = NoLease
    /\ ctr = [present |-> FALSE, val |-> 0, ver |-> 0]
    /\ spec = "running"
    /\ nd = [n \in Nodes |-> NodeInit]
    /\ bud = [claims |-> 0, crashes |-> 0, clock |-> 0, gens |-> 0,
              snaps |-> 0, recreates |-> 0, partials |-> 0]
    /\ started = {}
    /\ staleFail = FALSE
    /\ poisoned = FALSE

-----------------------------------------------------------------------------
(* Helpers *)

NonGen(s) == SelectSeq(s, LAMBDA r : r.k # "gen")
OutputsOf(s) == SelectSeq(s, LAMBDA r : r.k = "out")

\* Position of the last record of a kind in `kinds`, 0 if none.
LastOf(s, kinds) ==
    LET ps == {p \in 1..Len(s) : s[p].k \in kinds}
    IN IF ps = {} THEN 0 ELSE Max(ps)

LastEpoch(s) ==
    LET p == LastOf(s, {"out", "claim", "fail", "nop", "snap"}) IN IF p = 0 THEN 0 ELSE s[p].e

\* Position of the last record on .out that is not a generation start.
LastRec(s) == LastOf(s, {"out", "claim", "fail", "nop"})

Failed(s) == \E j \in 1..Len(s) : s[j].k = "fail"

\* A needs-operator record: no node can run the spec until an operator acts.
\* The model has one spec, so any such record is for it.
NeedsOp(s) == \E j \in 1..Len(s) : s[j].k = "nop"

MaxEpochOn(s) ==
    LET es == {s[p].e : p \in {q \in 1..Len(s) : s[q].k # "gen"}}
    IN IF es = {} THEN 0 ELSE Max(es)

Owns(n) == lease.owner = n /\ lease.cid = nd[n].cid

InFlight(n) == Cardinality({m \in net : m.n = n})

\* After PutLease: running, or stale and not yet stopped.
Live(n) == nd[n].up /\ nd[n].pc \notin {"idle", "epoch", "putepoch", "putlease", "parked"}

AlreadyWritten(n, i, x) ==
    LET s == nd[n].skip
    IN s.on /\ (i < s.i \/ (i = s.i /\ (x <= s.x \/ ~IndexFold)))

Replaying(n, i) == nd[n].skip.on /\ i < nd[n].skip.i

\* The next output record at or after offset w and before offset e.
RECURSIVE NextOut(_, _)
NextOut(w, e) ==
    IF w >= e THEN e ELSE IF out[w + 1].k = "out" THEN w ELSE NextOut(w + 1, e)

\* Compares the first k of recs with the output records from offset w on.
RECURSIVE CheckSkipped(_, _, _, _, _)
CheckSkipped(recs, j, k, w, e) ==
    IF j > k THEN [ok |-> TRUE, w |-> w]
    ELSE LET p == NextOut(w, e)
         IN IF p >= e \/ out[p + 1].i # recs[j].i \/ out[p + 1].x # recs[j].x
            THEN [ok |-> FALSE, w |-> w]
            ELSE CheckSkipped(recs, j + 1, k, p + 1, e)

\* The node gives the workload up.
Left(n) == [nd EXCEPT ![n] = [NodeInit EXCEPT !.up = nd[n].up]]

\* The streams cannot be resumed from. In this model every such state is a
\* bug, since the workload is deterministic.
Poison(n) ==
    /\ nd' = [nd EXCEPT ![n].pc = "failing", ![n].await = NoMsg, ![n].rep = "none"]
    /\ poisoned' = TRUE

Superseded(n) ==
    nd' = [nd EXCEPT ![n].pc = "sup", ![n].await = NoMsg, ![n].rep = "none"]

\* A failure record is on the stream: the workload has failed, and the node
\* writes that into the spec instead of running.
FoundFailure(n) ==
    nd' = [nd EXCEPT ![n].pc = "derive", ![n].await = NoMsg, ![n].rep = "none"]

\* A needs-operator record is on the stream: the node releases the lease and
\* leaves the workload alone.
FoundNeedsOp(n) ==
    nd' = [nd EXCEPT ![n].pc = "parking", ![n].await = NoMsg, ![n].rep = "none"]

Send(n, m, pc) ==
    /\ net' = net \cup {m}
    /\ nd' = [nd EXCEPT ![n].pc = pc, ![n].await = m, ![n].rep = "none"]

-----------------------------------------------------------------------------
(* Claiming *)

Claim(n) ==
    /\ nd[n].up /\ nd[n].pc = "idle" /\ spec = "running"
    /\ lease.owner = NoNode
    /\ bud.claims < MaxEpoch
    /\ lease' = [owner |-> n, cid |-> bud.claims + 1, e |-> 0]
    /\ bud' = [bud EXCEPT !.claims = @ + 1]
    /\ nd' = [nd EXCEPT ![n].pc = "epoch", ![n].cid = bud.claims + 1,
                        ![n].ok = TRUE, ![n].desync = FALSE]
    /\ UNCHANGED <<out, snap, net, ctr, spec, ghosts>>

\* Reads the floor (and the .out tail the marker will expect), then the counter.
TakeEpoch(n) ==
    /\ nd[n].pc = "epoch"
    /\ LET floor == IF EpochFloor THEN Max2(LastEpoch(out), LastEpoch(snap)) ELSE 0
           last == IF ctr.present THEN ctr.val ELSE 0
       IN nd' = [nd EXCEPT ![n].pc = "putepoch", ![n].want = Max2(last, floor) + 1,
                           ![n].kt = Len(out),
                           ![n].cseen = [present |-> ctr.present, ver |-> ctr.ver]]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

PutEpoch(n) ==
    /\ nd[n].pc = "putepoch"
    /\ IF ctr.present = nd[n].cseen.present /\ (~ctr.present \/ ctr.ver = nd[n].cseen.ver)
       THEN /\ ctr' = [present |-> TRUE, val |-> nd[n].want, ver |-> ctr.ver + 1]
            /\ nd' = [nd EXCEPT ![n].pc = "putlease", ![n].ep = nd[n].want, ![n].want = 0,
                                ![n].cseen = NodeInit.cseen]
            /\ UNCHANGED lease
       ELSE /\ lease' = IF Owns(n) THEN NoLease ELSE lease
            /\ nd' = Left(n)
            /\ UNCHANGED ctr
    /\ UNCHANGED <<out, snap, net, spec, bud, ghosts>>

\* Rewrites the lease with the epoch at the claim's version. Like a renewal,
\* an applied write starts a fresh deadline in step with the broker.
PutLease(n) ==
    /\ nd[n].pc = "putlease"
    /\ IF Owns(n)
       THEN /\ lease' = [lease EXCEPT !.e = nd[n].ep]
            /\ nd' = [nd EXCEPT ![n].pc = IF ClaimMarker THEN "marker" ELSE "probe",
                                ![n].ok = TRUE, ![n].desync = FALSE]
       ELSE /\ nd' = Left(n)
            /\ UNCHANGED lease
    /\ UNCHANGED <<out, snap, net, ctr, spec, bud, ghosts>>

\* With FailRecord, a failure record last before the expected tail ends the
\* claim instead.
Marker(n) ==
    /\ nd[n].pc = "marker" /\ nd[n].ok
    /\ InFlight(n) < MaxInflight
    /\ LET p == LastRec(SubSeq(out, 1, Min2(nd[n].kt, Len(out))))
       IN IF FailRecord /\ p > 0 /\ out[p].k = "fail"
          THEN FoundFailure(n) /\ UNCHANGED net
          ELSE IF FailRecord /\ p > 0 /\ out[p].k = "nop"
          THEN FoundNeedsOp(n) /\ UNCHANGED net
          ELSE Send(n, [n |-> n, s |-> "out", at |-> nd[n].kt,
                        recs |-> <<Rec("claim", nd[n].ep, nd[n].cid, 0, 0)>>], "mwait")
    /\ UNCHANGED <<out, snap, lease, ctr, spec, bud, ghosts>>

MarkerAck(n) ==
    /\ nd[n].pc = "mwait" /\ nd[n].rep = "ok"
    /\ nd' = [nd EXCEPT ![n].pc = "rsnap", ![n].kt = @ + 1, ![n].await = NoMsg,
                        ![n].rep = "none"]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* Refused or unanswered: read past the expected tail.
MarkerRetry(n) ==
    /\ nd[n].pc = "mwait" /\ nd[n].rep \in {"refused", "lost"}
    /\ LET found == NonGen(SubSeq(out, nd[n].kt + 1, Len(out)))
       IN IF \E j \in 1..Len(found) : found[j].e > nd[n].ep
          THEN Superseded(n)
          ELSE IF FailRecord /\ Failed(found)
          THEN FoundFailure(n)
          ELSE IF FailRecord /\ NeedsOp(found)
          THEN FoundNeedsOp(n)
          ELSE IF \E j \in 1..Len(found) : found[j].k = "claim" /\ found[j].e = nd[n].ep
          THEN nd' = [nd EXCEPT ![n].pc = "rsnap", ![n].kt = Len(out),
                                ![n].await = NoMsg, ![n].rep = "none"]
          ELSE nd' = [nd EXCEPT ![n].pc = "marker", ![n].kt = Len(out),
                                ![n].await = NoMsg, ![n].rep = "none"]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

-----------------------------------------------------------------------------
(* Resuming *)

\* A resume once the claim is on the stream (OutputLog::claim with no tail):
\* the tail, and the superseded check on the last record of any kind.
Probe(n) ==
    /\ nd[n].pc = "probe"
    /\ LET p == LastRec(out)
       IN IF p > 0 /\ out[p].e > nd[n].ep
          THEN Superseded(n)
          ELSE IF FailRecord /\ p > 0 /\ out[p].k = "fail"
          THEN FoundFailure(n)
          ELSE IF FailRecord /\ p > 0 /\ out[p].k = "nop"
          THEN FoundNeedsOp(n)
          ELSE nd' = [nd EXCEPT ![n].pc = "rsnap", ![n].kt = Len(out)]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

ReadSnap(n) ==
    /\ nd[n].pc = "rsnap"
    /\ LET p == LastOf(snap, {"snap"})
       IN IF p > 0 /\ snap[p].e > nd[n].ep
          THEN Superseded(n)
          ELSE nd' = [nd EXCEPT ![n].pc = "rout", ![n].kst = Len(snap),
                                ![n].sseen = IF p = 0 THEN NoSnap
                                             ELSE [on |-> TRUE, i |-> snap[p].i, t |-> snap[p].x]]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

ReadOut(n) ==
    /\ nd[n].pc = "rout"
    /\ LET T == nd[n].kt
           po == LastOf(SubSeq(out, 1, T), {"out"})
           s == nd[n].sseen
           st == IF s.on THEN s.t ELSE 0
           later == IF FoldOnEvents THEN po > 0 /\ po - 1 >= st ELSE T > st
           bad == \/ T < st
                  \/ later /\ (po = 0 \/ (s.on /\ out[po].i <= s.i))
           sk == IF later THEN [on |-> TRUE, i |-> out[po].i, x |-> out[po].x] ELSE NoSkip
       IN IF po > 0 /\ out[po].e > nd[n].ep
          THEN Superseded(n) /\ UNCHANGED <<started, poisoned>>
          ELSE IF bad
          THEN Poison(n) /\ UNCHANGED started
          ELSE /\ nd' = [nd EXCEPT ![n].pc = "run", ![n].pos = IF s.on THEN s.i + 1 ELSE 0,
                                   ![n].skip = sk, ![n].wnext = st, ![n].wend = T,
                                   ![n].batch = <<>>, ![n].sseen = NoSnap]
               /\ started' = IF ~ClaimMarker /\ ~\E x \in started : x.e = nd[n].ep
                             THEN started \cup {[e |-> nd[n].ep, at |-> T]}
                             ELSE started
               /\ UNCHANGED poisoned
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, staleFail>>

-----------------------------------------------------------------------------
(* Running *)

\* Steps the next input. Outputs already on the stream are checked and
\* dropped; the rest form one batch.
Step(n) ==
    /\ nd[n].pc = "run" /\ nd[n].pos < N
    /\ LET i == nd[n].pos
           recs == StepOut(nd[n].ep, nd[n].cid, i)
           k == Cardinality({j \in 1..Len(recs) : AlreadyWritten(n, i, j - 1)})
           chk == CheckSkipped(recs, 1, k, nd[n].wnext, nd[n].wend)
           b == SubSeq(recs, k + 1, Len(recs))
           done == ~Replaying(n, i)
       IN IF ~chk.ok
          THEN Poison(n)
          ELSE /\ nd' = [nd EXCEPT ![n].wnext = IF done THEN 0 ELSE chk.w, ![n].batch = b,
                                   ![n].skip = IF done THEN NoSkip ELSE @,
                                   ![n].wend = IF done THEN 0 ELSE @,
                                   ![n].pos = IF b = <<>> THEN i + 1 ELSE i,
                                   ![n].pc = IF b = <<>> THEN "post" ELSE "osend"]
               /\ UNCHANGED poisoned
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, started, staleFail>>

OSend(n) ==
    /\ nd[n].pc = "osend" /\ nd[n].ok
    /\ InFlight(n) < MaxInflight
    /\ Send(n, [n |-> n, s |-> "out", at |-> nd[n].kt, recs |-> nd[n].batch], "owait")
    /\ UNCHANGED <<out, snap, lease, ctr, spec, bud, ghosts>>

OAck(n) ==
    /\ nd[n].pc = "owait" /\ nd[n].rep = "ok"
    /\ nd' = [nd EXCEPT ![n].kt = @ + Len(nd[n].batch), ![n].pos = @ + 1,
                        ![n].batch = <<>>, ![n].pc = "post",
                        ![n].await = NoMsg, ![n].rep = "none"]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* output::reconcile on the records past the expected tail.
Reconcile(n) ==
    /\ nd[n].pc = "owait" /\ nd[n].rep \in {"refused", "lost"}
    /\ LET T == Len(out)
           kt == nd[n].kt
           b == nd[n].batch
           found == NonGen(SubSeq(out, kt + 1, T))
           outsF == OutputsOf(found)
           cmp == Min2(Len(outsF), Len(b))
           rest == SubSeq(b, Len(outsF) + 1, Len(b))
           clear == [nd EXCEPT ![n].await = NoMsg, ![n].rep = "none"]
       IN CASE T < kt -> Poison(n)
            [] T = kt -> nd' = [clear EXCEPT ![n].pc = "osend"] /\ UNCHANGED poisoned
            [] LaterEpochWins /\ \E j \in 1..Len(found) : found[j].e > nd[n].ep ->
                    Superseded(n) /\ UNCHANGED poisoned
            [] FailRecord /\ Failed(found) -> FoundFailure(n) /\ UNCHANGED poisoned
            [] \E j \in 1..cmp : outsF[j].i # b[j].i \/ outsF[j].x # b[j].x -> Poison(n)
            [] Len(outsF) > Len(b) ->
                    \* Behind: an earlier owner got further. Resume again.
                    nd' = [clear EXCEPT ![n].pc = "probe", ![n].batch = <<>>]
                    /\ UNCHANGED poisoned
            [] OTHER ->
                    /\ nd' = IF rest = <<>>
                             THEN [clear EXCEPT ![n].kt = T, ![n].pos = @ + 1,
                                                ![n].batch = <<>>, ![n].pc = "post"]
                             ELSE [clear EXCEPT ![n].kt = T, ![n].batch = rest,
                                                ![n].pc = "osend"]
                    /\ UNCHANGED poisoned
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, started, staleFail>>

\* After a step the owner may snapshot or go on. Choosing freely covers every
\* snapshot_every.
Continue(n) ==
    /\ nd[n].pc = "post"
    /\ nd' = [nd EXCEPT ![n].pc = "run"]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

SnapSend(n) ==
    /\ nd[n].pc = "post" /\ nd[n].ok
    /\ bud.snaps < MaxSnaps
    /\ ReplayGuard => ~Replaying(n, nd[n].pos - 1)
    /\ InFlight(n) < MaxInflight
    /\ LET r == Rec("snap", nd[n].ep, nd[n].cid, nd[n].pos - 1, nd[n].kt)
       IN /\ net' = net \cup {[n |-> n, s |-> "snap", at |-> nd[n].kst, recs |-> <<r>>]}
          /\ nd' = [nd EXCEPT ![n].pc = "swait", ![n].batch = <<r>>, ![n].rep = "none",
                              ![n].await = [n |-> n, s |-> "snap", at |-> nd[n].kst,
                                            recs |-> <<r>>]]
    /\ bud' = [bud EXCEPT !.snaps = @ + 1]
    /\ UNCHANGED <<out, snap, lease, ctr, spec, ghosts>>

SSend(n) ==
    /\ nd[n].pc = "ssend" /\ nd[n].ok
    /\ InFlight(n) < MaxInflight
    /\ Send(n, [n |-> n, s |-> "snap", at |-> nd[n].kst, recs |-> nd[n].batch], "swait")
    /\ UNCHANGED <<out, snap, lease, ctr, spec, bud, ghosts>>

SAck(n) ==
    /\ nd[n].pc = "swait" /\ nd[n].rep = "ok"
    /\ nd' = [nd EXCEPT ![n].kst = @ + 1, ![n].pc = "run", ![n].batch = <<>>,
                        ![n].await = NoMsg, ![n].rep = "none"]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

SRefused(n) ==
    /\ nd[n].pc = "swait" /\ nd[n].rep = "refused"
    /\ LET found == NonGen(SubSeq(snap, nd[n].kst + 1, Len(snap)))
       IN IF \E j \in 1..Len(found) : found[j].e > nd[n].ep
          THEN Superseded(n)
          ELSE nd' = [nd EXCEPT ![n].pc = "ssend", ![n].kst = Max2(Len(snap), @),
                                ![n].await = NoMsg, ![n].rep = "none"]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* A snapshot write that errors ends the run; it resumes from the streams.
SLost(n) ==
    /\ nd[n].pc = "swait" /\ nd[n].rep = "lost"
    /\ nd' = [nd EXCEPT ![n].pc = "probe", ![n].batch = <<>>,
                        ![n].await = NoMsg, ![n].rep = "none"]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

-----------------------------------------------------------------------------
(* Failing *)

\* A later epoch was found. Only an applied renewal at the held version shows
\* the lease is still this node's; then epochs went backwards and it fails
\* the workload. Without the Confirmer the node goes by its own clock.
Sup(n) ==
    /\ nd[n].pc = "sup"
    /\ IF nd[n].ok /\ (Confirmer => Owns(n))
       THEN nd' = [nd EXCEPT ![n].pc = "failing",
                             ![n].desync = IF Confirmer THEN FALSE ELSE @]
       ELSE nd' = Left(n)
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* A failure only this node sees: an injected fault, or a trap that depends on
\* the instance's history.
LocalFault(n) ==
    /\ LocalFaults
    /\ nd[n].pc \in {"run", "post"}
    /\ nd' = [nd EXCEPT ![n].pc = "failing"]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

ConfirmFail(n) ==
    /\ Confirmer /\ nd[n].pc = "failing"
    /\ IF spec = "running" /\ Owns(n) /\ nd[n].ok
       THEN /\ nd' = [nd EXCEPT ![n].pc = "confirmed", ![n].desync = FALSE]
            /\ UNCHANGED lease
       ELSE /\ lease' = IF Owns(n) THEN NoLease ELSE lease
            /\ nd' = Left(n)
    /\ UNCHANGED <<out, snap, net, ctr, spec, bud, ghosts>>

\* Without FailRecord: the spec write, guarded by the local fence, then the
\* lease is released.
WriteFail(n) ==
    /\ ~FailRecord
    /\ nd[n].pc = IF Confirmer THEN "confirmed" ELSE "failing"
    /\ nd[n].ok
    /\ IF spec = "running"
       THEN /\ spec' = "failed"
            /\ staleFail' = (staleFail \/ ~Owns(n))
       ELSE UNCHANGED <<spec, staleFail>>
    /\ lease' = IF Owns(n) THEN NoLease ELSE lease
    /\ nd' = Left(n)
    /\ UNCHANGED <<out, snap, net, ctr, bud, started, poisoned>>

\* With FailRecord: the failure record at the tail just read, unless the last
\* record there is from a later epoch or is already a failure. A node that
\* needs an operator sends a needs-operator record the same way, and takes
\* one already there as its own; a failure record does not.
FailSend(n) ==
    /\ FailRecord
    /\ nd[n].pc = IF Confirmer THEN "confirmed" ELSE "failing"
    /\ nd[n].ok
    /\ InFlight(n) < MaxInflight
    /\ LET p == LastRec(out)
       IN IF p > 0 /\ out[p].e > nd[n].ep
          THEN nd' = Left(n) /\ UNCHANGED net
          ELSE IF p > 0 /\ out[p].k = "fail"
          THEN FoundFailure(n) /\ UNCHANGED net
          ELSE IF nd[n].nop /\ p > 0 /\ out[p].k = "nop"
          THEN FoundNeedsOp(n) /\ UNCHANGED net
          ELSE Send(n, [n |-> n, s |-> "out", at |-> Len(out),
                        recs |-> <<Rec(IF nd[n].nop THEN "nop" ELSE "fail",
                                       nd[n].ep, nd[n].cid, 0, 0)>>], "fwait")
    /\ UNCHANGED <<out, snap, lease, ctr, spec, bud, ghosts>>

FailAck(n) ==
    /\ nd[n].pc = "fwait" /\ nd[n].rep = "ok"
    /\ IF nd[n].nop THEN FoundNeedsOp(n) ELSE FoundFailure(n)
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* Refused or unanswered: read the tail again and decide, either at once
\* (record_failure's own loop) or after confirming again (mark_failed's,
\* once that loop gives up).
FailRetry(n) ==
    /\ nd[n].pc = "fwait" /\ nd[n].rep \in {"refused", "lost"}
    /\ \E pc \in {"failing", IF Confirmer THEN "confirmed" ELSE "failing"} :
          nd' = [nd EXCEPT ![n].pc = pc, ![n].await = NoMsg, ![n].rep = "none"]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* No node of this build can run the workload until an operator acts: found
\* before the claim (a module refused or missing, a spec not in the history
\* or conflicting with the output), when the node claims only to record it,
\* or while running (a restore out of fuel). It is recorded like a failure.
NeedsOperator(n) ==
    /\ FailRecord /\ LocalFaults
    /\ nd[n].pc \in {"marker", "run", "post"}
    /\ nd' = [nd EXCEPT ![n].pc = "failing", ![n].nop = TRUE]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* The node releases the lease and leaves the workload alone. It is not
\* failed: the spec stays running, and only a spec change, which the model
\* does not have, would clear the record.
Park(n) ==
    /\ nd[n].pc = "parking"
    /\ lease' = IF Owns(n) THEN NoLease ELSE lease
    /\ nd' = [nd EXCEPT ![n] = [NodeInit EXCEPT !.up = nd[n].up, !.pc = "parked"]]
    /\ UNCHANGED <<out, snap, net, ctr, spec, bud, ghosts>>

\* The spec follows the record, so this write needs no fence.
Derive(n) ==
    /\ nd[n].pc = "derive"
    /\ spec' = "failed"
    /\ lease' = IF Owns(n) THEN NoLease ELSE lease
    /\ nd' = Left(n)
    /\ UNCHANGED <<out, snap, net, ctr, bud, ghosts>>

-----------------------------------------------------------------------------
(* The lease over time *)

\* A renewal that matters: it puts the clocks back in step, or is refused.
Renew(n) ==
    /\ Live(n) /\ nd[n].ok
    /\ nd[n].desync \/ ~Owns(n)
    /\ nd' = IF Owns(n) THEN [nd EXCEPT ![n].desync = FALSE] ELSE Left(n)
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* The holder's own clock reaches 2/3 TTL past its last applied renewal.
LocalDeadline(n) ==
    /\ TimingFaults
    /\ nd[n].pc # "idle" /\ nd[n].ok
    /\ nd' = [nd EXCEPT ![n].ok = FALSE]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

StopLocal(n) ==
    /\ nd[n].pc \notin {"idle", "parked"} /\ ~nd[n].ok
    /\ nd' = Left(n)
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* The holder is suspended (its monotonic clock stops) or the broker's clock
\* jumps: the broker may expire the lease while the holder's fence holds.
ClockFault(n) ==
    /\ TimingFaults
    /\ bud.clock < MaxClockFaults
    /\ nd[n].pc # "idle" /\ nd[n].ok /\ ~nd[n].desync
    /\ FaultsInFailWindow \/ nd[n].pc # "confirmed"
    /\ nd' = [nd EXCEPT ![n].desync = TRUE]
    /\ bud' = [bud EXCEPT !.clock = @ + 1]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, ghosts>>

\* The broker expires the lease once its holder is not holding in step.
Expire ==
    /\ lease.owner # NoNode
    /\ LET h == lease.owner
       IN ~(nd[h].up /\ nd[h].pc # "idle" /\ nd[h].cid = lease.cid
            /\ nd[h].ok /\ ~nd[h].desync)
    /\ lease' = NoLease
    /\ UNCHANGED <<out, snap, net, ctr, spec, nd, bud, ghosts>>

-----------------------------------------------------------------------------
(* Felix and the environment *)

Deliver(m) ==
    /\ m \in net
    /\ LET s == IF m.s = "out" THEN out ELSE snap
           applied == Len(s) = m.at
           s2 == IF applied THEN s \o m.recs ELSE s
       IN /\ out' = IF m.s = "out" THEN s2 ELSE out
          /\ snap' = IF m.s = "snap" THEN s2 ELSE snap
          /\ started' =
                IF applied /\ m.s = "out" /\ m.recs[1].k = "claim"
                   /\ ~\E x \in started : x.e = m.recs[1].e
                THEN started \cup {[e |-> m.recs[1].e, at |-> Len(out)]}
                ELSE started
          /\ nd' = IF nd[m.n].up /\ nd[m.n].await = m /\ nd[m.n].rep = "none"
                   THEN [nd EXCEPT ![m.n].rep = IF applied THEN "ok" ELSE "refused"]
                   ELSE nd
          /\ staleFail' = \/ staleFail
                          \/ /\ applied /\ m.s = "out" /\ m.recs[1].k \in {"fail", "nop"}
                             /\ \E x \in started : x.e > m.recs[1].e
    /\ net' = net \ {m}
    /\ UNCHANGED <<lease, ctr, spec, bud, poisoned>>

\* A multi-record batch lands in part: a Quorum leader replicated a prefix of
\* it and lost the shard, and the follower promoted with that prefix keeps it.
\* The sender gets an error.
DeliverPrefix(m) ==
    /\ m \in net /\ m.s = "out" /\ Len(m.recs) > 1 /\ Len(out) = m.at
    /\ bud.partials < MaxPartials
    /\ \E k \in 1..(Len(m.recs) - 1) : out' = out \o SubSeq(m.recs, 1, k)
    /\ nd' = IF nd[m.n].up /\ nd[m.n].await = m /\ nd[m.n].rep = "none"
             THEN [nd EXCEPT ![m.n].await = NoMsg, ![m.n].rep = "lost"]
             ELSE nd
    /\ net' = net \ {m}
    /\ bud' = [bud EXCEPT !.partials = @ + 1]
    /\ UNCHANGED <<snap, lease, ctr, spec, ghosts>>

\* The sender gives up waiting. The request may still land.
Timeout(n) ==
    /\ nd[n].await # NoMsg /\ nd[n].rep = "none"
    /\ InFlight(n) < MaxInflight
    /\ nd' = [nd EXCEPT ![n].await = NoMsg, ![n].rep = "lost"]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* A new shard leader appends a record that takes an offset and is never read.
GenStart ==
    /\ bud.gens < MaxGens
    /\ \/ out' = Append(out, GenRec) /\ UNCHANGED snap
       \/ snap' = Append(snap, GenRec) /\ UNCHANGED out
    /\ bud' = [bud EXCEPT !.gens = @ + 1]
    /\ UNCHANGED <<net, lease, ctr, spec, nd, ghosts>>

Crash(n) ==
    /\ nd[n].up /\ bud.crashes < MaxCrashes
    /\ nd' = [Left(n) EXCEPT ![n].up = FALSE]
    /\ bud' = [bud EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, ghosts>>

\* A restarted agent claims only through Claim, so it never adopts its old
\* lease.
Restart(n) ==
    /\ ~nd[n].up
    /\ nd' = [nd EXCEPT ![n].up = TRUE]
    /\ UNCHANGED <<out, snap, net, lease, ctr, spec, bud, ghosts>>

\* Epochs that may still be written: by a node past TakeEpoch or a request in
\* flight.
Unwritten ==
    {nd[n].want : n \in {m \in Nodes : nd[m].pc = "putepoch"}}
    \cup {nd[n].ep : n \in {m \in Nodes : Live(m) \/ nd[m].pc = "putlease"}}
    \cup {m.recs[1].e : m \in net}

AllWritten == \A e \in Unwritten : e <= Max2(MaxEpochOn(out), MaxEpochOn(snap))

\* The `epochs` cache is recreated and starts again.
Recreate ==
    /\ bud.recreates < MaxRecreates
    /\ ctr.present
    /\ RecreateWhenWritten => AllWritten
    /\ ctr' = [present |-> FALSE, val |-> 0, ver |-> 0]
    /\ bud' = [bud EXCEPT !.recreates = @ + 1]
    /\ UNCHANGED <<out, snap, net, lease, spec, nd, ghosts>>

-----------------------------------------------------------------------------

NodeStep(n) ==
    \/ Claim(n) \/ TakeEpoch(n) \/ PutEpoch(n) \/ PutLease(n)
    \/ Marker(n) \/ MarkerAck(n) \/ MarkerRetry(n)
    \/ Probe(n) \/ ReadSnap(n) \/ ReadOut(n)
    \/ Step(n) \/ OSend(n) \/ OAck(n) \/ Reconcile(n)
    \/ Continue(n) \/ SnapSend(n) \/ SSend(n) \/ SAck(n) \/ SRefused(n) \/ SLost(n)
    \/ Sup(n) \/ ConfirmFail(n) \/ WriteFail(n)
    \/ FailSend(n) \/ FailAck(n) \/ FailRetry(n) \/ Derive(n) \/ Park(n)
    \/ Renew(n) \/ StopLocal(n) \/ Restart(n)

NodeFault(n) ==
    \/ Crash(n) \/ LocalDeadline(n) \/ ClockFault(n) \/ LocalFault(n) \/ NeedsOperator(n)
    \/ Timeout(n)

Next ==
    \/ \E n \in Nodes : NodeStep(n) \/ NodeFault(n)
    \/ \E m \in net : Deliver(m) \/ DeliverPrefix(m)
    \/ Expire
    \/ GenStart
    \/ Recreate

Fairness ==
    /\ \A n \in Nodes : WF_vars(NodeStep(n))
    /\ WF_vars(\E m \in net : Deliver(m))
    /\ WF_vars(Expire)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

Symmetry == Permutations(Nodes)

-----------------------------------------------------------------------------
(* Properties *)

OutsOn == OutputsOf(out)

\* The output records are the deterministic outputs of the inputs, in order:
\* no duplicate, no gap.
OutputsExact ==
    /\ Len(OutsOn) <= Len(Expected)
    /\ \A j \in 1..Len(OutsOn) : OutsOn[j].i = Expected[j].i /\ OutsOn[j].x = Expected[j].x

Monotone(s) ==
    \A p, q \in 1..Len(s) :
        (p < q /\ s[p].k # "gen" /\ s[q].k # "gen") => s[p].e <= s[q].e

EpochMonotone == Monotone(out) /\ Monotone(snap)

\* Once an owner has begun (its claim record landed, or without the marker, it
\* read the tail to resume), nothing from an earlier epoch lands after.
NoInterleave ==
    \A st \in started : \A p \in 1..Len(out) :
        (out[p].k # "gen" /\ out[p].e < st.e) => p <= st.at

\* No stale node fails the workload. Without FailRecord: no node writes
\* `failed` while the lease is not its own. With it: `failed` follows a
\* failure record, that record landed before any later owner began, and
\* nothing but a generation start lands after it. The same holds for a
\* needs-operator record, so it is written once and every later claimer
\* leaves the workload alone.
NoStaleFailure ==
    /\ ~staleFail
    /\ FailRecord =>
         /\ spec = "failed" => Failed(out)
         /\ \A p, q \in 1..Len(out) :
               (p < q /\ out[p].k \in {"fail", "nop"}) => out[q].k = "gen"

NoPoison == ~poisoned

\* No two claims run, or write, with the same epoch.
UniqueEpoch ==
    \A n \in Nodes : Live(n) =>
        /\ \A m \in Nodes \ {n} : Live(m) => nd[m].ep # nd[n].ep
        /\ \A p \in 1..Len(out) :
              (out[p].k # "gen" /\ out[p].e = nd[n].ep) => out[p].c = nd[n].cid
        /\ \A p \in 1..Len(snap) :
              (snap[p].k # "gen" /\ snap[p].e = nd[n].ep) => snap[p].c = nd[n].cid
        /\ \A m \in net : m.recs[1].e = nd[n].ep => m.recs[1].c = nd[n].cid

\* A snapshot (X, T): outputs below T are of inputs up to X, the rest later.
SnapshotsSplit ==
    \A q \in 1..Len(snap) : snap[q].k = "snap" =>
        /\ snap[q].x <= Len(out)
        /\ \A p \in 1..Len(out) : out[p].k = "out" =>
              ((p - 1 < snap[q].x) <=> (out[p].i <= snap[q].i))

\* A free, running workload is claimed, unless it is left to an operator.
FreeIsClaimed ==
    (lease.owner = NoNode /\ spec = "running")
        ~> (lease.owner # NoNode \/ spec # "running" \/ NeedsOp(out))

\* Every output is written, or the workload failed or is left to an operator.
EventuallyDone == <>(Len(OutsOn) = Len(Expected) \/ spec # "running" \/ NeedsOp(out))

=============================================================================
