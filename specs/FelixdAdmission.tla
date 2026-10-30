-------------------------- MODULE FelixdAdmission --------------------------
(***************************************************************************)
(* Admission (#114): the decisions on `wl.admission.out`, the projector    *)
(* that records the accepted ones on `sys.specs`, and the gate a node      *)
(* passes a spec record through before it runs it.                         *)
(*                                                                         *)
(* The rule. Once the init marker exists, a spec record for workload X     *)
(* runs only if the decision it names is on `wl.admission.out`, accepted   *)
(* X and exactly that spec, and no verified record for X in the part of    *)
(* `sys.specs` the node read names a later decision. A record is verified  *)
(* when its decision is all of those things. Reserved ids are refused      *)
(* through intake. The projector copies each accepted decision to          *)
(* `sys.specs` with a conditional append at the tail it read; a decision   *)
(* whose module is missing gets a skip record instead. Its progress is the *)
(* latest decision a verified record names, so it never needs to remember *)
(* anything of its own across a restart or failover.                       *)
(*                                                                         *)
(* Admission's decisions are outputs of an ordinary workload, so their     *)
(* exactly-once and fencing are Felixd.tla's: here a decision lands in one *)
(* step. The marker is written by `felixd init` and read fresh, so the     *)
(* model starts after it with the gate on, and with a record written       *)
(* before admission still on `sys.specs`, where `workloads` points.        *)
(*                                                                         *)
(* The adversary may append any spec record for X to `sys.specs` (any     *)
(* spec, any decision offset including none, past the tail or a copy of an *)
(* old one) and point `workloads` at any record. Projectors crash and a    *)
(* stale one races a new one; nodes crash and read a prefix of `sys.specs` *)
(* that only grows (a node that read less after a restart is refused by   *)
(* the claim floor, which FelixdSpecs.tla checks).                         *)
(*                                                                         *)
(* Knobs, each turned off in a twin that TLC finds a violation in:         *)
(*   GateOnMarker    the gate is on iff the marker exists, and reads every *)
(*                   record up to the tail, not only those before the one  *)
(*                   `workloads` points at.                                *)
(*   VerifiedOnly    only verified records count as naming a later         *)
(*                   decision.                                             *)
(*   LaterByDecision a record is superseded by a later decision, not a     *)
(*                   later record.                                         *)
(*   RecordSkips     the projector writes skip records and derives its     *)
(*                   progress from every verified record.                  *)
(*   ReserveIds      admission refuses reserved ids.                       *)
(*   CondWrite       projections are conditional appends.                  *)
(*   FreshFold       a node judges each record against the decisions as    *)
(*                   they are now, never by a verdict kept from an earlier *)
(*                   read.                                                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Projectors,
    Nodes,
    SpecVals,          \* the specs a request may ask for, "stop" among them
    MaxDecisions,
    MaxReserved,       \* requests for a reserved id
    Rejections,        \* admission may reject a request
    ModuleFaults,      \* a module may be missing, and uploaded later
    MaxForges,         \* records the adversary appends
    MaxRepoints,       \* times it points `workloads` elsewhere
    MaxStarts,         \* projector starts
    MaxCrashes,        \* projector and node crashes
    GateOnMarker,
    VerifiedOnly,
    LaterByDecision,
    RecordSkips,
    ReserveIds,
    CondWrite,
    FreshFold

ASSUME /\ {"a", "stop"} \subseteq SpecVals
       /\ \A b \in {Rejections, ModuleFaults, GateOnMarker, VerifiedOnly,
                    LaterByDecision, RecordSkips, ReserveIds, CondWrite,
                    FreshFold} : b \in BOOLEAN

\* Spec sets for the configurations.
AStop == {"a", "stop"}
ABStop == {"a", "b", "stop"}

X == "x"            \* the workload
Sys == "admission"  \* a reserved id

None == [d |-> 0, s |-> "none"]

\* A record on `sys.specs`: kind spec, skip or adm (admission's own, which
\* `felixd init` writes), the id, the spec, the decision it names (0 for
\* none) and, for the invariants only, who wrote it.
Rec(k, id, s, d, w) == [k |-> k, id |-> id, s |-> s, d |-> d, w |-> w]

Max2(a, b) == IF a > b THEN a ELSE b
MaxOf(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x

VARIABLES
    specs,    \* sys.specs
    dec,      \* wl.admission.out: [id, s, acc, mod]
    ptr,      \* the record X's `workloads` entry points at
    pj,       \* each projector's state
    nd,       \* each node's state
    bud,      \* fault and bound counters
    skipped   \* ghost: decisions a projector passed over for good

vars == <<specs, dec, ptr, pj, nd, bud, skipped>>

\* pc: idle, run, point (recorded, `workloads` not yet written). tl: the
\* `sys.specs` tail it read. next: the next decision to look at. at: the
\* record to point `workloads` at.
ProjInit == [pc |-> "idle", tl |-> 0, next |-> 1, at |-> 0]

\* run: what the last check started, None after a refusal or a crash, or
\* [d, "stop"] for a stop. hw: the latest decision the node acted on. view,
\* pt and dl: the `sys.specs` prefix, the record checked and the decisions
\* there were at the last check. bad: records judged unverified for good
\* (only without FreshFold).
NodeInit == [run |-> None, hw |-> 0, view |-> 0, chk |-> FALSE, pt |-> 0,
             dl |-> 0, bad |-> {}]

Init ==
    /\ specs = <<Rec("spec", X, "a", 0, "init"), Rec("adm", Sys, "a", 0, "init")>>
    /\ dec = <<>>
    /\ ptr = 1
    /\ pj = [p \in Projectors |-> ProjInit]
    /\ nd = [n \in Nodes |-> NodeInit]
    /\ bud = [starts |-> 0, crashes |-> 0, forges |-> 0, repoints |-> 0, reserved |-> 0]
    /\ skipped = {}

-----------------------------------------------------------------------------
(* Helpers *)

\* Record r names a decision among the first dl that accepted its id and
\* exactly its spec.
VerifiedIn(r, dl) ==
    /\ r.k \in {"spec", "skip"}
    /\ r.d \in 1..dl
    /\ dec[r.d].acc
    /\ dec[r.d].id = r.id
    /\ dec[r.d].s = r.s

Verified(r) == VerifiedIn(r, Len(dec))

IsX(r) == r.k = "spec" /\ r.id = X

\* The projector's progress from the first t records: past the latest
\* decision any verified record, projection or skip, names.
Progress(t) == 1 + MaxOf({specs[j].d : j \in {i \in 1..t : Verified(specs[i])}})

\* Without RecordSkips: the latest decision a verified spec record of `id`
\* among the first t names, all the projector keeps (`Recorded`).
LatestSpec(t, id) ==
    MaxOf({specs[j].d : j \in {i \in 1..t : specs[i].k = "spec" /\ specs[i].id = id
                                            /\ Verified(specs[i])}})

\* The last record among the first t that names X's latest verified decision.
NewestAt(t) ==
    LET js == {j \in 1..t : IsX(specs[j]) /\ Verified(specs[j])}
        top == MaxOf({specs[j].d : j \in js})
    IN MaxOf({j \in js : specs[j].d = top})

\* `specs::project` only moves `workloads` forward.
Forward(o) == IF o > ptr THEN o ELSE ptr

-----------------------------------------------------------------------------
(* Admission *)

\* Admission decides a request for X.
Decide ==
    /\ Len(dec) < MaxDecisions
    /\ \E s \in SpecVals, acc \in (IF Rejections THEN BOOLEAN ELSE {TRUE}),
          mod \in (IF ModuleFaults THEN BOOLEAN ELSE {TRUE}) :
          dec' = Append(dec, [id |-> X, s |-> s, acc |-> acc, mod |-> mod])
    /\ UNCHANGED <<specs, ptr, pj, nd, bud, skipped>>

\* A request through intake for a reserved id.
DecideReserved ==
    /\ Len(dec) < MaxDecisions
    /\ bud.reserved < MaxReserved
    /\ dec' = Append(dec, [id |-> Sys, s |-> "a", acc |-> ~ReserveIds, mod |-> TRUE])
    /\ bud' = [bud EXCEPT !.reserved = @ + 1]
    /\ UNCHANGED <<specs, ptr, pj, nd, skipped>>

\* A missing module is uploaded after its request was decided.
Upload ==
    /\ ModuleFaults
    /\ \E d \in 1..Len(dec) :
          /\ ~dec[d].mod
          /\ dec' = [dec EXCEPT ![d].mod = TRUE]
    /\ UNCHANGED <<specs, ptr, pj, nd, bud, skipped>>

-----------------------------------------------------------------------------
(* The projector *)

\* A node takes admission's lease: it reads `sys.specs` to its tail, takes
\* its progress from the records, and points `workloads` at X's latest
\* record in case the last projector stopped before it did.
Start(p) ==
    /\ pj[p].pc = "idle"
    /\ bud.starts < MaxStarts
    /\ pj' = [pj EXCEPT ![p] = [pc |-> "run", tl |-> Len(specs),
                                next |-> IF RecordSkips THEN Progress(Len(specs)) ELSE 1,
                                at |-> 0]]
    /\ ptr' = Forward(NewestAt(Len(specs)))
    /\ bud' = [bud EXCEPT !.starts = @ + 1]
    /\ UNCHANGED <<specs, dec, nd, skipped>>

\* The next decision: passed over if rejected, otherwise recorded (or a skip
\* record if its module is missing) with a conditional append at the tail
\* read. A refused append reads the records past the tail.
Step(p) ==
    /\ pj[p].pc = "run"
    /\ pj[p].next <= Len(dec)
    /\ LET d == pj[p].next
           D == dec[d]
           r == Rec(IF D.mod THEN "spec" ELSE "skip", D.id, D.s, d, "proj")
       IN IF ~D.acc \/ (~RecordSkips /\ d <= LatestSpec(pj[p].tl, D.id))
          THEN /\ pj' = [pj EXCEPT ![p].next = d + 1]
               /\ UNCHANGED <<specs, skipped>>
          ELSE IF ~D.mod /\ ~RecordSkips
          THEN /\ pj' = [pj EXCEPT ![p].next = d + 1]
               /\ skipped' = skipped \cup {d}
               /\ UNCHANGED specs
          ELSE IF ~CondWrite \/ Len(specs) = pj[p].tl
          THEN /\ specs' = Append(specs, r)
               /\ pj' = [pj EXCEPT ![p] =
                     [pc |-> IF D.mod /\ D.id = X THEN "point" ELSE "run",
                      tl |-> Len(specs) + 1, next |-> d + 1, at |-> Len(specs) + 1]]
               /\ skipped' = IF D.mod THEN skipped ELSE skipped \cup {d}
          ELSE /\ pj' = [pj EXCEPT ![p].tl = Len(specs),
                                   ![p].next = IF RecordSkips
                                               THEN Max2(d, Progress(Len(specs)))
                                               ELSE d]
               /\ UNCHANGED <<specs, skipped>>
    /\ UNCHANGED <<dec, ptr, nd, bud>>

\* The `workloads` write after a projection.
Point(p) ==
    /\ pj[p].pc = "point"
    /\ ptr' = Forward(pj[p].at)
    /\ pj' = [pj EXCEPT ![p].pc = "run"]
    /\ UNCHANGED <<specs, dec, nd, bud, skipped>>

\* A crash or a lost lease. A stale projector is one that has not crashed.
CrashProjector(p) ==
    /\ pj[p].pc # "idle"
    /\ bud.crashes < MaxCrashes
    /\ pj' = [pj EXCEPT ![p] = ProjInit]
    /\ bud' = [bud EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<specs, dec, ptr, nd, skipped>>

-----------------------------------------------------------------------------
(* The adversary *)

\* Any spec record for X: any spec, naming no decision, any decision or one
\* past the tail. Copies of earlier records are among them.
Forge ==
    /\ bud.forges < MaxForges
    /\ \E s \in SpecVals, d \in 0..(MaxDecisions + 1) :
          specs' = Append(specs, Rec("spec", X, s, d, "adv"))
    /\ bud' = [bud EXCEPT !.forges = @ + 1]
    /\ UNCHANGED <<dec, ptr, pj, nd, skipped>>

\* `workloads` pointed at any record.
Repoint ==
    /\ bud.repoints < MaxRepoints
    /\ \E o \in 1..Len(specs) :
          /\ o # ptr
          /\ ptr' = o
    /\ bud' = [bud EXCEPT !.repoints = @ + 1]
    /\ UNCHANGED <<specs, dec, pj, nd, skipped>>

-----------------------------------------------------------------------------
(* Nodes *)

\* Node n reads `sys.specs` up to L, at least as far as before, and checks
\* the record `workloads` points at (`check_admitted`).
Check(n, L) ==
    LET r == specs[ptr]
        bad == IF FreshFold THEN {}
               ELSE nd[n].bad \cup {j \in (nd[n].view + 1)..L : ~Verified(specs[j])}
        gateOn == GateOnMarker \/ \E j \in 1..(ptr - 1) : specs[j].k = "adm"
        scope == IF GateOnMarker THEN 1..L ELSE 1..(ptr - 1)
        counts(j) == /\ IsX(specs[j])
                     /\ j \notin bad
                     /\ IF VerifiedOnly THEN Verified(specs[j]) ELSE specs[j].d > 0
        later == IF LaterByDecision
                 THEN \E j \in scope : counts(j) /\ specs[j].d > r.d
                 ELSE \E j \in scope : counts(j) /\ j > ptr
        result == IF ~IsX(r) THEN None
                  ELSE IF ~gateOn THEN [d |-> r.d, s |-> r.s]
                  ELSE IF Verified(r) /\ ~later THEN [d |-> r.d, s |-> r.s]
                  ELSE None
    IN /\ ptr <= L
       /\ nd[n].view <= L
       /\ nd' = [nd EXCEPT ![n] =
             [run |-> result,
              hw |-> IF result = None THEN nd[n].hw ELSE Max2(nd[n].hw, result.d),
              view |-> L, chk |-> TRUE, pt |-> ptr, dl |-> Len(dec), bad |-> bad]]
       /\ UNCHANGED <<specs, dec, ptr, pj, bud, skipped>>

CrashNode(n) ==
    /\ nd[n].chk
    /\ bud.crashes < MaxCrashes
    /\ nd' = [nd EXCEPT ![n].run = None, ![n].chk = FALSE]
    /\ bud' = [bud EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<specs, dec, ptr, pj, skipped>>

-----------------------------------------------------------------------------

Next ==
    \/ Decide \/ DecideReserved \/ Upload \/ Forge \/ Repoint
    \/ \E p \in Projectors : Start(p) \/ Step(p) \/ Point(p) \/ CrashProjector(p)
    \/ \E n \in Nodes : CrashNode(n) \/ \E L \in 1..Len(specs) : Check(n, L)

Spec == Init /\ [][Next]_vars

\* Projectors and nodes keep taking their steps, and a node eventually
\* reads `sys.specs` to its tail.
LiveSpec ==
    /\ Spec
    /\ \A p \in Projectors : WF_vars(Start(p)) /\ WF_vars(Step(p)) /\ WF_vars(Point(p))
    /\ \A n \in Nodes : WF_vars(Check(n, Len(specs)))

Symmetry == Permutations(Projectors)

-----------------------------------------------------------------------------
(* Invariants *)

Running(v) == v.s \notin {"none", "stop"}

\* What the rule says node n's last check runs, from the records and the
\* decisions it read: the record checked, if it names the latest decision
\* any verified record of X there names and that is not a stop.
TV(n, j) == IsX(specs[j]) /\ VerifiedIn(specs[j], nd[n].dl)

Newest(n) == MaxOf({specs[j].d : j \in {i \in 1..nd[n].view : TV(n, i)}})

Expected(n) ==
    LET r == specs[nd[n].pt]
    IN IF TV(n, nd[n].pt) /\ r.d = Newest(n) /\ r.s # "stop"
       THEN [d |-> r.d, s |-> r.s]
       ELSE None

\* What runs for X is the spec of its latest verified decision, if the node
\* was pointed at a record of it, and nothing if that decision is a stop.
OnlyAdmittedRuns ==
    \A n \in Nodes : nd[n].chk =>
        (IF Running(nd[n].run) THEN nd[n].run ELSE None) = Expected(n)

\* A node never acts on a decision older than one it acted on: a stop or a
\* newer version is never undone.
NoRollback ==
    \A n \in Nodes : nd[n].run.s # "none" => nd[n].run.d = nd[n].hw

\* No projector records a decision a projector passed over for good.
SkippedStaysSkipped ==
    \A j \in 1..Len(specs) :
        (specs[j].w = "proj" /\ specs[j].k = "spec") => specs[j].d \notin skipped

\* Projectors record each decision at most once, in decision order.
ProjectedAtMostOnce ==
    \A i, j \in 1..Len(specs) :
        (i < j /\ specs[i].w = "proj" /\ specs[j].w = "proj") => specs[i].d < specs[j].d

\* No request for a reserved id is accepted.
ReservedNeverViaIntake ==
    \A d \in 1..Len(dec) : dec[d].id = Sys => ~dec[d].acc

\* An accepted request whose module is there is eventually acted on, by a
\* node running it or something later.
LiveRuns ==
    \A d \in 1..MaxDecisions :
        (d <= Len(dec) /\ dec[d].acc /\ dec[d].id = X /\ dec[d].mod)
            ~> (\E n \in Nodes : nd[n].hw >= d)

=============================================================================
