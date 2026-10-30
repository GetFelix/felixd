---------------------------- MODULE FelixdMerge ----------------------------
(***************************************************************************)
(* The merge log of a workload with several inputs, `wl.<id>.merge`, and  *)
(* the outputs stepped from it on `wl.<id>.out`.                          *)
(*                                                                         *)
(* A holder takes an epoch above the counter and the last `.out` record,  *)
(* reading the `.out` tail as it does. It claims `.out` at that tail, then *)
(* claims the merge log at a tail it reads then, unless the last record    *)
(* there is from a later epoch (it stops) or its own (it does not claim    *)
(* again). It resumes from the last output: it steps the entries from the *)
(* fold's next step up to its claim, which covers entries an earlier      *)
(* holder merged and never stepped, and then merges the inputs' records   *)
(* in any order, up to PerWrite in one append, and steps each batch once   *)
(* it is on the log. Every append to either stream is `publish_if` at the *)
(* tail its holder expects, stamped with its epoch.                        *)
(*                                                                         *)
(* Leases are reduced to the fence, which Felixd.tla checks: any idle node *)
(* may begin at any time, as if the lease had expired, and a stale holder *)
(* keeps running until a record from a later epoch stops it. A frozen     *)
(* holder is one that takes no steps for a while. Requests travel through *)
(* `net`: they land when delivered, if the tail is still the one they     *)
(* expect, and their answer may be lost, or the sender may stop waiting   *)
(* while they are still in flight. A batch of entries may land in part.   *)
(*                                                                         *)
(* An input record is <<input, offset>>, offsets from 1, and its output   *)
(* is a function of it. A step's number is its entry's offset on the      *)
(* merge log, and an output names it.                                     *)
(*                                                                         *)
(* Knobs, each turned off in a twin that TLC finds a violation in         *)
(* (crates/agent/src/merge.rs, runner.rs):                                *)
(*   MergeClaim     a holder appends a claim before its first entry.      *)
(*   ClaimCheck     it stops if the last record is from a later epoch.    *)
(*   OutFirst       it claims the merge log after its `.out` claim lands. *)
(*   CatchUp        it steps the entries before its claim before merging. *)
(*   SameEpochSkip  it does not claim again over a record of its epoch.   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,
    NoNode,
    Ins,               \* Ins[i]: how many records input i has
    MaxBegins,         \* holders the model starts
    MaxCrashes,
    MaxLost,           \* lost answers and timeouts
    MaxPartials,       \* batches that land in part
    MaxRecreates,      \* times the `epochs` cache is recreated
    PerWrite,          \* the most entries one append holds
    MergeClaim,
    ClaimCheck,
    OutFirst,
    CatchUp,
    SameEpochSkip

ASSUME /\ NoNode \notin Nodes
       /\ PerWrite >= 1
       /\ \A b \in {MergeClaim, ClaimCheck, OutFirst, CatchUp, SameEpochSkip} : b \in BOOLEAN

\* Input shapes for the configurations.
OneEach == <<1, 1>>
TwoOne == <<2, 1>>
TwoEach == <<2, 2>>

NI == Len(Ins)
Zero == [i \in 1..NI |-> 0]

Max2(a, b) == IF a > b THEN a ELSE b
MaxOf(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y

\* `h` is the holder that wrote the record; only the invariants read it.
\* An entry has input `i`, offset `o` and the cursor `nx` after it. An
\* output names its step `s` and carries its record's <<i, o>>.
Rec(k, e, h, s, i, o, nx) == [k |-> k, e |-> e, h |-> h, s |-> s, i |-> i, o |-> o, nx |-> nx]

NoMsg == [n |-> NoNode, s |-> "none", at |-> 0, recs |-> <<>>]

VARIABLES
    merge,     \* wl.<id>.merge
    out,       \* wl.<id>.out
    net,       \* conditional appends in flight
    ctr,       \* the `epochs` counter, 0 once recreated
    nd,        \* each node's state
    bud,       \* fault and bound counters
    mstarted,  \* ghost: [e, at] once epoch e began on the merge log
    ostarted   \* ghost: [e, at] once epoch e began on `.out`

vars == <<merge, out, net, ctr, nd, bud, mstarted, ostarted>>

\* pc: idle, oclaim, oclaimw, mread, mclaim, mclaimw, probe, resume, run,
\* ow (an output in flight), mw (a merge append in flight).
\* ot and mt: the `.out` and merge log tails the holder expects. nx: each
\* input's next offset after the merge log's last record. todo: merge
\* offsets to step. bat: the entries of the merge append in flight not yet
\* found on the log; land: the offsets of those that are.
NodeInit == [pc |-> "idle", e |-> 0, h |-> 0, oc |-> FALSE, ot |-> 0, mt |-> 0,
             nx |-> Zero, todo |-> <<>>, bat |-> <<>>, land |-> <<>>,
             aw |-> NoMsg, rep |-> "none"]

Init ==
    /\ merge = <<>>
    /\ out = <<>>
    /\ net = {}
    /\ ctr = 0
    /\ nd = [n \in Nodes |-> NodeInit]
    /\ bud = [begins |-> 0, crashes |-> 0, lost |-> 0, partials |-> 0, recreates |-> 0]
    /\ mstarted = {}
    /\ ostarted = {}

-----------------------------------------------------------------------------
(* Helpers *)

LastEpoch(s) == IF s = <<>> THEN 0 ELSE s[Len(s)].e

MaxEpochOn(s) == MaxOf({s[p].e : p \in 1..Len(s)})

\* The offsets in lo..hi of s whose record is of kind k, in order.
RECURSIVE PosOf(_, _, _, _)
PosOf(s, k, lo, hi) ==
    IF lo > hi THEN <<>>
    ELSE (IF s[lo].k = k THEN <<lo>> ELSE <<>>) \o PosOf(s, k, lo + 1, hi)

\* The entries for batch b (inputs, in the order merged) of length len,
\* each moving the cursor c past it.
RECURSIVE Build(_, _, _, _, _, _)
Build(e, h, b, len, c, j) ==
    IF j > len THEN <<>>
    ELSE LET c2 == [c EXCEPT ![b[j]] = @ + 1]
         IN <<Rec("entry", e, h, 0, b[j], c2[b[j]], c2)>> \o Build(e, h, b, len, c2, j + 1)

\* Orders in which a holder at cursor c may merge len records: any
\* interleaving of the inputs, each input's records in its order.
Batches(c, len) ==
    {b \in [1..len -> 1..NI] :
        \A i \in 1..NI : c[i] + Cardinality({j \in 1..len : b[j] = i}) <= Ins[i]}

OutRec(n, p) == Rec("out", nd[n].e, nd[n].h, p, merge[p].i, merge[p].o, Zero)
OClaimRec(n) == Rec("claim", nd[n].e, nd[n].h, 0, 0, 0, Zero)

\* The node gives the run up: superseded, crashed or out of retries.
Stop(n) == nd' = [nd EXCEPT ![n] = NodeInit]

Send(n, m, pc) ==
    /\ net' = net \cup {m}
    /\ nd' = [nd EXCEPT ![n].pc = pc, ![n].aw = m, ![n].rep = "none"]

\* Where a holder goes once its claim on the merge log is on.
AfterMergeClaim(n) == IF nd[n].oc THEN "resume" ELSE "oclaim"

\* Where a holder goes once its claim on `.out` is on.
AfterOutClaim == IF OutFirst THEN "mread" ELSE "resume"

-----------------------------------------------------------------------------
(* Claiming *)

\* The lease, the epoch floor over `.out` and the counter, and the `.out`
\* tail read with the floor.
Begin(n) ==
    /\ nd[n].pc = "idle"
    /\ bud.begins < MaxBegins
    /\ LET e == Max2(ctr, LastEpoch(out)) + 1
       IN /\ ctr' = e
          /\ nd' = [nd EXCEPT ![n] = [NodeInit EXCEPT
                       !.pc = IF OutFirst THEN "oclaim" ELSE "mread",
                       !.e = e, !.h = bud.begins + 1, !.ot = Len(out)]]
    /\ bud' = [bud EXCEPT !.begins = @ + 1]
    /\ UNCHANGED <<merge, out, net, mstarted, ostarted>>

\* `OutputLog::claim`: the claim at the tail read with the floor.
OClaim(n) ==
    /\ nd[n].pc = "oclaim"
    /\ Send(n, [n |-> n, s |-> "out", at |-> nd[n].ot, recs |-> <<OClaimRec(n)>>], "oclaimw")
    /\ UNCHANGED <<merge, out, ctr, bud, mstarted, ostarted>>

\* A refused claim, or one whose answer was lost, reads the records past
\* the tail: any from a later epoch stops it, its own claim among them means
\* it is on, and otherwise it claims again at the new tail.
OClaimAns(n) ==
    /\ nd[n].pc = "oclaimw" /\ nd[n].rep # "none"
    /\ LET past == nd[n].ot + 1..Len(out)
       IN IF nd[n].rep = "ok"
          THEN nd' = [nd EXCEPT ![n].pc = AfterOutClaim, ![n].oc = TRUE,
                                ![n].ot = nd[n].ot + 1, ![n].rep = "none"]
          ELSE IF \E p \in past : out[p].e > nd[n].e
          THEN Stop(n)
          ELSE IF \E p \in past : out[p] = OClaimRec(n)
          THEN nd' = [nd EXCEPT ![n].pc = AfterOutClaim, ![n].oc = TRUE,
                                ![n].ot = Len(out), ![n].rep = "none"]
          ELSE nd' = [nd EXCEPT ![n].pc = "oclaim", ![n].ot = Len(out), ![n].rep = "none"]
    /\ UNCHANGED <<merge, out, net, ctr, bud, mstarted, ostarted>>

\* `MergeLog::claim`: the merge log's tail and last record, read after the
\* `.out` claim has landed.
MRead(n) ==
    /\ nd[n].pc = "mread"
    /\ LET le == LastEpoch(merge)
           cur == IF merge = <<>> THEN Zero ELSE merge[Len(merge)].nx
       IN IF ClaimCheck /\ le > nd[n].e
          THEN /\ Stop(n)
               /\ UNCHANGED mstarted
          ELSE IF (SameEpochSkip /\ merge # <<>> /\ le = nd[n].e) \/ ~MergeClaim
          THEN /\ nd' = [nd EXCEPT ![n].pc = AfterMergeClaim(n), ![n].mt = Len(merge),
                                   ![n].nx = cur]
               \* Without the claim, a holder begins when it reads the tail.
               /\ mstarted' = IF ~MergeClaim /\ ~\E x \in mstarted : x.e = nd[n].e
                              THEN mstarted \cup {[e |-> nd[n].e, at |-> Len(merge)]}
                              ELSE mstarted
          ELSE /\ nd' = [nd EXCEPT ![n].pc = "mclaim", ![n].mt = Len(merge), ![n].nx = cur]
               /\ UNCHANGED mstarted
    /\ UNCHANGED <<merge, out, net, ctr, bud, ostarted>>

MClaim(n) ==
    /\ nd[n].pc = "mclaim"
    /\ Send(n, [n |-> n, s |-> "merge", at |-> nd[n].mt,
                recs |-> <<Rec("claim", nd[n].e, nd[n].h, 0, 0, 0, nd[n].nx)>>], "mclaimw")
    /\ UNCHANGED <<merge, out, ctr, bud, mstarted, ostarted>>

\* A refused claim, or one whose answer was lost, reads the tail again.
MClaimAns(n) ==
    /\ nd[n].pc = "mclaimw" /\ nd[n].rep # "none"
    /\ nd' = IF nd[n].rep = "ok"
             THEN [nd EXCEPT ![n].pc = AfterMergeClaim(n), ![n].mt = nd[n].mt + 1,
                             ![n].rep = "none"]
             ELSE [nd EXCEPT ![n].pc = "mread", ![n].rep = "none"]
    /\ UNCHANGED <<merge, out, net, ctr, bud, mstarted, ostarted>>

\* A run that ended in a transient error starts again under the same lease:
\* it does not claim `.out` again, and stops if the last record there is
\* from a later epoch.
Probe(n) ==
    /\ nd[n].pc = "probe"
    /\ IF LastEpoch(out) > nd[n].e
       THEN Stop(n)
       ELSE nd' = [nd EXCEPT ![n].pc = "mread", ![n].ot = Len(out), ![n].todo = <<>>,
                             ![n].bat = <<>>, ![n].land = <<>>, ![n].rep = "none"]
    /\ UNCHANGED <<merge, out, net, ctr, bud, mstarted, ostarted>>

\* The last output below the tail, the superseded check on it, and the fold:
\* the next step is the one after the last output's. Then the entries from
\* there up to the claim, which an earlier holder may have merged and not
\* stepped, in merge log order.
Resume(n) ==
    /\ nd[n].pc = "resume"
    /\ LET outs == PosOf(out, "out", 1, nd[n].ot)
           last == IF outs = <<>> THEN 0 ELSE outs[Len(outs)]
           next == IF last = 0 THEN 1 ELSE out[last].s + 1
       IN IF last # 0 /\ out[last].e > nd[n].e
          THEN Stop(n)
          ELSE nd' = [nd EXCEPT ![n].pc = "run",
                         ![n].todo = IF CatchUp THEN PosOf(merge, "entry", next, nd[n].mt)
                                     ELSE <<>>]
    /\ UNCHANGED <<merge, out, net, ctr, bud, mstarted, ostarted>>

-----------------------------------------------------------------------------
(* Running *)

\* The step for the next entry, its output appended at the `.out` tail.
OSend(n) ==
    /\ nd[n].pc = "run" /\ nd[n].todo # <<>>
    /\ Send(n, [n |-> n, s |-> "out", at |-> nd[n].ot,
                recs |-> <<OutRec(n, Head(nd[n].todo))>>], "ow")
    /\ UNCHANGED <<merge, out, ctr, bud, mstarted, ostarted>>

\* `OutputLog::append`'s reconcile: its own output past the tail landed, a
\* later epoch stops it, and nothing past the tail means send it again. A
\* lost answer may also end the run with an error, after the retries.
OAns(n) ==
    /\ nd[n].pc = "ow" /\ nd[n].rep # "none"
    /\ LET ot == nd[n].ot
           past == ot + 1..Len(out)
           mine == OutRec(n, Head(nd[n].todo))
           landed == nd[n].rep = "ok" \/ (past # {} /\ out[ot + 1] = mine)
       IN \/ /\ landed
             /\ nd' = [nd EXCEPT ![n].pc = "run", ![n].ot = ot + 1,
                                 ![n].todo = Tail(nd[n].todo), ![n].rep = "none"]
          \/ /\ ~landed /\ \E p \in past : out[p].e > nd[n].e
             /\ Stop(n)
          \/ /\ ~landed /\ ~\E p \in past : out[p].e > nd[n].e
             /\ nd' = [nd EXCEPT ![n].pc = IF past = {} THEN "run" ELSE "probe",
                                 ![n].rep = "none"]
          \/ /\ nd[n].rep = "lost"
             /\ nd' = [nd EXCEPT ![n].pc = "probe", ![n].rep = "none"]
    /\ UNCHANGED <<merge, out, net, ctr, bud, mstarted, ostarted>>

\* Records from the inputs merged in any order: one append of up to
\* PerWrite entries at the merge log tail, before any of them is stepped.
Merge(n) ==
    /\ nd[n].pc = "run" /\ nd[n].todo = <<>>
    /\ \E len \in 1..PerWrite : \E b \in Batches(nd[n].nx, len) :
        LET recs == Build(nd[n].e, nd[n].h, b, len, nd[n].nx, 1)
            m == [n |-> n, s |-> "merge", at |-> nd[n].mt, recs |-> recs]
        IN /\ net' = net \cup {m}
           /\ nd' = [nd EXCEPT ![n].pc = "mw", ![n].aw = m, ![n].rep = "none",
                               ![n].bat = recs, ![n].land = <<>>]
    /\ UNCHANGED <<merge, out, ctr, bud, mstarted, ostarted>>

\* `MergeLog::append` with `merge::reconcile` on a refusal or a lost answer:
\* its own entries past the tail are taken as written, a later epoch stops
\* it, and anything else is a transient error. Once the whole batch is on
\* the log, its entries are stepped.
MAns(n) ==
    /\ nd[n].pc = "mw" /\ nd[n].rep # "none"
    /\ LET mt == nd[n].mt
           bat == nd[n].bat
           found == SubSeq(merge, mt + 1, Len(merge))
           bad == {j \in 1..Len(found) :
                     found[j].e > nd[n].e \/ j > Len(bat) \/ found[j] # bat[j]}
           k == IF nd[n].rep = "ok" THEN Len(bat) ELSE Len(found)
           tail == IF nd[n].rep = "ok" THEN mt + Len(bat) ELSE Len(merge)
           land == nd[n].land \o [j \in 1..k |-> mt + j]
           rest == SubSeq(bat, k + 1, Len(bat))
           m == [n |-> n, s |-> "merge", at |-> tail, recs |-> rest]
       IN IF nd[n].rep # "ok" /\ bad # {}
          THEN IF found[MinOf(bad)].e > nd[n].e
               THEN /\ Stop(n)
                    /\ UNCHANGED net
               ELSE /\ nd' = [nd EXCEPT ![n].pc = "probe", ![n].rep = "none"]
                    /\ UNCHANGED net
          ELSE IF rest = <<>>
          THEN /\ nd' = [nd EXCEPT ![n].pc = "run", ![n].mt = tail,
                                   ![n].nx = bat[Len(bat)].nx, ![n].todo = land,
                                   ![n].bat = <<>>, ![n].land = <<>>, ![n].rep = "none"]
               /\ UNCHANGED net
          ELSE \/ /\ net' = net \cup {m}
                  /\ nd' = [nd EXCEPT ![n].mt = tail, ![n].bat = rest, ![n].land = land,
                                      ![n].aw = m, ![n].rep = "none"]
               \/ /\ nd[n].rep = "lost"
                  /\ nd' = [nd EXCEPT ![n].pc = "probe", ![n].rep = "none"]
                  /\ UNCHANGED net
    /\ UNCHANGED <<merge, out, ctr, bud, mstarted, ostarted>>

-----------------------------------------------------------------------------
(* Felix and the environment *)

Answers(m) == nd[m.n].aw = m /\ nd[m.n].rep = "none"

Started(st, m, s) ==
    IF m.recs[1].k = "claim" /\ ~\E x \in st : x.e = m.recs[1].e
    THEN st \cup {[e |-> m.recs[1].e, at |-> Len(s)]}
    ELSE st

\* The request lands if the stream's tail is still the one it expects. The
\* sender hears, unless the answer is lost.
Land(m, lost) ==
    /\ m \in net
    /\ LET s == IF m.s = "out" THEN out ELSE merge
           applied == Len(s) = m.at
       IN /\ out' = IF m.s = "out" /\ applied THEN out \o m.recs ELSE out
          /\ merge' = IF m.s = "merge" /\ applied THEN merge \o m.recs ELSE merge
          /\ ostarted' = IF m.s = "out" /\ applied THEN Started(ostarted, m, out) ELSE ostarted
          /\ mstarted' = IF m.s = "merge" /\ applied THEN Started(mstarted, m, merge) ELSE mstarted
          /\ nd' = IF Answers(m)
                   THEN [nd EXCEPT ![m.n].rep = IF lost THEN "lost"
                                                ELSE IF applied THEN "ok" ELSE "refused"]
                   ELSE nd
    /\ net' = net \ {m}

Deliver(m) ==
    /\ Land(m, FALSE)
    /\ UNCHANGED <<ctr, bud>>

LoseAnswer(m) ==
    /\ Answers(m) /\ bud.lost < MaxLost
    /\ Land(m, TRUE)
    /\ bud' = [bud EXCEPT !.lost = @ + 1]
    /\ UNCHANGED ctr

\* The sender stops waiting. The request may still land, or never.
Timeout(n) ==
    /\ nd[n].aw \in net /\ nd[n].rep = "none" /\ bud.lost < MaxLost
    /\ nd' = [nd EXCEPT ![n].rep = "lost"]
    /\ bud' = [bud EXCEPT !.lost = @ + 1]
    /\ UNCHANGED <<merge, out, net, ctr, mstarted, ostarted>>

\* Part of a batch lands and the sender gets an error.
DeliverPrefix(m) ==
    /\ m \in net /\ m.s = "merge" /\ Len(m.recs) > 1 /\ Len(merge) = m.at
    /\ bud.partials < MaxPartials
    /\ \E k \in 1..(Len(m.recs) - 1) : merge' = merge \o SubSeq(m.recs, 1, k)
    /\ nd' = IF Answers(m) THEN [nd EXCEPT ![m.n].rep = "lost"] ELSE nd
    /\ net' = net \ {m}
    /\ bud' = [bud EXCEPT !.partials = @ + 1]
    /\ UNCHANGED <<out, ctr, mstarted, ostarted>>

\* A crash, or a holder that gives up the workload for good.
Crash(n) ==
    /\ nd[n].pc # "idle" /\ bud.crashes < MaxCrashes
    /\ Stop(n)
    /\ bud' = [bud EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<merge, out, net, ctr, mstarted, ostarted>>

\* The `epochs` cache is recreated, only while every epoch a holder or a
\* request in flight could still write is on `.out` (design.md, Leases).
Recreate ==
    /\ bud.recreates < MaxRecreates /\ ctr > 0
    /\ \A e \in {nd[n].e : n \in {x \in Nodes : nd[x].pc # "idle"}} \cup {m.recs[1].e : m \in net} :
           e <= MaxEpochOn(out)
    /\ ctr' = 0
    /\ bud' = [bud EXCEPT !.recreates = @ + 1]
    /\ UNCHANGED <<merge, out, net, nd, mstarted, ostarted>>

-----------------------------------------------------------------------------

NodeStep(n) ==
    \/ Begin(n) \/ OClaim(n) \/ OClaimAns(n) \/ MRead(n) \/ MClaim(n) \/ MClaimAns(n)
    \/ Probe(n) \/ Resume(n) \/ OSend(n) \/ OAns(n) \/ Merge(n) \/ MAns(n)

Next ==
    \/ \E n \in Nodes : NodeStep(n) \/ Crash(n) \/ Timeout(n)
    \/ \E m \in net : Deliver(m) \/ LoseAnswer(m) \/ DeliverPrefix(m)
    \/ Recreate

Spec == Init /\ [][Next]_vars

Symmetry == Permutations(Nodes)

-----------------------------------------------------------------------------
(* Properties *)

\* How many entries of input i are at offsets up to p.
MergedUpTo(i, p) == Cardinality({q \in 1..p : merge[q].k = "entry" /\ merge[q].i = i})

\* Every record of every input is merged at most once and in its input's
\* order, and each record's cursor follows from the entries up to it, so the
\* last record alone says where each input carries on.
MergeExact ==
    \A p \in 1..Len(merge) :
        /\ merge[p].k = "entry" =>
              /\ merge[p].o = MergedUpTo(merge[p].i, p)
              /\ merge[p].o <= Ins[merge[p].i]
        /\ merge[p].nx = [i \in 1..NI |-> MergedUpTo(i, p)]

Monotone(s) == \A p, q \in 1..Len(s) : p < q => s[p].e <= s[q].e

\* Epochs on the merge log never decrease, and nothing from an earlier epoch
\* lands after a later holder began on it.
MergeEpochMonotone ==
    /\ Monotone(merge)
    /\ \A st \in mstarted : \A p \in st.at + 1..Len(merge) : merge[p].e >= st.e

\* The same for `.out`, and every entry follows its own epoch's claim, as
\* `felixd check` demands (MergeUnclaimed).
OutputsFenced ==
    /\ Monotone(out)
    /\ \A st \in ostarted : \A p \in st.at + 1..Len(out) : out[p].e >= st.e

EntriesClaimed ==
    \A p \in 1..Len(merge) : merge[p].k = "entry" =>
        LET cs == {q \in 1..p - 1 : merge[q].k = "claim"}
        IN cs # {} /\ merge[MaxOf(cs)].e = merge[p].e

\* One holder writes under each epoch, on both streams: after a claim, its
\* holder is the only writer until a later one claims.
OneWriter ==
    LET w == {[e |-> merge[p].e, h |-> merge[p].h] : p \in 1..Len(merge)}
             \cup {[e |-> out[p].e, h |-> out[p].h] : p \in 1..Len(out)}
    IN \A a, b \in w : a.e = b.e => a.h = b.h

Entries == PosOf(merge, "entry", 1, Len(merge))
Outs == PosOf(out, "out", 1, Len(out))

\* The outputs on `.out` are a prefix of the outputs of the merge log's
\* entries in log order: no gap, no duplicate.
OutputsFollowMerge ==
    /\ Len(Outs) <= Len(Entries)
    /\ \A j \in 1..Len(Outs) :
          /\ out[Outs[j]].s = Entries[j]
          /\ out[Outs[j]].i = merge[Entries[j]].i
          /\ out[Outs[j]].o = merge[Entries[j]].o

\* Each output names the merge log offset of the entry it was stepped from,
\* so a replay of the log steps the same records in the same order.
StepsFollowMerge ==
    \A j \in 1..Len(Outs) :
        LET r == out[Outs[j]]
        IN /\ r.s \in 1..Len(merge)
           /\ merge[r.s].k = "entry"
           /\ merge[r.s].i = r.i /\ merge[r.s].o = r.o
           /\ j > 1 => out[Outs[j - 1]].s < r.s

=============================================================================
