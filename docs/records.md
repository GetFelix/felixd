# Record formats

The formats live in `felixd-core`, which does no I/O. Binary records start with a version byte. A
reader refuses a version it does not know, so an older felixd fails a workload whose stream holds a
newer record instead of misreading it. Integers are big endian.

## Output stream records

On `wl.<id>.out`. The version is 4, followed by a kind byte.

| Bytes | Field |
|---|---|
| 1 | version, 4 |
| 1 | kind: 0 for an output, 1 for a claim, 2 for an unclassed failure, 3 for a rejection, 4 for a claim under a spec, 5 for a failure, 6 for needs operator, 7 for a trim |
| 8 | the writer's lease epoch |

An output continues with:

| Bytes | Field |
|---|---|
| 8 | input offset of the step that produced the output |
| 4 | index among that step's outputs, from 0, whatever their names |
| 1 | name length |
| name length | the output's name: 1 to 64 bytes of `a-z`, `0-9` and `-` |
| 1 | key flag, 0 or 1 |
| 4 | key length, if the flag is 1 |
| key length | the key, if the flag is 1 |
| rest | the payload the workload returned |

The name and key are the ones the workload's `step` returned ([contract.md](contract.md)). An
output whose name is not valid fails the workload instead of being written.

A claim continues with the new owner's node id, in UTF-8, to the end of the record, so a reader can
tell which node wrote each epoch. Claims written before that end after the epoch, and a reader takes
their node as unknown. A new owner appends one before it resumes, so a stale owner's next
`publish_if` is refused. A claim under a spec, which this build writes, continues with the
`sys.specs` offset (8) of the spec record the owner runs, the floor (8), then the node id as above.
The floor is the highest spec record claimed on the stream so far, this claim's own included. An
owner takes it from the last claim before its own, and from any claim it finds past the tail when
its claim is refused, so the last claim alone says whether an older spec is being run, even when
that claim's owner refused its spec. A needs-operator record after the last claim raises the floor
to its spec record too, since it can be written with no claim. Kind 1 claims
still read, with no spec. `felixd check` and `felixd replay --diff` use the spec each claim names to
judge the inputs after it, passing over claims whose owner refused the spec. An agent refuses to
run a spec whose module or input differ from the one the stream's first claim names, or whose
record is below the floor. Needs-operator records before the first claim ran nothing and are passed
over when finding it. A failure, kind 5, continues with:

| Bytes | Field |
|---|---|
| 8 | the `workloads` cache version of the spec the workload failed under |
| 1 | the error class: 0 transient, 1 needs operator, 2 poison |
| rest | the reason, UTF-8, for people |

A holder appends it when the workload fails, before it writes `failed` into the spec. It is the
last record of that run: an owner of the same spec version that finds it writes `failed` into the
spec and stops. A kind 2 failure, from an older build, has no class byte and reads as poison. An
unknown class is refused.

A needs-operator record, kind 6, says that no node can run a spec record until an operator acts. It
continues with:

| Bytes | Field |
|---|---|
| 8 | the `sys.specs` offset of the spec record |
| 2 | the reason: 1 module refused, 2 module missing, 3 spec not recorded, 4 spec conflicts with the output, 5 spec stale, 6 restore out of fuel, 7 input record unreadable by this build, 8 input stream behind the workload, 9 a record the workload needs was trimmed |
| rest | detail for people, UTF-8 |

A holder appends it the way it appends a failure, behind everything of earlier epochs, and then
releases the lease. A node that finds the problem before it claims claims only to write the record,
with no claim record before it. A reason this build does not know is refused when reading the
record in full, but a node checking whether to run still reads the spec record from the header and
leaves the workload alone, so an older build never runs what a newer one parked. Any node whose spec
names that record and that finds it last on the stream, before it claims or while claiming, leaves
the workload alone, so it is written once per workload, spec record and reason. It is not a failure
and the spec stays `running`. A new spec record, which `felixd run` writes for any change, or
removing the workload clears it. A stale spec is never recorded, since only the node holding it
sees the problem, and a node checks for one before it records anything else. A spec record that a
node still reads as past the tail of `sys.specs` after 30 seconds is recorded as spec not recorded,
so a spec-history read lagging that long stops the workload on every node until the spec is
submitted again.

A rejection says the workload's `step` returned `reject` for an input and carried on. It continues with:

| Bytes | Field |
|---|---|
| 8 | input offset of the rejected input |
| 4 | index, always 0 |
| 4 | the error code |
| rest | detail for people |

A rejection is that input's only output. The resume fold, the skipped-output checks, reconcile and
`felixd check` match it by `(input offset, index)` and compare it exactly like an output, so a
stream with a rejection where a replay produces an output, or the other way round, diverges. They
pass over claims, failures and needs-operator records. A workload whose input is another workload's output stream reads only
the outputs addressed to one name, `out`, and passes over these, rejections and outputs of
other names (`output::addressed_to`). Its step gets the output's key and payload, not the record. It
decides by the version and kind bytes and reads the body of outputs only, so a failure or
needs-operator record with a class or reason this build does not know is still passed over. A
version or kind it does not know is refused, never skipped: the reader cannot tell whether it holds
an input, so the workload is recorded as needing an operator (reason 7), and upgrading the node
fixes it. That includes kinds 5 and 6 read by a build from before them, so upgrade every node before
a workload can write them. An output that does not decode is poison.

A trim, kind 7, names the floors below which this workload's resume reads nothing
([design.md](design.md#retention)). `felixd check` and `felixd replay` still read from the start
while the records are there. Its holder appends it after a snapshot of its own when the spec sets
`keep_snapshots`, with `publish_if` at the tail like an output. It continues with:

| Bytes | Field |
|---|---|
| 8 | the `.snap` offset of the oldest kept snapshot |
| 8 | that snapshot's step: its input offset, or merge log offset with several inputs |
| 8 | the `.snap` floor: the lowest offset a kept snapshot's manifest or chunks sit at |
| 8 | the merge log floor, the step's own entry, kept for its `next`; 0 with one input |
| 2 | input count |
| 8 each | per input, in the spec's order, this workload's floor: the record the step read with one input; the entry's `next`, lowered to the entry's own record, with several |

Bytes after the last floor are refused. Readers pass over a trim like a claim, and only its epoch is
checked. Nothing is deleted at these floors yet (Felix #1662), and an input floor is one reader's:
an input shared with other readers is deleted only below all of theirs (#150).

A version 3 record has the same layout without a name or key in an output, and no rejections. Its
outputs read as name `out` with no key. A version 2 record is an output with no kind byte: version,
input offset (8), index (4), epoch (8), payload. Both still read.

## Snapshot records

On `wl.<id>.snap`. A version 3 snapshot is chunk records followed by one manifest record, each
`version (1) | kind (1) | epoch (8) | body`.

| Kind | Body |
|---|---|
| 0, chunk | sha256 of the bytes (32), the bytes |
| 1, manifest | input offset (8), output tail (8), chunk count (4), then per chunk its sha256 (32) and the offset (8) on `.snap` of a chunk record holding it |

The input offset is the last input the state includes, and the output tail is the output stream's
next offset when the snapshot was taken. Only a manifest is a snapshot: chunks without one are
ignored, and `felixd check` checks only manifests. A manifest may name chunk records written for an
earlier snapshot. `snapshot::plan` takes the previous manifest and the new chunks and says which
chunks to write; a chunk the previous manifest holds, or one already in this snapshot, is referenced
instead. Reading a chunk back checks its bytes against the hash the manifest names
(`snapshot::read_chunk`). A snapshot has at most 65,536 chunks of at most 4 MiB.

A version 2 record is a whole snapshot and reads as one chunk:

| Bytes | Field |
|---|---|
| 1 | version, 2 |
| 8 | offset of the last input the state includes |
| 8 | the output stream's next offset when the snapshot was taken |
| 8 | the writer's lease epoch |
| rest | what the workload's `snapshot` returned |

A version 1 record has no epoch field and reads as epoch 0. This build writes version 3 snapshots
and restores and replays all three.

An owner writes a snapshot's new chunks with `publish_if` at the snapshot stream's tail, as many to a
write as fit in 8 MiB, and the manifest last, in the same write as the last chunks when it fits.
Each write checks the records it finds past the tail as before, so the fence and the superseded check
are unchanged. Chunks that land without their manifest, after a crash or a partial batch, are never
read. A new owner reads back from the tail for the last manifest or version 2 record, passing over
chunks, reads the chunks it names and checks each hash; a mismatch fails the workload.

## Merge log records

On `wl.<id>.merge`, for a workload with several inputs. It records the order the lease holder chose
to step their records in. Each record is `version (1) | kind (1) | epoch (8) | body`, version 1.

| Kind | Body |
|---|---|
| 0, entry | input (2), offset (8), timestamp (8), next |
| 1, claim | next, then the holder's node id in UTF-8 to the end of the record |

An entry names one record: the input's position in the spec's `inputs`, the record's offset on that
input and its append time in microseconds. `next` is a count (2) followed by that many offsets (8),
one per input: the offset after the last record of that input merged so far. The last record alone
says where each input carries on, however long one has been idle. A reader refuses another version
or kind; an agent that finds one records the workload as needing an operator (reason 7).

The entry's offset on the merge log is the step's offset. Outputs, rejections and snapshots name
it where a one-input workload's name the input offset, so the resume fold, reconcile, `felixd
check` and `felixd replay` work on merge log offsets unchanged. The step is given the record's own
input stream, offset and append time, read from the entry, so a replay passes exactly what the live
step got.

The log is fenced like the output stream. A holder appends a claim after its claim on `.out` has
landed and before it merges anything, at the tail it reads, once it has checked that the last record
there is from an epoch no later than its own. A later epoch means it stops. Its claim carries `next`
from the record before it. Every append after that is a `publish_if` at the tail it expects, of at most as many entries
as fit in 8 MiB (`merge::entries_per_write`), so a long gap read back after a dropped delivery goes
out in several writes rather than one Felix would refuse on every retry. A stale holder's append
is refused once the claim is on. A refused append reads the records past the
tail: its own, whose answer was lost, are taken as written, and one from a later epoch ends the run.

Entries an earlier holder appended before the claim stay. Every record of every input appears once,
in its input's order, and `felixd check` reports one merged twice, out of order, missing from its
input or passed over, an entry whose append time is not its record's, an entry of an epoch whose
claim is not the last claim before it, a `next` that does not follow from the entries before it,
and an epoch that goes backwards. With `--complete` it also reports records no entry names yet.

## Spec records

On `sys.specs`, one shard. Each record is JSON: `format` (1), `id`, the workload id, and `spec`, a
workload spec as below with `state` `running` or `stopped`. A reader refuses another format. The
record's offset is the spec's version, so a later record for an id supersedes an earlier one and
the stream is the spec history. Records are never rewritten; `failed` stays in the cache.

A record that admission's projector wrote also has `admitted`, the `wl.admission.out` offset of
the decision that accepted it. Once a record for `admission` is on the stream, a node runs a later
record of any other workload only if `admitted` names a decision accepting that workload and
exactly that spec; otherwise the spec needs an operator with reason `spec_unadmitted` (code 10),
which is logged and counted but not written to the output stream. So is a record whose decision is
no later than one an earlier record of the same workload names, and one whose decision Felix no
longer holds. Records of the system workloads, `admission` and `scheduler`, need no decision. Only earlier
records whose own decision verifies count. Nodes read `sys.specs` from offset 0, so it must
never be trimmed. A record that does not decode
counts as admission's, so a node fails closed on one from a newer build.

## Requests and decisions

Requests are on `sys.intake`, one shard. Each is JSON: `format` (1), `id`, the workload id, `spec`,
a workload spec as below with no `record` or `failure`, `module_bytes`, the module's length, and
`replication_factor`. Who asked is not in the payload, which a publisher can fill with anything.

Admission's outputs on `wl.admission.out` are ordinary output records under the name `out`, keyed
by the workload id (no key for a request that did not decode). Each payload is JSON: `format` (1),
`request`, the `sys.intake` offset it answers, `id`, and `outcome`:

- `accepted`, with `spec`, `module_bytes` and `replication_factor` as asked.
- `rejected`, with `reason` and `detail`, for people. Reasons and their codes: `unreadable` (1),
  `invalid_id` (2), `reserved` (3), `invalid_inputs` (4), `invalid_spec` (5), `over_limit` (6),
  `module_too_large` (7).

Every request gets exactly one decision. A reader refuses another format.

## Node facts and assignments

Node facts are on `sys.facts`, one shard, and are the scheduler's second input. Each is
`{"format": 1, "value": ...}` with `node`, the node's id, `every_ms`, how often it appends one when
nothing changes, `max_workloads`, `held`, how many workloads it holds, `holding`, their ids (at most
`max_workloads`, left out when empty), and the booleans `draining` and `blocked` (it cannot read
its drain mark). Those after `max_workloads` default to 0, empty and false.
An agent appends one every `--facts-every-ms` while the `scheduler` workload runs, and within half
a second of a change. The scheduler counts a node as gone once the latest `sys.facts` append time
is more than three of the node's `every_ms` past its last fact, and forgets it after thirty.

The scheduler's outputs on `wl.scheduler.out` are ordinary output records under the name `out`.
Each payload is `{"format": 1, "value": ...}` with `specs`, the next `sys.specs` offset it has not
stepped, and `kind`:

- `assign`, keyed by the workload id, with `workload` and `node`, the node it should run on.
  `node` is left out once the workload no longer runs.
- `table`, with no key, with `assignments`, an object from workload id to node. It replaces every
  assignment before it. The first step writes one, and another follows every ten minutes of input
  time.
- `progress`, with no key: a spec record that changed no assignment.

A reader starts at the latest table and folds what follows; the last word on a workload is in
force, and its offset is what `felixd ps` shows after the node. A running workload with no
assignment whose spec record is below `specs` was left unassigned on purpose, for want of room.

## Caches

Values in `workloads`, `leases`, `epochs`, `nodes` and `drains` are JSON of the form
`{"format": 1, "value": ...}`, each with its own format number. A reader refuses a format it does
not know. The value sits under its own key so that a build from before formats, which reads the
fields at the top, refuses it instead of reading what it can. A value with no `format` is from such
a build and still reads.

- A workload spec has `module` (hex sha256), `input` or `inputs`, `output`, `snapshot_every` (default 1000),
  `snapshot_every_ms` (absent unless set), `keep_snapshots` (absent unless set: keep every snapshot), `state` (`running`, `stopped` or `failed`) and `limits`, with `fuel_per_call` (default 50,000,000),
  `max_memory_bytes` (default 64 MiB) and `snapshot_fuel` (default 500,000,000, for a whole
  snapshot or restore; a snapshot that runs out is skipped, a restore that runs out
needs an operator). A spec without `limits` gets the defaults. A failed spec
  also has `failure`: `class` (`transient`, `needs-operator` or `poison`) and `detail`. A
  `failure` that is a string is from an older build and reads as poison. The cache value also has `record`, the `sys.specs` offset of the spec record
  it comes from. An agent runs a spec only when that record says the same, state aside, and leaves
  one without `record` to an operator. A workload with one input has `input`, the stream's name.
  One with several has `inputs`, a list of 2 to 64 distinct names and no `input`, so a build that
  reads only `input` refuses it.
- A lease has `owner` and `epoch`. Each claim takes an epoch above every earlier claim's and every epoch on the output stream; it is 0 until the claim has taken it. The last epoch handed out is kept in the `epochs` cache, as a number.
- A node has `version`, `max_workloads` and `held`, and `draining`, the nonce of the drain mark
  it has read, once it also has no claim under way. `draining` is left out when there is none.
- A drain mark is `nonce`, a number other than 0 that `felixd drain` picks anew each time, under a
  node's id, with no TTL. A mark without `nonce` reads as 0, which no node acknowledges. `felixd drain` writes it and
  `felixd undrain` deletes it. While it is there the node gives up its leases and claims nothing.
  A mark in a format this build does not know still counts as a drain.

## The resume fold

`resume::fold` takes the latest snapshot header, the header of the last output or rejection record on the output
stream and that stream's next offset, which is past the new owner's claim. It returns whether to restore the snapshot, the input offset to
subscribe from, and which outputs the stream already holds: those at or before the last record's
`(input offset, index)`, starting at the snapshot's output tail. For a workload with several inputs the
offsets are merge log offsets ([Merge log records](#merge-log-records)): it steps the entries from
there, including any an earlier holder merged and never stepped, before it merges more. The owner runs `step` for those
inputs and drops the outputs, after `resume::check_skipped` has compared each with the record in its
place. `Resume::replaying` says when the owner is still before the last written input, which is when
it must not snapshot.

It takes the last output record's offset as well as the stream's next offset,
because claims and Felix's generation-start records take offsets that hold no output.
Whether outputs exist past the snapshot is decided by that offset.

It refuses a stream that is shorter than the snapshot expects, or whose newest output belongs to an
input the snapshot already covers, because a correct owner never produces either. Any of these
errors fails the workload.
