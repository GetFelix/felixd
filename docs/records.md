# Record formats

The formats live in `felixd-core`, which does no I/O. Binary records start with a version byte. A
reader refuses a version it does not know, so an older felixd fails a workload whose stream holds a
newer record instead of misreading it. Integers are big endian.

## Output stream records

On `wl.<id>.out`. The version is 4, followed by a kind byte.

| Bytes | Field |
|---|---|
| 1 | version, 4 |
| 1 | kind: 0 for an output, 1 for a claim, 2 for a failure, 3 for a rejection |
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

This build writes every output under the name `out` with no key. Named and keyed outputs come with
workload contract 0.2 ([contract.md](contract.md)).

A claim continues with the new owner's node id, in UTF-8, to the end of the record, so a reader can
tell which node wrote each epoch. Claims written before that end after the epoch, and a reader takes
their node as unknown. A new owner appends one before it resumes, so a stale owner's next
`publish_if` is refused. A failure continues with:

| Bytes | Field |
|---|---|
| 8 | the `workloads` cache version of the spec the workload failed under |
| rest | the reason, UTF-8 |

A holder appends it when the workload fails, before it writes `failed` into the spec. It is the
last record of that run: an owner of the same spec version that finds it writes `failed` into the
spec and stops.

A rejection says the workload rejected an input and carried on. It continues with:

| Bytes | Field |
|---|---|
| 8 | input offset of the rejected input |
| 4 | index, always 0 |
| 4 | the error code |
| rest | detail for people |

A rejection is that input's only output. The resume fold, the skipped-output checks, reconcile and
`felixd check` match it by `(input offset, index)` and compare it exactly like an output, so a
stream with a rejection where a replay produces an output, or the other way round, diverges. They
pass over claims and failures. A workload whose input is another workload's output stream reads only
the outputs addressed to one name, `out`, and passes over claims, failures, rejections and outputs of
other names (`output::addressed_to`).

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

A version 1 record has no epoch field and reads as epoch 0. This build still writes version 2
snapshots, and restores and replays only version 2 and 1.

## Caches

Values in `workloads` and `leases` are JSON. They are to become versioned typed records
([design.md, System records and errors](design.md#system-records-and-errors)).

- A workload spec has `module` (hex sha256), `input`, `output`, `snapshot_every` (default 1000),
  `snapshot_every_ms` (absent unless set), `state` (`running`, `stopped` or `failed`) and `limits`, with `fuel_per_call` (default 50,000,000)
  and `max_memory_bytes` (default 64 MiB). A spec without `limits` gets the defaults. A failed spec
  also has `failure`.
- A lease has `owner` and `epoch`. Each claim takes an epoch above every earlier claim's and every epoch on the output stream; it is 0 until the claim has taken it. The last epoch handed out is kept in the `epochs` cache, as a decimal number.

## The resume fold

`resume::fold` takes the latest snapshot header, the header of the last output or rejection record on the output
stream and that stream's next offset, which is past the new owner's claim. It returns whether to restore the snapshot, the input offset to
subscribe from, and which outputs the stream already holds: those at or before the last record's
`(input offset, index)`, starting at the snapshot's output tail. The owner runs `step` for those
inputs and drops the outputs, after `resume::check_skipped` has compared each with the record in its
place. `Resume::replaying` says when the owner is still before the last written input, which is when
it must not snapshot.

It takes the last output record's offset as well as the stream's next offset,
because claims and Felix's generation-start records take offsets that hold no output.
Whether outputs exist past the snapshot is decided by that offset.

It refuses a stream that is shorter than the snapshot expects, or whose newest output belongs to an
input the snapshot already covers, because a correct owner never produces either. Any of these
errors fails the workload.
