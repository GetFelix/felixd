# Record formats

The formats live in `felixd-core`, which does no I/O. Binary records start with a version byte: `1`
for output records, `2` for snapshots. A reader refuses a version it does not know. Integers are big endian.

## Output records

On `wl.<id>.out`.

| Bytes | Field |
|---|---|
| 1 | version |
| 8 | input offset of the step that produced the output |
| 4 | index among that step's outputs, from 0 |
| rest | the payload the workload returned |

## Snapshot records

On `wl.<id>.snap`.

| Bytes | Field |
|---|---|
| 1 | version |
| 8 | offset of the last input the state includes |
| 8 | the output stream's next offset when the snapshot was taken |
| 8 | the writer's lease epoch |
| rest | what the workload's `snapshot` returned |

The version is 2. A version 1 record has no epoch field and reads as epoch 0.

## Caches

Values in `workloads` and `leases` are JSON.

- A workload spec has `module` (hex sha256), `input`, `output`, `snapshot_every` (default 1000),
  `state` (`running`, `stopped` or `failed`) and `limits`, with `fuel_per_call` (default 50,000,000)
  and `max_memory_bytes` (default 64 MiB). A spec without `limits` gets the defaults. A failed spec
  also has `failure`.
- A lease has `owner` and `epoch`. Each claim takes an epoch above every earlier claim's and every epoch on the output stream; it is 0 until the claim has taken it. The last epoch handed out is kept in the `epochs` cache, as a decimal number.

## The resume fold

`resume::fold` takes the latest snapshot header, the header of the last record on the output stream
and that stream's next offset. It returns whether to restore the snapshot, the input offset to
subscribe from, and which outputs the stream already holds: those at or before the last record's
`(input offset, index)`, starting at the snapshot's output tail. The owner runs `step` for those
inputs and drops the outputs, after `resume::check_skipped` has compared each with the record in its
place. `Resume::replaying` says when the owner is still before the last written input, which is when
it must not snapshot.

It takes the last output record's offset as well as the stream's next offset,
because Felix's generation-start records take offsets that hold no output.
Whether outputs exist past the snapshot is decided by that offset.

It refuses a stream that is shorter than the snapshot expects, or whose newest output belongs to an
input the snapshot already covers, because a correct owner never produces either. Any of these
errors fails the workload.
