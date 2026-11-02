# The workload contract, version 0.2

This is the interface between felixd and a workload, and what each side
promises. `wit/workload.wit` holds the WIT below; it replaced the 0.1 world. It settles the
outline in [design.md, The workload contract, version 0.2](design.md#the-workload-contract-version-02)
(#64) and leaves room for effects (#71) and typed records (#73) without
another breaking change.

## Naming and versions

The WIT package is `felixd:workload@0.2.0`. A workload exports two
interfaces and imports nothing:

| Export | Holds |
|---|---|
| `felixd:workload/step@0.2.0` | the input and output types and `step` |
| `felixd:workload/state@0.2.0` | `snapshot`, `snapshot-chunk`, `restore`, `restore-chunk`, `inspect` |

The world is `felixd:workload/workload@0.2.0`. The types live in the `step`
interface rather than a types-only interface, because a component that uses
a types-only interface imports it, and a workload imports nothing.

The host identifies the contract by the exported interface names, which carry
the version. A minor version (0.2.x) may only add what an older guest never
sees. Anything that changes a type a guest already uses is a new version, as
the component model requires anyway: an added variant case does not
typecheck against the old one. That is why `output` and `input` below already
have the cases effects need.

## The interface

```wit
package felixd:workload@0.2.0;

interface step {
  /// Where an input came from, as recorded in the workload's spec when it
  /// was written. Never looked up again, so replay sees the same values.
  record source {
    /// The input stream's name.
    %stream: string,
    /// The Felix incarnation the name resolved to. 0 until specs record
    /// incarnations (#72).
    incarnation: u64,
  }

  /// One input record and where it sits in the input.
  record input {
    source: source,
    /// The record's offset in the input stream.
    offset: u64,
    /// The broker's append time, in microseconds since the Unix epoch.
    timestamp-micros: u64,
    body: input-body,
  }

  variant input-body {
    /// A record from the input stream. `key` is the key it was addressed
    /// with, when it is another workload's output.
    %record(record-in),
    /// The answer to an effect request. No version of felixd delivers one
    /// yet (#71).
    response(effect-response),
  }

  record record-in {
    key: option<list<u8>>,
    payload: list<u8>,
  }

  /// Something a step produces.
  variant output {
    /// A record for the named output, under an optional key.
    %record(addressed),
    /// A request for an effect. Refused until effects exist (#71).
    request(effect-request),
  }

  record addressed {
    /// Lowercase ASCII letters, digits and `-`, 1 to 64 bytes.
    name: string,
    key: option<list<u8>>,
    payload: list<u8>,
  }

  record effect-request {
    /// The versioned interface the body follows, such as
    /// `felixd:http/fetch@0.1.0`. It states the kind's guarantee.
    kind: string,
    /// A credential's name, resolved by the executor. Never a secret.
    credential: option<string>,
    body: list<u8>,
  }

  record effect-response {
    /// The request's id: the output header it was written with.
    request: effect-id,
    outcome: outcome,
  }

  record effect-id {
    input-offset: u64,
    index: u32,
    epoch: u64,
  }

  variant outcome {
    ok(list<u8>),
    error(error-code),
    timeout,
    /// The executor was lost after it may have acted.
    unknown,
    /// The executor's queue was full; nothing was done.
    backpressure,
  }

  record error-code {
    /// What a program compares.
    code: u32,
    /// Detail for people. Never compared.
    detail: string,
  }

  variant step-error {
    /// The input is bad and the workload carries on. Recorded on the
    /// output stream; produces no outputs.
    reject(error-code),
    /// The workload cannot go on. It fails with this reason.
    poison(string),
  }

  step: func(input: input) -> result<list<output>, step-error>;
}

interface state {
  /// Starts a snapshot and returns how many chunks it has. The host calls
  /// `snapshot-chunk` for each index in order before anything else.
  snapshot: func() -> u32;
  /// Chunk `index` of the snapshot `snapshot` started.
  snapshot-chunk: func(index: u32) -> list<u8>;
  /// Starts a restore of `chunks` chunks on a fresh instance. The host then
  /// calls `restore-chunk` for each index in order.
  restore: func(chunks: u32);
  restore-chunk: func(index: u32, chunk: list<u8>);
  /// The state as JSON, for replay and inspection.
  inspect: func() -> string;
}

world workload {
  export step;
  export state;
}
```

## What each side guarantees

### `step`

The host calls `step` once per input record, in input order, with the
metadata above. A workload with several inputs gets their records in the order
its merge log records ([Several inputs](#several-inputs)). Every field of
`input` is a function of the input streams, the merge log and the spec alone, so a replay, a resume after takeover, `felixd check` and
`felixd replay` pass identical values. The host never fills in a field from
the node it runs on.

The guest returns its outputs in order, or an error:

- **Outputs.** Each lands on the workload's output stream with the header it
  has today (input offset, index, epoch) plus its name and key. Index counts
  every output of the step, whatever its name. They are written exactly once,
  in order, across takeovers, as outputs are now.
- **`reject`.** The host writes one rejection record at that input's
  `(offset, index 0)`, fenced and reconciled like an output, and steps the
  next input. The guest's state stays as the step left it; replay reproduces
  it either way, so the host does not roll it back. A rejection is not a
  failure: the workload keeps its lease and stays `running`.
- **`poison`**, a trap or running out of fuel: the workload fails, with a
  failure record and `failed` in the spec as today, error class poison
  (design.md, Trap classes). A trap that can depend on the host is retried
  once on a fresh instance first, when that lands.

A step whose result breaks the contract is poison too, since it would break
the same way on any node: an output name that is not valid, a request (until
effects exist), or more output than one write to Felix may hold.

### `state`

Snapshots are taken where they are today (every N inputs, at interval starts,
never while replaying). Between `snapshot` and the last `snapshot-chunk` the
host calls nothing else. `snapshot` must not change the state, and restoring
a snapshot's chunks on a fresh instance must give a workload that behaves
exactly like the one snapshotted. A chunk may be empty. A chunk larger than
4 MiB, or more than 65,536 chunks, is poison.

The snapshot calls and the restore calls together run under
`limits.snapshot_fuel`, separate from `fuel_per_call` (design.md, State
size). The default is ten times the step default. `inspect` runs under
`fuel_per_call`.

Running out of snapshot fuel is not poison. A snapshot that runs out is
abandoned: nothing is written, the previous snapshot stays current, the host
counts and logs it, and the workload keeps running and replays from the older
snapshot if it restarts. A trap leaves the guest instance unusable, so the
agent drops it, resumes from the older snapshot and does not retry a snapshot
until `snapshot_every` more inputs have passed. A restore that runs out puts the workload in
needs-operator, not failed, so the limit can be raised and the workload
resumed. Fuel metering is deterministic, so every replica skips the same
snapshots.

Chunks are the unit a snapshot writes. A guest that keeps stable chunk
boundaries (by key range, say) lets the host write only the chunks that
changed: the host hashes each chunk and does not write one whose sha256 the
previous snapshot already holds.

## Snapshot layouts

A workload's snapshot is the state it saved after some input. Its snapshot
layout is the format of that state: how it is split into chunks and what the
bytes of each chunk mean. A new version of a module can start from the old
version's snapshot only if it understands the old layout.

A module says which layouts it knows in a custom section of its component
named `felixd.snapshot`. The section is optional. `felixd replay --module`
reads it to start a new module from a recorded snapshot instead of replaying
the whole history ([design.md, Checking an upgrade](design.md#checking-an-upgrade)).

### Declaring layouts

The section holds UTF-8 JSON with two fields:

```json
{"format": "counter.2", "reads": ["counter"]}
```

| Field | Meaning |
|---|---|
| `format` | The layout of the snapshots this module writes. |
| `reads` | Other layouts this module's `restore` accepts. A module always reads its own `format`, so `reads` may be left out. |

Here the module writes layout `counter.2` and can also restore snapshots of
layout `counter`, which an earlier version wrote.

A layout id is 1 to 128 bytes of printable ASCII without spaces. `reads`
lists at most 64. The section appears once and has no other fields.

### Adding the section

In Rust, a static with a `link_section` puts its bytes in a custom section of
the core module. The array length is the length of the JSON:

```rust
#[used]
#[unsafe(link_section = "felixd.snapshot")]
static SNAPSHOT_LAYOUT: [u8; 42] = *br#"{"format":"counter.2","reads":["counter"]}"#;
```

Build the core module and wrap it into a component as usual, with
`cargo build --release --target wasm32-unknown-unknown` and `wasm-tools
component new`. The wrapping keeps the section, inside the component's core
module. `strings counter.wasm | grep felixd.snapshot` prints the section's name
and JSON when it is there. The `counter` example in
[workloads/counter](../workloads/counter/README.md) wraps the static in a small
macro and builds the variants the tests use. Any toolchain that can add a
custom section to the core module or to the component will do.

### What a declaration promises

Two modules are involved: the one that wrote the snapshots, and the one that
restores them.

- The module that wrote a snapshot promises that the `format` it names is the
  layout of its snapshots. Two modules that name the same `format` promise that
  chunks of it mean the same state to both.
- The module that restores lists the layout in `reads` (or names it as its own
  `format`). It promises that `restore` on chunks of that layout gives the
  state the writing module had, and that `step` carries on from there.
- A module with no section writes a layout only the exact same module, by hash,
  can read. A recorded module that declares nothing therefore has no snapshot
  another module can start from.

The WIT is unchanged and a module without a section is still a valid 0.2
workload.

### What the host does with it

The host does not check the promise. It runs a workload without looking at the
section, and a workload resumes from its own module's snapshots.

`felixd replay --module` reads the section of the workload's module and of the
new module, but only when the new module is not the workload's own and a
snapshot lies before the replay's first input. It then starts the new module
from that snapshot if the layout of the snapshot is the new module's `format`
or in its `reads`, and from the initial state otherwise, saying why. With no
`--module`, with the workload's own module, with `--from-start`, or when no
snapshot qualifies (`--from 0`, the default, has none), the section is never
read.

If the new module declares the layout and then traps or runs out of fuel
restoring a snapshot, the command stops with "snapshot not readable by the new
module" and the snapshot's input offset. That is not reported as a divergence
of outputs. A module that restores a layout but misreads it shows up as
outputs that differ, so a declaration is worth no more than the tests behind
it.

A section that does not parse, in either module, is an error when it is read.
The message names the module and says that `--from-start` replays without
reading it. The module still runs.

### Limits

- The host takes the section from any core module or component inside the file.
  A composed component that holds two workloads with sections fails with "more
  than one `felixd.snapshot` section".
- A replay with another module reads and decodes the whole snapshot stream to
  find the nearest snapshot, as any replay does. A snapshot record that does
  not decode fails it even with `--from 0`.

## Records

The output stream moves to version 4 and the snapshot stream to version 3.
Older felixd builds already refuse a version they do not know, so they stop
on these records instead of misreading them. This build reads every older
version.

Output stream, version 4: `version | kind | epoch | body` as now, with:

| Kind | Body |
|---|---|
| 0, output | input offset (8), index (4), name length (1), name, key flag (1), key length (4) and key if the flag is 1, payload |
| 1, claim | unchanged |
| 2, failure | unchanged |
| 3, rejection | input offset (8), index (4, always 0), code (4), detail |

A version 3 output reads as name `out` with no key. Readers pass over claims,
failures and rejections when the stream is another workload's input; the
fold, reconcile and `felixd check` treat a rejection as that input's only
output.

Snapshot stream, version 3: a snapshot is chunk records followed by one
manifest record. Only the manifest makes a snapshot; chunks with no manifest
are ignored.

| Kind | Body |
|---|---|
| 0, chunk | sha256 (32), bytes |
| 1, manifest | input offset (8), output tail (8), chunk count (4), then per chunk its sha256 (32) and the offset (8) on `.snap` of a chunk record holding it |

Every record carries the writer's epoch after the kind and is appended with
`publish_if` at the snapshot tail, so the fence and the superseded check are
the ones snapshots have now. A manifest may name chunk records written for an
earlier snapshot. Restore reads the manifest, reads each chunk at its offset
and checks its hash; a mismatch is a resume error. A version 2 snapshot
restores as one chunk.

A chunk is held by offset on `.snap` until the content-addressed store (#77)
exists. The manifest already records each chunk's sha256, so moving to that
store is a storage swap, not a format change. A skipped snapshot writes no
chunks, since chunks go out only once the whole snapshot has run, so a
restore never sees a partial one. Retention (#63) must keep every chunk the
live manifest references.

## Inputs from another workload

A spec's input may be another workload's output stream, as now. It gets the
outputs addressed to one name, `out`, with their keys; a spec cannot name
another yet. Claims, failures, needs-operator, trim, degraded and divergence records,
rejections and other names are passed over ([design.md, Pipelines](design.md#pipelines)).

## Several inputs

A spec may name several input streams. The lease holder merges their records
as they arrive and appends the order it chose to the workload's merge log,
`wl.<id>.merge`, before it steps any of them. Each step gets the record's own
`source.stream`, `offset` and `timestamp-micros`, so a step can tell its
inputs apart. Within one input the records come in offset order; across inputs
the order is whatever the holder recorded, and the guest must not assume
another. Replay, a resume after takeover and `felixd check` read the merge log
and step the same records in the same order. An output's input offset is the
step's offset on the merge log ([records.md](records.md#merge-log-records)).

## 0.1 workloads

The host refuses a component that does not export the 0.2 interfaces. It
names what it found: a component with the 0.1 top-level `step`, `snapshot`,
`restore` and `inspect` functions is reported as "built against
felixd:workload@0.1.0, rebuild it against 0.2". `felixd run` refuses it
before storing anything. An agent that finds one in a spec does not run
the workload and does not mark it failed: it is a needs-operator condition,
recorded once on the output stream (design.md, System records and errors),
and the fix is a new module under a new id, as for any new code.

There is no shim. A 0.1 workload's outputs would need a name, its snapshots a
chunk layout and its errors a class it never stated, and a shim that chose
them would be a second contract to keep deterministic.

## Effects, later

Nothing here performs an effect. The contract only fixes where effects go so
that adding them needs no new version:

- A request is an `output` of kind `request`. Its id is the output header,
  which is unique and deterministic, so executors use it as the idempotency
  key. It is written to the output stream exactly once like any output.
- A response comes back as an `input` whose body is `response`, from the
  workload's response stream through the recorded merge order (#58). Replay
  reads the recorded response and performs nothing.
- `outcome` has every case design.md names, including `unknown` and
  `backpressure`.

Until #71, a step that returns a request is poison, with the reason that this
build has no effect executors.

## Not in this version

- Module upgrades at a recorded input offset (#64's last item). Spec history
  is now records on `sys.specs` (#72); the upgrade itself is not built.
- The system records defined in WIT (#73). They wait for the effect records,
  which are not built. Until then they are defined once in `felixd-core`
  ([records.md](records.md)), and the error classes here are the ones they
  carry.

## Decisions

design.md fixes the outline. These are the choices this note adds:

1. One fenced output stream, `wl.<id>.out`, holds every named output, with the
   name and key in the header, instead of a stream per name. A stream per name
   would need a claim on each and a fold across several tails, and
   `publish_if` orders one shard only.
2. Rejections are records on the output stream at `(offset, 0)`, so they are
   exactly once and replay checks them.
3. Snapshot chunks are records on `.snap`, referenced by offset from a
   manifest that also records each chunk's sha256, until #77.
4. `snapshot_fuel` is one budget per snapshot or restore, default ten times
   the step default. A snapshot that runs out is skipped; a restore that runs
   out is needs-operator.
5. 0.1 components are refused, not shimmed, and the refusal does not fail the
   workload.
6. The default output name is `out`, and a downstream workload reads one
   named output. Reading several outputs at once waits for the determinism
   work in #62.
7. Snapshot compatibility is declared in an optional custom section, not in the
   WIT, so declaring it needs no new contract version and a module without it
   stays valid.
