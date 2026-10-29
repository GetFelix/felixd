# felixd-core

The shared vocabulary of felixd: record formats, cache and stream names, lease and placement
rules, and the resume fold. It does no I/O, so everything in it is a pure function over bytes and
values that tests can run offline.

It depends on no other felixd crate. `felixd-runtime` takes its `Limits` from it, and
`felixd-agent` and the `felixd` binary build on all of it.

## What it provides

| Module | Holds |
|---|---|
| `names` | The caches (`workloads`, `leases`, `epochs`, `nodes`, `modules`) and how a workload id maps to its `wl.<id>.out` and `wl.<id>.snap` streams |
| `output` | Output stream records: outputs, claims, failures and rejections, with the name and key of each output |
| `snapshot` | Snapshot chunk and manifest records, `plan` (which chunks a new snapshot writes) and `read_chunk` (checks a chunk against its hash) |
| `spec`, `lease`, `node` | The JSON values in the `workloads`, `leases` and `nodes` caches, including `Limits` |
| `placement` | Which free workloads a node claims (`FirstClaimWins`) |
| `module` | How a module is identified: the hex sha256 of its bytes |
| `resume` | `fold`, which says where a new owner restores from, where it subscribes, and which outputs the stream already holds |
| `replay` | Where a replay starts and stops |
| `check` | What is wrong with an output stream, measured against a replay |

Guarantees worth knowing:

- Binary records start with a version byte, and integers are big endian. A reader refuses a
  version it does not know, so an older felixd fails a workload whose stream holds a newer record
  instead of misreading it. The current output records are version 4 and snapshots version 3;
  older versions still read. [docs/records.md](../../docs/records.md) has every layout.
- A write is at most `MAX_WRITE_BYTES` (8 MiB). A snapshot has at most 65,536 chunks of at most
  4 MiB.
- Lease TTLs are at least 2 s (`lease::MIN_TTL`) and default to 10 s.
- `resume::fold` refuses a stream that is shorter than its snapshot expects, or whose newest
  output belongs to an input the snapshot already covers. A correct owner never produces either.

## Build and test

Everything runs offline, with no Felix broker:

```
cargo build -p felixd-core
cargo test -p felixd-core
```

The tests sit next to the code, in `<module>/tests.rs`.

For depth, see [docs/records.md](../../docs/records.md) and [docs/design.md](../../docs/design.md).
