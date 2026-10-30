# felixd-core

The shared vocabulary of felixd: record formats, cache and stream names, lease and placement
rules, and the resume fold. It does no I/O, so everything in it is a pure function over bytes and
values that tests can run offline.

It depends on no other felixd crate. `felixd-runtime` takes its `Limits` from it, and
`felixd-agent` and the `felixd` binary build on all of it.

## What it provides

| Module | Holds |
|---|---|
| `names` | The caches (`workloads`, `leases`, `epochs`, `nodes`, `drains`, `modules`), the `sys.specs` and `sys.facts` streams, the `scheduler` workload id, and how a workload id maps to its `wl.<id>.out`, `wl.<id>.snap` and `wl.<id>.merge` streams |
| `output` | Output stream records: outputs, claims, failures and rejections, with the name and key of each output |
| `merge` | Merge log records of a workload with several inputs: entries, claims, the cursor and `reconcile` |
| `snapshot` | Snapshot chunk and manifest records, `plan` (which chunks a new snapshot writes) and `read_chunk` (checks a chunk against its hash) |
| `spec`, `lease`, `node` | The JSON values in the `workloads`, `leases`, `nodes` and `drains` caches, including `Limits`, and spec records on `sys.specs` with `History`, their fold for one workload |
| `placement` | Which free workloads a node claims: `FirstClaimWins`, and `Scheduled`, which follows the scheduler's assignments with first claim wins as the fallback |
| `schedule` | The scheduler workload's fold (`Scheduler`), the node facts it reads from `sys.facts` and the assignments it writes |
| `module` | How a module is identified: the hex sha256 of its bytes |
| `resume` | `fold`, which says where a new owner restores from, where it subscribes, and which outputs the stream already holds |
| `replay` | Where a replay starts and stops |
| `status` | What `felixd ps` says about a workload: its state from the spec, lease and last output record, and its next step |
| `check` | What is wrong with an output stream, measured against a replay, and with a merge log, measured against its inputs |

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
