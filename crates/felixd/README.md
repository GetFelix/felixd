# felixd

The command line. It parses flags and hands the work to `felixd-agent`.

It depends on `felixd-core`, `felixd-runtime` and `felixd-agent`. `felixd-testkit` is a
dev-dependency, for its integration tests.

## Build

```
cargo build --release -p felixd
```

The binary is `target/release/felixd`; `felixd --version` prints the package version.

## Commands

Every command takes the connection flags below. Logging follows `RUST_LOG`, and defaults to
`felixd=info,felixd_agent=info`.

| Command | What it does |
|---|---|
| `felixd run [flags] <module>` | Stores the module, creates the workload's streams and starts it. `--id` and `--input` are required; repeat `--input` for several, merged through a merge log. |
| `felixd agent` | Runs workloads on this node: claims the lease of each free one and runs it while the lease holds. |
| `felixd check` | Replays each workload over its input, or its merge log, and reports problems in its output stream and merge log. Exits 1 if it finds any. |
| `felixd replay` | Rebuilds a workload as of `--to` and prints what its `inspect` returns, or with `--diff` compares a replay with the recorded outputs. Writes nothing. |
| `felixd ps` | Shows each workload's state, holder, assignment (while a scheduler runs), epoch, output stream tail and position on each input, and each node. Reads only. |
| `felixd drain <node>` | Marks a node as draining: it releases its leases so other nodes take its workloads over at once, and claims nothing. |
| `felixd undrain <node>` | Lifts the mark, so the node claims work again. |

Flags of `run`: `--id`, `--input` (up to 64), `--snapshot-every` (1000), `--snapshot-every-ms`, `--fuel`,
`--max-memory`, `--snapshot-fuel` and `--replication-factor` (3; use 1 against a single broker).
The limits are stored in the spec, so every node runs the workload under the same ones.

Flags of `agent`: `--node-id` (`FELIXD_NODE_ID`; host name and process id by default),
`--lease-ttl-ms` (10000, at least 2000; use the same value on every node), `--max-workloads` (64),
`--claim-delay-ms` (1000), `--assignment-bound-ms` (15000), the longest a free workload is left to
an assignee that can take it, or waits for the scheduler to read its spec, before first claim
wins, and `--facts-every-ms` (three lease TTLs), how often the node appends its facts for the
scheduler when they have not changed.

Flags of `check`: `--id` (repeat for several; every workload by default), `--complete`, which
also reports outputs missing from the end of a stream, and `--upstream`, which also checks every
workload whose output a checked one reads, up the pipeline. Stages are checked upstream first.

Flags of `replay`: `--id`, `--to` (an input offset or an RFC 3339 time such as
`2026-10-29T14:02:00Z`; for a workload with several inputs, a merge log offset or a time compared
with when entries were merged), `--diff`, `--from` (with `--diff`) and `--module`, the hash of a module in
the `modules` cache to run instead of the workload's own. `--diff` exits 1 on a divergence.

Flags of `ps`: `--id` (repeat for several; every workload by default).

Flags of `drain`: `--wait`, which returns once no live lease names the node and the node says it
claims nothing (or it is down), `--timeout-secs` (300) for `--wait`, after which it exits 1, and
`--replication-factor` (3) for the `drains` cache if this creates it.

Connection flags, each also read from an environment variable: `--brokers` (`FELIX_BROKERS`),
`--server-name` (`FELIX_SERVER_NAME`), `--tenant` (`FELIX_AUTH_TENANT`), `--namespace`
(`FELIX_NAMESPACE`), `--token-file` (`FELIX_AUTH_TOKEN_FILE`), `--ca-file` (`FELIX_CA_FILE`),
`--controlplane-url` (`FELIX_CONTROLPLANE_URL`) and `--controlplane-token-file`
(`FELIX_CONTROLPLANE_TOKEN_FILE`, needed by `run` and `drain`). A node needs read on the `drains`
cache, or it claims nothing. `felixd <command> --help` lists them all.

To place workloads with the scheduler, submit it as the workload `scheduler` reading `sys.specs`
and `sys.facts` (the container image has it at `/usr/local/share/felixd/scheduler.wasm`):

```
felixd run --id scheduler --input sys.specs --input sys.facts scheduler.wasm
```

Nodes then follow its assignments, with first claim wins as the fallback. Stopping it (removing
its spec) puts every node back on first claim wins. A node needs publish on `sys.facts` and read
on `wl.scheduler.out` for this.

Submit a workload and start a node (set the variables as in the top-level
[README](../../README.md#try-it) first):

```
felixd run --id counter --input in --replication-factor 1 counter.wasm
felixd agent
```

## Test

```
cargo test -p felixd --bin felixd   # offline: argument parsing
cargo test -p felixd --test cli     # offline: runs the built binary
```

The other integration tests (`restart`, `failover`, `takeover`, `fence`, `replay`, `check` and the
rest) need a Felix broker and control plane. Start the test stack in `tests/felix/`, set the
`FELIX_*` variables as the top-level README shows, and run for example:

```
FELIXD_REQUIRE_FELIX=1 cargo test -p felixd --test restart -- --test-threads=1
```

Without `FELIX_BROKERS` those tests print a skip message and pass, unless `FELIXD_REQUIRE_FELIX`
is set. The `cluster` test runs against three brokers and is skipped unless `FELIXD_CLUSTER` names
`tests/felix/cluster.yaml`; CI runs it with `fence`, `batches`, `answers` and `pipeline` on that cluster.
The tests build the example workloads on demand through `felixd-testkit`.
