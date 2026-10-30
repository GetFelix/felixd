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

Flags of `run`: `--id`, `--input` (up to 64), `--snapshot-every` (1000), `--snapshot-every-ms`, `--fuel`,
`--max-memory`, `--snapshot-fuel` and `--replication-factor` (3; use 1 against a single broker).
The limits are stored in the spec, so every node runs the workload under the same ones.

Flags of `agent`: `--node-id` (`FELIXD_NODE_ID`; host name and process id by default),
`--lease-ttl-ms` (10000, at least 2000; use the same value on every node), `--max-workloads` (64)
and `--claim-delay-ms` (1000).

Flags of `check`: `--id` (repeat for several; every workload by default), `--complete`, which
also reports outputs missing from the end of a stream, and `--upstream`, which also checks every
workload whose output a checked one reads, up the pipeline. Stages are checked upstream first.

Flags of `replay`: `--id`, `--to` (an input offset, a merge log offset for a workload with
several inputs, or an RFC 3339 time such as
`2026-10-29T14:02:00Z`), `--diff`, `--from` (with `--diff`) and `--module`, the hash of a module in
the `modules` cache to run instead of the workload's own. `--diff` exits 1 on a divergence.

Connection flags, each also read from an environment variable: `--brokers` (`FELIX_BROKERS`),
`--server-name` (`FELIX_SERVER_NAME`), `--tenant` (`FELIX_AUTH_TENANT`), `--namespace`
(`FELIX_NAMESPACE`), `--token-file` (`FELIX_AUTH_TOKEN_FILE`), `--ca-file` (`FELIX_CA_FILE`),
`--controlplane-url` (`FELIX_CONTROLPLANE_URL`) and `--controlplane-token-file`
(`FELIX_CONTROLPLANE_TOKEN_FILE`, needed by `run`). `felixd <command> --help` lists them all.

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
