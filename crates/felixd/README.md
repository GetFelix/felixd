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
| `felixd init <module>` | Brings admission up: creates the namespace and its system namespace, writes the grants of felixd's roles there, and writes admission's spec directly with the control plane token. Running it again with the same module changes nothing. |
| `felixd run [flags] <module>` | Where admission runs, uploads the module, publishes a request on `sys.intake` and prints admission's decision, exiting 1 on a rejection. Otherwise stores the module, creates the workload's streams and starts it. `--id` and `--input` are required; repeat `--input` for several, merged through a merge log. |
| `felixd agent` | Runs workloads on this node: claims the lease of each free one and runs it while the lease holds. |
| `felixd check` | Replays each workload over its input, or its merge log, and reports problems in its output stream and merge log. Exits 1 if it finds any. |
| `felixd replay` | Rebuilds a workload as of `--to` and prints what its `inspect` returns, or with `--diff` compares a replay with the recorded outputs. Writes nothing. |
| `felixd ps` | Shows each workload's state, holder, assignment (while a scheduler runs), epoch, output stream tail and position on each input, and each node, and where a failed workload's replay diverged from its output stream. Reads only. |
| `felixd drain <node>` | Marks a node as draining: it releases its leases so other nodes take its workloads over at once, and claims nothing. |
| `felixd undrain <node>` | Lifts the mark, so the node claims work again. |

Flags of `run`: `--id`, `--input` (up to 64), `--snapshot-every` (1000), `--snapshot-every-ms`, `--fuel`,
`--max-memory`, `--snapshot-fuel`, `--replication-factor` (3; use 1 against a single broker) and
`--wait-secs` (60), how long to wait for admission's decision and spec record.
The limits are stored in the spec, so every node runs the workload under the same ones.

Flags of `agent`:

| Flag | Default | What it does |
|---|---|---|
| `--node-id` (`FELIXD_NODE_ID`) | host name and process id | This node's name in its leases. Each agent needs its own. |
| `--lease-ttl-ms` | 10000, at least 2000 | How long a lease lives without renewal. Use the same value on every node. |
| `--max-workloads` | 64 | The most workloads the node holds at once. Refused above about 125,000, where a node's facts would not fit in one record. |
| `--claim-delay-ms` | 1000 | The longest random wait before claiming a free workload. |
| `--assignment-bound-ms` | 15000 | The longest a free workload is left to the node the scheduler assigned it to, if that node can take it. |
| `--assignment-grace-ms` | 2000 | The longest an unassigned workload waits for a healthy scheduler to read its spec. |
| `--facts-every-ms` | three lease TTLs | How often the node appends its facts for the scheduler when they have not changed. |
| `--stuck-after-ms` | 60000, or the lease TTL if longer | How long a held workload may keep failing on a transient error, with no new input stepped and no record appended, before the node records it as `degraded` and releases the lease. At least the lease TTL. 0 turns it off, which a rolling upgrade from a build that predates the record needs. |
| `--degraded-backoff-ms` | 120000 | How long a node that released a workload as degraded leaves it to other nodes. |
| `--metrics-bind` (`FELIXD_METRICS_BIND`) | off | Serves Prometheus metrics at `/metrics` on this address. |
| `--metrics-max-workloads` | 100 | The most workloads with series of their own. Needs `--metrics-bind`. |
| `--admission-token-file` (`FELIXD_ADMISSION_TOKEN_FILE`) | the node's own token | The broker token of admission's credential. |

Flags of `check`: `--id` (repeat for several; every workload by default), `--complete`, which
also reports outputs missing from the end of a stream, and `--upstream`, which also checks every
workload whose output a checked one reads, up the pipeline. Stages are checked upstream first.

Flags of `replay`:

- `--id`: the workload.
- `--to`: an input offset or an RFC 3339 time such as `2026-10-29T14:02:00Z`. For a workload with
  several inputs, a merge log offset or a time compared with when entries were merged. Required
  unless `--diff`.
- `--diff`: compare each output with the recorded one. It exits 1 on a divergence.
- `--from` (with `--diff`): the first input compared. It also picks the snapshot the diff starts
  from, the nearest one before it, so the default 0 starts from the initial state.
- `--all` (with `--diff`): report every divergence, not only the first. With `--module` on admission
  it lists each recorded request a policy change decides otherwise.
- `--module`: run another module instead of the workload's own, to check an upgrade. A component
  file, or the sha256 of one in the `modules` cache.
- `--from-start`: start from the initial state even where a snapshot could be restored.

Without `--module`, a replay restores the nearest snapshot before its first input (`--from` for a
diff, the end of the range for `--to`). A new module restores it only if the `felixd.snapshot`
section of its component names the layout, the format of the saved state, that the workload's
module wrote ([docs/contract.md](../../docs/contract.md#snapshot-layouts)). Otherwise it starts
from the initial state. Stderr says where the replay started and, when a snapshot was not used,
why. A new module that declares it reads the snapshot and then traps or runs out of fuel restoring
it is an error, "snapshot not readable by the new module", with the snapshot's input offset. It
exits 1 like a divergence, but writes only to stderr. A diff starts at input 0 unless `--from` says
otherwise, and no snapshot is before input 0, so an upgrade check from a snapshot needs `--from`.
[docs/design.md](../../docs/design.md#checking-an-upgrade) has an example and every case.

Flags of `ps`: `--id` (repeat for several; every workload by default).

Flags of `drain`: `--wait`, which returns once no live lease names the node and the node says it
claims nothing (or it is down), `--timeout-secs` (300) for `--wait`, after which it exits 1, and
`--replication-factor` (3) for the `drains` cache if this creates it.

Connection flags, each also read from an environment variable: `--brokers` (`FELIX_BROKERS`),
`--server-name` (`FELIX_SERVER_NAME`), `--tenant` (`FELIX_AUTH_TENANT`), `--namespace`
(`FELIX_NAMESPACE`), `--token-file` (`FELIX_AUTH_TOKEN_FILE`), `--ca-file` (`FELIX_CA_FILE`),
`--controlplane-url` (`FELIX_CONTROLPLANE_URL`) and `--controlplane-token-file`
(`FELIX_CONTROLPLANE_TOKEN_FILE`, needed by `init`, by `run` where admission does not run, by
`drain` and by a node that is to hold admission). A node needs read on the `drains`
cache, or it claims nothing. `felixd <command> --help` lists them all.

`--brokers` takes `ip:port` or `host:port` (an IPv6 address in brackets), comma separated. A name is
resolved when the agent connects and again whenever no known broker answers, so brokers that move
to new addresses are found again. The broker, control plane and admission token files are read again each time the token is used, so
a token rotated in place is picked up before the old one expires. A file that is missing, empty or
half written keeps the previous token until a good one appears; a file unreadable at startup is an
error. `--admission-token-file` behaves the same way.

To place workloads with the scheduler, submit it as the workload `scheduler` reading `sys.specs`
and `sys.facts` (the container image has it at `/usr/local/share/felixd/scheduler.wasm`):

```
felixd run --id scheduler --input sys.specs --input sys.facts scheduler.wasm
```

Nodes then follow its assignments, with first claim wins as the fallback. Stopping it (removing
its spec) puts every node back on first claim wins. A node needs publish on `sys.facts` and read
on `wl.scheduler.out` for this.

To admit work through admission, run `felixd init admission.wasm` (the container image has it at
`/usr/local/share/felixd/admission.wasm`) once per namespace. From then on `felixd run` publishes
a request on `sys.intake`, and a node runs a spec record written after admission's own only if it
names an accepting decision on `wl.admission.out` for exactly that spec. Only nodes with a control
plane token hold admission, since its holder creates the streams of what it accepts.
Every node must run a build that knows admission before `init`: an older one ignores it.

`init` also writes the grants of three roles, `role:felixd.<namespace>.node`, `.admission` and
`.submitter` (the table is in [docs/design.md](../../docs/design.md#grants)); assign principals to
them with `felixctl rbac grouping add`. The operator running `init` needs `rbac.view` and
`rbac.policy.manage`. A node holding admission runs with the admission principal's broker token in
`--admission-token-file` and its control plane token in `--controlplane-token-file`; only that
credential may write `sys.specs`. The `sys.*` streams live in the namespace `<namespace>.sys`, so a
node's grant on every stream of the namespace does not reach them.

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
