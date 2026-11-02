# Operating felixd

How to run felixd for real: what to start, how to configure it, who may do what, how long to
make the lease TTL, what to watch, and what a failover does and does not promise. For how it
works inside, see [design.md](design.md). For a first run on one machine, see the
[README](../README.md#try-it).

## What runs where

felixd keeps all of its state in [Felix](https://docs.getfelix.dev/) and nothing on its own disk
that matters, so a node can lose its disk and rejoin. A cluster is:

- **Felix**: brokers and a control plane. Use three brokers and replication factor 3
  (`Quorum`) to survive a broker failure. One broker is fine for trying things out.
- **One `felixd agent` per machine.** Each agent claims workloads, runs them and writes their
  output to Felix. Several agents on one machine are not independent failure domains.
- **The `felixd` command line**, run from anywhere that can reach Felix, to submit workloads
  (`run`), look at the cluster (`ps`, `check`, `replay`) and move work (`drain`, `undrain`).

The agents never talk to each other. They find out about each other, and about the work to be
done, by reading Felix.

## Before you start

- A Felix release that felixd is built on. `.felix-version` in the repository names it, and the
  container image and the compose files use it.
- A tenant and a namespace for felixd (`default` unless you set `FELIX_NAMESPACE`).
- For a three-broker cluster, the Felix features failover needs, finalized once by a Felix
  operator: `generation_start`, `majority_ack`, `lease_free_reads` and `fenced_caches`.
  `deploy/compose.yaml` shows the calls in its `felix-fleet` service, and Felix's own docs say
  more.
- A token for each role below, in a file.

## Run a node

Install a release binary ([README](../README.md#install)) or build the container image:

```
docker build -f deploy/felixd.Dockerfile -t felixd:local .
```

The image holds the `felixd` binary and the example workloads, and runs as user 65532. Start
one agent per machine and give each a unique `--node-id`:

```
felixd agent --node-id node-1 --lease-ttl-ms 10000
```

An agent runs until it is interrupted. On SIGINT (Ctrl-C) it stops its workloads and deletes
its leases, so other nodes take over at once. It does not handle SIGTERM, so a plain `docker
stop` or `systemctl stop` waits for the stop timeout and then kills it, and its leases wait out
the TTL. Either [drain](#upgrade-or-retire-a-node) the node first, or make your supervisor send
SIGINT (`stop_signal: SIGINT` in Compose, `KillSignal=SIGINT` in systemd).

If the agent exits, restart it. A restarted agent does not adopt the leases it held before,
which makes two processes with the same `--node-id` safe; it waits for them to expire.

## Configuration

Every command reads the same connection settings, as flags or environment variables.

| Variable | Flag | Meaning |
|---|---|---|
| `FELIX_BROKERS` | `--brokers` | Broker addresses, `host:port` or `ip:port`, comma separated. One is enough; the rest are discovered |
| `FELIX_SERVER_NAME` | `--server-name` | The name on the brokers' certificate (`localhost`) |
| `FELIX_AUTH_TENANT` | `--tenant` | The Felix tenant |
| `FELIX_NAMESPACE` | `--namespace` | The namespace for felixd's streams and caches (`default`) |
| `FELIX_AUTH_TOKEN_FILE` | `--token-file` | File holding the broker token |
| `FELIX_CA_FILE` | `--ca-file` | PEM file to trust for the brokers' certificate. The platform roots are used when left out |
| `FELIX_CONTROLPLANE_URL` | `--controlplane-url` | The control plane, such as `http://felix-controlplane:8443` |
| `FELIX_CONTROLPLANE_TOKEN_FILE` | `--controlplane-token-file` | File holding the control plane token. Needed to create streams and caches, so by `run`, `init`, `drain` and an agent that runs admission |

Tokens are read from files only, never from arguments or the environment, and are read again each
time they are used. Replace the file in place to rotate a token; an agent keeps its leases. A file
caught empty or half written keeps the previous token. felixd never writes a token to a log.

`RUST_LOG` sets the log level (`felixd=info,felixd_agent=info` by default).

The agent's own flags, all optional:

| Flag | Default | Meaning |
|---|---|---|
| `--node-id` (`FELIXD_NODE_ID`) | host name and process id | This node's name in leases, `ps` and metrics. Must be unique and should be stable |
| `--lease-ttl-ms` | 10000 (at least 2000) | How long a lease lives without renewal. Use the same value on every node. See [sizing](#sizing) |
| `--max-workloads` | 64 | The most workloads this node holds. A full node claims nothing more |
| `--claim-delay-ms` | 1000 | The longest random wait before claiming a free workload, so the first node to see it does not take everything |
| `--stuck-after-ms` | 60000, or the TTL if longer | How long a held workload may keep failing on a transient error, with no progress, before the node records it as degraded and releases it. At least the TTL; 0 turns it off |
| `--degraded-backoff-ms` | 120000 | How long a node leaves a workload alone after releasing it as degraded, so another node gets a turn |
| `--assignment-bound-ms`, `--assignment-grace-ms`, `--facts-every-ms` | 15000, 2000, three TTLs | Only matter while a [scheduler](design.md#placement) runs |
| `--metrics-bind` (`FELIXD_METRICS_BIND`) | off | Serve Prometheus metrics, such as `127.0.0.1:9464` |
| `--metrics-max-workloads` | 100 | Workloads that get their own metric series |
| `--admission-token-file` (`FELIXD_ADMISSION_TOKEN_FILE`) | the node's own token | Broker token of admission's credential, for a node that runs admission |

`felixd <command> --help` lists everything, including the limits `run` stores in a spec.

## Who may do what

Felix decides, with RBAC policies on each token. felixd needs a different set for each job, so use
a different principal for each:

| Principal | Used by | May |
|---|---|---|
| Node | `felixd agent` | Read every stream and cache of the namespace and its system namespace; publish to workload streams and `sys.facts`; write `leases`, `epochs`, `nodes` and `workloads` |
| Admission | the node that runs admission | The only role that writes `sys.specs`; creates the streams of accepted workloads |
| Submitter | `felixd run` | Put modules, publish requests on `sys.intake`, read decisions and specs |
| Viewer | `felixd ps`, `check`, `replay` | Read everything. `check` and `replay` also probe a stream's tail with a conditional publish, so they need publish on the streams they read |
| Operator | `felixd init`, `drain`, `undrain`, `run` without admission | Create namespaces, streams and caches; `init` also needs `rbac.view` and `rbac.policy.manage` |

[design.md](design.md#grants) has the exact rules. To set them up:

1. Run `felixd init admission.wasm` once per namespace, as an operator. It creates the namespace
   and its system namespace (`<namespace>.sys`, where `sys.specs`, `sys.intake` and `sys.facts`
   live, out of reach of a node's grant on the workload streams), writes the policies of three
   roles, `role:felixd.<namespace>.node`, `.admission` and `.submitter`, and starts admission.
   Running it again changes nothing. The image has the module at
   `/usr/local/share/felixd/admission.wasm`.
2. Assign each principal to its role with `felixctl`:

   ```
   felixctl rbac grouping add p:node-1 role:felixd.default.node
   felixctl rbac grouping add p:ci role:felixd.default.submitter
   ```

3. From then on `felixd run` goes through admission, and nodes run only what it accepted.

Today a node can write any workload's output stream. The fence keeps the output correct, but a
compromised node could write garbage. Per-lease tokens, which narrow a node to the workloads it
holds, wait on Felix ([#74](https://github.com/GetFelix/felixd/issues/74)).

## Sizing

**Lease TTL.** A node renews every TTL/3 and stops its workloads two thirds of a TTL after its
last successful renewal, before anyone else can claim. When a node dies, its workloads resume
elsewhere in about one TTL. So:

- A shorter TTL means faster failover and more renewal traffic. A renewal is a write to Felix
  per workload, three times per TTL.
- A TTL that is too short for your broker's worst pause makes healthy nodes lose leases. Watch
  `felixd_lease_handoffs_total{reason="lost"}`; it should stay at 0 unless you drain or kill a
  node.
- Set the same value on every node. The compose files use 4000 for quick demonstrations. The
  default of 10000 is a safer start.

**Workloads per node.** `--max-workloads` caps one node. Fewer, larger nodes give less spare
capacity when one fails: the survivors must have room for its workloads, or they stay
`waiting`. Keep the sum of the survivors' caps above the total.

**Cluster size.** The scale test measures how many workloads a cluster carries; see
[Scale](design.md#scale).

**Replication.** Use `--replication-factor 3` on `run` and `init` against three brokers. The
default is 3; 1 is for a single broker.

**Snapshots.** `run --snapshot-every` (1000 inputs by default) and `--snapshot-every-ms` bound
how much a new owner replays after a failover. `--keep-snapshots K` lets the history before the
oldest of K snapshots be trimmed.

**Modules.** A module can be up to 64 MiB; one over 8 MiB is stored in chunks
([design.md](design.md#modules)).

## What failover and replay guarantee

When a node dies or is drained, another claims its workloads and carries on:

```mermaid
sequenceDiagram
    participant A as Node A (holds W)
    participant F as Felix
    participant B as Node B
    A->>F: renew lease (every TTL/3)
    Note over A: A freezes or dies
    F-->>B: lease for W expired
    B->>F: claim lease, new epoch
    B->>F: write claim record on W's output
    B->>F: restore latest snapshot, resume at its input offset
    Note over A: A wakes up
    A->>F: write output at the old tail
    F-->>A: refused, the claim record moved the tail
```

What you can rely on:

- **Each workload runs on one node at a time, as far as output goes.** Two nodes may briefly both
  believe they hold a workload, for example after a long pause. Only the one at the output's
  current tail can write; the other's write is refused and it stops. The lease decides who
  should run, and the fence decides what is written.
- **Each input produces its outputs once.** Not lost and not repeated, through lost answers,
  partial batches, crashes and takeovers. `felixd check` verifies it from the log.
- **Takeover resumes from a snapshot**, not from the start, and costs the inputs since the last
  snapshot.
- **Replay is exact.** A workload is deterministic, so `felixd replay` rebuilds its state at any
  input offset or time, and `felixd replay --diff` compares a replay, or a new module, with what
  was recorded.

What it does not promise:

- **No guarantee about timing.** Failover takes about one TTL. Input published meanwhile waits in
  the log and is processed afterwards.
- **A slow subscriber's view is not the log.** Always judge a workload by its output stream.
- **Lease exclusivity rests on the broker's clock.** A broker whose clock jumps forward can
  expire leases early. That costs duplicate work and refused writes, never wrong output.
- **No protection from a hostile node** until per-lease tokens exist (see above).

## Try a failover on three nodes

`deploy/compose.yaml` starts three Felix brokers, a control plane and three felixd nodes with
development credentials. Use `docker compose` or `podman compose` (the docker-compose provider);
the [README](../README.md#try-it-on-three-nodes) has the Podman setup. It publishes no ports, so
the commands run inside the stack.

```
docker compose -f deploy/compose.yaml up -d --build
docker compose -f deploy/compose.yaml run --rm felixd run --id counter \
  --input in --replication-factor 3 /usr/local/share/felixd/counter.wasm
docker compose -f deploy/compose.yaml --profile producer up -d producer
docker compose -f deploy/compose.yaml run --rm felixd ps
```

`ps` lists the workloads and the nodes. Find the node that holds `counter` and kill its
container, which is `felixd-<node id>`:

```
docker kill felixd-node-1
docker compose -f deploy/compose.yaml run --rm felixd ps
```

Within a few seconds (the compose file uses a 4 s TTL) `ps` shows another holder with a higher
epoch. To move a workload without a failure, drain its node; the move is immediate:

```
docker compose -f deploy/compose.yaml run --rm felixd drain node-2 --wait
docker compose -f deploy/compose.yaml run --rm felixd undrain node-2
```

Stop the producer and check the output stream. `check` writes nothing; `--complete` also reports
outputs missing from the end of the stream, which is what you want once the workload has caught
up:

```
docker compose -f deploy/compose.yaml stop producer
docker compose -f deploy/compose.yaml run --rm felixd check --complete
docker compose -f deploy/compose.yaml down -v
```

`scripts/compose-failover.sh` runs this sequence, and CI runs it on every pull request under
Docker and under rootless Podman.

## Watching a cluster

- **`felixd ps`** is the view from the log, and the same from any machine. It shows each
  workload's state (`running`, `claiming`, `waiting` for a holder, `needs-operator`, `degraded`,
  `failed`, `stopped`), its holder and epoch, how far behind each input it is, and each node's state.
- **Metrics.** Start agents with `--metrics-bind` and scrape each one. [metrics.md](metrics.md)
  lists every metric and the questions they answer. The endpoint is plain HTTP without
  authentication and names workloads and streams, so keep it on an internal network. `/live`
  answers `ok` for a probe.
- **`felixd check`** replays every workload over its input and compares the result with its
  output stream. Run it on a schedule. It exits 1 on anything it finds.

## When a workload needs you

| `ps` shows | Meaning | What to do |
|---|---|---|
| `waiting` | No node holds it | If every node is draining or full, undrain a node or raise `--max-workloads`. Otherwise give it a few seconds |
| `needs-operator` with a reason | Something no node can get past: a missing module, a module built for contract 0.1, a restore out of fuel | Fix the cause, such as raising the limit, and run `felixd run` again for the same id. A new spec record clears it |
| `degraded` | A node kept failing on a transient error, such as an unreachable Felix, and released the workload so another node can try | Check the node's log and Felix. Nothing else to do if another node has taken it |
| `failed` with a reason | A trap, out of fuel, `poison`, or a contract break. The workload is stopped | Read the reason. Submit a fixed module under a new id |
| `failed` with a divergence line under the table | A node replayed the inputs and got an output other than the one on the stream | The line names the input, the node, the build and both digests. Find out whether the module, the inputs or the build changed; `felixd replay --diff` reproduces it |

Stop a workload by deleting its entry in the `workloads` cache:

```
felixctl cache del workloads counter
```

Its streams stay. felixd has no stop command yet ([#101](https://github.com/GetFelix/felixd/issues/101)).

## Upgrade or retire a node

Drain first. `felixd drain node-1 --wait` makes the node release each workload so others carry
on at once, and returns when it holds nothing (or after `--timeout-secs`, 300). Then stop it,
replace the binary, start it and `felixd undrain node-1`. Do the nodes one at a time, and let
`ps` show every workload running before you move to the next.

A node that meets a record from a newer build leaves that workload alone and never fails it.
Upgrade every node before you rely on a feature of the new build.

A build from before the degraded record cannot read it and fails the workload as poison, so
while any such node is left, start the new agents with `--stuck-after-ms 0`. The new nodes then
retry a stuck workload for ever and write no record. Restart them without it once every node is
new.
