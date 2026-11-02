<p align="center"><img src="docs/brand/felixd-mark.png" alt="felixd" width="200"></p>

# felixd

A node agent that runs programs across a cluster with
[Felix](https://github.com/GetFelix/felix) as its only backend.

> **Status:** milestone 3 done. Agents hold a lease per workload, spread
> workloads across nodes, and take a dead node's workloads over within one
> lease TTL. A new owner fences the output stream with a claim record, so a
> node frozen past its lease writes no output when it wakes, and each output
> lands once through lost answers, partial batches and crashes. A three-node
> failover test that keeps killing lease holders runs on every pull request
> and ends with `felixd check`. `felixd replay`
> rebuilds a workload as of any input offset or time, and `--diff` checks a
> replay, or a new module, against the recorded outputs. Workloads implement
> [contract 0.3](docs/contract.md) (0.2 still runs): named and keyed outputs,
> rejections, chunked snapshots and the authenticated publisher of each input. Pipelines, where one workload reads another's output,
> survive failover at every stage. Operations so far: `felixd ps`, `felixd
> drain`, Prometheus metrics, and an optional scheduler, itself a felixd
> workload, whose assignments the nodes follow with first claim wins as the
> fallback. The plan is in
> [docs/design.md](docs/design.md) and the work is tracked in
> [milestones](https://github.com/GetFelix/felixd/milestones).

## Install

Each `v*` release has builds for Linux x86_64, Linux aarch64 and macOS aarch64
(Apple silicon), a `SHA256SUMS` file and its Sigstore signature. Download the
archive for your platform with those two files from the
[releases page](https://github.com/GetFelix/felixd/releases), then check the
signature, which proves the checksums came from this repository's release
workflow, and the archive:

```
cosign verify-blob \
  --bundle SHA256SUMS.sigstore.json \
  --certificate-identity-regexp '^https://github.com/GetFelix/felixd/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS
sha256sum --ignore-missing --check SHA256SUMS   # shasum -a 256 -c on macOS
tar -xzf felixd-<tag>-<target>.tar.gz
```

The builds are reproducible. The workflow builds each target twice from
different directories and fails unless the archives match byte for byte, with
a locked `Cargo.lock`, the pinned toolchain, `SOURCE_DATE_EPOCH` set to the
commit time and build paths remapped. To check one yourself, check out the
tag and run `scripts/release-build.sh <archive-name> out` with the same
toolchain on the same platform, then compare the archive's hash.

## Try it

You need Rust and a container runtime. The test stack starts one Felix broker
and control plane with development credentials:

```
docker compose -f tests/felix/compose.yaml up -d --wait
tests/felix/credentials.sh /tmp/felix
export FELIX_BROKERS=127.0.0.1:5000 FELIX_AUTH_TENANT=dev \
  FELIX_AUTH_TOKEN_FILE=/tmp/felix/client.jwt FELIX_CA_FILE=/tmp/felix/broker-cert.pem \
  FELIX_CONTROLPLANE_URL=http://127.0.0.1:8443 \
  FELIX_CONTROLPLANE_TOKEN_FILE=/tmp/felix/controlplane.jwt
```

Run the restart test, which submits the example workloads, kills the agent
mid-stream, restarts it and checks the output stream:

```
FELIXD_REQUIRE_FELIX=1 cargo test -p felixd --test restart -- --test-threads=1
```

The failover test runs three agents and six workloads under a steady
producer, kills the live node holding the most leases every 3 s and starts
it again, every other time with a fault that also crashes it between an
append and its answer, then checks every output stream. `FELIXD_FAILOVER_SECS` sets how long
it runs (30 by default; a nightly CI job runs it for 10 minutes):

```
FELIXD_REQUIRE_FELIX=1 cargo test -p felixd --test failover -- --test-threads=1
```

By hand, with a workload component and a durable input stream that already exists. Build a
component from an example workload, then submit it:

```
cargo install wasm-tools
cargo build --manifest-path workloads/Cargo.toml --release --target wasm32-unknown-unknown --package counter
wasm-tools component new workloads/target/wasm32-unknown-unknown/release/counter.wasm -o counter.wasm
```

```
felixd run --id counter --input in --replication-factor 1 counter.wasm
felixd agent
```

`felixd run` stores the module in the `modules` cache, creates the `wl.<id>.out` and
`wl.<id>.snap` streams (and `wl.<id>.merge` when `--input` is given more
than once), appends the spec as a record to the `sys.specs` stream and points the `workloads` cache at it. `sys.specs` keeps every version of every
spec. A module can be up to 64 MiB. One over 8 MiB is stored as 4 MiB chunks
and a manifest; see [Modules](docs/design.md#modules). It creates felixd's caches and
streams with three `Quorum` replicas unless `--replication-factor` says
otherwise; the single-broker test stack needs 1. The agent watches the
`workloads` cache, checks each spec against its record, claims the lease of each
one nobody holds and runs it while the lease holds. Start a second agent and kill the first: within one lease TTL
(10 s by default and at least 2 s, `--lease-ttl-ms`) the second carries on from the right input,
with nothing missing and nothing repeated. Each agent needs its own
`--node-id`; the default is the host name and process id. An agent holds at
most `--max-workloads` (default 64) and waits a random time of up to
`--claim-delay-ms` (default 1000) before claiming a free workload, so that the
first agent to see it does not take everything. Each agent also writes itself
to the `nodes` cache, with the same TTL as its leases.
The connection settings are the environment variables `felixctl` reads, plus
`FELIX_NAMESPACE` (default `default`).
`felixd check` replays each workload's module over its input and compares
the result with its output stream. It reports gaps, duplicates, records out of
order, epoch regressions, outputs that differ from the replay and snapshots
whose output tail does not match their input offset, and exits with 1 if it
finds any. `--id` limits it to one workload; `--complete` also
reports outputs missing from the end of a stream, for when the workloads
should have caught up. `--upstream` also checks the workloads a checked one
reads from, up the pipeline. It replays from the first input; once Felix has
deleted the head of an output stream it starts from the oldest snapshot the
workload keeps (`--keep-snapshots`), which the output line says, and
`--from-snapshot` asks for that earlier. A snapshot's state is taken as given.
It writes nothing.

If Felix refuses a create, `felixd run` stops and prints Felix's reason. The usual
cause is a stream or cache with that name that was deleted earlier: Felix will not
reuse a deleted name until `stream_incarnations` (or `cache_incarnations`) is
finalized, and its reason names the call. A new cluster does this when its first
broker registers (brokers need `FELIX_NODE_ID`). The single-broker test stack never
does, so use a new `--id` there.

A workload can read another's output stream. This pipeline counts the inputs
on `in`, then counts the first stage's outputs:

```
felixd run --id first --input in --replication-factor 1 counter.wasm
felixd run --id second --input wl.first.out --replication-factor 1 counter.wasm
felixd check --id second --upstream --complete
```

A downstream gets only the records its upstream addresses to `out`, with
their keys. Claims, failures, needs-operator, trim, degraded and divergence
records, rejections and other names are passed over, so
a stage can fail over, even in the middle of a batch, without the next stage
seeing anything but the same outputs in the same order.

`felixd replay --id counter --to 41` rebuilds the workload's state after
input 41, and `--to 2026-10-29T14:02:00Z` its state after every input appended
before that time. For a workload with several inputs both count on its merge
log: an offset is a merge log offset, and a time covers the records merged
before it, which is not when they were appended to their own inputs. It
restores the nearest snapshot at or before that point on a fresh instance, runs the inputs after it and prints what the workload's
`inspect` returns. `--diff` replays a range (`--from`, `--to`, by default every
input with a recorded output) and compares each output with the one on the
stream, reporting the first divergence with the node, epoch, input offset and
both digests, and exits with 1 if it finds one. Replay writes nothing,
but until Felix can report a stream's tail to a reader
([Felix #1621](https://github.com/GetFelix/felix/issues/1621)) it needs publish
grants on the streams it reads, as `felixd check` does.

To check an upgrade before it goes live, run the new version of a module over
the recorded inputs and compare its outputs with the recorded ones. This builds
a changed `counter`, which counts by two once the total reaches ten, and checks
it against the `counter` workload from above. Submit that workload with
`--snapshot-every 5` and give it 12 inputs first:

```
cargo build --manifest-path workloads/Cargo.toml --release --target wasm32-unknown-unknown --package counter --features changed
wasm-tools component new workloads/target/wasm32-unknown-unknown/release/counter.wasm -o counter-changed.wasm
felixd replay --id counter --diff --from 5 --module counter-changed.wasm
```

It runs the new module from input 5 and prints the first input whose output
differs, then exits with 1:

```
first divergence at input 10 output 0: recorded by node <node> at epoch 1 with sha256 <recorded>, replayed with sha256 <replayed>
```

A new module that behaves the same prints
`inputs from 5 below 12: 7 outputs match` and exits with 0. `--module` takes a
component file, as here, or the sha256 of a module already in the `modules`
cache. `sha256sum counter-changed.wasm` gives the hash of a file, and `felixd
run` stores the module it submits in that cache.

Before it runs input 5, the new module needs the state the workload had there.
Stderr says where it got it from. Here it says `started from the snapshot of
input 4`: the new module restored the snapshot the old module saved after input
4, which is quick. The alternative is to start from the initial state and run
every earlier input, which takes as long as the history is long.

A new module can restore the old module's snapshot only if it says it
understands the snapshot's layout, the format of the saved state. It says so in
a custom section of its component named `felixd.snapshot`, which names the
layout it writes and the others it can restore. The `counter` example declares
one ([Snapshot layouts](docs/contract.md#snapshot-layouts)). When the new module
declares none, or not that layout, the command starts from the initial state
and says why. `--from-start` does that on purpose. A diff starts at input 0
unless `--from` says otherwise, and there is no snapshot before input 0, so
pass a `--from` after a snapshot. [Checking an
upgrade](docs/design.md#checking-an-upgrade) lists every case.

`felixd run --snapshot-every 1000 --snapshot-every-ms 60000` snapshots a
workload every 1000 inputs and at the first input of each minute, by the
inputs' append times on the broker, whichever comes first, so a slow input
still gets snapshots and every node takes them at the same inputs. With
`--keep-snapshots 3` the holder records, after each snapshot, what history
before the oldest of the last three its resume no longer reads (a trim record
on the output stream), which `felixd check --from-snapshot` starts from.
Nothing is deleted until Felix can trim a stream
at an offset ([Felix #1662](https://github.com/GetFelix/felix/issues/1662)).
A workload is a Wasm component of [workload contract 0.3](docs/contract.md)
(`wit/0.3/workload.wit`); components of 0.2 (`wit/0.2/workload.wit`) run too.
Each step gets the input's stream, offset, append time, key, payload and, from
0.3, the publisher: the identity the broker authenticated the write from, which
is `none` unless the broker records publishers (see below). A step returns outputs, each with a name and an optional key, or
rejects the input. A rejection is recorded on the output stream and the
workload carries on. Snapshots are written in chunks, and a chunk the last
snapshot already holds is not written again. `felixd run` refuses a component
built against contract 0.1, and an agent that finds one in a spec leaves it
to an operator rather than failing it.
`felixd run --fuel --max-memory --snapshot-fuel` sets the workload's limits:
fuel per step, memory, and fuel for a whole snapshot or restore. A snapshot that
runs out of fuel is skipped and the workload resumes from the previous one; a restore that
runs out is left to an operator, who can raise the limit. They are stored in
its spec, so every node runs it under the same ones. Running `felixd run`
again for the same id restarts the workload under the new spec, but a new
module or input is refused once the workload has written anything; use a new
id for new code. `felixd run` refuses it, and so does the agent when the output
stream's first claim names a spec, which claims written before spec records
don't. Each claim names the spec it runs, so `felixd check` and
`felixd replay --diff` judge each output under the module and limits it ran with. A workload that traps, runs
out of fuel, returns `poison`, breaks the contract (an invalid output name, an
effect request, a chunk over 4 MiB) or finds its streams in a state it can't resume from is marked
`failed` in its spec, with the reason. The failure is first a record on its
output stream, fenced by epoch like an output, so a node that was frozen
while another took over can't fail it. Its class says whether it is poison or
needs an operator.

`felixd ps` shows where every workload is: its state (`running`, `waiting`
for a holder, `needs-operator` with the reason, `degraded` with the class and
detail, `failed` with the reason and, if it failed on a replay that differed,
where it diverged, or `stopped`), the node holding it and its epoch, the output stream's tail, and
for each input the next offset it steps, the input's tail and the lag between
them. A workload with several inputs gets a line per input, for how far each
is merged, and one for its merge log. Below that it lists the nodes: up or
not, leases held, `--max-workloads`, version and whether draining. It reads
everything from the caches and streams, so it shows the same on any machine,
and it writes nothing. A workload's position comes from its last output or
snapshot, so one that writes nothing for a while shows behind where it is.

A workload shows `degraded` when the node holding it gave up on it. While a
node holds a workload it retries errors from Felix, such as a stream it cannot
read or write. If the retries keep failing for `--stuck-after-ms` (60 s by
default, never less than the lease TTL) and no attempt got further, meaning it
stepped no new input and appended no output, the node writes a `degraded`
record with the error, deletes its lease and leaves the workload alone for
`--degraded-backoff-ms` (120 s by default). Another node can then claim the
workload, after its usual random `--claim-delay-ms`. While a scheduler runs and
the workload is assigned to the node that gave up, the others first wait for
that node for up to `--assignment-bound-ms` (15 s). Time a node spends running
well does not count: an attempt that ran for a whole `--stuck-after-ms` before
it failed starts the count again, so a quiet workload whose connection drops
now and then is left alone. The spec stays `running`,
so nothing has to be done for the workload to carry on. `ps` shows `degraded`,
the node and the error until the next holder's claim record is written:

```
WORKLOAD  STATE     HOLDER  EPOCH  OUTPUTS  INPUT  NEXT  TAIL  LAG
counter   degraded  -       (7)    120      in     120   140   20
counter: degraded (transient) on node-2: the input stream closed
```

If a workload keeps coming back as `degraded`:

1. Read the line under the table. It names the node that gave up and the error.
2. On that node, search its log for `stuck, recording it and releasing the
   lease`. The line has the workload, the epoch and the node, and the
   `retrying` warnings just before it show each error.
3. A node that cannot reach Felix at all loses its leases first, at two thirds
   of the lease TTL, so `degraded` usually points at the workload's own
   streams or module rather than at the node. If it comes back on every node,
   look there; `felixd check --id counter` reads them. Draining the node only
   moves the problem. `felixd_degraded_total` in a node's metrics counts how
   often it gave up.

The limits belong to each node. To change them:

```
felixd agent --lease-ttl-ms 10000 --stuck-after-ms 30000 --degraded-backoff-ms 60000
```

An agent refuses to start if `--stuck-after-ms` is above 0 and below the lease
TTL. `--stuck-after-ms 0` turns the bound off: the node retries for ever and
writes no degraded record. Use it when upgrading from a build that predates
the record, which cannot read it. An old node that claims a workload whose last
record is a degraded one marks the workload `failed`. One that meets it as the
first record of the stream never claims the workload and logs `could not read
the output stream`. A workload reading that stream needs an operator. So start
the new build with `--stuck-after-ms 0` on every node, and restart without it
once no old node is left.

A workload fails as `poison` with `replayed output differs from the record` or
`records ... that differ from this workload's output` when a node replays its
inputs and gets an output other than the one the stream holds. Either the
stream was written by something else, or the workload is not deterministic, or
the node's build runs the module differently from the one that wrote the
stream. The node that finds it appends a `divergence` record just before the
failure record, and `ps` prints it under the failure line:

```
WORKLOAD  STATE   HOLDER  EPOCH  OUTPUTS  INPUT  NEXT  TAIL  LAG
counter   failed  -       (7)    54       in     41    60    19
counter: failed (poison): replayed output differs from the record at offset 52 of the output stream
counter: diverged at input 41 output 0 on node-2 at epoch 7: the stream holds sha256 7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed, the replay produced sha256 3fc4ccfe745870e2c0d99f71f30ff0656c8dedd41cc1d7d3d376b0dbe685e2f3, started from the snapshot of input 30, contract 0.2.0, wasmtime 50.0.0, felixd 0.1.0: replayed output differs from the record at offset 52 of the output stream
```

The line gives the input and output that differ, the node and epoch that found
it, the sha256 of what the stream holds there and of what the node's replay
produced, the snapshot the replay started from (or the initial state), and the
build: the workload contract, wasmtime and felixd versions. Each digest covers
the whole output as compared (name, key and payload, not the writer's epoch),
so it is not the payload digest `felixd replay --diff` prints. To look into it:

1. Run `felixd replay --id counter --diff` on your own machine. It replays the
   same inputs and prints the first output that differs, and says which
   divergence record the stream holds for it.
2. If it finds nothing, your build replays the stream cleanly, so the node that
   found it ran something different. Compare the build in the record with
   yours: `felixd --version` and the VERSION column of `ps` give the felixd
   version.
3. If it finds the same input, the stream holds a record this module did not
   write, perhaps an older version of it. `felixd replay --id counter --diff
   --module <old>` finds out.

The record is only written for a workload that is failing already. A build
from before it reads it as an unknown kind: an old node that claims the
workload after its spec is submitted again fails the new spec as poison, and
an old `ps`, `check` or `replay` reports the stream unreadable, and a
downstream workload run by an old node needs an operator. There is no
switch, so finish a rolling upgrade before submitting a failed workload again.
[docs/records.md](docs/records.md) has the layout.

`felixd drain node-1` takes a node out of service without waiting out lease
TTLs: the node stops each workload it holds and deletes the lease, so another
node claims it at once and carries on from the same input, and the node
claims nothing more. `--wait` returns once it holds no lease and has said it
claims nothing, or gives up after `--timeout-secs` (300) with exit code 1.
Nodes need read on the `drains` cache; one that cannot read its mark claims
nothing, and `ps` shows a mark it has not acknowledged as `pending`. `felixd undrain
node-1` lets it claim again. Draining the only node that is up leaves its
workloads `waiting`, and the command says so. The node reads its mark every
second and again before each claim, so a node drained while it was stopped
does not take work back when it wakes.

Placement is first claim wins unless a scheduler runs. `felixd run --id
scheduler --input sys.specs --input sys.facts scheduler.wasm` starts one: a
workload that reads spec records and the facts each node appends to
`sys.facts`, and writes an assignment each time where a workload should run
changes. Nodes claim what is assigned to them at once. They leave a workload
to its assignee only while the scheduler's holder renews and the assignee
can take it, for at most `--assignment-bound-ms` (15 s), and wait for an
assignment at most `--assignment-grace-ms` (2 s); otherwise it is first
claim wins at once.
Every claim still goes through the lease and the fence, so a bad assignment
delays work and nothing else. `ps` then shows each workload's assignment, and
a drained node's workloads go where the scheduler moves them. See
[Placement](docs/design.md#placement).

`felixd init admission.wasm` brings up admission, itself a felixd workload.
From then on `felixd run` publishes a request on `sys.intake` and prints
admission's decision, accepted or rejected with a reason code, and nodes run
only specs admission accepted. Each decision records the requester, the
principal the broker authenticated the request from, which admission reads from
the record's publisher and never from the payload. That needs a broker that
stores publishers: `FELIX_RECORD_PUBLISHERS=true` on a single broker, or the
`publisher_principal` fleet feature finalized in a cluster
(`POST /v1/fleet/features/publisher_principal/finalize`). Finalizing is
one-way: records with a publisher are storage format v6, which older brokers
refuse to open, so take a backup and read [Felix's upgrades page](https://docs.getfelix.dev/deployment/upgrades/) first. Without it admission
fails closed and rejects every request as `no_requester`. The test stack and
`deploy/compose.yaml` turn it on. Its decisions replay, so `felixd replay --id
admission --diff --all --module <hash>` shows which past requests a new policy
would decide otherwise. See [Submitting work](docs/design.md#submitting-work).

`felixd agent --metrics-bind 127.0.0.1:9464` serves Prometheus metrics at
`/metrics` (plain HTTP, no authentication, off unless given). Each node
reports its own leases, claims, handoffs, steps and step latency, input lag,
output writes and fenced refusals, restarts, needs-operator stops by reason,
snapshot size and time, merge log appends and whether it is draining. Series
per workload are capped at `--metrics-max-workloads` (100). See
[docs/metrics.md](docs/metrics.md) for every metric.

What no node can get past without an operator, such as a module that is
still missing after 30 seconds, a 0.1 module or a restore out of fuel, is
not a failure: the spec stays `running`, and the node writes one
needs-operator record with the reason to the output stream, fenced the same
way. Every node then leaves the workload alone until its spec changes or it
is removed. Cache values and stream records carry a format version, so an
older felixd refuses a newer record instead of misreading it. If a newer felixd recorded a
reason an older one does not know, `felixd run` from the older build still submits the new
spec. Nodes on the older build leave the workload alone, so `felixd ps` shows it as
`waiting`; nodes on the newer build run it. For a workload
that reads another workload's output stream, refusing means it needs an
operator: an older felixd stops there once a record kind it does not know
lands on the stream it reads. Upgrade every node first.

## Try it on three nodes

`deploy/compose.yaml` starts three Felix brokers, a control plane and three
felixd nodes in containers, with development credentials. It works with
`docker compose` and with `podman compose` (use the docker-compose provider,
`PODMAN_COMPOSE_PROVIDER=docker-compose`). You do not need Rust.

```
docker compose -f deploy/compose.yaml up -d --build
docker compose -f deploy/compose.yaml run --rm felixd run --id counter \
  --input in --replication-factor 3 /usr/local/share/felixd/counter.wasm
docker compose -f deploy/compose.yaml --profile producer up -d producer
```

The first build compiles felixd and the example workloads, which takes a few
minutes. `counter` counts the records the producer publishes to `in`. Find
the node that holds it, kill that node and watch another claim it:

```
docker compose -f deploy/compose.yaml run --rm felixd ps
docker kill felixd-node-1
docker compose -f deploy/compose.yaml run --rm felixd ps
```

`ps` names the holder `node-N`, whose container is `felixd-node-N`; kill that
one. To move the workload off a node
without killing it, drain it instead; the next `ps` shows another holder
straight away:

```
docker compose -f deploy/compose.yaml run --rm felixd drain node-2 --wait
docker compose -f deploy/compose.yaml run --rm felixd ps
docker compose -f deploy/compose.yaml run --rm felixd undrain node-2
```

Stop the producer, then check the output stream. Run `felixd check` as often
as you like; it writes nothing:

```
docker compose -f deploy/compose.yaml stop producer
docker compose -f deploy/compose.yaml run --rm felixd check --complete
docker compose -f deploy/compose.yaml down -v
```

`window.wasm` and `scheduler.wasm` are in the same directory; run the
scheduler with `--id scheduler --input sys.specs --input sys.facts`. `FELIX_VERSION` picks the Felix image
tag. The compose file publishes no ports, so the CLI runs in the stack.
`scripts/compose-failover.sh` runs this sequence and CI runs it on every
pull request, under Docker and under rootless Podman.

With Podman, install the docker-compose provider and start the Podman API
socket, then run the same commands with `podman` in place of `docker` (use
`podman kill` for the failover step):

```
systemctl --user enable --now podman.socket
export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock
export PODMAN_COMPOSE_PROVIDER=docker-compose
ENGINE=podman scripts/compose-failover.sh
```

## What we are trying to achieve

The long-term goal is a distributed operating system built on one idea:
every change to the system is a durable, ordered record in a log. That covers
what should run, where it runs, what each program received and what it
produced. If the log is the system's memory, a program's state at any moment
can be rebuilt from it. That one property gives you:

- **Failover without lost or repeated work.** When a machine dies, its programs
  continue on another machine from the last record they handled.
- **Migration.** Moving a program is the same operation as failover, done on
  purpose.
- **Time travel.** You can rebuild any program as it was at any past moment,
  inspect it, and step forward record by record.
- **An audit trail.** Every decision the system made is on the record, in
  order.

Today's cluster managers keep a snapshot of current state, so they can
restart programs but can't do any of the above. Felix already provides the
log, replication, conditional writes and per-client resume that the idea
needs. felixd is the first, deliberately small step: a node agent that proves
the idea on real machines.

## Prior work

None of the pieces are new. felixd puts known ideas together in one place.

| Project | What it does | How felixd differs |
|---|---|---|
| [Plan 9](https://9p.io/plan9/), [Sprite](https://www2.eecs.berkeley.edu/Research/Projects/CS/sprite/sprite.html), [Amoeba](https://en.wikipedia.org/wiki/Amoeba_(operating_system)) | Research distributed operating systems: one namespace across machines, and process migration in Sprite | They share live state between machines. felixd rebuilds state from a log, so migration and recovery are the same operation. |
| [Borg](https://research.google/pubs/large-scale-cluster-management-at-google-with-borg/), [Kubernetes](https://kubernetes.io/), [Nomad](https://www.nomadproject.io/) | Cluster managers that keep desired and observed state in a consistent store (etcd, Raft) | They restart a program after a failure and its in-memory state is lost. felixd resumes it from its last input, and keeps history to replay. |
| [Temporal](https://temporal.io/), [Azure Durable Functions](https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-overview) | Durable execution: deterministic workflow code replayed from an event history | This is the closest idea. Determinism there is a coding rule enforced by the SDK, and the history lives in a database behind a service. In felixd the Wasm sandbox enforces determinism, and workloads read any Felix stream. |
| [Golem](https://www.golem.cloud/) | Durable Wasm workers that replay an operation log after a failure | Also close: Wasm plus replay. Golem is a full platform with its own storage. felixd uses only Felix, and a workload's inputs and outputs are ordinary streams that other programs can read. |
| [wasmCloud](https://wasmcloud.com/) | Wasm components spread across hosts over a NATS lattice | Hosts talk over NATS, and a component's state is not rebuilt from a log after a failure. felixd records every input, so it can be. |
| [Flink](https://flink.apache.org/), [Kafka Streams](https://kafka.apache.org/documentation/streams/) | Stream processing with checkpoints and exactly-once output | These are dataflow frameworks with a coordinator and checkpoint storage beside Kafka. felixd runs general programs, and the log is the only coordinator. |
| [Corfu and Tango](https://www.microsoft.com/en-us/research/publication/tango-distributed-data-structures-over-a-shared-log/), [Delos](https://www.usenix.org/conference/osdi20/presentation/balakrishnan) | Distributed systems built on a shared log as the source of truth | The same foundation, applied to data structures and metadata stores. felixd applies it to running programs. |

## Why build it

- **One substrate.** Each project above needs its own coordinator or database
  next to the log. We want to find out whether a single replicated log, with
  conditional writes and leases, is enough to run programs on: no etcd, no
  workflow database, no job manager.
- **Determinism enforced by the platform.** A Wasm component with no clock,
  randomness or I/O can't break replay by accident. That moves determinism
  from a coding rule you must remember to something the runtime guarantees.
- **History you can query.** Every input, output and ownership change is a
  stream that ordinary Felix clients can read. Debugging a past incident
  means replaying it, not reconstructing it from logs.
- **A demanding user for Felix.** felixd depends on Felix's ordering, fencing
  and resume guarantees under node failure. Anything that falls short shows
  up as a `felix-gap` issue against Felix.

## The first step

felixd runs on every node. It runs WebAssembly programs ("workloads") whose
only inputs and outputs are Felix streams. The first version has to show
three things:

1. **Kill a node mid-run.** Each of its workloads resumes on another node, and
   its output has no gaps and no duplicates.
2. **Freeze a node and let it come back late.** The stale copy of a workload
   cannot write anything once another node has taken over. The new owner's
   first write is a claim record on the output stream, so the stale copy's
   next append is refused even if the new owner never writes an output.
3. **Replay a workload to any past offset or time** and inspect its state.

Scheduling policy, resource limits, networking and native processes come
after that works. [docs/design.md](docs/design.md) explains the path from
here to an operating system.

## Scenarios and use cases

felixd fits work that keeps state, reacts to a stream of events, and must not
lose or repeat results when a machine fails. These are the cases we are
designing for. Failover, exactly-once output, replay and pipelines work today; operations
are still planned.

- **Stateful stream processing without a separate cluster.** Running totals,
  sessions, fraud scores over a window, deduplication. Today that usually
  means Flink or Kafka Streams with checkpoint storage beside the broker. With
  felixd it is a small Wasm workload on Felix, and a node loss doesn't
  double-count or drop anything.
- **State machines where history matters.** An order book, inventory,
  billing or a ledger, where you must be able to answer "what did the system
  believe at 14:02, and why?" `felixd replay --to 14:02` rebuilds that state
  from the same inputs.
- **Fleets of unreliable machines.** Edge gateways, factory floors, home labs:
  machines that reboot, lose power or drop off the network. A workload moves
  to a machine that's up and continues from its last input, with no operator
  involved.
- **Authoritative game and simulation servers.** A match or a simulation tick
  loop is a deterministic state machine fed by player inputs. If its server
  dies, the match continues on another one, and any match can be replayed
  afterwards. [felix-arena](https://github.com/GetFelix/felix-arena) and
  [felix-metro](https://github.com/GetFelix/felix-metro) already use logs this
  way inside one process.
- **Reproducing production bugs.** Instead of guessing from logs, replay the
  workload on your laptop to the offset just before the bad output and step
  through it.
- **Checking an upgrade before it goes live.** Run a new version of a module
  against recorded inputs and compare its outputs with what the current
  version produced, before switching it on. `felixd replay --id orders --diff
  --from 120000 --module orders-next.wasm` runs the new version from the
  snapshot before input 120000, if it declares it can read that snapshot, and
  reports the first input whose output differs, or that every output matches.
  [Try it](#try-it) has a runnable example.

A worked example: a payments team runs a fraud-scoring workload per merchant.
A node dies during a traffic spike. Within one lease TTL, its workloads are
running on the other nodes, every transaction is scored exactly once, and
the late-arriving copy on the recovered node is refused. A week later, a
merchant disputes a decision, and the team replays that merchant's workload
to the moment of the transaction to see exactly what the score was based on.

felixd is not a fit, at least for now, for low-latency request and reply
services, programs that need the network or files directly (that comes with
effects, step 4 in the design), or heavy compute where replaying from a
snapshot would take too long.

## What you will be able to do

- Start felixd on a few machines pointed at one Felix cluster
- Submit a Wasm workload and the stream it reads from
- Kill or freeze any node and watch its workloads move
- Chain workloads, so one's output is the next one's input
- Replay a workload to an offset or a time and print its state

## Felix features it uses

| Need | Felix feature |
|---|---|
| What should run | A cache, watched with retained values |
| Which node runs a workload | A cache entry with a TTL, taken and renewed with `cache_put_if` |
| Which nodes are alive | A cache with a TTL |
| A workload's input | A durable stream, resumed with `subscribe_from` |
| Output that a stale owner cannot write | `publish_if` against the output stream's tail |
| The order several inputs are stepped in | A durable stream per workload, written with `publish_if` like the output |
| Surviving a broker's loss | `Quorum` streams and caches |
| Snapshots | A durable stream per workload |
| Replay to a time | `offset_for_time` and range reads |
| What each node may write | RBAC grants on each node's token |
| Who asked for a workload | The record's stored publisher (`FELIX_RECORD_PUBLISHERS`, or the `publisher_principal` fleet feature) |

## How it works

![Node A freezes while holding workload W. Node B claims the expired lease, restores the latest snapshot, resumes the input at offset 7, skips the output already written for 7 and writes from 8. When A wakes up, its write is refused. felixd replay rebuilds W at any offset from a snapshot and the input.](docs/how-it-works.svg)

A workload is a Wasm component with no access to clocks, randomness, files or
the network. It sees only the records on its input stream, so given the same
records it always reaches the same state. That determinism is what makes
failover and replay safe.

Each felixd watches the list of workloads and claims unowned ones by writing a
lease with a conditional put. While it holds the lease it feeds the workload
input records, appends its output with a write that only succeeds at the
output stream's current tail, and writes a snapshot every so often. If the
node stops renewing, the lease expires and another felixd claims the workload,
loads the latest snapshot and resumes from the input offset recorded in it.
Each output record carries the input offset that produced it, so the new
owner knows which outputs already exist and does not write them again.
A workload can read several streams. The holder records the order it steps
their records in on a merge log, fenced the same way, and a new owner, replay
and `felixd check` follow that log, so the order is the same everywhere.
[docs/design.md](docs/design.md) has the details.

## Status

| M | Milestone | What it proves | State |
|---|---|---|---|
| 0 | One node, one workload | The workload contract works; a restart resumes where it stopped | Done |
| 1 | Leases and failover | Kill a node and its workloads continue elsewhere | Done |
| 2 | Exactly-once output | A frozen node that comes back cannot write | Done |
| 3 | Replay | Rebuild any workload at any offset or time | Done |
| 4 | Pipelines and operations | Chained workloads, several inputs, drain, status, metrics | Pipelines, several inputs, `ps`, `drain`, metrics and the upgrade check done |
| 5 | Self-hosting | Anyone can run a cluster from the docs | Planned |

## Crates

| Crate | What it is |
|---|---|
| [`crates/core`](crates/core/README.md) | Record formats, names, lease and placement rules and the resume fold. No I/O. |
| [`crates/runtime`](crates/runtime/README.md) | The wasmtime host that runs a workload component under fuel and memory limits. |
| [`crates/agent`](crates/agent/README.md) | The node loop: leases, fencing, resume, output and snapshots. |
| [`crates/felixd`](crates/felixd/README.md) | The `felixd` command line. |
| [`crates/testkit`](crates/testkit/README.md) | Test helpers that build the example workloads. |
| [`workloads`](workloads/README.md) | The example workloads, [`counter`](workloads/counter/README.md) and [`window`](workloads/window/README.md), and the test fixture [`faulty`](workloads/faulty/README.md). |

## Documentation

- [docs/design.md](docs/design.md): goals, design, data model and build order
- [docs/records.md](docs/records.md): record formats and the resume fold
- [docs/metrics.md](docs/metrics.md): the Prometheus metrics a node serves
- [Felix docs](https://docs.getfelix.dev/)

## Felix version

The Felix release felixd builds on is in [`.felix-version`](.felix-version).
When Felix publishes a release, a bot opens a "chore: use Felix <version>" PR
that moves every pin listed in [`.github/felix-bump.sh`](.github/felix-bump.sh)
and the lockfile.

Every night a workflow ([`felix-nightly.yml`](.github/workflows/felix-nightly.yml))
points felixd at the commit behind Felix's `nightly` tag and runs
[`.github/felix-nightly.sh`](.github/felix-nightly.sh): it starts the test
stacks from the `:nightly` Felix images and runs the integration tests against
one broker and against three, then the failover check on the deploy stack. When
a run on `main` fails it opens an issue labelled `felix-nightly`, and a green
run closes it, so a Felix change that breaks felixd shows up before the next
release. See the latest run with
`gh run list -R GetFelix/felixd --workflow "Felix nightly" --limit 1`, or start
one from the Actions tab.

## Contributing

The plan is open for discussion. Start with
[what to build first](https://github.com/GetFelix/felixd/discussions/31) and the
[open design questions](https://github.com/GetFelix/felixd/discussions/33), or
browse [all discussions](https://github.com/GetFelix/felixd/discussions).
See [CONTRIBUTING.md](CONTRIBUTING.md) for how changes are made.

## License

MIT. See [LICENSE](LICENSE).
