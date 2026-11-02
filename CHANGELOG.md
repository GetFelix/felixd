# Changelog

## Unreleased

- Degraded workloads (#73). A workload that keeps failing on a transient
  error, such as a stream its node cannot read, used to be retried for ever
  on that node, out of sight. Now `felixd ps` shows it as `degraded` with the
  error, and another node takes it. The holder gives up when attempts have
  failed for `--stuck-after-ms` (default 60000, or the lease TTL if longer;
  the agent refuses a value between 0 and the TTL) with no progress in
  between, meaning no new input stepped and no record appended. It appends a
  `degraded` record (output record kind 8: spec record, error class, node id,
  detail) fenced by epoch, deletes its lease so another
  node can claim it, and leaves the workload alone for
  `--degraded-backoff-ms` (default 120000). The spec stays `running`, so
  nothing has to be done. `felixd_degraded_total{class}` counts releases.
  A build from before kind 8 refuses the record: it fails a workload whose
  last record it is, never claims one whose first record it is, and a
  downstream workload reading the stream needs an operator. During a rolling
  upgrade run the new build with `--stuck-after-ms 0`, which turns the bound
  off, until no old node is left. Counting degraded records across nodes and
  blocking claims is #99.

- Modules over 8 MiB (#21). A module over 8 MiB is stored in the `modules`
  cache as 4 MiB chunks under `<sha256>/<n>` with a JSON manifest under the
  module's own key, chunks first and manifest last. A module of 8 MiB or less,
  which is all a build before this could hold, is stored as before. Readers
  (nodes, `felixd check`, `felixd replay`, admission's projector) read four
  chunks at a time, check each chunk and then the whole against the hash; a
  missing chunk is the `module missing` needs-operator reason and a wrong one
  fails the workload as poison. Admission's default `max_module_bytes` and
  `felixd run`'s limit are now 64 MiB, up from 8 MiB. New
  `felixd_agent::module_store` and `felixd_core::module` items;
  `felixd-testkit` gains `padded`.
- Upgrading for module chunks (#21): upgrade every node before submitting a
  module over 8 MiB, since an older node reads the manifest as a module that
  does not match its hash. An admission module already running keeps the
  8 MiB limit it was built with; to raise it, build the new admission module,
  check it with `felixd replay --id admission --diff --all --module <hash>` and
  run `felixd init` with it.
- Refused stream creates (#149). `felixd run`, `felixd init` and `felixd drain`
  read a stream or cache back after Felix answers 409, and now exit with an
  error carrying Felix's reason where they used to succeed and leave the next
  read waiting for a stream that was never created. Felix refuses a stream
  under the name of a deleted one until the cluster has finalized stream
  incarnations; a control plane no broker registers with, like the
  single-broker test stack, never finalizes them. Admission's projector hits the
  same refusal when it creates an accepted workload's streams: it logs Felix's
  reason and retries every second until the name can be reused, instead of
  recording a spec whose streams do not exist.
- Felix 0.7.0-preview.2. felixd builds on `felix-client` 0.7.0-preview.2 and the
  compose files and test stacks default to the 0.7.0-preview.2 images. Nothing
  changes for users. This release is the first with conditional head trims,
  which later changes use to delete history. The client's cluster type is now
  called `Client`.
- Upgrade check from a snapshot (#29). `felixd replay --diff --module` and
  `replay --to ... --module` start the new module from the workload's nearest
  snapshot (before `--from` for a diff, before the end of the range for `--to`)
  when the new module's `felixd.snapshot` custom section says it reads the
  layout the workload's module wrote, and from the initial state when it does
  not, saying which and why on stderr. The section is optional JSON,
  `{"format": "<id>", "reads": ["<id>", ...]}`, additive to contract 0.2
  (docs/contract.md, "Snapshot layouts"); a module without one is readable only
  by itself. A new module that declares the layout and traps or runs out of
  fuel restoring it is reported as "snapshot not readable by the new module",
  not as a divergence. A section that does not parse is an error when it is
  read, and the message points at `--from-start`. New `--from-start` always
  replays from the initial state. `--module` takes a component file as well as
  the hash of one in the `modules` cache. The `--to` form now prints two lines
  on stderr, `started from ...` and `ran N more of the inputs below E`, where
  it printed one. In `felixd_agent::replay`, `replay` and `diff` take
  `from_start`, `Replayed` carries a `Start` in place of `restored` and `Diffed`
  gains `start` (breaking for library users). The `counter` example declares
  its layout and has cargo features that build the variants the tests replay
  against it; `felixd_testkit::workload_with` builds one.
- Broker host names and token rotation (#160, #159). `--brokers` takes
  `host:port` as well as `ip:port`; names are resolved on connect and again
  when no known broker answers. `--token-file` and
  `--controlplane-token-file` are read again each time the token is used, so a
  token rotated in place keeps the agent's leases; a missing or half-written
  file keeps the last token. `felixd_agent::settings::Settings` holds
  `Seed` brokers and `TokenSource` tokens (breaking for library users).
- Grants (#117). `felixd init` writes Felix RBAC policies for three roles
  per namespace, `role:felixd.<namespace>.node`, `.admission` and
  `.submitter`, and writes nothing it finds already there; the operator
  running it needs `rbac.view` and `rbac.policy.manage`. Only admission's
  credential may write `sys.specs` and create workload streams; a submitter
  publishes requests and puts modules; a node holds no control plane rights.
  Breaking: the `sys.*` streams move to the namespace `<namespace>.sys`, so a
  node's grant on every workload stream does not reach them; `felixd run` and
  `felixd init` create it. `felixd init`, which grants nodes publish on
  `<namespace>/*`, refuses while an old `sys.*` stream is in the workload
  namespace, so that check is what protects an upgrade; agents do not check.
  The migration steps are in docs/design.md ("Migrating the system streams"). `init` also
  refuses, writing nothing, when a felixd role holds a rule beyond its
  grants or one principal holds two roles, and needs `rbac.view` over the
  whole tenant. Namespaces ending in `.sys` are refused. New `felixd agent --admission-token-file`
  (`FELIXD_ADMISSION_TOKEN_FILE`), the broker token admission's projector
  writes with. A node reading a spec record past the tail of `sys.specs` no
  longer needs publish there.
- Admission (#114). `felixd init admission.wasm` brings up the admission
  workload, reading the new `sys.intake` stream; it is the one spec still
  written directly. From then on `felixd run` uploads the module, publishes a
  request on `sys.intake` and prints admission's decision with how long it
  took, exiting 1 on a rejection, and needs no control plane token. Admission
  outputs one decision per request on `wl.admission.out`, accepting the spec
  or rejecting it with a reason code (limits, module size, ids, inputs), and
  decides from the request alone, so `felixd replay --id admission --diff
  --all --module <hash>` lists the recorded requests a new policy decides
  otherwise (`--all` is new). The node holding admission's lease records
  each accepted decision on `sys.specs`, naming it, after checking the
  module's hash and length and creating the streams; only nodes with a control
  plane token hold admission. A node then runs a spec record written after
  admission's own only if it names a decision that accepted exactly that
  spec; any other needs an operator with the new reason `spec_unadmitted` and
  never claims. New fault `crash-after-projection-sent=N` for tests.
- Resubmitting a workload a newer build left to an operator (#136). `felixd run`
  from an older build submits a new spec over a needs-operator record whose
  reason it cannot read, which is how the workload is released. Nodes on the
  older build leave the workload alone until a node that can read the record
  takes it. Such a record also raises the highest spec claimed, as one it can
  read does.
- System workloads (`admission`, `scheduler`) are refused through intake;
  `felixd run` writes their specs directly with the control plane token as
  before. A spec record runs only if no record of the workload anywhere on
  `sys.specs` names a later accepting decision, so an old acceptance cannot
  run again, whether it is appended again or `workloads` is pointed back at
  it. An accepted decision whose module does not match gets a skip record
  (`skipped`), and a projector resumes after the newest decision any verified
  record names, so an acceptance one passed over is never recorded later.
- If `felixd init` dies between appending admission's spec record and
  writing `workloads`, run it again: it writes the spec again and nodes pick
  admission up.
- Upgrading for admission: run `init` only once every node runs this build,
  since an older node ignores admission. After `init`, every workload but the
  system ones needs a decision, so resubmit existing workloads through
  intake. Without `init` nothing changes. The
  requester, grants (#117) and quotas are not built; see
  [Submitting work](docs/design.md#submitting-work).
- felixd keeps its Felix and control plane tokens out of what it writes
  (part of #74). They are held in a type whose `Debug` and `Display` print a
  placeholder, and a new integration test runs a workload through a failover
  with every log at trace level, then scans every record, cache entry, metric,
  log line and refused-credential error for the token bytes. Per-lease tokens
  wait on scoped token minting in Felix (GetFelix/felix#1625).
- The requester (#182). Workload contract 0.3 adds `publisher` to `record-in`:
  the principal the broker authenticated an input record's write from, `none`
  when the broker stored none and for another workload's output. felixd runs
  0.3 and 0.2 workloads side by side, chosen by the interfaces a component
  exports; a 0.2 workload is unchanged and never sees the publisher, and
  `wit/workload.wit` moved to `wit/0.2/workload.wit` beside the new
  `wit/0.3/workload.wit`. The agent asks the broker for publishers and passes
  the stored value on every path (live, takeover, resume, merge, `check`,
  `replay`). Admission is built on 0.3 and records the requester in each
  decision (`requester`, left out when none). Breaking: a request whose
  publisher the broker did not record is rejected as `no_requester`, so a
  deployment that brings up the 0.3 admission needs a broker that stores
  publishers, `FELIX_RECORD_PUBLISHERS=true` on a lone broker or the
  `publisher_principal` fleet feature finalized in a cluster (one-way: storage
  format v6, which older brokers refuse), before `felixd init`. A deployment
  that already runs admission keeps its 0.2 module, which records no requester,
  until module upgrades exist: a workload's module cannot change once it has
  written output, and admission's id is fixed. Upgrade every node and the CLI
  before submitting a 0.3 module or running the 0.3 admission: a felixd from
  this release on leaves a module of a newer contract to an operator, and
  earlier builds fail the workload; an older `felixd run` cannot read the new
  reasons. Decision `reason` now tolerates a code a newer build wrote
  (`unknown`). New `Policy::requesters` limits who may ask (`not_permitted`; the
  `admission-requesters` example). `felixd_runtime::Input` and
  `felixd_core::admission::decide` take the publisher. The test stacks turn
  recording on.
- A scheduler that is itself a felixd workload (#60). Submit
  `scheduler.wasm` (in the container image) as `felixd run --id scheduler
  --input sys.specs --input sys.facts`. It reads spec records and node facts
  and writes an assignment, workload to node, to `wl.scheduler.out` each time
  one changes, a progress record for a spec record that changes nothing, and
  its whole table every ten minutes of input time. Its clocks are its inputs'
  append times, so a replay assigns the same way, and it fails over like any
  workload. While it runs, each node appends a fact to the new `sys.facts`
  stream every `--facts-every-ms` (three lease TTLs) and within half a second
  of a change. A node follows assignments only while the scheduler's holder
  renews its lease: it claims what is assigned to it at once, leaves a
  workload to an assignee whose recent `nodes` entry says it can take it for
  at most `--assignment-bound-ms` (15 s), and waits for a scheduler that has
  not read a new spec yet for at most `--assignment-grace-ms` (2 s).
  Anything else is first claim wins at once. Claims still go through the lease and the
  fence. A drained node's workloads move to where the scheduler assigns them,
  and `felixd ps` shows an `ASSIGNED` column. Without the scheduler nothing
  changes. `nodes` entries gain `blocked`, set while a node cannot read its
  drain mark.
- Upgrading: `felixd run` creates `sys.facts`. To use the scheduler, grant
  nodes publish on `sys.facts` and read on `wl.scheduler.out`. An older node
  ignores assignments and claims by first claim wins, which is safe; upgrade
  every node to get the placement. `sys.facts` is not trimmed yet (#164).
- `felixd agent --metrics-bind <addr>` (or `FELIXD_METRICS_BIND`) serves
  Prometheus metrics at `/metrics` on that address, and `/live`. Off unless
  given. Per node: leases held, claims by result, handoffs, workloads that
  need an operator by reason, failures, snapshots and their time. Per
  workload: held, steps and step latency, input lag per input, output
  records and fenced refusals, merge log appends, restarts and snapshot
  size. Workloads past `--metrics-max-workloads` (100) are reported together
  as `workload="_other"`. The endpoint is plain HTTP with no authentication
  (#18, [docs/metrics.md](docs/metrics.md)).
- `felixd ps` shows each workload's state (running, waiting, needs operator
  with the reason, failed, stopped), holder, epoch, output stream tail and how
  far it is along each input, and each node, all read from records (#17).
- Trim records carry the stream's lineage, and `felixd check` starts from a
  snapshot (#150). A trim record on the output stream now also holds the spec
  record of the stream's first claim, copied from trim to trim, and the highest
  spec record claimed so far. Once a cut head has taken the first claim, the
  agent, `felixd run` and `felixd check` read what the stream was written under
  from the oldest record Felix keeps, and the stale-spec check stops at a trim
  record. Trim records without the new fields still read. `felixd check`
  still replays from the first input; once the head of the output stream is
  deleted it restores the oldest snapshot the last trim record keeps and checks
  what follows it, the merge log from the entry at its step. `--from-snapshot`
  asks for that earlier and `--from-start` refuses it. A snapshot's state is
  taken as given and outputs below its tail are not compared.
  Deleting `.out` and the floors downstream readers need remain (#150).
- `felixd drain <node>` makes a node release its leases, so other nodes take
  its workloads over without waiting for them to expire, and claim nothing
  until `felixd undrain <node>` (#17). `felixd run` creates the new `drains`
  cache. `--wait` waits until the node holds nothing and says it claims
  nothing, up to `--timeout-secs`.
- Upgrading: a node must be able to read the `drains` cache, or it claims
  nothing. Grant node credentials read on `drains` before upgrading the
  nodes; `felixd ps` then shows a drain mark as `pending` on any node that
  has not said it read it.
- `felixd run --keep-snapshots K` keeps a workload's last K snapshots. After
  each snapshot of its own, the holder appends a trim record to the output
  stream naming the floors below which this workload's resume reads nothing:
  on the snapshot stream, the merge log and each input. It is fenced by the
  lease epoch like an output. Nothing is deleted yet: Felix has no call to
  trim a stream at an offset
  ([Felix #1662](https://github.com/GetFelix/felix/issues/1662)). `felixd
  check` and `felixd replay` still read from the start. The output stream is
  not trimmed, and a shared input will be trimmed only below every reader's
  floor (#150).
- Upgrading: a trim record is a new output record kind (7). A node from an
  earlier build that takes over a workload with one fails it as poison, and
  one whose workload reads that output stream leaves it to an operator. Upgrade
  every node before setting `--keep-snapshots`.
- Reads that walk back from a stream's tail stop at the oldest offset Felix
  holds instead of failing with `CursorTooOld`. With age or size retention on
  an input, a resume that finds its last stepped record gone has no append
  time for it, so the next `snapshot_every_ms` snapshot can fall on a
  different input than before. Outputs are unaffected.
- A record a running workload needs that Felix no longer holds, after
  retention passed it, now leaves the workload to an operator (reason 9,
  history trimmed) instead of retrying. `felixd replay` and `felixd check`
  say which stream and offset were trimmed.

## 0.1.0-preview.1

The first release. Tested against Felix 0.6.2, the version the compose files
pin.

What it does:

- Runs WebAssembly workloads on a node agent, with Felix as the only backend.
- Each workload has one owner at a time through a lease. A new owner fences the
  output stream with a claim record, so a node frozen past its lease writes
  nothing when it wakes, and each output lands once through lost answers,
  partial batches and crashes.
- Spec history, `felixd replay` to rebuild a workload as of any input offset or
  time, and `--diff` to check a replay or a new module against recorded outputs.
- Pipelines, where one workload reads another's output, survive failover at
  every stage.
- Compose files for Docker and Podman.
- Reproducible release binaries for Linux x86_64, Linux aarch64 and macOS
  aarch64, with a signed `SHA256SUMS`.

Not there yet:

- A workload reads one input only (#58).
- No `ps` or `drain` (#17).
- No snapshot or input trimming, so streams grow without bound (#19).
- No metrics (#18).
- Operator docs are still to come (#24).
