# Changelog

## Unreleased

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
