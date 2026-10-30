# Changelog

## Unreleased

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
