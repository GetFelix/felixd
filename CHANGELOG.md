# Changelog

## Unreleased

- `felixd run --keep-snapshots K` keeps a workload's last K snapshots. After
  each snapshot of its own, the holder appends a trim record to the output
  stream naming what no resume, replay or check of the workload reads again:
  the snapshot stream below the oldest kept snapshot's records and chunks, the
  merge log before that snapshot's step, and each input before where that
  snapshot carries on. It is fenced by the lease epoch like an output.
  Nothing is deleted yet: Felix has no call to trim a stream at an offset
  ([Felix #1662](https://github.com/GetFelix/felix/issues/1662)). The output
  stream itself is not trimmed. A trim record is a new output record kind (7),
  so upgrade every node before setting `--keep-snapshots` on a workload that
  another workload reads.
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
