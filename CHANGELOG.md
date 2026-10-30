# Changelog

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
