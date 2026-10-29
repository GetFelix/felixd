# Contributing

[docs/design.md](docs/design.md) describes what is being built and why. Questions, ideas and design debate go in [Discussions](https://github.com/GetFelix/felixd/discussions). Issues are for work with a clear "done when".

## Code

- Write the simplest thing that meets the milestone. Don't add abstractions it doesn't need yet.
- Felix is the only backend. A feature that needs a database, a queue or a coordinator process of ours is out of scope.
- Write the rules once. Record formats, key and stream names, lease rules and the resume fold live in `core/`, which does no I/O.
- Correctness never rests on the lease alone. Output is fenced with `publish_if`; a test that freezes a node must still pass.
- Workloads stay deterministic. The host gives a workload nothing outside its input records. Every example workload is checked with `felixd replay --diff` in CI.
- Rust only. Format with `cargo fmt`, lint with `cargo clippy -D warnings`, and pin dependency versions.

## Build and test

```
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --lib --bins        # unit
cargo test --workspace --test '*'          # integration
```

CI runs the same three as the jobs `lint`, `unit` and `integration`. The `integration` job includes the
three-node failover test for 30 s; the `Soak` workflow runs it nightly for 10 minutes. The restart test in
`integration` needs a Felix broker; the README says how to start one. Without `FELIX_BROKERS` set
those tests skip, and CI sets `FELIXD_REQUIRE_FELIX` so a missing broker fails instead. The restart tests
crash the agent at chosen points with `FELIXD_FAULTS`, described in `crates/agent/src/faults.rs`. Only the
tests' build of the binary reads it: the `faults` feature is turned on by a dev-dependency, and
`scripts/check-no-faults.sh` (run in `unit`) fails if a normal build still carries it.

The `cluster` job runs `tests/cluster.rs` against three brokers with three-replica `Quorum` streams, and
kills the broker that leads a workload's output stream. To run it locally:

```
docker compose -f tests/felix/cluster.yaml up -d --wait broker-1 broker-2 broker-3
docker compose -f tests/felix/cluster.yaml run --rm felix-fleet
tests/felix/credentials.sh /tmp/felix cluster
export FELIX_BROKERS=127.0.0.1:5100,127.0.0.1:5200,127.0.0.1:5300 \
  FELIX_OPERATOR_TOKEN_FILE=/tmp/felix/operator.jwt FELIXD_CLUSTER=$PWD/tests/felix/cluster.yaml
cargo test -p felixd --test cluster
```

plus the other `FELIX_*` variables the README lists. With Podman, set `COMPOSE` and `FELIXD_COMPOSE` to
`podman compose`. The test leaves the killed broker running again at the end.

## Comments and docs

- Only comment where the code is unclear, such as a constraint the code can't show.
- Write docs in plain sentences, and update them, including the README's status table, in the same pull request as the behaviour they describe.

## Pull requests

- CI must pass before merge. Merges are squash only.
- No AI attribution in commits or pull request descriptions.
- List design calls under "Review notes", and Felix gaps you hit under "Felix gaps found", with the issue each one is filed as.
