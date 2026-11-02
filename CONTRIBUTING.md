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
cargo test -p felixd --test cluster --test fence
```

plus the other `FELIX_*` variables the README lists. With Podman, set `COMPOSE` and `FELIXD_COMPOSE` to
`podman compose`. The test leaves the killed broker running again at the end.

## Running the scale test

The `Scale` workflow measures how many workloads one Felix cluster carries. It starts three Felix brokers and
three felixd agents on the runner, creates N counter workloads, and reports how long that took, how much memory
the brokers use, and how long recovery takes when an agent and then a broker is killed. It never runs on its own.

```
gh workflow run scale.yml -R GetFelix/felixd -f counts=1000,10000
```

Or use "Run workflow" on the Scale page of the Actions tab. `counts` lists the sizes to measure, smallest
first, each on a fresh cluster. `build_secs` bounds creating and starting the workloads, `failover_secs` bounds the first output and
each failover, and the job gives up after `timeout_minutes`. A count can carry a lease TTL in seconds, as
`1000:30`. The default counts are `100,500,1000,10000`. A run took up to about 90 minutes on 0.6.2 and
0.7.0-preview.2; the limits allow 4 hours at most for the default counts. If the smallest count never gets all
workloads running, the run stops there (a broken cluster or harness).

When the run ends, its summary page shows one row per count, and the `scale-results` artifact holds
`scale-<N>.json` with every figure (including the lease census while the workloads started, and memory at each
phase), plus the end of each container's log. The columns:

- **Streams**: shards created for the namespace (two per workload, plus the shared ones).
- **Create**: time for N `felixd run` calls, 16 at a time.
- **All running**: time from the first call until every workload's lease is held by an agent.
- **First output**: time for every workload to answer one input.
- **Agent failover**: time from killing the agent holding the most workloads until every workload has answered
  an input published right after the kill.
- **Broker failover**: the same, killing a broker. The brokers lead equal numbers of shards, and ties go to the
  highest name. **Leaders moved** is when the control plane had no shard left on it.
- **Broker memory** and **Control plane memory**: the largest broker and the control plane in the running
  phase. [docs/design.md](docs/design.md#scale) reports peaks over all phases instead, so its figures are
  higher.

A cell reads `no` when the figure was not measured or did not happen, such as a failover after the count never
settled. A green job does not mean every count settled: a phase that fails or times out is recorded as a note in
`scale-<N>.json` and shown as `no`, so the next count still runs. "All running" can be a few seconds off at 1000
workloads, because it comes from polling a lease census that is slow to read at that size.

`Scale nightly` (`scale-nightly.yml`) runs this workflow every night with `counts=1000`, `build_secs=1500` and
`felix_image_tag=nightly`, so the brokers and control plane are the ones built from Felix main while felixd keeps
the felix-client in `.felix-version`. A run whose 1000 workloads never all start fails, and a failed scheduled run
opens or comments on an issue labelled `felix-nightly-scale`; the next green one closes it. `felix_image_tag` is
also a dispatch input of `Scale`, to measure any Felix image tag by hand.

The measurements and what they showed are in [docs/design.md](docs/design.md#scale). The harness is
`crates/felixd/tests/scale.rs`; it skips unless `FELIXD_SCALE_COUNT` is set, so `cargo test` does not run it.

## Comments and docs

- Only comment where the code is unclear, such as a constraint the code can't show.
- Write docs in plain sentences, and update them, including the README's status table, in the same pull request as the behaviour they describe.

## Pull requests

- CI must pass before merge. Merges are squash only.
- No AI attribution in commits or pull request descriptions.
- List design calls under "Review notes", and Felix gaps you hit under "Felix gaps found", with the issue each one is filed as.
