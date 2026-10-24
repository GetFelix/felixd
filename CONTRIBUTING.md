# Contributing

[docs/design.md](docs/design.md) describes what is being built and why. Questions, ideas and design debate go in [Discussions](https://github.com/GetFelix/felixd/discussions). Issues are for work with a clear "done when".

## Code

- Write the simplest thing that meets the milestone. Don't add abstractions it doesn't need yet.
- Felix is the only backend. A feature that needs a database, a queue or a coordinator process of ours is out of scope.
- Write the rules once. Record formats, key and stream names, lease rules and the resume fold live in `core/`, which does no I/O.
- Correctness never rests on the lease alone. Output is fenced with `publish_if`; a test that freezes a node must still pass.
- Workloads stay deterministic. The host gives a workload nothing outside its input records. Every example workload is checked with `felixd replay --diff` in CI.
- Rust only. Format with `cargo fmt`, lint with `cargo clippy -D warnings`, and pin dependency versions.

## Comments and docs

- Only comment where the code is unclear, such as a constraint the code can't show.
- Write docs in plain sentences, and update them, including the README's status table, in the same pull request as the behaviour they describe.

## Pull requests

- CI must pass before merge. Merges are squash only.
- No AI attribution in commits or pull request descriptions.
- List design calls under "Review notes", and Felix gaps you hit under "Felix gaps found", with the issue each one is filed as.
