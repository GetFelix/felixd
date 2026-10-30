# felixd as a container, with the example workloads built as components.
#
# Built from the repository root:
#   docker build -f deploy/felixd.Dockerfile -t localhost/felixd:local .
#
# Compiles from source with the pinned toolchain and Cargo.lock; it does not
# use the release archives, which are built per host by scripts/release-build.sh.

# Docker Hub by default. CI can pass a mirror of the same official images.
ARG BASE_REGISTRY=docker.io/library

FROM ${BASE_REGISTRY}/rust:1.97-bookworm AS build
WORKDIR /felixd
RUN rustup target add wasm32-unknown-unknown
# wasm-tools 1.N pairs with wit-component 0.N, which the tests use.
RUN cargo install wasm-tools --locked --version 1.262.0
COPY . .
RUN cargo build --release --locked -p felixd --bin felixd \
    && strip target/release/felixd
RUN cd workloads \
    && cargo build --release --locked --target wasm32-unknown-unknown --package counter --package window --package scheduler \
    && for w in counter window scheduler; do \
         wasm-tools component new target/wasm32-unknown-unknown/release/$w.wasm -o /$w.wasm; \
       done

FROM ${BASE_REGISTRY}/debian:bookworm-slim AS runtime
RUN groupadd --gid 65532 felixd \
    && useradd --uid 65532 --gid 65532 --home-dir /home/felixd --create-home felixd
COPY --from=build /felixd/target/release/felixd /usr/local/bin/felixd
COPY --from=build /counter.wasm /window.wasm /scheduler.wasm /usr/local/share/felixd/
USER 65532:65532
WORKDIR /home/felixd
ENTRYPOINT ["felixd"]
