#!/usr/bin/env bash
# Build the felixd release binary for the host and package it, reproducibly.
#   scripts/release-build.sh <name> <out-dir>
# Builds from a clean export of HEAD in a throwaway directory, so the output
# does not depend on the checkout path. <name> labels the archive, for example
# felixd-v0.1.0-x86_64-unknown-linux-gnu. Prints the binary's sha256 last.
set -euo pipefail

name=${1:?archive name}
out=$(mkdir -p "${2:?output dir}" && cd "$2" && pwd)
repo=$(git rev-parse --show-toplevel)

# Commit time, not wall time: the same commit always gives the same value.
export SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(git -C "$repo" log -1 --format=%ct)}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/src"
git -C "$repo" archive HEAD | tar -x -C "$work/src"

# Embedded paths are the only machine-specific input left: the build tree
# (source and target dir; build scripts embed the latter through OUT_DIR),
# the cargo registry and the toolchain's own sources.
cargo_home=${CARGO_HOME:-$HOME/.cargo}
rustup_home=${RUSTUP_HOME:-$HOME/.rustup}
export RUSTFLAGS="--remap-path-prefix=$work=/build \
--remap-path-prefix=$cargo_home=/cargo \
--remap-path-prefix=$rustup_home=/rustup"
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$work/target"

(cd "$work/src" && cargo build --release --locked -p felixd --bin felixd)

stage="$work/stage/$name"
mkdir -p "$stage"
cp "$work/target/release/felixd" "$work/src/README.md" "$work/src/LICENSE" "$stage/"

# Fixed order, owner, mode and times, and no gzip timestamp, so the archive
# is a pure function of its contents.
python3 -I - "$work/stage" "$name" "$out/$name.tar.gz" <<'PY'
import gzip, os, sys, tarfile, io
root, name, dest = sys.argv[1:]
epoch = int(os.environ["SOURCE_DATE_EPOCH"])
buf = io.BytesIO()
with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tar:
    for rel in [name] + [f"{name}/{f}" for f in ("LICENSE", "README.md", "felixd")]:
        path = os.path.join(root, rel)
        info = tar.gettarinfo(path, arcname=rel)
        info.uid = info.gid = 0
        info.uname = info.gname = ""
        info.mtime = epoch
        info.mode = 0o755 if rel.endswith("felixd") or os.path.isdir(path) else 0o644
        info.pax_headers = {}
        if info.isdir():
            tar.addfile(info)
        else:
            with open(path, "rb") as f:
                tar.addfile(info, f)
with open(dest, "wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
    gz.write(buf.getvalue())
PY

shasum -a 256 "$work/target/release/felixd" | cut -d' ' -f1
