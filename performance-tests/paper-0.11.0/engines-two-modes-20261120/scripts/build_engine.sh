#!/usr/bin/env bash
# Build one Rust engine from a clean source checkout with the given toolchain and -C target-cpu=native,
# using the release profile of the project (both use fat LTO and one codegen unit), and write a receipt.
# Usage: build_engine.sh <label> <source-dir> <cargo-package> <cargo-bin> <toolchain>
set -euo pipefail
LABEL=$1 SRC=$2 PKG=$3 BIN=$4 TOOLCHAIN=$5
R=/Users/mwiewior/research/benchmarks/engines-two-modes-20261120
[ -z "$(git -C "$SRC" status --porcelain)" ] || { echo "error: $SRC is not clean" >&2; exit 1; }
START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
(
  cd "$SRC"
  env -u CONDA_PREFIX -u VIRTUAL_ENV RUSTUP_TOOLCHAIN="$TOOLCHAIN" RUSTFLAGS="-C target-cpu=native" \
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$R/target-$LABEL" \
    cargo build --release --locked -p "$PKG" --bin "$BIN"
) > "$R/logs/$LABEL-build.log" 2>&1 || { tail -20 "$R/logs/$LABEL-build.log" >&2; exit 1; }
cp "$R/target-$LABEL/release/$BIN" "$R/bin/$LABEL"
cat > "$R/logs/$LABEL-build.json" << EOF
{
  "label": "$LABEL",
  "commit": "$(git -C "$SRC" rev-parse HEAD)",
  "started_utc": "$START",
  "ended_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "command": "cargo build --release --locked -p $PKG --bin $BIN",
  "rustc": "$(RUSTUP_TOOLCHAIN=$TOOLCHAIN rustc --version)",
  "rustflags": "-C target-cpu=native",
  "binary": "$R/bin/$LABEL",
  "sha256": "$(shasum -a 256 "$R/bin/$LABEL" | cut -d' ' -f1)"
}
EOF
cat "$R/logs/$LABEL-build.json"
