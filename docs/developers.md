# Developers guide

## Prerequisites

- [Rust toolchain](https://rustup.rs/) (stable)
- Python 3.10+ with [uv](https://docs.astral.sh/uv/)

## Building from source

```bash
git clone git@github.com:biodatageeks/vepyr.git
cd vepyr

# Development build
uv sync

# Rebuild after Rust changes (faster iteration)
uv run maturin develop

# Release build with native CPU optimizations
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
```

## Running tests

```bash
# Full test suite
uv run pytest -v

# Single test
uv run pytest tests/test_annotate.py::test_annotate_parquet -v

# Rust tests
cargo test
```

## Linting and formatting

```bash
# Rust
cargo fmt
cargo clippy --all-targets -- -D warnings

# Python
uv run ruff format .
uv run ruff check .
```

## Building documentation locally

```bash
# Install docs dependencies
uv pip install mkdocs mkdocs-material mkdocstrings mkdocstrings-python

# Serve locally with hot reload
mkdocs serve

# Build static site
mkdocs build
```

## Release wheels

```bash
uv run maturin build --release
```

Wheels are produced for Linux (x86_64), macOS (x86_64, aarch64), and Windows (x64).
