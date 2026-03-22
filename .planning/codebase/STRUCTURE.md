# Structure

**Analysis Date:** 2026-03-22

## Directory Layout

```text
.
├── Cargo.toml
├── pyproject.toml
├── uv.lock
├── README.md
├── src/
│   ├── lib.rs
│   ├── annotate.rs
│   ├── convert.rs
│   └── vepyr/
│       ├── __init__.py
│       ├── _core.pyi
│       ├── py.typed
│       └── _core.abi3.so
└── tests/
    ├── test_import.py
    ├── test_annotate.py
    ├── test_golden.py
    └── data/golden/
        ├── prepare.py
        ├── input.vcf.gz
        ├── golden.vcf
        └── reference.fa
```

## Key Locations

**Build and package metadata:**
- `Cargo.toml` - Rust crate definition and native dependencies
- `pyproject.toml` - Python metadata, dev dependencies, and maturin config
- `uv.lock` - pinned Python dependency resolution for local/dev installs

**Native core:**
- `src/lib.rs` - PyO3 module definition and exported function wiring
- `src/annotate.rs` - streaming annotator and DataFusion annotation query setup
- `src/convert.rs` - cache entity conversion logic and parquet writer tuning

**Python surface:**
- `src/vepyr/__init__.py` - public API and orchestration logic
- `src/vepyr/_core.pyi` - typing surface for the extension module
- `src/vepyr/py.typed` - package typing marker

**Tests and fixtures:**
- `tests/test_import.py` - smoke tests for import surface
- `tests/test_annotate.py` - integration coverage for lazy annotation behavior
- `tests/test_golden.py` - comparison against VEP golden output
- `tests/data/golden/prepare.py` - regenerates trimmed fixture data from external sources

## Naming Conventions

**Rust files:**
- flat snake_case module files in `src/`
- conceptual split by workflow rather than by trait/interface layers: `annotate.rs` vs `convert.rs`

**Python package files:**
- package root under `src/vepyr/`
- module naming is simple and package-level; most public functionality is concentrated in `__init__.py`

**Tests:**
- pytest naming pattern `tests/test_*.py`
- large shared data lives under `tests/data/golden/`

## Organization Notes

- The repository uses a mixed-language layout: Rust sources and Python package sources share the same `src/` root
- There is no separate `docs/`, `scripts/`, or `ci/` code hierarchy beyond fixture prep under `tests/data/golden/`
- The committed binary `src/vepyr/_core.abi3.so` means the tree may contain build artifacts alongside source, so file-based automation should avoid assuming `src/vepyr/` is source-only

## Useful Paths for Future Work

- Adding new Python API options: `src/vepyr/__init__.py`
- Changing FFI signatures or exported classes: `src/lib.rs`
- Modifying annotation SQL/runtime behavior: `src/annotate.rs`
- Modifying cache conversion heuristics and parquet output: `src/convert.rs`
- Extending tests with real fixture data: `tests/test_annotate.py` and `tests/test_golden.py`

---

*Structure analysis: 2026-03-22*
*Update when directories or ownership boundaries change*
