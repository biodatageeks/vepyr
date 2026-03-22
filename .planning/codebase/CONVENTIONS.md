# Coding Conventions

**Analysis Date:** 2026-03-22

## Naming Patterns

**Files:**
- Rust modules use snake_case filenames in `src/` such as `annotate.rs` and `convert.rs`
- Python tests follow pytest’s `test_*.py` naming under `tests/`
- The Python package surface is concentrated in `src/vepyr/__init__.py` rather than split across many modules

**Functions:**
- Rust functions use snake_case (`create_streaming_annotator`, `writer_properties`, `build_query_multi_chrom`)
- Python functions also use snake_case (`build_cache`, `_download_with_progress`, `annotate`)
- Helper/private functions are prefixed with `_` on the Python side

**Variables:**
- Rust constants use `UPPER_SNAKE_CASE` (`MAIN_CHROMS`)
- Rust locals and Python variables use snake_case
- Python internal aliases for native imports use a leading underscore (`_convert_entity`, `_create_annotator`)

**Types:**
- Rust structs and enums use PascalCase (`StreamingAnnotator`, `EnsemblEntityKind`)
- Python typing uses builtin generics and quoted forward refs where needed

## Code Style

**Formatting:**
- Rust style matches `rustfmt` defaults: 4-space indentation, trailing commas, wrapped builder chains
- Python style is PEP 8-like with 4-space indentation and double-quoted docstring examples
- Long function signatures and conditionals are split across multiple lines rather than compressed

**Linting:**
- No explicit lint config files (`ruff.toml`, `mypy.ini`, `clippy.toml`, etc.) were found in the repo root
- Style discipline appears to rely on standard formatter behavior and review rather than enforced lint configs

## Import Organization

**Order:**
1. Standard library imports
2. Third-party imports
3. Local/native package imports

**Grouping:**
- Both Python and Rust files use grouped imports with blank lines between major groups
- Rust commonly groups `std::` imports first, then crate/external imports

**Path Aliases:**
- None detected. Imports are package-relative or direct module paths.

## Error Handling

**Patterns:**
- Python validates user misuse with explicit `ValueError` or `FileNotFoundError`
- Rust converts lower-level errors to `PyRuntimeError` at the FFI boundary
- Rust uses `Result` propagation heavily internally and only formats strings when crossing into Python

**Error Types:**
- Expected invalid-user-input paths are handled in Python before entering the engine
- Execution/runtime failures are surfaced as runtime exceptions with context strings like `"Failed to open VCF: ..."` or `"Annotation stream error: ..."`
- One special non-error sentinel exists in `src/lib.rs`: the string `"skipped"` maps to `None` for Python callers

## Logging

**Framework:**
- Python uses the stdlib `logging` module
- Rust uses direct stderr output for progress and does not yet integrate the `log` crate in the visible source files

**Patterns:**
- Logs are informational and operational rather than structured
- Progress feedback is user-oriented, especially in cache build loops and downloads

## Comments

**When to Comment:**
- Comments explain domain-specific reasoning and edge cases rather than obvious control flow
- Examples: chromosome partitioning, Ensembl URL conventions, and why a `LazyFrame` is made rerunnable

**Docstrings:**
- Python public functions have detailed docstrings with parameters, returns, and examples
- Rust exported/public items have short descriptive doc comments, especially around the PyO3 surface

## Function Design

**Size:**
- Small boundary functions in `src/lib.rs`
- Large orchestration functions are accepted when they model a complete workflow, especially `build_cache()` and `annotate()` in `src/vepyr/__init__.py`

**Parameters:**
- Python favors rich keyword-heavy signatures for user-facing APIs
- Rust helpers often accept explicit primitives and references rather than wrapper structs

**Return Values:**
- Python returns concrete user-facing objects (`list[tuple[str, int]]`, `pl.LazyFrame`)
- Rust returns `PyResult<T>` at the boundary and standard `Result` internally

## Module Design

**Exports:**
- Public Python API is curated through `__all__ = ["build_cache", "annotate"]`
- Rust exports only the functions/classes registered in the `_core` module

**Barrel Files:**
- `src/vepyr/__init__.py` effectively acts as the barrel/module facade for the Python package
- No additional index/barrel pattern is present elsewhere

---

*Convention analysis: 2026-03-22*
*Update when patterns change*
