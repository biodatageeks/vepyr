# Codebase Concerns

**Analysis Date:** 2026-03-22

## Tech Debt

**Large Python orchestration module:**
- Issue: Most public Python behavior, download logic, cache building, and annotation option mapping all live in `src/vepyr/__init__.py`
- Why: The package is still small and optimized for shipping a single import surface quickly
- Impact: Adding new features risks turning one file into a merge hotspot and making regressions harder to isolate
- Fix approach: Split transport/download, cache build, and annotation option logic into dedicated Python modules while preserving the current public API

**Stringly typed control flow across FFI:**
- Issue: `src/lib.rs` interprets the exact error string `"skipped"` as a non-error `None` return
- Why: It is a lightweight way to express “no source files for this entity” without a richer Rust enum crossing the Python boundary
- Impact: Refactors can silently break skip behavior if the sentinel string changes
- Fix approach: Replace sentinel strings with a dedicated Rust enum or typed result wrapper before the PyO3 boundary

## Known Bugs

**Unsafe tar extraction path handling risk:**
- Symptoms: A malicious tarball could potentially write files outside the target directory during `build_cache()`
- Trigger: Downloading and extracting untrusted archives in `src/vepyr/__init__.py` with `tar.extractall(path=cache_dir)`
- Workaround: Only use trusted Ensembl sources
- Root cause: No member path validation before extraction

**Incomplete fixture entity alignment risk:**
- Symptoms: Golden tests may diverge from production cache layout assumptions if new required cache entities appear upstream
- Trigger: `tests/data/golden/prepare.py` hardcodes a subset of entity directories and special-cases `translation_core`
- Workaround: Manually update the fixture prep script when upstream cache format changes
- Root cause: Test fixture generation is tightly coupled to the current cache entity set

## Security Considerations

**Remote archive extraction:**
- Risk: Downloaded tarballs are extracted without explicit path sanitization or checksum verification
- Current mitigation: Source URLs are limited to Ensembl endpoints; no user-supplied arbitrary URL surface exists in tracked code
- Recommendations: Validate tar members before extraction and optionally verify published checksums

**Filesystem trust boundary:**
- Risk: Public APIs accept raw filesystem paths for caches, VCFs, and reference FASTA
- Current mitigation: Basic existence checks and downstream DataFusion/provider errors
- Recommendations: Add clearer validation for directory shape and expected file presence before long-running work starts

## Performance Bottlenecks

**Single-file Python orchestration overhead:**
- Problem: `annotate()` creates a schema probe annotator before registering the lazy IO source, so each call pays an upfront setup cost
- Measurement: No timing is tracked in the repo, but the extra annotator creation is visible in `src/vepyr/__init__.py`
- Cause: Polars schema registration requires a schema before streaming begins
- Improvement path: Cache schema metadata or expose a lighter-weight schema introspection path from Rust

**Entity conversion memory/runtime tuning is static:**
- Problem: `row_group_size()` and partition choices in `src/convert.rs` are fixed heuristics
- Measurement: No benchmark files are committed, but settings vary by entity and are clearly hand-tuned
- Cause: Tunings are embedded in code rather than measured dynamically
- Improvement path: Add benchmark-driven configuration or auto-tuning hooks for large datasets

## Fragile Areas

**Annotation SQL string assembly:**
- Why fragile: `src/annotate.rs` builds SQL strings manually and escapes only single quotes in some fields
- Common failures: Option expansion mistakes, quoting regressions, or upstream SQL signature changes
- Safe modification: Add targeted tests around generated SQL behavior and keep Python option serialization changes paired with annotation integration tests
- Test coverage: Integration tests cover happy-path behavior but not many malformed/edge option combinations

**Mixed source and built artifact package directory:**
- Why fragile: `src/vepyr/` contains both source files and a compiled `_core.abi3.so`
- Common failures: Stale binary artifacts masking source/build mismatches or confusing tooling
- Safe modification: Rebuild the extension after native changes and be careful about relying on checked-in compiled outputs
- Test coverage: Import smoke tests help, but they do not guarantee the binary matches the current Rust source

## Scaling Limits

**Local-machine execution model:**
- Current capacity: bound by local CPU, RAM, and disk throughput; not horizontally scalable from within the current package design
- Limit: very large VCF/cache jobs will compete for local resources and long-running conversions
- Symptoms at limit: slow conversion, high memory pressure, extended runtime, and large filesystem footprints
- Scaling path: expose worker/distributed execution options or chunk orchestration at a higher layer

## Dependencies at Risk

**Pinned git dependencies from biodatageeks repos:**
- Risk: the project depends on unreleased git SHAs in `Cargo.toml`
- Impact: builds can break if upstream repos disappear or if a new local need requires unpinned compatibility work
- Migration plan: move to crates.io releases when available, or vendor/fork critical upstream components

## Test Coverage Gaps

**Cache conversion path:**
- What's not tested: direct assertions around `build_cache()` and Rust conversion internals
- Risk: regressions in parquet writing, row group sizing, or entity skipping may slip through
- Priority: High
- Difficulty to test: Requires large source fixtures or carefully minimized synthetic cache inputs

**Failure and retry paths for downloads:**
- What's not tested: redirect handling, HTTP failure modes, and extraction failures in `_download_cache()` / `_download_with_progress()`
- Risk: network-related breakage will be discovered only in live use
- Priority: Medium
- Difficulty to test: Needs HTTP fixture/mocking infrastructure that the repo does not currently use

---

*Concerns audit: 2026-03-22*
*Update as issues are fixed or new ones discovered*
