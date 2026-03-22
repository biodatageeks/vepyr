# Phase 1: Supported Workflow Contract - Research

**Researched:** 2026-03-22
**Domain:** Supported Ensembl VEP workflow contract, cache metadata, and Python-side validation
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
### Supported workflow contract
- **D-01:** v1 is pinned to one supported workflow: `homo_sapiens`, `GRCh38`, Ensembl release `115`.
- **D-02:** The only supported cache flavor in v1 is the standard Ensembl `vep` cache. `merged` and `refseq` are out of supported scope for v1.
- **D-03:** The supported FASTA contract is one documented reference flavor only. Inputs outside that documented reference contract are unsupported even if they appear mostly compatible.
- **D-04:** Unsupported workflow overrides may exist later, but Phase 1 should not add a public Python API override path yet.

### Validation behavior
- **D-05:** Runtime validation should use a hybrid strategy: fail fast on hard blockers, while aggregating related supported-workflow mismatches when feasible so the user gets a clearer correction path.
- **D-06:** FASTA validation should be strict. Validation must check file presence plus supported-workflow metadata and contig/header compatibility before annotation proceeds.
- **D-07:** Unsupported cache, FASTA, release, species, assembly, or workflow combinations must produce clear user-facing Python errors rather than falling through to ambiguous downstream engine failures.

### Metadata exposure
- **D-08:** The supported workflow contract must be visible in both cache metadata and runtime/API behavior.
- **D-09:** The same supported contract must also be documented in repository docs/tests so later phases reuse the exact same workflow definition without reinterpretation.

### Claude's Discretion
- Exact error class structure and message formatting, as long as errors remain explicit and user-facing.
- Exact storage format for persisted metadata, as long as it is readable before annotation starts and supports compatibility validation.
- Exact location of user-facing documentation updates, as long as the supported contract is discoverable from the repo and test fixtures.

### Deferred Ideas (OUT OF SCOPE)
- Public Python API support for unsupported or experimental workflow overrides.
- Supporting `merged` or `refseq` cache flavors in v1.
- Broadening the supported FASTA contract beyond one documented reference flavor.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SCOPE-01 | User can run vepyr against one explicitly documented supported workflow: `homo_sapiens`, `GRCh38`, and one pinned Ensembl release | Use one immutable internal contract object and publish it in docs, tests, and cache manifest |
| SCOPE-02 | User gets a clear error when cache, assembly, FASTA, or release inputs do not match the supported workflow | Validate at Python boundary before `_create_annotator()` and aggregate non-fatal contract mismatches into one actionable error |
| CACHE-03 | User can tell which release/assembly/species a prepared cache belongs to before annotation starts | Persist a human-readable manifest in the prepared cache root; do not rely on directory names alone |
| API-02 | User gets actionable Python-side validation errors for unsupported scope, missing FASTA, missing cache, or invalid workflow configuration | Keep validation in Python, use explicit exception classes or subclasses, and test error text directly |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- Use the existing Rust + PyO3 + DataFusion architecture; do not replace the engine
- Keep the product Python-first for v1
- Validate expected invalid-user-input paths in Python before entering the engine
- Use `uv` + `maturin` workflow for development and `uv run pytest` for tests
- Match repository conventions: public Python API remains concentrated in `src/vepyr/__init__.py`, pytest-based tests under `tests/`, explicit `ValueError` / `FileNotFoundError` for user misuse
- Do not make direct repo edits outside GSD workflow; this research artifact is the planning input for subsequent phase execution

## Summary

Phase 1 is a contract-definition phase, not an engine-capability phase. The existing code already has the right enforcement boundary: `build_cache()` controls cache preparation, and `annotate()` is the last Python checkpoint before Rust/DataFusion execution. Today the supported workflow is only implicit in defaults, fixture filenames, and golden data. That is not sufficient for `SCOPE-01`, `SCOPE-02`, `CACHE-03`, or `API-02`.

The strongest implementation path is to add one internal supported-workflow definition, use it to reject unsupported `build_cache()` and `annotate()` combinations, and persist the same contract into a readable cache-side manifest. Official Ensembl VEP documentation confirms that release `115` human GRCh38 indexed caches exist, that `merged` and `refseq` are separate cache flavors, and that HGVS/offline workflows depend on a compatible genomic FASTA. The planner should therefore treat cache tuple validation and FASTA contract validation as first-class Python responsibilities.

The main design choice is metadata shape. Recommend a single JSON manifest stored in the prepared parquet cache root, with fields for release, species, assembly, cache flavor, and reference FASTA contract. This is simpler and more inspectable than relying on per-file Parquet schema metadata, and it can be read before annotation starts. Use the golden fixtures and docs to pin the supported reference contract to Ensembl GRCh38 primary-assembly-compatible FASTA, with explicit allowance for the trimmed test fixture as a derived fixture rather than a second supported flavor.

**Primary recommendation:** Add one Python-side supported-workflow module plus a cache manifest sidecar, and make both `build_cache()` and `annotate()` refuse anything outside that contract before calling Rust.

## Standard Stack

### Core
| Library / Tool | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Python | 3.12.8 local, `>=3.10` package support | Public API, validation, manifest I/O | Current repo runtime and natural place for user-facing contract enforcement |
| PyO3 | `0.25` | Python/Rust boundary | Already pinned in repo; no reason to move validation into Rust for this phase |
| DataFusion | `50.3` | Annotation execution engine | Existing execution substrate; Phase 1 should validate before invoking it |
| Arrow | `56` | Batch/schema transport | Existing schema layer; useful context, not the right primary place for workflow contract metadata |
| Python stdlib `json` / `pathlib` / `dataclasses` | stdlib | Manifest persistence and validation helpers | No new dependency needed for Phase 1 |

### Supporting
| Library / Tool | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `uv` | `0.8.17` | Environment and test runner entrypoint | Standard local install and test execution |
| `pytest` | `9.0.2` local, `>=8.0` declared | Contract/error-path tests | Add focused Phase 1 validation tests |
| Ensembl VEP cache | release `115` | Ground-truth workflow tuple and cache naming | Validate exact supported cache tuple and docs |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| JSON sidecar manifest in prepared cache root | Per-file Parquet schema metadata only | Harder to inspect before engine startup; fragmented across files; weaker as user-facing contract |
| Python-boundary validation | Rust/DataFusion-side validation | Worse user error messages and later failure point |
| One locked supported tuple | Early public override/config system | Contradicts locked scope and expands test matrix immediately |

**Installation:**
```bash
uv sync
```

**Version verification:** Repo-pinned versions were verified from `Cargo.toml` / `pyproject.toml`. Local execution environment was verified on 2026-03-22 as Python `3.12.8`, `uv 0.8.17`, and `pytest 9.0.2`. The live Ensembl release-115 indexed cache URL for `homo_sapiens_vep_115_GRCh38.tar.gz` returned `HTTP 200` on 2026-03-22.

## Architecture Patterns

### Recommended Project Structure
```text
src/vepyr/
├── __init__.py              # Public build_cache()/annotate() surface
├── _workflow_contract.py    # Supported tuple, manifest schema, validators
└── _errors.py               # Optional small user-facing validation exceptions

tests/
├── test_annotate.py
├── test_import.py
└── test_supported_workflow_contract.py
```

### Pattern 1: Single Source Of Truth Contract
**What:** Define one immutable internal object for the supported workflow tuple and reference contract.
**When to use:** Everywhere the code or docs need release/species/assembly/cache flavor/FASTA rules.
**Example:**
```python
# Source: repository pattern recommendation based on src/vepyr/__init__.py
from dataclasses import dataclass


@dataclass(frozen=True)
class SupportedWorkflow:
    species: str
    assembly: str
    release: int
    cache_flavor: str
    fasta_contract: str


SUPPORTED_WORKFLOW = SupportedWorkflow(
    species="homo_sapiens",
    assembly="GRCh38",
    release=115,
    cache_flavor="vep",
    fasta_contract="ensembl_grch38_primary_assembly",
)
```

### Pattern 2: Persist Contract Metadata Next To Prepared Cache
**What:** Write a readable manifest like `workflow.json` into the prepared parquet cache root, for example `cache_dir/parquet/115_GRCh38_vep/workflow.json`.
**When to use:** At the end of a successful cache build or local-cache conversion, and before annotation starts.
**Example:**
```python
# Source: repository pattern recommendation based on CACHE-03 and current cache layout
{
  "schema_version": 1,
  "species": "homo_sapiens",
  "assembly": "GRCh38",
  "release": 115,
  "cache_flavor": "vep",
  "reference_contract": {
    "id": "ensembl_grch38_primary_assembly",
    "contig_style": "no_chr",
    "notes": "Trimmed fixtures must declare they are derived from this source"
  }
}
```

### Pattern 3: Hybrid Python-Side Validation
**What:** Fail fast on missing files or unreadable manifest; aggregate supported-workflow mismatches into one actionable error.
**When to use:** In `annotate()` before `_create_annotator()` and in `build_cache()` before download/conversion starts.
**Example:**
```python
# Source: repository pattern recommendation based on D-05 and current annotate() validation
problems = []
if release != SUPPORTED_WORKFLOW.release:
    problems.append("release must be 115")
if species != SUPPORTED_WORKFLOW.species:
    problems.append("species must be homo_sapiens")
if assembly != SUPPORTED_WORKFLOW.assembly:
    problems.append("assembly must be GRCh38")
if method != SUPPORTED_WORKFLOW.cache_flavor:
    problems.append("cache flavor must be 'vep'")

if problems:
    raise ValueError("Unsupported workflow: " + "; ".join(problems))
```

### Anti-Patterns to Avoid
- **Directory-name inference as truth:** Do not decide cache identity from `115_GRCh38_vep` path strings alone.
- **Two sources of truth:** Do not copy the tuple into code, tests, and docs independently.
- **Late validation:** Do not let `_create_annotator()` or DataFusion be the first place unsupported workflow errors appear.
- **Filename-only FASTA checks:** Do not treat `reference.fa` or `*.primary_assembly.fa` naming as sufficient validation.
- **Live-network test paths:** Do not make Phase 1 tests download the release-115 cache tarball.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Cache identity | Regex over path names only | Explicit manifest file | Path strings are easy to spoof and do not encode FASTA contract |
| Supported-scope matrix | Generic override/config framework | One locked internal constant | v1 scope is intentionally a single tuple |
| User-facing contract errors | Pass-through Rust/DataFusion strings | Python-side validation exceptions | Requirement is explicit Python-side guidance |
| FASTA compatibility | Literal filename equality | Declared FASTA contract + header/contig validation | Tests use a trimmed derived FASTA; exact filename is too brittle |

**Key insight:** Phase 1 should reduce ambiguity, not increase flexibility. The cheapest reliable implementation is one locked contract plus one persisted manifest.

## Common Pitfalls

### Pitfall 1: Treating Defaults As A Contract
**What goes wrong:** The code defaults to human GRCh38 `vep`, but callers can still pass other tuples and hit inconsistent downstream behavior.
**Why it happens:** Defaults are convenience, not enforcement.
**How to avoid:** Validate every public `build_cache()` and `annotate()` entry path against the same internal contract object.
**Warning signs:** Unsupported inputs still reach `_download_cache()` or `_create_annotator()`.

### Pitfall 2: Making The Manifest A Second-Class Artifact
**What goes wrong:** Metadata is written somewhere obscure or partially duplicated across parquet files.
**Why it happens:** It feels convenient to reuse existing schema metadata.
**How to avoid:** Use one readable manifest in the prepared cache root and load it before annotation.
**Warning signs:** Users need custom code to discover cache release/species/assembly.

### Pitfall 3: Over-Constraining FASTA By Filename
**What goes wrong:** The implementation rejects the trimmed test fixture or accepts a mislabeled but incompatible FASTA.
**Why it happens:** Filename checks are cheap but weak.
**How to avoid:** Validate file existence, declared FASTA contract, and header/contig compatibility; document trimmed fixture derivation explicitly.
**Warning signs:** Tests pass only with one exact filename, or obviously wrong FASTA files still get through.

### Pitfall 4: Deferring Phase 1 Validation To Phase 3
**What goes wrong:** Parity work proceeds against shifting assumptions about release/cache/FASTA.
**Why it happens:** Annotation correctness feels more urgent than contract work.
**How to avoid:** Finish manifest + validation + docs first so later phases target a stable workflow.
**Warning signs:** Golden and benchmark artifacts keep restating the workflow in different words.

### Pitfall 5: Coupling Tests To Live Ensembl Downloads
**What goes wrong:** Fast contract tests become slow, flaky, and network-bound.
**Why it happens:** `build_cache()` currently downloads real data.
**How to avoid:** Test Phase 1 with temp dirs, monkeypatched conversion/download steps, and existing golden fixtures.
**Warning signs:** CI or local tests require a 25 GB cache tarball for contract-only coverage.

## Code Examples

Verified patterns from official and repository sources:

### Release-115 Human Indexed Cache
```bash
# Source: https://www.ensembl.org/info/docs/tools/vep/script/VEP_script_documentation.pdf
curl -O https://ftp.ensembl.org/pub/release-115/variation/indexed_vep_cache/homo_sapiens_vep_115_GRCh38.tar.gz
tar xzf homo_sapiens_vep_115_GRCh38.tar.gz
```

### Python-Side FASTA Requirement Before Engine Startup
```python
# Source: current repository pattern in src/vepyr/__init__.py
if (everything or hgvs) and not reference_fasta:
    raise ValueError(
        "reference_fasta is required when everything=True or hgvs=True"
    )
```

### Recommended Manifest Load Before Annotation
```python
# Source: repository pattern recommendation for Phase 1
manifest_path = Path(cache_dir) / "workflow.json"
if not manifest_path.is_file():
    raise FileNotFoundError(
        f"Prepared cache metadata not found: {manifest_path}"
    )
manifest = json.loads(manifest_path.read_text())
validate_cache_manifest(manifest, SUPPORTED_WORKFLOW)
validate_reference_fasta(reference_fasta, manifest["reference_contract"])
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Implicit workflow via defaults and fixture assumptions | Explicit single workflow contract with persisted manifest | Phase 1 | Later parity and benchmark work target one stable definition |
| Directory names as cache identity | Readable manifest checked before annotation | Phase 1 | Satisfies `CACHE-03` and improves user error quality |
| Engine/discovery errors for bad inputs | Python-side actionable validation | Phase 1 | Satisfies `API-02` and `SCOPE-02` |

**Deprecated/outdated:**
- Accepting `merged` / `refseq` as public v1-supported cache flavors: outdated for this phase because the locked scope is standard `vep` only.
- Treating the golden fixture as the contract source: outdated because the contract must be explicit in code and cache metadata.

## Open Questions

1. **Should Phase 1 require a FASTA index (`.fai`) as part of the supported contract?**
   - What we know: Official Ensembl docs require compatible genomic FASTA for HGVS/offline workflows and the repo fixture includes `reference.fa.fai`.
   - What's unclear: The current Python layer does not validate index presence, and Phase 1 requirements do not explicitly mention it.
   - Recommendation: Do not block Phase 1 on `.fai` enforcement unless the engine proves it is required; keep the contract focused on file presence plus metadata/header compatibility.

2. **How should trimmed test FASTA be represented without creating a second supported flavor?**
   - What we know: `tests/data/golden/reference.fa` is derived from `Homo_sapiens.GRCh38.dna.primary_assembly.fa` but is not the full upstream file.
   - What's unclear: Whether the manifest should encode a `derived_from` field or tests should inject fixture-specific metadata separately.
   - Recommendation: Represent the fixture as derived from the single supported primary-assembly contract, not as a separate contract.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python | Package API tests | ✓ | 3.12.8 | — |
| `uv` | Standard install/test commands | ✓ | 0.8.17 | `python -m pytest` if env already synced |
| `pytest` | Validation architecture | ✓ | 9.0.2 | — |

**Missing dependencies with no fallback:**
- None

**Missing dependencies with fallback:**
- None

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.0.2 |
| Config file | `pyproject.toml` |
| Quick run command | `uv run pytest tests/test_supported_workflow_contract.py -q -x` |
| Full suite command | `uv run pytest -q` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SCOPE-01 | Supported tuple is explicit and exposed consistently | integration | `uv run pytest tests/test_supported_workflow_contract.py -q -x` | ❌ Wave 0 |
| SCOPE-02 | Unsupported cache / release / assembly / FASTA combinations raise clear Python errors | integration | `uv run pytest tests/test_supported_workflow_contract.py -q -x` | ❌ Wave 0 |
| CACHE-03 | Prepared cache metadata reveals release / assembly / species before annotation | integration | `uv run pytest tests/test_supported_workflow_contract.py -q -x` | ❌ Wave 0 |
| API-02 | Missing FASTA / missing manifest / invalid workflow config surface actionable Python errors | integration | `uv run pytest tests/test_supported_workflow_contract.py -q -x` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `uv run pytest tests/test_supported_workflow_contract.py -q -x`
- **Per wave merge:** `uv run pytest tests/test_annotate.py -q -x`
- **Phase gate:** `uv run pytest -q`

### Wave 0 Gaps
- [ ] `tests/test_supported_workflow_contract.py` — focused contract, manifest, and error-path coverage for all Phase 1 requirements
- [ ] Manifest fixture helper or temp-dir builder inside the new test file — avoids live download/conversion in contract tests

## Sources

### Primary (HIGH confidence)
- Official Ensembl VEP documentation PDF - cache download paths, offline/HGVS FASTA requirements, indexed cache guidance, and cache flavor distinctions  
  https://www.ensembl.org/info/docs/tools/vep/script/VEP_script_documentation.pdf
- Live Ensembl FTP cache URL - verified existence of release-115 indexed human GRCh38 cache on 2026-03-22  
  https://ftp.ensembl.org/pub/release-115/variation/indexed_vep_cache/homo_sapiens_vep_115_GRCh38.tar.gz
- Repository code and fixtures - current enforcement boundary, existing validation behavior, and golden workflow artifacts  
  `src/vepyr/__init__.py`, `src/convert.rs`, `tests/test_annotate.py`, `tests/data/golden/prepare.py`, `tests/data/golden/golden.vcf`

### Secondary (MEDIUM confidence)
- None

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - repo-pinned versions and local environment were directly verified
- Architecture: HIGH - recommendation follows current code boundaries and locked project decisions
- Pitfalls: MEDIUM-HIGH - grounded in current code and fixtures; FASTA-derivation details still need implementation choice

**Research date:** 2026-03-22
**Valid until:** 2026-04-21
