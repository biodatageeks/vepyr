# Testing Patterns

**Analysis Date:** 2026-03-22

## Test Framework

**Runner:**
- Pytest 8+ from the `dev` dependency group in `pyproject.toml`
- No separate pytest config file was found; defaults plus in-file fixtures are used

**Assertion Library:**
- Pytest bare `assert` style
- Exception assertions via `pytest.raises(...)`

**Run Commands:**
```bash
uv run pytest                      # Run all tests
uv run pytest tests/test_import.py # Single file
uv run pytest tests/test_golden.py # Golden integration suite
```

## Test File Organization

**Location:**
- All tracked tests live under `tests/`
- Shared integration fixtures live under `tests/data/golden/`

**Naming:**
- Import smoke tests: `tests/test_import.py`
- Runtime/integration tests: `tests/test_annotate.py`
- Golden comparison tests: `tests/test_golden.py`

**Structure:**
```text
tests/
  test_import.py
  test_annotate.py
  test_golden.py
  data/golden/
    prepare.py
    input.vcf.gz
    golden.vcf
    reference.fa
```

## Test Structure

**Suite Organization:**
- Module-level fixture constants define paths such as `CACHE_DIR`, `INPUT_VCF`, and `REFERENCE_FASTA`
- Shared setup uses `@pytest.fixture(scope="module")`
- Related assertions are grouped in classes like `TestAnnotate` and `TestGoldenComparison`

**Patterns:**
- Heavy tests guard themselves with `pytest.skip(...)` if local fixture data is missing
- Tests emphasize real execution with actual files and collected Polars frames rather than extensive mocking
- Assertions focus on observable outputs: columns present, row counts, consequence match rates

## Mocking

**Framework:**
- No mocking framework is used in the tracked test suite

**What to Mock:**
- Nothing currently mocked; tests prefer real file-backed integration paths

**What NOT to Mock:**
- Core annotation and conversion behavior, which is intentionally exercised end-to-end

## Fixtures and Factories

**Test Data:**
- Golden fixtures are prepared by `tests/data/golden/prepare.py`
- Fixture prep depends on full external source datasets and genomics CLI tools, then trims them into repo-local test assets

**Location:**
- Shared fixture data and preparation logic live in `tests/data/golden/`
- Per-module fixtures live at the top of each test file

## Coverage

**Requirements:**
- No explicit coverage target or CI enforcement is declared in tracked files

**Coverage Shape:**
- Good coverage for importability and annotation happy-path behavior
- Some error-path coverage for Python argument validation
- No tracked tests directly exercising Rust conversion helpers at unit granularity

## Test Types

**Smoke Tests:**
- `tests/test_import.py` verifies the extension and public API import cleanly

**Integration Tests:**
- `tests/test_annotate.py` checks `LazyFrame` behavior, projection/filter flows, and validation errors
- `tests/test_golden.py` compares output against a known Ensembl VEP baseline

**Fixture Generation:**
- `tests/data/golden/prepare.py` is effectively a developer utility test-support script, not a normal test

## Common Patterns

**Async Testing:**
- No direct async pytest patterns are used; async work is hidden behind the library API

**Error Testing:**
```python
with pytest.raises(ValueError, match="reference_fasta"):
    vepyr.annotate(INPUT_VCF, CACHE_DIR, everything=True)
```

**Data Assertions:**
- Table-like assertions check frame shape, column presence, and match ratios instead of exact full-frame equality
- Golden tests normalize chromosome naming differences before comparison

---

*Testing analysis: 2026-03-22*
*Update when test patterns change*
