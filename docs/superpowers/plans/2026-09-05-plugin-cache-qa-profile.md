# Plugin Cache QA Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Polars tool that verifies a plugin cache's structural invariants, profiles its content into `qa_profile.json`, regenerates a `## Quality profile` section in the plugin's Hugging Face card, and optionally publishes shards + manifest + JSON + card as one Hub commit with a moved tag.

**Architecture:** One package `e2e-testing/scripts/cache_qa/` mirroring `e2e-testing/scripts/comparison/` (pure functions per module, a thin `cli.py`, a `Runner` protocol for subprocesses so tests never touch the network). Every invariant is a per-shard lazy Polars scan reduced to counts; the content profile is one lazy scan over all shards collected on the streaming engine. The manifest is the only source of truth; the vepyr-plugins source manifest is never read.

**Tech Stack:** Python 3.10+, Polars 1.39 (`scan_parquet`, `collect_schema`, `collect(engine="streaming")`), stdlib `argparse`/`json`/`dataclasses`/`subprocess`, pytest. The `hf` CLI (huggingface_hub 1.28) for publishing.

**Spec:** `docs/superpowers/specs/2026-09-05-plugin-cache-qa-profile-design.md`

## Global Constraints

- Package path: `e2e-testing/scripts/cache_qa/`; entry point `e2e-testing/scripts/profile_plugin_cache.py`; tests `tests/test_cache_qa_*.py`. `tests/conftest.py` already puts `e2e-testing/scripts` on `sys.path`, so tests import `from cache_qa import manifest`.
- Inputs: `<root>/plugin/<name>/manifest.json` and the shards it names. Shard `chrom` values are bare Ensembl contigs (`22`, `X`, `MT`); manifest `chroms[].chrom` is `chr22` / `chrX` / `chrMT`; shard files are `chr<contig>.parquet`.
- Real shard schema (verified on the v0.1.1 caches): `chrom String, start UInt32, end UInt32, allele_string String, <match…>, <value…>, tier Int8`. Manifest value types seen: `Utf8`, `Int32`, `Float32`.
- Order key (engine #237): `(tier, start, allele_string, <match columns…>)`.
- Dedup policy: manifest `assume_unique` (`true` → duplicates warn, `false` → fail, absent → per-plugin fallback table `{clinvar, alphamissense, dbnsfp: dedup; spliceai, cadd: assume unique}`, unknown plugin → dedup/strict) and the detail says when the fallback was used.
- A contig with `rows == 0` may have no file; any other missing file is a `manifest_files` failure. Profile still runs on present shards.
- Exit codes: `0` pass/warn, `1` an invariant failed (nothing uploaded), `2` usage or I/O error.
- Card markers `<!-- qa-profile:start -->` / `<!-- qa-profile:end -->`; insert before `## Usage` when absent, else append. Rendering is byte-idempotent except `generated_at`.
- `qa_profile.json` `schema_version` is `1`. Tool block records `vepyr`, `polars` versions.
- No network in tests. All Hub calls go through `Runner.run(argv) -> subprocess.CompletedProcess`.
- Commit after every task with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and the session trailer from the harness.
- Lint with `uv run ruff check e2e-testing/scripts/cache_qa tests` and format with `uv run ruff format` before each commit. Run tests with `uv run pytest tests/test_cache_qa_*.py -q`.

---

### Task 1: Synthetic cache fixture and manifest loader

**Files:**
- Create: `e2e-testing/scripts/cache_qa/__init__.py`
- Create: `e2e-testing/scripts/cache_qa/manifest.py`
- Create: `tests/cache_qa_synthetic.py` (helper, not a test)
- Test: `tests/test_cache_qa_manifest.py`

**Interfaces:**
- Produces `cache_qa.manifest`:
  - `class ManifestError(ValueError)`
  - `@dataclass(frozen=True) ContigEntry(chrom: str, file: str, rows: int, warm: int, cold: int)` with `.bare` property (`chr22`→`22`, `chrMT`→`MT`).
  - `@dataclass(frozen=True) ColumnSpec(name: str, role: str, dtype: pl.DataType)`; roles `"key" | "match" | "value" | "tier"`.
  - `@dataclass(frozen=True) CacheManifest(plugin: str, plugin_dir: Path, cache_source_version: str | None, allele_match: str, assume_unique: bool | None, key_columns: list[str], match_columns: list[str], value_columns: list[tuple[str, str]], contigs: list[ContigEntry])` with methods `expected_schema() -> list[ColumnSpec]`, `order_key() -> list[str]` (`["tier","start","allele_string", *match]`), `probe_key() -> list[str]` (`[*key_columns, *match]`), `shard_path(entry) -> Path`, `present_shards() -> list[tuple[ContigEntry, Path]]`.
  - `load_manifest(plugin_dir: Path) -> CacheManifest`
  - `dedup_policy(m: CacheManifest) -> tuple[bool, str]` returning `(assume_unique, detail)`.
  - `MANIFEST_TYPES: dict[str, pl.DataType]`.
- Produces `tests/cache_qa_synthetic.py`: `SyntheticCache(root: Path, plugin="demo", match_columns=("symbol",), allele_match="exact", assume_unique=False)` with `.plugin_dir`, `.rows: dict[str, pl.DataFrame]` (contig → frame), `.write()` (writes shards + manifest from `.rows`, returns `plugin_dir`), `.manifest_dict()` and `.set_manifest(dict)`.

- [ ] **Step 1: Create the synthetic cache helper**

`tests/cache_qa_synthetic.py`:

```python
"""Build tiny three-contig plugin caches for cache_qa tests."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

CONTIGS = ("chr1", "chr2", "chrX")


def _rows(bare: str, n: int, symbol: str) -> pl.DataFrame:
    starts = [100 + 10 * i for i in range(n)]
    tiers = [0 if i < 2 else 1 for i in range(n)]
    return pl.DataFrame(
        {
            "chrom": [bare] * n,
            "start": pl.Series(starts, dtype=pl.UInt32),
            "end": pl.Series(starts, dtype=pl.UInt32),
            "allele_string": ["A/G" if i % 2 == 0 else "C/T" for i in range(n)],
            "symbol": [symbol] * n,
            "score": ["0.5" if i % 3 else "" for i in range(n)],
            "raw": pl.Series([float(i) for i in range(n)], dtype=pl.Float32),
            "tier": pl.Series(tiers, dtype=pl.Int8),
        }
    ).sort(["tier", "start", "allele_string", "symbol"])


class SyntheticCache:
    def __init__(
        self,
        root: Path,
        plugin: str = "demo",
        match_columns: tuple[str, ...] = ("symbol",),
        allele_match: str = "exact",
        assume_unique: bool | None = False,
    ) -> None:
        self.root = root
        self.plugin = plugin
        self.plugin_dir = root / "plugin" / plugin
        self.match_columns = list(match_columns)
        self.allele_match = allele_match
        self.assume_unique = assume_unique
        self.rows: dict[str, pl.DataFrame] = {
            "chr1": _rows("1", 6, "GENE1"),
            "chr2": _rows("2", 4, "GENE2"),
            "chrX": _rows("X", 3, "GENEX"),
        }
        self._manifest_override: dict | None = None

    def manifest_dict(self) -> dict:
        if self._manifest_override is not None:
            return self._manifest_override
        chroms = []
        for chrom, df in self.rows.items():
            chroms.append(
                {
                    "chrom": chrom,
                    "file": f"{chrom}.parquet",
                    "rows": df.height,
                    "warm": int((df["tier"] == 0).sum()),
                    "cold": int((df["tier"] == 1).sum()),
                }
            )
        m = {
            "plugin_name": self.plugin,
            "source_manifest": f"{self.plugin}.source.toml",
            "key_columns": ["chrom", "start", "end", "allele_string"],
            "match_columns": [{"column": c, "template": "{SYMBOL}"} for c in self.match_columns],
            "value_columns": [
                {"column": "score", "csq_field": "DEMO_SCORE", "type": "Utf8"},
                {"column": "raw", "csq_field": "DEMO_RAW", "type": "Float32"},
            ],
            "chroms": chroms,
            "sources": [],
            "cache_source_version": "v0.1.1@3e1c039405344ccb800f89f9a032c688fd048bec",
            "allele_match": self.allele_match,
            "field_order": "alphabetical",
        }
        if self.assume_unique is not None:
            m["assume_unique"] = self.assume_unique
        return m

    def set_manifest(self, manifest: dict) -> None:
        self._manifest_override = manifest

    def write(self) -> Path:
        self.plugin_dir.mkdir(parents=True, exist_ok=True)
        for chrom, df in self.rows.items():
            df.write_parquet(self.plugin_dir / f"{chrom}.parquet")
        (self.plugin_dir / "manifest.json").write_text(
            json.dumps(self.manifest_dict(), indent=2) + "\n"
        )
        return self.plugin_dir
```

- [ ] **Step 2: Write the failing manifest tests**

`tests/test_cache_qa_manifest.py`:

```python
import json

import polars as pl
import pytest

from cache_qa import manifest
from cache_qa_synthetic import SyntheticCache


def test_load_manifest_reads_columns_and_contigs(tmp_path):
    plugin_dir = SyntheticCache(tmp_path).write()
    m = manifest.load_manifest(plugin_dir)
    assert m.plugin == "demo"
    assert m.key_columns == ["chrom", "start", "end", "allele_string"]
    assert m.match_columns == ["symbol"]
    assert m.value_columns == [("score", "Utf8"), ("raw", "Float32")]
    assert [c.chrom for c in m.contigs] == ["chr1", "chr2", "chrX"]
    assert m.contigs[2].bare == "X"
    assert m.assume_unique is False
    assert m.allele_match == "exact"


def test_expected_schema_order_and_types(tmp_path):
    m = manifest.load_manifest(SyntheticCache(tmp_path).write())
    specs = m.expected_schema()
    assert [s.name for s in specs] == [
        "chrom", "start", "end", "allele_string", "symbol", "score", "raw", "tier"
    ]
    assert [s.role for s in specs] == [
        "key", "key", "key", "key", "match", "value", "value", "tier"
    ]
    assert specs[1].dtype == pl.UInt32
    assert specs[5].dtype == pl.String
    assert specs[6].dtype == pl.Float32
    assert specs[7].dtype == pl.Int8


def test_keys(tmp_path):
    m = manifest.load_manifest(SyntheticCache(tmp_path).write())
    assert m.order_key() == ["tier", "start", "allele_string", "symbol"]
    assert m.probe_key() == ["chrom", "start", "end", "allele_string", "symbol"]


def test_present_shards_skips_missing_files(tmp_path):
    cache = SyntheticCache(tmp_path)
    plugin_dir = cache.write()
    (plugin_dir / "chrX.parquet").unlink()
    m = manifest.load_manifest(plugin_dir)
    assert [e.chrom for e, _ in m.present_shards()] == ["chr1", "chr2"]


def test_bare_contig_names():
    assert manifest.ContigEntry("chrMT", "chrMT.parquet", 0, 0, 0).bare == "MT"
    assert manifest.ContigEntry("chr22", "chr22.parquet", 1, 1, 0).bare == "22"


def test_missing_manifest_raises(tmp_path):
    with pytest.raises(manifest.ManifestError) as e:
        manifest.load_manifest(tmp_path / "plugin" / "nope")
    assert "manifest.json" in str(e.value)


def test_missing_key_names_the_key(tmp_path):
    cache = SyntheticCache(tmp_path)
    m = cache.manifest_dict()
    del m["chroms"]
    cache.set_manifest(m)
    plugin_dir = cache.write()
    with pytest.raises(manifest.ManifestError) as e:
        manifest.load_manifest(plugin_dir)
    assert "chroms" in str(e.value)


def test_unknown_value_type_raises(tmp_path):
    cache = SyntheticCache(tmp_path)
    m = cache.manifest_dict()
    m["value_columns"][0]["type"] = "Decimal"
    cache.set_manifest(m)
    with pytest.raises(manifest.ManifestError) as e:
        manifest.load_manifest(cache.write())
    assert "Decimal" in str(e.value)


@pytest.mark.parametrize(
    "plugin,key,expected,fallback",
    [
        ("demo", True, True, False),
        ("demo", False, False, False),
        ("spliceai", None, True, True),
        ("cadd", None, True, True),
        ("clinvar", None, False, True),
        ("unknown", None, False, True),
    ],
)
def test_dedup_policy(tmp_path, plugin, key, expected, fallback):
    cache = SyntheticCache(tmp_path, plugin=plugin, assume_unique=key)
    m = manifest.load_manifest(cache.write())
    assume_unique, detail = manifest.dedup_policy(m)
    assert assume_unique is expected
    assert ("fallback" in detail) is fallback
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_qa_manifest.py -q`
Expected: `ModuleNotFoundError: No module named 'cache_qa'`

- [ ] **Step 4: Implement the package init and manifest module**

`e2e-testing/scripts/cache_qa/__init__.py`:

```python
"""Plugin cache QA: invariants, content profile, card section, publishing."""
```

`e2e-testing/scripts/cache_qa/manifest.py`:

```python
"""Load and validate a plugin cache manifest.json; resolve shards and policy."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl


class ManifestError(ValueError):
    """manifest.json is missing, unreadable, or lacks a required key."""


MANIFEST_TYPES: dict[str, pl.DataType] = {
    "Utf8": pl.String,
    "String": pl.String,
    "Float32": pl.Float32,
    "Float64": pl.Float64,
    "Int8": pl.Int8,
    "Int16": pl.Int16,
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "UInt8": pl.UInt8,
    "UInt16": pl.UInt16,
    "UInt32": pl.UInt32,
    "UInt64": pl.UInt64,
    "Boolean": pl.Boolean,
}

KEY_TYPES: dict[str, pl.DataType] = {
    "chrom": pl.String,
    "start": pl.UInt32,
    "end": pl.UInt32,
    "allele_string": pl.String,
}

# Used only when manifest.json predates engine #234 and has no assume_unique key.
FALLBACK_ASSUME_UNIQUE: dict[str, bool] = {
    "clinvar": False,
    "alphamissense": False,
    "dbnsfp": False,
    "spliceai": True,
    "cadd": True,
}


@dataclass(frozen=True)
class ContigEntry:
    chrom: str
    file: str
    rows: int
    warm: int
    cold: int

    @property
    def bare(self) -> str:
        """Shard `chrom` value for this contig: `chr22` -> `22`, `chrMT` -> `MT`."""
        return self.chrom[3:] if self.chrom.startswith("chr") else self.chrom


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    role: str  # key | match | value | tier
    dtype: pl.DataType


@dataclass(frozen=True)
class CacheManifest:
    plugin: str
    plugin_dir: Path
    cache_source_version: str | None
    allele_match: str
    assume_unique: bool | None
    key_columns: list[str]
    match_columns: list[str]
    value_columns: list[tuple[str, str]]  # (column, manifest type name)
    contigs: list[ContigEntry]

    def expected_schema(self) -> list[ColumnSpec]:
        specs = [ColumnSpec(c, "key", KEY_TYPES.get(c, pl.String)) for c in self.key_columns]
        specs += [ColumnSpec(c, "match", pl.String) for c in self.match_columns]
        specs += [ColumnSpec(c, "value", MANIFEST_TYPES[t]) for c, t in self.value_columns]
        specs.append(ColumnSpec("tier", "tier", pl.Int8))
        return specs

    def order_key(self) -> list[str]:
        return ["tier", "start", "allele_string", *self.match_columns]

    def probe_key(self) -> list[str]:
        return [*self.key_columns, *self.match_columns]

    def shard_path(self, entry: ContigEntry) -> Path:
        return self.plugin_dir / entry.file

    def present_shards(self) -> list[tuple[ContigEntry, Path]]:
        out = []
        for entry in self.contigs:
            path = self.shard_path(entry)
            if path.exists():
                out.append((entry, path))
        return out


def _require(raw: dict, key: str, path: Path):
    if key not in raw:
        raise ManifestError(f"{path}: missing key '{key}'")
    return raw[key]


def load_manifest(plugin_dir: Path) -> CacheManifest:
    path = Path(plugin_dir) / "manifest.json"
    if not path.exists():
        raise ManifestError(f"{path}: no such file")
    try:
        raw = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise ManifestError(f"{path}: invalid JSON: {e}") from e
    value_columns = []
    for v in _require(raw, "value_columns", path):
        t = v.get("type", "Utf8")
        if t not in MANIFEST_TYPES:
            raise ManifestError(f"{path}: value column '{v.get('column')}' has unknown type '{t}'")
        value_columns.append((v["column"], t))
    contigs = [
        ContigEntry(c["chrom"], c["file"], int(c["rows"]), int(c["warm"]), int(c["cold"]))
        for c in _require(raw, "chroms", path)
    ]
    return CacheManifest(
        plugin=_require(raw, "plugin_name", path),
        plugin_dir=Path(plugin_dir),
        cache_source_version=raw.get("cache_source_version"),
        allele_match=raw.get("allele_match", "exact"),
        assume_unique=raw.get("assume_unique"),
        key_columns=list(_require(raw, "key_columns", path)),
        match_columns=[m["column"] for m in raw.get("match_columns", [])],
        value_columns=value_columns,
        contigs=contigs,
    )


def dedup_policy(m: CacheManifest) -> tuple[bool, str]:
    """(assume_unique, detail). Manifest key wins; otherwise the per-plugin fallback."""
    if m.assume_unique is not None:
        return m.assume_unique, f"manifest assume_unique={str(m.assume_unique).lower()}"
    value = FALLBACK_ASSUME_UNIQUE.get(m.plugin, False)
    return value, (
        f"manifest has no assume_unique key; fallback table says "
        f"{'assume_unique' if value else 'deduplicated'} for '{m.plugin}'"
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_qa_manifest.py -q`
Expected: all pass.

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check e2e-testing/scripts/cache_qa tests/test_cache_qa_manifest.py tests/cache_qa_synthetic.py && uv run ruff format e2e-testing/scripts/cache_qa tests/test_cache_qa_manifest.py tests/cache_qa_synthetic.py
git add e2e-testing/scripts/cache_qa tests/test_cache_qa_manifest.py tests/cache_qa_synthetic.py
git commit -m "feat(cache_qa): manifest loader, dedup policy, synthetic cache fixture"
```

---

### Task 2: Per-shard invariants: schema, contig, tier_domain, positions, allele_form

**Files:**
- Create: `e2e-testing/scripts/cache_qa/invariants.py`
- Test: `tests/test_cache_qa_invariants.py`

**Interfaces:**
- Consumes `cache_qa.manifest.CacheManifest`, `ContigEntry`, `ColumnSpec`.
- Produces `cache_qa.invariants`:
  - `@dataclass InvariantResult(id: str, status: str, detail: str, per_contig: dict[str, int] | None = None)`; `status in {"pass","fail","warn"}`.
  - `check_schema(m)`, `check_contig(m)`, `check_tier_domain(m)`, `check_positions(m)`, `check_allele_form(m)` each `-> InvariantResult`.
  - helper `_count_per_shard(m, expr_builder) -> dict[str, int]` where `expr_builder(entry) -> pl.Expr` is a boolean expression; the count of true rows per present shard.

- [ ] **Step 1: Write the failing tests**

`tests/test_cache_qa_invariants.py` (first half; Task 3 appends):

```python
import polars as pl

from cache_qa import invariants, manifest
from cache_qa_synthetic import SyntheticCache


def _load(cache: SyntheticCache) -> manifest.CacheManifest:
    return manifest.load_manifest(cache.write())


def test_schema_passes_on_clean_cache(tmp_path):
    r = invariants.check_schema(_load(SyntheticCache(tmp_path)))
    assert r.status == "pass"
    assert "3 shards" in r.detail


def test_schema_fails_on_wrong_type(tmp_path):
    cache = SyntheticCache(tmp_path)
    cache.rows["chr2"] = cache.rows["chr2"].with_columns(pl.col("start").cast(pl.Int64))
    r = invariants.check_schema(_load(cache))
    assert r.status == "fail"
    assert "chr2" in r.detail and "start" in r.detail


def test_schema_fails_on_wrong_order(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr1"]
    cache.rows["chr1"] = df.select([c for c in df.columns if c != "score"] + ["score"])
    r = invariants.check_schema(_load(cache))
    assert r.status == "fail" and "chr1" in r.detail


def test_contig_counts_foreign_rows(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr1"]
    cache.rows["chr1"] = df.with_columns(
        pl.when(pl.arange(0, df.height) == 0).then(pl.lit("2")).otherwise(pl.col("chrom")).alias("chrom")
    )
    r = invariants.check_contig(_load(cache))
    assert r.status == "fail"
    assert r.per_contig == {"chr1": 1}


def test_contig_passes(tmp_path):
    assert invariants.check_contig(_load(SyntheticCache(tmp_path))).status == "pass"


def test_tier_domain_fails_on_two(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chrX"]
    cache.rows["chrX"] = df.with_columns(
        pl.when(pl.arange(0, df.height) == df.height - 1).then(2).otherwise(pl.col("tier")).cast(pl.Int8).alias("tier")
    )
    r = invariants.check_tier_domain(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chrX": 1}


def test_positions_flags_end_before_start_minus_one(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr2"]
    cache.rows["chr2"] = df.with_columns(
        pl.when(pl.arange(0, df.height) == 1).then(pl.col("start") - 2).otherwise(pl.col("end")).cast(pl.UInt32).alias("end")
    )
    r = invariants.check_positions(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chr2": 1}


def test_positions_allows_insertion(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr2"]
    cache.rows["chr2"] = df.with_columns((pl.col("start") - 1).cast(pl.UInt32).alias("end"))
    assert invariants.check_positions(_load(cache)).status == "pass"


def test_allele_form_exact_accepts_shared_base(tmp_path):
    cache = SyntheticCache(tmp_path, allele_match="exact")
    cache.rows["chr1"] = cache.rows["chr1"].with_columns(pl.lit("AC/AT").alias("allele_string"))
    assert invariants.check_allele_form(_load(cache)).status == "pass"


def test_allele_form_minimised_rejects_shared_base(tmp_path):
    cache = SyntheticCache(tmp_path, allele_match="minimised")
    df = cache.rows["chr1"]
    cache.rows["chr1"] = df.with_columns(
        pl.when(pl.arange(0, df.height) == 0).then(pl.lit("AC/AT")).otherwise(pl.col("allele_string")).alias("allele_string")
    )
    r = invariants.check_allele_form(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chr1": 1}


def test_allele_form_minimised_allows_dash(tmp_path):
    cache = SyntheticCache(tmp_path, allele_match="minimised")
    cache.rows["chr1"] = cache.rows["chr1"].with_columns(pl.lit("-/A").alias("allele_string"))
    assert invariants.check_allele_form(_load(cache)).status == "pass"


def test_allele_form_rejects_missing_slash_or_empty_part(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chrX"]
    cache.rows["chrX"] = df.with_columns(
        pl.when(pl.arange(0, df.height) == 0).then(pl.lit("A"))
        .when(pl.arange(0, df.height) == 1).then(pl.lit("A/"))
        .otherwise(pl.col("allele_string")).alias("allele_string")
    )
    r = invariants.check_allele_form(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chrX": 2}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_qa_invariants.py -q`
Expected: `ImportError: cannot import name 'invariants'`

- [ ] **Step 3: Implement the module**

`e2e-testing/scripts/cache_qa/invariants.py`:

```python
"""Structural invariants over a plugin cache. Each check is a lazy per-shard scan reduced to counts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import polars as pl

from cache_qa.manifest import CacheManifest, ContigEntry


@dataclass
class InvariantResult:
    id: str
    status: str  # pass | fail | warn
    detail: str
    per_contig: dict[str, int] | None = None


def _count_per_shard(
    m: CacheManifest, expr_builder: Callable[[ContigEntry], pl.Expr]
) -> dict[str, int]:
    """Count rows where `expr_builder(entry)` is true, per present shard (projection pushed down)."""
    counts: dict[str, int] = {}
    for entry, path in m.present_shards():
        n = (
            pl.scan_parquet(path)
            .select(expr_builder(entry).fill_null(False).cast(pl.UInt64).sum().alias("n"))
            .collect()
            .item()
        )
        counts[entry.chrom] = int(n or 0)
    return counts


def _result(id_: str, counts: dict[str, int], noun: str, status_on_hit: str = "fail") -> InvariantResult:
    bad = {k: v for k, v in counts.items() if v}
    if not bad:
        return InvariantResult(id_, "pass", f"0 {noun} in {len(counts)} shards")
    total = sum(bad.values())
    return InvariantResult(id_, status_on_hit, f"{total} {noun} in {len(bad)} shards", bad)


def check_schema(m: CacheManifest) -> InvariantResult:
    expected = [(s.name, s.dtype) for s in m.expected_schema()]
    problems = []
    n = 0
    for entry, path in m.present_shards():
        n += 1
        actual = list(pl.scan_parquet(path).collect_schema().items())
        for i, exp in enumerate(expected):
            act = actual[i] if i < len(actual) else None
            if act is None or act[0] != exp[0] or act[1] != exp[1]:
                problems.append(f"{entry.chrom}: expected {exp[0]} {exp[1]}, found {act}")
                break
        else:
            if len(actual) > len(expected):
                problems.append(f"{entry.chrom}: extra column {actual[len(expected)][0]}")
    if problems:
        return InvariantResult("schema", "fail", "; ".join(problems))
    return InvariantResult("schema", "pass", f"{n} shards match the manifest")


def check_contig(m: CacheManifest) -> InvariantResult:
    counts = _count_per_shard(m, lambda e: pl.col("chrom") != e.bare)
    return _result("contig", counts, "foreign-contig rows")


def check_tier_domain(m: CacheManifest) -> InvariantResult:
    counts = _count_per_shard(m, lambda e: pl.col("tier").is_null() | ~pl.col("tier").is_in([0, 1]))
    return _result("tier_domain", counts, "rows with tier outside {0,1}")


def check_positions(m: CacheManifest) -> InvariantResult:
    start = pl.col("start").cast(pl.Int64)
    end = pl.col("end").cast(pl.Int64)
    counts = _count_per_shard(m, lambda e: (start < 1) | (end < start - 1))
    return _result("positions", counts, "rows with start < 1 or end < start - 1")


def _allele_violation(allele_match: str) -> pl.Expr:
    parts = pl.col("allele_string").str.split_exact("/", 1)
    ref = parts.struct.field("field_0")
    alt = parts.struct.field("field_1")
    bad = ref.is_null() | alt.is_null() | (ref == "") | (alt == "")
    if allele_match == "minimised":
        shared = (ref.str.slice(0, 1) == alt.str.slice(0, 1)) & (ref != "-") & (alt != "-")
        bad = bad | shared.fill_null(False)
    return bad


def check_allele_form(m: CacheManifest) -> InvariantResult:
    counts = _count_per_shard(m, lambda e: _allele_violation(m.allele_match))
    return _result("allele_form", counts, "malformed allele strings")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_qa_invariants.py -q`
Expected: all pass. If `split_exact` on a value without `/` yields nulls rather than raising, the `A` case counts as a violation through `alt.is_null()`; if a Polars version raises instead, wrap with `.str.split_exact("/", 1)` on `pl.when(pl.col("allele_string").str.contains("/"))` and count the non-matching rows separately.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check e2e-testing/scripts/cache_qa tests/test_cache_qa_invariants.py && uv run ruff format e2e-testing/scripts/cache_qa tests/test_cache_qa_invariants.py
git add e2e-testing/scripts/cache_qa/invariants.py tests/test_cache_qa_invariants.py
git commit -m "feat(cache_qa): schema, contig, tier, position and allele-form invariants"
```

---

### Task 3: Cross-row invariants: order, duplicates, manifest_counts, manifest_files; run_all

**Files:**
- Modify: `e2e-testing/scripts/cache_qa/invariants.py`
- Test: `tests/test_cache_qa_invariants.py` (append)

**Interfaces:**
- Produces in `cache_qa.invariants`: `check_order(m)`, `check_duplicates(m)`, `check_manifest_counts(m)`, `check_manifest_files(m)`, `run_all(m) -> list[InvariantResult]` in the spec's table order (`schema, contig, order, tier_domain, manifest_counts, manifest_files, positions, allele_form, duplicates`), and `descending_steps(cols: list[str]) -> pl.Expr`.

- [ ] **Step 1: Append the failing tests**

```python
def test_order_passes_on_sorted_cache(tmp_path):
    r = invariants.check_order(_load(SyntheticCache(tmp_path)))
    assert r.status == "pass" and "0 descending steps" in r.detail


def test_order_counts_one_swapped_pair(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr1"]
    idx = list(range(df.height))
    idx[3], idx[4] = idx[4], idx[3]
    cache.rows["chr1"] = df[idx]
    r = invariants.check_order(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chr1": 1}


def test_order_uses_match_column_as_final_key(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr2"]
    # same tier/start/allele, symbol descending -> one descending step
    two = df.head(1).with_columns(pl.lit("B").alias("symbol")).vstack(
        df.head(1).with_columns(pl.lit("A").alias("symbol"))
    )
    cache.rows["chr2"] = two
    r = invariants.check_order(_load(cache))
    assert r.per_contig == {"chr2": 1}


def test_duplicates_fail_when_deduplicated(tmp_path):
    cache = SyntheticCache(tmp_path, assume_unique=False)
    df = cache.rows["chr1"]
    cache.rows["chr1"] = df.vstack(df.head(1)).sort(["tier", "start", "allele_string", "symbol"])
    r = invariants.check_duplicates(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chr1": 1}


def test_duplicates_warn_when_assume_unique(tmp_path):
    cache = SyntheticCache(tmp_path, assume_unique=True)
    df = cache.rows["chr1"]
    cache.rows["chr1"] = df.vstack(df.head(2)).sort(["tier", "start", "allele_string", "symbol"])
    r = invariants.check_duplicates(_load(cache))
    assert r.status == "warn" and r.per_contig == {"chr1": 2}
    assert "assume_unique" in r.detail


def test_duplicates_detail_mentions_fallback(tmp_path):
    cache = SyntheticCache(tmp_path, plugin="spliceai", assume_unique=None)
    r = invariants.check_duplicates(_load(cache))
    assert r.status == "pass" and "fallback" in r.detail


def test_manifest_counts_off_by_one(tmp_path):
    cache = SyntheticCache(tmp_path)
    m = cache.manifest_dict()
    m["chroms"][1]["warm"] += 1
    cache.set_manifest(m)
    r = invariants.check_manifest_counts(_load(cache))
    assert r.status == "fail" and "chr2" in r.detail and "warm" in r.detail


def test_manifest_counts_pass(tmp_path):
    assert invariants.check_manifest_counts(_load(SyntheticCache(tmp_path))).status == "pass"


def test_manifest_files_missing_with_rows_fails(tmp_path):
    cache = SyntheticCache(tmp_path)
    plugin_dir = cache.write()
    (plugin_dir / "chr2.parquet").unlink()
    r = invariants.check_manifest_files(manifest.load_manifest(plugin_dir))
    assert r.status == "fail" and "chr2.parquet" in r.detail


def test_manifest_files_missing_with_zero_rows_allowed(tmp_path):
    cache = SyntheticCache(tmp_path)
    m = cache.manifest_dict()
    m["chroms"].append({"chrom": "chrMT", "file": "chrMT.parquet", "rows": 0, "warm": 0, "cold": 0})
    cache.set_manifest(m)
    r = invariants.check_manifest_files(_load(cache))
    assert r.status == "pass"


def test_manifest_files_unlisted_shard_fails(tmp_path):
    cache = SyntheticCache(tmp_path)
    plugin_dir = cache.write()
    cache.rows["chr1"].write_parquet(plugin_dir / "chr9.parquet")
    r = invariants.check_manifest_files(manifest.load_manifest(plugin_dir))
    assert r.status == "fail" and "chr9.parquet" in r.detail


def test_run_all_order_and_ids(tmp_path):
    results = invariants.run_all(_load(SyntheticCache(tmp_path)))
    assert [r.id for r in results] == [
        "schema", "contig", "order", "tier_domain", "manifest_counts",
        "manifest_files", "positions", "allele_form", "duplicates",
    ]
    assert all(r.status == "pass" for r in results)
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `uv run pytest tests/test_cache_qa_invariants.py -q`
Expected: the new tests fail with `AttributeError: module 'cache_qa.invariants' has no attribute 'check_order'` (or similar); Task 2 tests still pass.

- [ ] **Step 3: Append the implementations**

Add to `e2e-testing/scripts/cache_qa/invariants.py` (below `check_allele_form`):

```python
def descending_steps(cols: list[str]) -> pl.Expr:
    """True where row i is lexicographically smaller than row i-1 on `cols` (nulls never count)."""
    step = pl.lit(False)
    equal_prefix = pl.lit(True)
    for c in cols:
        cur, prev = pl.col(c), pl.col(c).shift(1)
        step = step | (equal_prefix & (cur < prev))
        equal_prefix = equal_prefix & (cur == prev)
    return step.fill_null(False)


def check_order(m: CacheManifest) -> InvariantResult:
    key = m.order_key()
    counts = _count_per_shard(m, lambda e: descending_steps(key))
    return _result("order", counts, "descending steps")


def check_duplicates(m: CacheManifest) -> InvariantResult:
    from cache_qa.manifest import dedup_policy

    assume_unique, policy = dedup_policy(m)
    key = m.probe_key()
    counts: dict[str, int] = {}
    for entry, path in m.present_shards():
        n = (
            pl.scan_parquet(path)
            .group_by(key)
            .len()
            .filter(pl.col("len") > 1)
            .select((pl.col("len") - 1).sum().alias("n"))
            .collect(engine="streaming")
            .item()
        )
        counts[entry.chrom] = int(n or 0)
    r = _result("duplicates", counts, "duplicate probe keys", "warn" if assume_unique else "fail")
    r.detail = f"{r.detail} ({policy})"
    return r


def check_manifest_counts(m: CacheManifest) -> InvariantResult:
    problems = []
    checked = 0
    for entry, path in m.present_shards():
        checked += 1
        found = (
            pl.scan_parquet(path)
            .select(
                pl.len().alias("rows"),
                (pl.col("tier") == 0).sum().alias("warm"),
                (pl.col("tier") == 1).sum().alias("cold"),
            )
            .collect()
            .row(0, named=True)
        )
        for field in ("rows", "warm", "cold"):
            expected = getattr(entry, field)
            if int(found[field]) != expected:
                problems.append(f"{entry.chrom} {field}: manifest {expected}, shard {int(found[field])}")
    if problems:
        return InvariantResult("manifest_counts", "fail", "; ".join(problems))
    return InvariantResult("manifest_counts", "pass", f"rows/warm/cold match in {checked} shards")


def check_manifest_files(m: CacheManifest) -> InvariantResult:
    missing = [e.file for e in m.contigs if e.rows > 0 and not m.shard_path(e).exists()]
    listed = {e.file for e in m.contigs}
    unlisted = sorted(p.name for p in m.plugin_dir.glob("chr*.parquet") if p.name not in listed)
    problems = []
    if missing:
        problems.append("missing: " + ", ".join(missing))
    if unlisted:
        problems.append("unlisted: " + ", ".join(unlisted))
    if problems:
        return InvariantResult("manifest_files", "fail", "; ".join(problems))
    return InvariantResult("manifest_files", "pass", f"{len(listed)} manifest contigs, no stray shards")


def run_all(m: CacheManifest) -> list[InvariantResult]:
    return [
        check_schema(m),
        check_contig(m),
        check_order(m),
        check_tier_domain(m),
        check_manifest_counts(m),
        check_manifest_files(m),
        check_positions(m),
        check_allele_form(m),
        check_duplicates(m),
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_qa_invariants.py -q`
Expected: all pass.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check e2e-testing/scripts/cache_qa tests/test_cache_qa_invariants.py && uv run ruff format e2e-testing/scripts/cache_qa tests/test_cache_qa_invariants.py
git add e2e-testing/scripts/cache_qa/invariants.py tests/test_cache_qa_invariants.py
git commit -m "feat(cache_qa): order, duplicate, manifest count and file invariants"
```

---

### Task 4: Content profile

**Files:**
- Create: `e2e-testing/scripts/cache_qa/profile.py`
- Test: `tests/test_cache_qa_profile.py`

**Interfaces:**
- Consumes `CacheManifest`.
- Produces `cache_qa.profile`:
  - `@dataclass ContigStats(chrom, file, rows, warm, cold, warm_share, bytes, start_min, start_max)`
  - `@dataclass NumericStats(parsable_share: float, min: float, max: float, mean: float, p50: float, p95: float)`
  - `@dataclass ColumnStats(name, role, dtype: str, null_share, empty_share: float | None, distinct: int | None, approx: bool, top_values: list[tuple[str, int]] | None, numeric: NumericStats | None, per_contig: dict[str, dict[str, float]])`
  - `@dataclass Profile(contigs: list[ContigStats], columns: list[ColumnStats], rows: int, warm: int, cold: int, bytes: int)`
  - `profile_cache(m: CacheManifest) -> Profile`
  - constants `EXACT_DISTINCT_MAX = 10_000`, `TOP_VALUES_MAX_DISTINCT = 50`, `TOP_VALUES_N = 10`.

- [ ] **Step 1: Write the failing tests**

`tests/test_cache_qa_profile.py`:

```python
import polars as pl
import pytest

from cache_qa import manifest, profile
from cache_qa_synthetic import SyntheticCache


@pytest.fixture
def prof(tmp_path):
    cache = SyntheticCache(tmp_path)
    # chr1: 6 rows, score empty at i % 3 == 0 -> 2 empties; one null raw
    df = cache.rows["chr1"]
    cache.rows["chr1"] = df.with_columns(
        pl.when(pl.arange(0, df.height) == 5).then(None).otherwise(pl.col("raw")).cast(pl.Float32).alias("raw")
    )
    m = manifest.load_manifest(cache.write())
    return profile.profile_cache(m), m


def test_contig_table(prof):
    p, m = prof
    by = {c.chrom: c for c in p.contigs}
    assert by["chr1"].rows == 6 and by["chr1"].warm == 2 and by["chr1"].cold == 4
    assert by["chr1"].warm_share == pytest.approx(2 / 6)
    assert by["chr1"].start_min == 100 and by["chr1"].start_max == 150
    assert by["chr1"].bytes == (m.plugin_dir / "chr1.parquet").stat().st_size
    assert p.rows == 13 and p.warm == 6 and p.cold == 7
    assert p.bytes == sum(c.bytes for c in p.contigs)


def test_column_roles_and_null_share(prof):
    p, _ = prof
    cols = {c.name: c for c in p.columns}
    assert cols["chrom"].role == "key" and cols["tier"].role == "tier"
    assert cols["symbol"].role == "match" and cols["score"].role == "value"
    assert cols["raw"].null_share == pytest.approx(1 / 13)
    assert cols["raw"].per_contig["chr1"]["null_share"] == pytest.approx(1 / 6)
    assert cols["chrom"].distinct is None  # key columns get only the null share


def test_empty_share_counts_empty_string_and_dot(tmp_path):
    cache = SyntheticCache(tmp_path)
    cache.rows["chr2"] = cache.rows["chr2"].with_columns(pl.lit(".").alias("score"))
    p = profile.profile_cache(manifest.load_manifest(cache.write()))
    score = next(c for c in p.columns if c.name == "score")
    # chr1: 2 empties of 6; chr2: 4 dots of 4; chrX: 1 empty of 3
    assert score.empty_share == pytest.approx(7 / 13)
    assert score.per_contig["chr2"]["empty_share"] == pytest.approx(1.0)


def test_exact_distinct_and_top_values(prof):
    p, _ = prof
    symbol = next(c for c in p.columns if c.name == "symbol")
    assert symbol.distinct == 3 and symbol.approx is False
    assert symbol.top_values[0] == ("GENE1", 6)
    assert [v for v, _ in symbol.top_values] == ["GENE1", "GENE2", "GENEX"]


def test_numeric_on_parsable_text(prof):
    p, _ = prof
    score = next(c for c in p.columns if c.name == "score")
    assert score.numeric is not None
    assert score.numeric.parsable_share == pytest.approx(9 / 13)
    assert score.numeric.min == 0.5 and score.numeric.max == 0.5


def test_numeric_on_float_column(prof):
    p, _ = prof
    raw = next(c for c in p.columns if c.name == "raw")
    assert raw.numeric.parsable_share == pytest.approx(12 / 13)
    assert raw.numeric.min == 0.0 and raw.numeric.max == 5.0
    assert raw.empty_share is None


def test_top_values_capped_at_ten_and_absent_above_fifty_distinct(tmp_path, monkeypatch):
    cache = SyntheticCache(tmp_path)
    for chrom, df in cache.rows.items():
        cache.rows[chrom] = df.with_columns(
            (pl.lit(chrom) + pl.arange(0, df.height).cast(pl.String)).alias("symbol")
        )
    m = manifest.load_manifest(cache.write())
    symbol = next(c for c in profile.profile_cache(m).columns if c.name == "symbol")
    assert symbol.distinct == 13 and len(symbol.top_values) == profile.TOP_VALUES_N
    monkeypatch.setattr(profile, "TOP_VALUES_MAX_DISTINCT", 12)
    symbol = next(c for c in profile.profile_cache(m).columns if c.name == "symbol")
    assert symbol.top_values is None


def test_profile_runs_with_missing_zero_row_shard(tmp_path):
    cache = SyntheticCache(tmp_path)
    m = cache.manifest_dict()
    m["chroms"].append({"chrom": "chrMT", "file": "chrMT.parquet", "rows": 0, "warm": 0, "cold": 0})
    cache.set_manifest(m)
    p = profile.profile_cache(manifest.load_manifest(cache.write()))
    assert [c.chrom for c in p.contigs] == ["chr1", "chr2", "chrX", "chrMT"]
    assert p.contigs[-1].rows == 0 and p.contigs[-1].bytes == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_qa_profile.py -q`
Expected: `ImportError: cannot import name 'profile'`

- [ ] **Step 3: Implement the module**

`e2e-testing/scripts/cache_qa/profile.py`:

```python
"""Content profile of a plugin cache: contig table and per-column statistics (Polars, streaming)."""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from cache_qa.manifest import CacheManifest, ColumnSpec

EXACT_DISTINCT_MAX = 10_000
TOP_VALUES_MAX_DISTINCT = 50
TOP_VALUES_N = 10


@dataclass
class ContigStats:
    chrom: str
    file: str
    rows: int
    warm: int
    cold: int
    warm_share: float
    bytes: int
    start_min: int | None
    start_max: int | None


@dataclass
class NumericStats:
    parsable_share: float
    min: float
    max: float
    mean: float
    p50: float
    p95: float


@dataclass
class ColumnStats:
    name: str
    role: str
    dtype: str
    null_share: float
    empty_share: float | None
    distinct: int | None
    approx: bool
    top_values: list[tuple[str, int]] | None
    numeric: NumericStats | None
    per_contig: dict[str, dict[str, float]] = field(default_factory=dict)


@dataclass
class Profile:
    contigs: list[ContigStats]
    columns: list[ColumnStats]
    rows: int
    warm: int
    cold: int
    bytes: int


def _is_text(spec: ColumnSpec) -> bool:
    return spec.dtype == pl.String


def _is_numeric(spec: ColumnSpec) -> bool:
    return spec.dtype.is_numeric()


def _share(num: int | float | None, den: int) -> float:
    return float(num or 0) / den if den else 0.0


def _contig_table(m: CacheManifest, lf: pl.LazyFrame) -> list[ContigStats]:
    per = (
        lf.group_by("chrom")
        .agg(
            pl.len().alias("rows"),
            (pl.col("tier") == 0).sum().alias("warm"),
            (pl.col("tier") == 1).sum().alias("cold"),
            pl.col("start").min().alias("start_min"),
            pl.col("start").max().alias("start_max"),
        )
        .collect(engine="streaming")
    )
    found = {r["chrom"]: r for r in per.to_dicts()}
    out = []
    for entry in m.contigs:
        path = m.shard_path(entry)
        size = path.stat().st_size if path.exists() else 0
        r = found.get(entry.bare)
        if r is None:
            out.append(ContigStats(entry.chrom, entry.file, 0, 0, 0, 0.0, size, None, None))
            continue
        out.append(
            ContigStats(
                entry.chrom, entry.file, int(r["rows"]), int(r["warm"]), int(r["cold"]),
                _share(r["warm"], int(r["rows"])), size, int(r["start_min"]), int(r["start_max"]),
            )
        )
    return out


def _empty_expr(name: str) -> pl.Expr:
    return (pl.col(name) == "") | (pl.col(name) == ".")


def _column_stats(m: CacheManifest, lf: pl.LazyFrame, total_rows: int, bare_to_chrom: dict[str, str]) -> list[ColumnStats]:
    specs = m.expected_schema()
    aggs: list[pl.Expr] = []
    for s in specs:
        aggs.append(pl.col(s.name).null_count().alias(f"{s.name}__null"))
        if s.role in ("match", "value") and _is_text(s):
            aggs.append(_empty_expr(s.name).sum().alias(f"{s.name}__empty"))
            aggs.append(pl.col(s.name).cast(pl.Float64, strict=False).is_not_null().sum().alias(f"{s.name}__parsable"))
        if s.role in ("match", "value"):
            aggs.append(pl.col(s.name).approx_n_unique().alias(f"{s.name}__approx_nunique"))
    overall = lf.select(aggs).collect(engine="streaming").row(0, named=True)
    per_contig = lf.group_by("chrom").agg(aggs).collect(engine="streaming").to_dicts()

    out = []
    for s in specs:
        null_share = _share(overall[f"{s.name}__null"], total_rows)
        pc: dict[str, dict[str, float]] = {}
        for r in per_contig:
            chrom = bare_to_chrom.get(r["chrom"], r["chrom"])
            # raw counts; profile_cache turns them into shares once contig row counts are known
            pc[chrom] = {"_null": int(r[f"{s.name}__null"])}
            if f"{s.name}__empty" in r:
                pc[chrom]["_empty"] = int(r[f"{s.name}__empty"])
        if s.role in ("key", "tier"):
            out.append(ColumnStats(s.name, s.role, str(s.dtype), null_share, None, None, False, None, None, pc))
            continue

        empty_share = _share(overall.get(f"{s.name}__empty"), total_rows) if _is_text(s) else None
        approx_n = int(overall[f"{s.name}__approx_nunique"] or 0)
        if approx_n <= EXACT_DISTINCT_MAX:
            distinct = int(lf.select(pl.col(s.name).n_unique()).collect(engine="streaming").item())
            approx = False
        else:
            distinct, approx = approx_n, True

        top_values = None
        if distinct <= TOP_VALUES_MAX_DISTINCT:
            top = (
                lf.group_by(s.name).len().sort(["len", s.name], descending=[True, False])
                .head(TOP_VALUES_N).collect(engine="streaming")
            )
            top_values = [(str(v), int(n)) for v, n in top.iter_rows()]

        numeric = None
        parsable = int(overall.get(f"{s.name}__parsable") or 0) if _is_text(s) else total_rows - int(overall[f"{s.name}__null"])
        if _is_numeric(s) or (_is_text(s) and parsable > 0):
            col = pl.col(s.name).cast(pl.Float64, strict=False)
            q = lf.select(
                col.min().alias("min"), col.max().alias("max"), col.mean().alias("mean"),
                col.quantile(0.5).alias("p50"), col.quantile(0.95).alias("p95"),
            ).collect(engine="streaming").row(0, named=True)
            numeric = NumericStats(
                _share(parsable, total_rows), float(q["min"]), float(q["max"]),
                float(q["mean"]), float(q["p50"]), float(q["p95"]),
            )
        out.append(ColumnStats(s.name, s.role, str(s.dtype), null_share, empty_share, distinct, approx, top_values, numeric, pc))
    return out


def profile_cache(m: CacheManifest) -> Profile:
    files = [str(p) for _, p in m.present_shards()]
    lf = pl.scan_parquet(files)
    contigs = _contig_table(m, lf)
    rows = sum(c.rows for c in contigs)
    bare_to_chrom = {e.bare: e.chrom for e in m.contigs}
    columns = _column_stats(m, lf, rows, bare_to_chrom)
    rows_by_chrom = {c.chrom: c.rows for c in contigs}
    for col in columns:  # turn raw per-contig counts into shares
        for chrom, raw in list(col.per_contig.items()):
            n = rows_by_chrom.get(chrom, 0)
            shares = {"null_share": _share(raw["_null"], n)}
            if "_empty" in raw:
                shares["empty_share"] = _share(raw["_empty"], n)
            col.per_contig[chrom] = shares
    return Profile(
        contigs=contigs,
        columns=columns,
        rows=rows,
        warm=sum(c.warm for c in contigs),
        cold=sum(c.cold for c in contigs),
        bytes=sum(c.bytes for c in contigs),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_qa_profile.py -q`
Expected: all pass. If `approx_n_unique` is unavailable on a dtype, replace it with `n_unique()` for that column (small caches) and keep `approx=False`.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check e2e-testing/scripts/cache_qa tests/test_cache_qa_profile.py && uv run ruff format e2e-testing/scripts/cache_qa tests/test_cache_qa_profile.py
git add e2e-testing/scripts/cache_qa/profile.py tests/test_cache_qa_profile.py
git commit -m "feat(cache_qa): contig table and per-column content profile"
```

---

### Task 5: Report assembly (`qa_profile.json`)

**Files:**
- Create: `e2e-testing/scripts/cache_qa/report.py`
- Test: `tests/test_cache_qa_report.py`

**Interfaces:**
- Consumes `InvariantResult`, `Profile`, `CacheManifest`.
- Produces `cache_qa.report`:
  - `SCHEMA_VERSION = 1`
  - `overall_status(results: list[InvariantResult]) -> str`
  - `build_report(m: CacheManifest, results, profile: Profile, generated_at: str, tool: dict[str, str]) -> dict`
  - `tool_versions() -> dict[str, str]` (`{"vepyr": …, "polars": …, "schema_version": 1}`; vepyr version from `importlib.metadata.version("vepyr")` falling back to `"unknown"`).
  - `write_report(report: dict, path: Path) -> None` (pretty JSON, trailing newline).

- [ ] **Step 1: Write the failing tests**

`tests/test_cache_qa_report.py`:

```python
import json

from cache_qa import invariants, manifest, profile, report
from cache_qa_synthetic import SyntheticCache


def _inputs(tmp_path):
    m = manifest.load_manifest(SyntheticCache(tmp_path).write())
    return m, invariants.run_all(m), profile.profile_cache(m)


def test_status_aggregation():
    R = invariants.InvariantResult
    assert report.overall_status([R("a", "pass", "")]) == "pass"
    assert report.overall_status([R("a", "pass", ""), R("b", "warn", "")]) == "warn"
    assert report.overall_status([R("a", "warn", ""), R("b", "fail", "")]) == "fail"


def test_report_keys_and_values(tmp_path):
    m, results, prof = _inputs(tmp_path)
    r = report.build_report(m, results, prof, "2026-09-05T12:00:00Z", {"vepyr": "0.4.0", "polars": "1.39.3", "schema_version": 1})
    assert set(r) == {"plugin", "cache_source_version", "generated_at", "tool", "status", "invariants", "summary", "contigs", "columns"}
    assert r["plugin"] == "demo" and r["status"] == "pass"
    assert r["tool"]["schema_version"] == 1
    assert r["summary"] == {"rows": 13, "warm": 6, "cold": 7, "bytes": prof.bytes, "contigs": 3}
    assert r["invariants"][0] == {"id": "schema", "status": "pass", "detail": "3 shards match the manifest"}
    assert r["contigs"][0]["chrom"] == "chr1" and r["contigs"][0]["warm_share"] == 2 / 6
    col = next(c for c in r["columns"] if c["name"] == "symbol")
    assert col["top_values"][0] == ["GENE1", 6] and col["numeric"] is None


def test_per_contig_only_when_present(tmp_path):
    m, results, prof = _inputs(tmp_path)
    results[1].per_contig = {"chr1": 2}
    r = report.build_report(m, results, prof, "t", report.tool_versions())
    assert r["invariants"][1]["per_contig"] == {"chr1": 2}
    assert "per_contig" not in r["invariants"][0]


def test_write_report_roundtrip(tmp_path):
    m, results, prof = _inputs(tmp_path)
    r = report.build_report(m, results, prof, "t", report.tool_versions())
    out = tmp_path / "qa_profile.json"
    report.write_report(r, out)
    text = out.read_text()
    assert text.endswith("}\n") and json.loads(text) == r


def test_tool_versions_has_polars():
    t = report.tool_versions()
    assert t["schema_version"] == 1 and t["polars"].count(".") >= 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_qa_report.py -q`
Expected: `ImportError: cannot import name 'report'`

- [ ] **Step 3: Implement the module**

`e2e-testing/scripts/cache_qa/report.py`:

```python
"""Assemble qa_profile.json from invariant results and the content profile."""

from __future__ import annotations

import json
from dataclasses import asdict
from importlib import metadata
from pathlib import Path

import polars as pl

from cache_qa.invariants import InvariantResult
from cache_qa.manifest import CacheManifest
from cache_qa.profile import Profile

SCHEMA_VERSION = 1


def overall_status(results: list[InvariantResult]) -> str:
    statuses = {r.status for r in results}
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    return "pass"


def tool_versions() -> dict[str, str | int]:
    try:
        vepyr_version = metadata.version("vepyr")
    except metadata.PackageNotFoundError:
        vepyr_version = "unknown"
    return {"vepyr": vepyr_version, "polars": pl.__version__, "schema_version": SCHEMA_VERSION}


def _invariant_dict(r: InvariantResult) -> dict:
    d = {"id": r.id, "status": r.status, "detail": r.detail}
    if r.per_contig:
        d["per_contig"] = dict(r.per_contig)
    return d


def _column_dict(c) -> dict:
    d = asdict(c)
    if d["top_values"] is not None:
        d["top_values"] = [[v, n] for v, n in d["top_values"]]
    return d


def build_report(
    m: CacheManifest,
    results: list[InvariantResult],
    profile: Profile,
    generated_at: str,
    tool: dict[str, str | int],
) -> dict:
    return {
        "plugin": m.plugin,
        "cache_source_version": m.cache_source_version,
        "generated_at": generated_at,
        "tool": dict(tool),
        "status": overall_status(results),
        "invariants": [_invariant_dict(r) for r in results],
        "summary": {
            "rows": profile.rows,
            "warm": profile.warm,
            "cold": profile.cold,
            "bytes": profile.bytes,
            "contigs": len(profile.contigs),
        },
        "contigs": [asdict(c) for c in profile.contigs],
        "columns": [_column_dict(c) for c in profile.columns],
    }


def write_report(report: dict, path: Path) -> None:
    Path(path).write_text(json.dumps(report, indent=2) + "\n")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_qa_report.py -q`
Expected: all pass.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check e2e-testing/scripts/cache_qa tests/test_cache_qa_report.py && uv run ruff format e2e-testing/scripts/cache_qa tests/test_cache_qa_report.py
git add e2e-testing/scripts/cache_qa/report.py tests/test_cache_qa_report.py
git commit -m "feat(cache_qa): qa_profile.json report assembly"
```

---

### Task 6: Card section rendering and splicing

**Files:**
- Create: `e2e-testing/scripts/cache_qa/card.py`
- Test: `tests/test_cache_qa_card.py`

**Interfaces:**
- Consumes the report `dict` from Task 5.
- Produces `cache_qa.card`:
  - `START = "<!-- qa-profile:start -->"`, `END = "<!-- qa-profile:end -->"`
  - `render_section(report: dict) -> str` (starts with `## Quality profile`, no markers)
  - `splice(readme: str, section: str) -> str` (replace between markers; else insert before `## Usage`; else append)
  - `format_int(n) -> str` (`4,439,569`), `format_bytes(n) -> str` (`7.4 MB`, `85 MB`, `2.2 GB`), `format_pct(x) -> str` (`2.6%`).

- [ ] **Step 1: Write the failing tests**

`tests/test_cache_qa_card.py`:

```python
from cache_qa import card


def _report(status="pass"):
    return {
        "plugin": "demo",
        "generated_at": "2026-09-05T12:00:00Z",
        "tool": {"vepyr": "0.4.0", "polars": "1.39.3", "schema_version": 1},
        "status": status,
        "invariants": [
            {"id": "schema", "status": "pass", "detail": "3 shards match the manifest"},
            {"id": "duplicates", "status": "warn", "detail": "412 duplicate keys (assume_unique)", "per_contig": {"chr1": 400}},
        ],
        "summary": {"rows": 4439569, "warm": 113018, "cold": 4326551, "bytes": 85012345, "contigs": 2},
        "contigs": [
            {"chrom": "chr1", "file": "chr1.parquet", "rows": 401099, "warm": 10445, "cold": 390654, "warm_share": 0.026, "bytes": 7400000, "start_min": 1, "start_max": 2},
            {"chrom": "chrMT", "file": "chrMT.parquet", "rows": 0, "warm": 0, "cold": 0, "warm_share": 0.0, "bytes": 0, "start_min": None, "start_max": None},
        ],
        "columns": [
            {"name": "chrom", "role": "key", "dtype": "String", "null_share": 0.0, "empty_share": None, "distinct": None, "approx": False, "top_values": None, "numeric": None, "per_contig": {}},
            {"name": "clnsig", "role": "value", "dtype": "String", "null_share": 0.0, "empty_share": 0.0012, "distinct": 31, "approx": False,
             "top_values": [["Uncertain_significance", 1900000], ["Likely_benign", 1200000]], "numeric": None, "per_contig": {}},
            {"name": "am", "role": "value", "dtype": "Float32", "null_share": 0.0, "empty_share": None, "distinct": 68000000, "approx": True, "top_values": None,
             "numeric": {"parsable_share": 1.0, "min": 0.0, "max": 1.0, "mean": 0.3, "p50": 0.121, "p95": 0.912}, "per_contig": {}},
        ],
    }


def test_render_contains_all_three_tables():
    s = card.render_section(_report())
    assert s.startswith("## Quality profile")
    assert "Generated 2026-09-05 by `profile_plugin_cache.py` (vepyr 0.4.0, Polars 1.39.3)" in s
    assert "| schema | ✅ pass | 3 shards match the manifest |" in s
    assert "| duplicates | ⚠️ warn | 412 duplicate keys (assume_unique) |" in s
    assert "| chr1 | 401,099 | 10,445 | 390,654 | 2.6% | 7.4 MB |" in s
    assert "| **total** | **4,439,569** | **113,018** | **4,326,551** | **2.5%** | **85 MB** |" in s
    assert "| clnsig | value | String | 0.00 | 0.12 | 31 | — | Uncertain_significance (1.9M), Likely_benign (1.2M) |" in s
    assert "| am | value | Float32 | 0.00 | — | ~68M | 0.000 / 0.121 / 0.912 / 1.000 | — |" in s
    assert "| chrom |" not in s  # key columns are not listed


def test_render_marks_fail():
    r = _report("fail")
    r["invariants"][0]["status"] = "fail"
    assert "| schema | ❌ fail |" in card.render_section(r)


def test_formatters():
    assert card.format_int(4439569) == "4,439,569"
    assert card.format_bytes(7400000) == "7.4 MB"
    assert card.format_bytes(85012345) == "85 MB"
    assert card.format_bytes(2237109762) == "2.2 GB"
    assert card.format_bytes(0) == "0 B"
    assert card.format_pct(0.026) == "2.6%"
    assert card.format_count_short(1900000) == "1.9M"
    assert card.format_count_short(68000000) == "68M"
    assert card.format_count_short(412) == "412"


def test_splice_inserts_before_usage():
    readme = "# T\n\n## Contents\n\nx\n\n## Usage\n\ny\n"
    out = card.splice(readme, "## Quality profile\n\nq\n")
    assert out.index(card.START) < out.index("## Usage")
    assert out.count(card.START) == 1 and out.count(card.END) == 1
    assert "## Contents\n\nx\n" in out and out.endswith("## Usage\n\ny\n")


def test_splice_replaces_between_markers_idempotently():
    readme = "# T\n\n## Usage\n\ny\n"
    once = card.splice(readme, "## Quality profile\n\nv1\n")
    twice = card.splice(once, "## Quality profile\n\nv2\n")
    assert "v1" not in twice and "v2" in twice
    assert twice.count(card.START) == 1
    assert card.splice(twice, "## Quality profile\n\nv2\n") == twice


def test_splice_appends_without_usage():
    out = card.splice("# T\n\nbody\n", "## Quality profile\n\nq\n")
    assert out.startswith("# T\n\nbody\n") and out.rstrip().endswith(card.END)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_qa_card.py -q`
Expected: `ImportError: cannot import name 'card'`

- [ ] **Step 3: Implement the module**

`e2e-testing/scripts/cache_qa/card.py`:

```python
"""Render the `## Quality profile` card section and splice it into a README."""

from __future__ import annotations

START = "<!-- qa-profile:start -->"
END = "<!-- qa-profile:end -->"
_STATUS_ICON = {"pass": "✅", "warn": "⚠️", "fail": "❌"}


def format_int(n: int) -> str:
    return f"{int(n):,}"


def format_bytes(n: int) -> str:
    n = int(n)
    if n < 1_000:
        return f"{n} B"
    for unit, div in (("KB", 1e3), ("MB", 1e6), ("GB", 1e9), ("TB", 1e12)):
        if n < div * 1000 or unit == "TB":
            v = n / div
            return f"{v:.1f} {unit}" if v < 10 else f"{v:.0f} {unit}"
    raise AssertionError("unreachable")


def format_pct(x: float) -> str:
    return f"{100 * float(x):.1f}%"


def format_count_short(n: int) -> str:
    n = int(n)
    if n >= 1_000_000:
        v = n / 1e6
        return f"{v:.1f}M" if v < 10 else f"{v:.0f}M"
    if n >= 1_000:
        v = n / 1e3
        return f"{v:.1f}K" if v < 10 else f"{v:.0f}K"
    return str(n)


def _numeric_cell(numeric: dict | None) -> str:
    if not numeric:
        return "—"
    return " / ".join(f"{numeric[k]:.3f}" for k in ("min", "p50", "p95", "max"))


def _top_cell(top: list | None) -> str:
    if not top:
        return "—"
    return ", ".join(f"{v} ({format_count_short(n)})" for v, n in top)


def render_section(report: dict) -> str:
    tool = report["tool"]
    lines = [
        "## Quality profile",
        "",
        f"Generated {report['generated_at'][:10]} by `profile_plugin_cache.py` "
        f"(vepyr {tool['vepyr']}, Polars {tool['polars']}) from the shards in this commit; "
        "machine-readable copy in [`qa_profile.json`](qa_profile.json).",
        "",
        "### Invariants",
        "",
        "| check | status | detail |",
        "|---|---|---|",
    ]
    for inv in report["invariants"]:
        icon = _STATUS_ICON[inv["status"]]
        lines.append(f"| {inv['id']} | {icon} {inv['status']} | {inv['detail']} |")
    lines += ["", "### Contigs", "", "| contig | rows | warm | cold | warm % | size |", "|---|--:|--:|--:|--:|--:|"]
    for c in report["contigs"]:
        lines.append(
            f"| {c['chrom']} | {format_int(c['rows'])} | {format_int(c['warm'])} | {format_int(c['cold'])} "
            f"| {format_pct(c['warm_share'])} | {format_bytes(c['bytes'])} |"
        )
    s = report["summary"]
    warm_share = s["warm"] / s["rows"] if s["rows"] else 0.0
    lines.append(
        f"| **total** | **{format_int(s['rows'])}** | **{format_int(s['warm'])}** | **{format_int(s['cold'])}** "
        f"| **{format_pct(warm_share)}** | **{format_bytes(s['bytes'])}** |"
    )
    lines += [
        "", "### Columns", "",
        "| column | role | type | null % | empty % | distinct | numeric (min / p50 / p95 / max) | top values |",
        "|---|---|---|--:|--:|--:|---|---|",
    ]
    for col in report["columns"]:
        if col["role"] not in ("match", "value"):
            continue
        empty = "—" if col["empty_share"] is None else f"{100 * col['empty_share']:.2f}"
        distinct = "—" if col["distinct"] is None else (
            f"~{format_count_short(col['distinct'])}" if col["approx"] else format_int(col["distinct"])
        )
        lines.append(
            f"| {col['name']} | {col['role']} | {col['dtype']} | {100 * col['null_share']:.2f} | {empty} "
            f"| {distinct} | {_numeric_cell(col['numeric'])} | {_top_cell(col['top_values'])} |"
        )
    return "\n".join(lines) + "\n"


def splice(readme: str, section: str) -> str:
    """Replace the marked block, else insert it before `## Usage`, else append it."""
    block = f"{START}\n{section.rstrip()}\n{END}\n"
    if START in readme and END in readme:
        head = readme[: readme.index(START)]
        tail = readme[readme.index(END) + len(END) :].lstrip("\n")
        return head + block + ("\n" + tail if tail else "")
    marker = "\n## Usage"
    if marker in readme:
        i = readme.index(marker) + 1
        return readme[:i] + block + "\n" + readme[i:]
    return readme.rstrip("\n") + "\n\n" + block
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_qa_card.py -q`
Expected: all pass.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check e2e-testing/scripts/cache_qa tests/test_cache_qa_card.py && uv run ruff format e2e-testing/scripts/cache_qa tests/test_cache_qa_card.py
git add e2e-testing/scripts/cache_qa/card.py tests/test_cache_qa_card.py
git commit -m "feat(cache_qa): quality-profile card section rendering and splicing"
```

---

### Task 7: Staging and publishing through an injected runner

**Files:**
- Create: `e2e-testing/scripts/cache_qa/stage.py`
- Test: `tests/test_cache_qa_stage.py`

**Interfaces:**
- Consumes `CacheManifest`.
- Produces `cache_qa.stage`:
  - `class Runner(Protocol): def run(self, argv: list[str]) -> subprocess.CompletedProcess: ...`
  - `class SubprocessRunner` (real; `subprocess.run(argv, capture_output=True, text=True)`).
  - `class PublishError(RuntimeError)`.
  - `build_stage(m: CacheManifest, qa_profile: Path, readme: Path, stage_dir: Path) -> list[Path]` (hard links; falls back to copy when `os.link` raises `OSError`; returns staged paths).
  - `publish(runner, repo: str, stage_dir: Path, tag: str, commit_message: str) -> str` (runs `hf upload`, `hf repos tag delete --yes`, `hf repos tag create`; returns the head sha parsed from `hf datasets info --format json`).
  - `verify(runner, repo: str, stage_dir: Path, tag: str) -> list[str]` (returns mismatches: size differences, missing files, tag not at head; empty list = ok). Uses `hf datasets info <repo> --format json --expand siblings` for sizes and sha; `hf datasets info <repo> --revision <tag> --format json` for the tag sha.
  - `check_hf_available(runner) -> None` raises `PublishError` when `hf auth whoami` fails.

- [ ] **Step 1: Write the failing tests**

`tests/test_cache_qa_stage.py`:

```python
import json
import subprocess

import pytest

from cache_qa import manifest, stage
from cache_qa_synthetic import SyntheticCache


class FakeRunner:
    def __init__(self, responses=None, fail_on=None):
        self.calls: list[list[str]] = []
        self.responses = responses or {}
        self.fail_on = fail_on

    def run(self, argv):
        self.calls.append(list(argv))
        key = " ".join(argv[:3])
        if self.fail_on and self.fail_on in " ".join(argv):
            return subprocess.CompletedProcess(argv, 1, "", "boom")
        out = self.responses.get(key, "")
        return subprocess.CompletedProcess(argv, 0, out, "")


def _staged(tmp_path):
    m = manifest.load_manifest(SyntheticCache(tmp_path).write())
    qa = m.plugin_dir / "qa_profile.json"
    qa.write_text("{}\n")
    readme = tmp_path / "README.md"
    readme.write_text("# card\n")
    stage_dir = tmp_path / "stage_demo"
    staged = stage.build_stage(m, qa, readme, stage_dir)
    return m, stage_dir, staged


def test_build_stage_links_every_listed_file(tmp_path):
    m, stage_dir, staged = _staged(tmp_path)
    names = sorted(p.name for p in staged)
    assert names == ["README.md", "chr1.parquet", "chr2.parquet", "chrX.parquet", "manifest.json", "qa_profile.json"]
    src = m.plugin_dir / "chr1.parquet"
    assert (stage_dir / "chr1.parquet").stat().st_ino == src.stat().st_ino  # hard link


def test_build_stage_skips_zero_row_missing_shard(tmp_path):
    cache = SyntheticCache(tmp_path)
    md = cache.manifest_dict()
    md["chroms"].append({"chrom": "chrMT", "file": "chrMT.parquet", "rows": 0, "warm": 0, "cold": 0})
    cache.set_manifest(md)
    m = manifest.load_manifest(cache.write())
    qa = m.plugin_dir / "qa_profile.json"; qa.write_text("{}\n")
    readme = tmp_path / "README.md"; readme.write_text("# c\n")
    staged = stage.build_stage(m, qa, readme, tmp_path / "s")
    assert "chrMT.parquet" not in {p.name for p in staged}


def test_publish_runs_upload_and_moves_tag(tmp_path):
    m, stage_dir, _ = _staged(tmp_path)
    runner = FakeRunner(responses={"hf datasets info": json.dumps({"sha": "abc123"})})
    head = stage.publish(runner, "org/repo", stage_dir, "v0.1.1", "msg")
    assert head == "abc123"
    argv = runner.calls
    assert argv[0][:3] == ["hf", "upload", "org/repo"] and str(stage_dir) in argv[0] and "--type" in argv[0] and "dataset" in argv[0]
    assert argv[0][argv[0].index("--commit-message") + 1] == "msg"
    assert ["hf", "repos", "tag", "delete", "org/repo", "v0.1.1", "--type", "dataset", "--yes"] == argv[1]
    assert argv[2][:6] == ["hf", "repos", "tag", "create", "org/repo", "v0.1.1"]


def test_publish_raises_when_upload_fails(tmp_path):
    m, stage_dir, _ = _staged(tmp_path)
    with pytest.raises(stage.PublishError) as e:
        stage.publish(FakeRunner(fail_on="hf upload"), "org/repo", stage_dir, "v0.1.1", "msg")
    assert "hf upload" in str(e.value) and "boom" in str(e.value)


def test_verify_reports_size_and_tag_mismatch(tmp_path):
    m, stage_dir, staged = _staged(tmp_path)
    siblings = [{"rfilename": p.name, "size": p.stat().st_size} for p in staged]
    siblings[0]["size"] += 1  # README size wrong
    siblings.pop()            # one file missing on the hub
    runner = FakeRunner(responses={
        "hf datasets info": json.dumps({"sha": "head1", "siblings": siblings}),
    })
    # the tag lookup uses the same argv prefix; make it answer a different sha
    orig = runner.run
    def run(argv):
        if "--revision" in argv:
            return subprocess.CompletedProcess(argv, 0, json.dumps({"sha": "old"}), "")
        return orig(argv)
    runner.run = run
    problems = stage.verify(runner, "org/repo", stage_dir, "v0.1.1")
    assert any("README.md" in p and "size" in p for p in problems)
    assert any("missing" in p for p in problems)
    assert any("tag" in p for p in problems)


def test_verify_clean(tmp_path):
    m, stage_dir, staged = _staged(tmp_path)
    siblings = [{"rfilename": p.name, "size": p.stat().st_size} for p in staged]
    runner = FakeRunner(responses={"hf datasets info": json.dumps({"sha": "h", "siblings": siblings})})
    assert stage.verify(runner, "org/repo", stage_dir, "v0.1.1") == []


def test_check_hf_available_raises(tmp_path):
    with pytest.raises(stage.PublishError):
        stage.check_hf_available(FakeRunner(fail_on="hf auth whoami"))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_qa_stage.py -q`
Expected: `ImportError: cannot import name 'stage'`

- [ ] **Step 3: Implement the module**

`e2e-testing/scripts/cache_qa/stage.py`:

```python
"""Hard-link staging directory and Hugging Face publishing through an injected runner."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Protocol

from cache_qa.manifest import CacheManifest


class PublishError(RuntimeError):
    """A Hub command failed or the published state does not match the staged files."""


class Runner(Protocol):
    def run(self, argv: list[str]) -> subprocess.CompletedProcess: ...


class SubprocessRunner:
    def run(self, argv: list[str]) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(argv, capture_output=True, text=True, check=False)
        except FileNotFoundError as e:
            raise PublishError(
                f"`{argv[0]}` is not on PATH; install with `curl -LsSf https://hf.co/cli/install.sh | bash`"
            ) from e


def _run_ok(runner: Runner, argv: list[str]) -> subprocess.CompletedProcess:
    cp = runner.run(argv)
    if cp.returncode != 0:
        raise PublishError(f"`{' '.join(argv)}` failed (exit {cp.returncode}): {cp.stderr.strip() or cp.stdout.strip()}")
    return cp


def check_hf_available(runner: Runner) -> None:
    """Fail before any upload when `hf` is missing or not logged in."""
    _run_ok(runner, ["hf", "auth", "whoami"])


def _link_or_copy(src: Path, dst: Path) -> None:
    if dst.exists():
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def build_stage(m: CacheManifest, qa_profile: Path, readme: Path, stage_dir: Path) -> list[Path]:
    stage_dir = Path(stage_dir)
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True)
    staged: list[Path] = []
    for entry, path in m.present_shards():
        dst = stage_dir / entry.file
        _link_or_copy(path, dst)
        staged.append(dst)
    for src, name in ((m.plugin_dir / "manifest.json", "manifest.json"), (Path(qa_profile), "qa_profile.json"), (Path(readme), "README.md")):
        dst = stage_dir / name
        _link_or_copy(src, dst)
        staged.append(dst)
    return staged


def _info(runner: Runner, repo: str, revision: str | None = None, expand_siblings: bool = False) -> dict:
    argv = ["hf", "datasets", "info", repo, "--format", "json"]
    if revision:
        argv += ["--revision", revision]
    if expand_siblings:
        argv += ["--expand", "siblings"]
    cp = _run_ok(runner, argv)
    try:
        return json.loads(cp.stdout)
    except json.JSONDecodeError as e:
        raise PublishError(f"`{' '.join(argv)}` returned non-JSON output: {cp.stdout[:200]}") from e


def publish(runner: Runner, repo: str, stage_dir: Path, tag: str, commit_message: str) -> str:
    _run_ok(runner, ["hf", "upload", repo, str(stage_dir), ".", "--type", "dataset", "--commit-message", commit_message])
    runner.run(["hf", "repos", "tag", "delete", repo, tag, "--type", "dataset", "--yes"])  # absent tag is fine
    _run_ok(runner, ["hf", "repos", "tag", "create", repo, tag, "--type", "dataset", "--message", commit_message])
    return str(_info(runner, repo)["sha"])


def verify(runner: Runner, repo: str, stage_dir: Path, tag: str) -> list[str]:
    problems: list[str] = []
    info = _info(runner, repo, expand_siblings=True)
    remote = {s["rfilename"]: s for s in info.get("siblings", [])}
    for path in sorted(Path(stage_dir).iterdir()):
        r = remote.get(path.name)
        if r is None:
            problems.append(f"{path.name}: missing on the hub")
            continue
        size = r.get("size")
        if size is not None and int(size) != path.stat().st_size:
            problems.append(f"{path.name}: size {path.stat().st_size} local vs {size} on the hub")
    head = str(info.get("sha"))
    tag_sha = str(_info(runner, repo, revision=tag).get("sha"))
    if tag_sha != head:
        problems.append(f"tag {tag} resolves to {tag_sha[:7]}, head is {head[:7]}")
    return problems
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_qa_stage.py -q`
Expected: all pass. On a filesystem where `tmp_path` and the fixture are on the same volume the inode assertion holds; if CI runs on a volume without hard links, the copy fallback makes the inode test fail — in that case assert `filecmp.cmp(src, dst, shallow=False)` instead.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check e2e-testing/scripts/cache_qa tests/test_cache_qa_stage.py && uv run ruff format e2e-testing/scripts/cache_qa tests/test_cache_qa_stage.py
git add e2e-testing/scripts/cache_qa/stage.py tests/test_cache_qa_stage.py
git commit -m "feat(cache_qa): hard-link staging and hf publish/verify via injected runner"
```

---

### Task 8: CLI, entry point, README-from-hub

**Files:**
- Create: `e2e-testing/scripts/cache_qa/cli.py`
- Create: `e2e-testing/scripts/profile_plugin_cache.py`
- Test: `tests/test_cache_qa_cli.py`

**Interfaces:**
- Consumes every module above.
- Produces `cache_qa.cli`:
  - `parse_args(argv: list[str] | None) -> argparse.Namespace` with `plugin`, `root`, `out`, `readme`, `readme_from_hub`, `publish`, `tag`, `commit_message`, `json_only`.
  - `main(argv=None, runner: Runner | None = None, now: Callable[[], str] | None = None) -> int`.
  - `fetch_readme(runner, repo: str, dest: Path) -> Path` (runs `hf download <repo> README.md --type dataset --local-dir <dir>`).
  - Exit codes 0/1/2 per the spec.

- [ ] **Step 1: Write the failing tests**

`tests/test_cache_qa_cli.py`:

```python
import json
import subprocess

import polars as pl
import pytest

from cache_qa import card, cli
from cache_qa_synthetic import SyntheticCache


class FakeRunner:
    def __init__(self, readme_text="# card\n\n## Usage\n\nu\n"):
        self.calls = []
        self.readme_text = readme_text

    def run(self, argv):
        self.calls.append(list(argv))
        if argv[:2] == ["hf", "download"]:
            local_dir = argv[argv.index("--local-dir") + 1]
            from pathlib import Path
            Path(local_dir).mkdir(parents=True, exist_ok=True)
            (Path(local_dir) / "README.md").write_text(self.readme_text)
            return subprocess.CompletedProcess(argv, 0, "", "")
        if argv[:3] == ["hf", "datasets", "info"]:
            from pathlib import Path
            stage_dir = next(p for c in self.calls if c[:2] == ["hf", "upload"] for p in [Path(c[3])])
            siblings = [{"rfilename": p.name, "size": p.stat().st_size} for p in stage_dir.iterdir()]
            return subprocess.CompletedProcess(argv, 0, json.dumps({"sha": "h", "siblings": siblings}), "")
        return subprocess.CompletedProcess(argv, 0, "", "")


def test_parse_requires_plugin_and_root():
    with pytest.raises(SystemExit):
        cli.parse_args([])
    a = cli.parse_args(["demo", "--root", "/r"])
    assert a.plugin == "demo" and a.root == "/r" and a.publish is None and a.json_only is False


def test_pass_writes_json_and_readme(tmp_path):
    cache = SyntheticCache(tmp_path)
    cache.write()
    readme = tmp_path / "README.md"
    readme.write_text("# T\n\n## Usage\n\nu\n")
    rc = cli.main(["demo", "--root", str(tmp_path), "--readme", str(readme)], now=lambda: "2026-09-05T00:00:00Z")
    assert rc == 0
    out = json.loads((cache.plugin_dir / "qa_profile.json").read_text())
    assert out["status"] == "pass" and out["generated_at"] == "2026-09-05T00:00:00Z"
    assert card.START in readme.read_text() and "## Quality profile" in readme.read_text()


def test_json_only_skips_readme(tmp_path):
    cache = SyntheticCache(tmp_path); cache.write()
    readme = tmp_path / "README.md"; readme.write_text("# T\n")
    assert cli.main(["demo", "--root", str(tmp_path), "--readme", str(readme), "--json-only"]) == 0
    assert readme.read_text() == "# T\n"


def test_failed_invariant_exits_1_and_blocks_publish(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chrX"]
    cache.rows["chrX"] = df.with_columns(pl.lit(2).cast(pl.Int8).alias("tier"))
    cache.write()
    runner = FakeRunner()
    rc = cli.main(["demo", "--root", str(tmp_path), "--readme-from-hub", "org/repo", "--publish", "org/repo", "--tag", "v1"], runner=runner)
    assert rc == 1
    assert not any(c[:2] == ["hf", "upload"] for c in runner.calls)
    assert json.loads((cache.plugin_dir / "qa_profile.json").read_text())["status"] == "fail"


def test_missing_manifest_exits_2(tmp_path):
    assert cli.main(["nope", "--root", str(tmp_path)]) == 2


def test_publish_requires_readme_source(tmp_path):
    SyntheticCache(tmp_path).write()
    assert cli.main(["demo", "--root", str(tmp_path), "--publish", "org/repo", "--tag", "v1"], runner=FakeRunner()) == 2


def test_publish_happy_path(tmp_path):
    cache = SyntheticCache(tmp_path); cache.write()
    runner = FakeRunner()
    rc = cli.main(
        ["demo", "--root", str(tmp_path), "--readme-from-hub", "org/repo", "--publish", "org/repo", "--tag", "v0.1.1", "--commit-message", "m"],
        runner=runner,
    )
    assert rc == 0
    kinds = [tuple(c[:2]) for c in runner.calls]
    assert ("hf", "download") in kinds and ("hf", "upload") in kinds
    upload = next(c for c in runner.calls if c[:2] == ["hf", "upload"])
    from pathlib import Path
    staged = sorted(p.name for p in Path(upload[3]).iterdir())
    assert staged == ["README.md", "chr1.parquet", "chr2.parquet", "chrX.parquet", "manifest.json", "qa_profile.json"]
    assert card.START in (Path(upload[3]) / "README.md").read_text()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_qa_cli.py -q`
Expected: `ImportError: cannot import name 'cli'`

- [ ] **Step 3: Implement the CLI and entry point**

`e2e-testing/scripts/cache_qa/cli.py`:

```python
"""profile_plugin_cache.py: verify, profile, render the card, optionally publish."""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

import polars as pl

from cache_qa import card, invariants, profile, report, stage
from cache_qa.manifest import ManifestError, load_manifest

EXIT_OK, EXIT_FAILED, EXIT_USAGE = 0, 1, 2


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="profile_plugin_cache.py",
        description="Check a plugin cache's invariants, profile its content, update its Hub card.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("plugin", help="plugin name, e.g. clinvar")
    p.add_argument("--root", required=True, help="plugin cache root containing plugin/<name>/")
    p.add_argument("--out", default=None, help="qa_profile.json path (default: <root>/plugin/<name>/qa_profile.json)")
    p.add_argument("--readme", default=None, help="README.md to update in place")
    p.add_argument("--readme-from-hub", default=None, metavar="REPO", help="fetch README.md from this dataset repo first")
    p.add_argument("--publish", default=None, metavar="REPO", help="upload shards, manifest, JSON and README as one commit")
    p.add_argument("--tag", default=None, help="tag to move to the new head (required with --publish)")
    p.add_argument("--commit-message", default=None)
    p.add_argument("--json-only", action="store_true", help="skip the card even if --readme is given")
    return p.parse_args(argv)


def fetch_readme(runner: stage.Runner, repo: str, dest_dir: Path) -> Path:
    cp = runner.run(["hf", "download", repo, "README.md", "--type", "dataset", "--local-dir", str(dest_dir)])
    if cp.returncode != 0:
        raise stage.PublishError(f"hf download README.md from {repo} failed: {cp.stderr.strip()}")
    path = Path(dest_dir) / "README.md"
    if not path.exists():
        raise stage.PublishError(f"hf download did not produce {path}")
    return path


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main(argv: list[str] | None = None, runner: stage.Runner | None = None, now: Callable[[], str] | None = None) -> int:
    args = parse_args(argv)
    runner = runner or stage.SubprocessRunner()
    now = now or _utc_now
    if args.publish and not (args.readme or args.readme_from_hub):
        print("error: --publish needs --readme or --readme-from-hub (the card would go stale)", file=sys.stderr)
        return EXIT_USAGE
    if args.publish and not args.tag:
        print("error: --publish needs --tag", file=sys.stderr)
        return EXIT_USAGE

    plugin_dir = Path(args.root) / "plugin" / args.plugin
    try:
        m = load_manifest(plugin_dir)
    except ManifestError as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_USAGE

    scratch = Path(tempfile.mkdtemp(prefix=f"cache_qa_{args.plugin}_"))
    readme_path: Path | None = Path(args.readme) if args.readme else None
    try:
        if args.publish:
            stage.check_hf_available(runner)
        if args.readme_from_hub:
            readme_path = fetch_readme(runner, args.readme_from_hub, scratch / "readme")
        try:
            results = invariants.run_all(m)
            prof = profile.profile_cache(m)
        except (OSError, pl.exceptions.PolarsError) as e:
            print(f"error: cannot read shards: {e}", file=sys.stderr)
            return EXIT_USAGE
        rep = report.build_report(m, results, prof, now(), report.tool_versions())
        out = Path(args.out) if args.out else plugin_dir / "qa_profile.json"
        report.write_report(rep, out)
        for r in results:
            print(f"{r.id:16s} {r.status:5s} {r.detail}")
        print(f"status={rep['status']} rows={rep['summary']['rows']:,} json={out}")

        if readme_path is not None and not args.json_only:
            readme_path.write_text(card.splice(readme_path.read_text(), card.render_section(rep)))
            print(f"card updated: {readme_path}")

        if rep["status"] == "fail":
            print("invariants failed; nothing published", file=sys.stderr)
            return EXIT_FAILED
        if args.publish:
            if readme_path is None:
                return EXIT_USAGE
            stage_dir = scratch / f"stage_{args.plugin}"
            stage.build_stage(m, out, readme_path, stage_dir)
            message = args.commit_message or f"{args.plugin}: quality profile {rep['generated_at'][:10]} ({m.cache_source_version or 'unversioned'})"
            head = stage.publish(runner, args.publish, stage_dir, args.tag, message)
            problems = stage.verify(runner, args.publish, stage_dir, args.tag)
            if problems:
                print("published but verification failed:\n  " + "\n  ".join(problems), file=sys.stderr)
                return EXIT_FAILED
            print(f"published {args.publish} @ {head[:7]}, tag {args.tag}")
        return EXIT_OK
    except stage.PublishError as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_USAGE
```

`e2e-testing/scripts/profile_plugin_cache.py`:

```python
#!/usr/bin/env python3
"""Verify a plugin cache's invariants, profile its content, update its Hub card.

See docs/superpowers/specs/2026-09-05-plugin-cache-qa-profile-design.md and
e2e-testing/README.md for usage.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cache_qa.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
```

Run `chmod +x e2e-testing/scripts/profile_plugin_cache.py`.

- [ ] **Step 4: Run the whole cache_qa suite**

Run: `uv run pytest tests/test_cache_qa_*.py -q`
Expected: all pass.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check e2e-testing/scripts/cache_qa e2e-testing/scripts/profile_plugin_cache.py tests/test_cache_qa_cli.py && uv run ruff format e2e-testing/scripts/cache_qa e2e-testing/scripts/profile_plugin_cache.py tests/test_cache_qa_cli.py
git add e2e-testing/scripts/cache_qa/cli.py e2e-testing/scripts/profile_plugin_cache.py tests/test_cache_qa_cli.py
git commit -m "feat(cache_qa): profile_plugin_cache.py CLI with publish and exit codes"
```

---

### Task 9: Run on the real caches, document, publish

**Files:**
- Modify: `docs/downloads.md` (add a "Quality profile" paragraph)
- Modify: `e2e-testing/README.md` (usage block for `profile_plugin_cache.py`)

**Interfaces:** none new; this task exercises the CLI on `~/workspace/data_vepyr/plugin_cache_v0.1.1`.

- [ ] **Step 1: Dry run on the smallest cache**

```bash
uv run python e2e-testing/scripts/profile_plugin_cache.py clinvar --root ~/workspace/data_vepyr/plugin_cache_v0.1.1 --json-only
```
Expected: nine `pass` lines (duplicates `pass`, detail `manifest assume_unique=false`), `status=pass rows=4,439,569`, JSON written next to the shards. Runtime a few seconds.

- [ ] **Step 2: Run the three larger local caches**

```bash
for p in alphamissense dbnsfp spliceai; do
  uv run python e2e-testing/scripts/profile_plugin_cache.py $p --root ~/workspace/data_vepyr/plugin_cache_v0.1.1 --json-only || echo "$p exit=$?"
done
```
Expected: `pass` or `warn` for each (SpliceAI duplicates may `warn` because the source repeats bare keys at overlapping-gene loci; the detail must say `manifest assume_unique=true`). Record wall time and peak RSS (`/usr/bin/time -l`) for the report; targets are under a minute for dbNSFP and SpliceAI. If `order` fails on any cache, stop: that is a real finding, not a tool bug, and needs an engine issue.

- [ ] **Step 3: Publish the card sections for the public caches**

```bash
for p in clinvar alphamissense spliceai; do
  uv run python e2e-testing/scripts/profile_plugin_cache.py $p \
    --root ~/workspace/data_vepyr/plugin_cache_v0.1.1 \
    --readme-from-hub biodatageeks/vepyr_116_GRCh38_plugin_$p \
    --publish biodatageeks/vepyr_116_GRCh38_plugin_$p --tag v0.1.1 \
    --commit-message "Add quality profile section and qa_profile.json"
done
```
Expected: each prints `published … tag v0.1.1`; the Hub card shows `## Quality profile` before `## Usage`; `qa_profile.json` is listed in the repo files. CADD is published separately once its full build lands (Rollout step 2 of the spec).

- [ ] **Step 4: Document**

Append to `docs/downloads.md` under the plugin caches section:

```markdown
### Quality profile

Every published plugin cache carries a `## Quality profile` section on its dataset card
and a machine-readable `qa_profile.json` next to the shards, generated by
`e2e-testing/scripts/profile_plugin_cache.py`. The section lists the structural
invariants the runtime relies on (schema, contig, row order, tier domain, manifest
counts and files, positions, allele form, duplicate probe keys) with pass/warn/fail,
a per-contig row and size table, and per-column null, empty, distinct and numeric
summaries. A failed invariant blocks publishing, so a card with the section always
describes shards that passed.
```

Add to `e2e-testing/README.md` next to the comparison usage:

```markdown
### Plugin cache QA

```bash
uv run python e2e-testing/scripts/profile_plugin_cache.py clinvar --root ~/workspace/data_vepyr/plugin_cache_v0.1.1
# with card update and publish
uv run python e2e-testing/scripts/profile_plugin_cache.py clinvar --root … \
  --readme-from-hub biodatageeks/vepyr_116_GRCh38_plugin_clinvar \
  --publish biodatageeks/vepyr_116_GRCh38_plugin_clinvar --tag v0.1.1
```

Exit 0 pass/warn, 1 an invariant failed (nothing uploaded), 2 usage or I/O error.
```

- [ ] **Step 5: Commit**

```bash
git add docs/downloads.md e2e-testing/README.md
git commit -m "docs: quality profile section and profile_plugin_cache.py usage"
```

---

## Self-review notes

- Spec coverage: inputs and dedup policy (Task 1); all nine invariants with the spec's ids, statuses and detail shapes (Tasks 2 and 3); content profile fields, exact/approx distinct rule, top values rule, numeric on parsable text (Task 4); `qa_profile.json` layout, `status` aggregation, `schema_version` (Task 5); card section text, tables, markers, insertion and idempotence (Task 6); staging by hard link, one `hf upload` commit, tag move, post-upload verification, `Runner` protocol (Task 7); CLI flags, exit codes, `--publish` refused on fail and without a README source, `hf` availability check (Task 8); rollout on the five caches, publishing three, docs (Task 9). Out of scope items are untouched.
- Type consistency: `InvariantResult(id, status, detail, per_contig)` is used identically by `report.build_report` and `card.render_section` (which reads the dict form). `Profile` field names (`contigs`, `columns`, `rows`, `warm`, `cold`, `bytes`) match `build_report`. `CacheManifest.present_shards()` is the single source of "which shards exist" for invariants, profile and staging.
- Known judgement calls recorded for the executor: `check_order` collects the projected key columns per shard in memory rather than streaming, because `shift` needs whole-column context; memory is bounded by the key columns of one shard (a few GB for CADD chr1). `check_duplicates` counts rows beyond the first per key. The card lists only match and value columns.
