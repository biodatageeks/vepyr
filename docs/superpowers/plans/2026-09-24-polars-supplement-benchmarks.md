# Polars Integration Supplement Benchmarks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build reproducible chr22 benchmarks comparing vepyr + Polars (+ polars-bio) with Ensembl VEP 116 + `filter_vep` on 18 variant-prioritisation queries, and write them up as the "Polars integration" section of a LaTeX supplement to the vepyr paper.

**Architecture:** A small Python package, `pibench`, holds the query catalogue (one hand-written `filter_vep` ↔ Polars pair per query), a CSQ adapter that gives a VEP-annotated VCF the same column shapes as vepyr's LazyFrame, and a measurement harness (one subprocess per run, `os.wait4` rusage, load guard). Thin scripts drive VEP/`filter_vep` in the `release_116.0` container and vepyr in child processes, gate every query on result parity first, then time experiments A (filter only), C (pushdown ablation) and B (end to end). A generator turns the raw JSON into LaTeX tables and PDF figures for `papers/vepyr/supplementary/`.

**Tech Stack:** Python 3.12, polars 1.39, polars-bio 0.36, vepyr (this repo, master ff1caf3 or later), Docker `ensemblorg/ensembl-vep:release_116.0` (native arm64), matplotlib, pdflatex/latexmk, pytest.

**Spec:** `docs/superpowers/specs/2026-09-24-polars-supplement-benchmarks-design.md`

**Process note:** CLAUDE.md asks for GSD workflows, but `.planning/ROADMAP.md` does not exist, so `/gsd:quick` fails. As in earlier sessions, this plan bypasses GSD with the user's approval. Work happens on branch `perf/polars-supplement` in the main checkout.

## Global Constraints

- Release 116 only. Image `ensemblorg/ensembl-vep:release_116.0`. vepyr caches `~/workspace/data_vepyr/cache/116_GRCh38_{ensembl,merged}`. Plugin cache root `~/workspace/data_vepyr/plugin_cache_116`.
- Input: HG002 chr22, 50,861 records. The frame's `start` equals VCF `POS` on all 50,861 (verified 2026-09-24).
- Process-count pairing: VEP no fork ↔ vepyr `workers=1`, `--fork 1` ↔ `2`, `--fork 3` ↔ `4`, `--fork 7` ↔ `8`. Ratios only on matched pairs.
- Only queries `filter_vep` can express. No FORMAT/sample fields.
- Keep the full CSQ: `filter_vep` runs **without** `--only_matched`.
- vepyr output paths: `collect()` (`skip_csq=True`), `pb.sink_vcf` (`skip_csq=False`), `sink_parquet(row_group_size=5000)` (`skip_csq=True`).
- Correctness before timing: a query/path that fails parity is never timed.
- Timing: one subprocess per run, 1 discarded warm-up, 3 timed repeats, median reported. Refuse to start a run while the 1-minute load average is ≥ 2.0.
- Run everything with `env -u CONDA_PREFIX` (`uv run` refuses otherwise here). Rebuild vepyr first: `env -u CONDA_PREFIX uv run maturin develop --release`.
- ruff only runs through pre-commit: `uv run pre-commit run ruff --all-files`. The hook's `--fix` can strip imports. Re-check after it runs.

## Deviations from the spec (fold into the spec in Task 1)

1. **Directory** `performance-tests/polars-integration/` instead of `performance-tests/polars/`. A directory named `polars` next to scripts could shadow the `polars` package on `sys.path`.
2. **Consequence terms use `match`, not `is`/`in`.** `filter_vep` splits a multi-value field only when a digit precedes the `&` (FilterSet.pm:487, `/[0-9](\,|\&)/`), so `Consequence is missense_variant` never matches `missense_variant&splice_region_variant`. `match` is a case-insensitive regex over the whole string (FilterSet.pm `filter_re`), which is well defined on both sides.
3. **Body parity for B compares every W against serial VEP (fork 0).** VEP's own fork output can differ from serial: PR #117 found `--fork 8` drops HGNC_ID on 115 chr22 records. VEP fork-N vs fork-0 drift is reported as a separate finding.
4. **VEP peak RSS is not reported.** From the host, only the Docker client process is visible. Memory is compared for vepyr and Polars only.
5. **Panel:** ACMG SF v3.2 as listed on the NCBI ClinVar page (78 symbols, fetched 2026-09-24). Only NF2 lies on chr22, so Q9 and R3 are highly selective on chr22. The supplement says so.
6. **Experiment A Polars side writes a VCF through `pb.sink_vcf`**, like `filter_vep` does. Record-set parity is the hard gate there, and body parity is reported.

## File map

```
performance-tests/polars-integration/
  README.md                     reproduction + results summary               (Task 13)
  pibench/__init__.py           empty                                         (Task 2)
  pibench/paths.py              every path/constant in one place              (Task 2)
  pibench/queries.py            Query dataclass + the 18 queries              (Task 2)
  pibench/csq.py                VEP VCF → vepyr-shaped LazyFrame              (Task 3)
  pibench/measure.py            subprocess timing, load guard, environment    (Task 4)
  pibench/parity.py             keys/body extraction and comparison           (Task 4)
  panels/acmg_sf_v3.2.txt       78 symbols                                    (Task 5)
  panels/README.md              provenance                                    (Task 5)
  panels/make_panel_bed.py      loci from the 116 transcript cache            (Task 5)
  panels/acmg_sf_v3.2_chr22.bed committed output                              (Task 5)
  scripts/prepare_chr22.sh      input slice + FASTA hardlink                  (Task 6)
  scripts/run_vep.py            VEP --fork runs (core / plugins)              (Task 7)
  scripts/filter_vep_once.sh    one filter_vep run in the container           (Task 8)
  scripts/polars_filter_once.py one Polars filter of a VEP VCF (child)        (Task 8)
  scripts/vepyr_once.py         one vepyr annotate→filter→output (child)      (Task 8)
  scripts/verify.py             the parity gate                               (Task 9)
  scripts/run_filter_only.py    Experiment A                                  (Task 10)
  scripts/run_pushdown.py       Experiment C                                  (Task 11)
  scripts/run_e2e.py            Experiment B                                  (Task 12)
  scripts/make_supplement.py    JSON → LaTeX tables + PDF figures             (Task 13)
  tests/test_pibench_queries.py                                               (Task 2)
  tests/test_pibench_csq.py                                                   (Task 3)
  tests/test_pibench_measure.py                                               (Task 4)
  tests/test_pibench_make_supplement.py                                       (Task 13)
  tests/conftest.py             puts the package dir on sys.path              (Task 2)
  outputs/116/macos_<host>_chr22_<YYYYMMDD>/   raw JSON (committed)            (Tasks 9-12)

~/workspace/data_vepyr/polars_integration/chr22/   large, never committed
  input/HG002_chr22.vcf.gz(.tbi), input/Homo_sapiens.GRCh38.dna.primary_assembly.fa(.fai)
  vep_plugin_ref/slices/…        plugin source slices (Task 6)
  vep/{core,plugins}/fork{N}/rep{k}.vcf
  runs/…                         every other run output

papers/vepyr/supplementary/     (separate repo, Task 14)
  vepyr-supplementary.tex, supplementary.bib, tables/*.tex, figures/*.pdf
```

---

### Task 1: Fold the deviations into the spec

**Files:**
- Modify: `docs/superpowers/specs/2026-09-24-polars-supplement-benchmarks-design.md`

- [ ] **Step 1: Apply the six deviations above to the spec text.**
  - Replace every `performance-tests/polars/` with `performance-tests/polars-integration/`.
  - In the catalogue table, rewrite Q7 and Q8 with `match`, exactly as in Task 2's `QUERIES`.
  - Under Experiment B, replace "and at every W against the same-N VEP output" with "every W against serial VEP (fork 0); VEP fork-N vs fork-0 drift is reported separately".
  - Under Measurement, add "VEP peak RSS is not reported (only the Docker client is visible from the host)".
  - Under Panel, state 78 symbols from the NCBI page and that only NF2 is on chr22.
  - Under Experiment A, state the Polars side writes through `pb.sink_vcf`.

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-09-24-polars-supplement-benchmarks-design.md
git commit -m "docs: fold filter_vep semantics and fork drift into the supplement spec"
```

---

### Task 2: Query catalogue

**Files:**
- Create: `performance-tests/polars-integration/pibench/__init__.py` (empty)
- Create: `performance-tests/polars-integration/pibench/paths.py`
- Create: `performance-tests/polars-integration/pibench/queries.py`
- Create: `performance-tests/polars-integration/tests/conftest.py`
- Test: `performance-tests/polars-integration/tests/test_pibench_queries.py`

**Interfaces:**
- Produces: `pibench.paths` constants (below). `pibench.queries.Query` (fields `id: str`, `tier: str`, `cache: str` in {"ensembl","merged"}, `plugins: bool`, `filter_vep: str`, `columns: tuple[str, ...]`, `expr: Callable[[], pl.Expr]`, `expr_no_pushdown: Callable[[], pl.Expr] | None`, `note: str`), `QUERIES: list[Query]`, `BY_ID: dict[str, Query]`, `KEY_COLUMNS = ("chrom", "start", "ref", "alt")`, `VEPYR_PLUGINS: list[str]`.

- [ ] **Step 1: Write `paths.py`**

```python
"""Every path and constant the benchmark uses, overridable by environment."""

from __future__ import annotations

import os
from pathlib import Path

PKG_ROOT = Path(__file__).resolve().parents[1]  # performance-tests/polars-integration
REPO_ROOT = PKG_ROOT.parents[1]
DATA = Path(os.environ.get("DATA_VEPYR_DIR", Path.home() / "workspace/data_vepyr"))
WORK = Path(os.environ.get("PIBENCH_WORK", DATA / "polars_integration/chr22"))
INPUT_VCF = WORK / "input/HG002_chr22.vcf.gz"
FASTA = WORK / "input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
VEPYR_CACHE = {
    "ensembl": DATA / "cache/116_GRCh38_ensembl",
    "merged": DATA / "cache/116_GRCh38_merged",
}
VEP_CACHE = {
    "ensembl": DATA / "homo_sapiens_ensembl/116_GRCh38",
    "merged": DATA / "homo_sapiens_merged/116_GRCh38",
}
PLUGIN_CACHE_ROOT = DATA / "plugin_cache_116"
PLUGIN_CODE = DATA / "output/116/plugins/plugin_code"
PLUGIN_SLICES = WORK / "vep_plugin_ref/slices"
PANELS = PKG_ROOT / "panels"
VEP_IMAGE = os.environ.get("VEP_IMAGE", "ensemblorg/ensembl-vep:release_116.0")
EXPECTED_RECORDS = 50_861
FORK_TO_WORKERS = {0: 1, 1: 2, 3: 4, 7: 8}
MAX_LOAD = float(os.environ.get("PIBENCH_MAX_LOAD", "2.0"))
```

- [ ] **Step 2: Write `tests/conftest.py`**

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
```

- [ ] **Step 3: Write the failing tests**

The frames here mimic vepyr's shapes, verified on the real chr22 frame on 2026-09-24:
- `IMPACT`, `Consequence`, `SYMBOL`, `CANONICAL`, `MANE_SELECT`, `SIFT`, `PolyPhen`, `am_class`, `SpliceAI_pred_DS_*` are `List(String)` aligned per CSQ entry.
- `Existing_variation` and `CLIN_SIG` are `List(String)` of distinct values, or null.
- `MAX_AF` and `gnomADg_AF` are `Float32`.
- `CADD_PHRED` and `ClinVar_CLNSIG` are `String`.

```python
import polars as pl
import pytest

from pibench.queries import BY_ID, QUERIES, panel_genes

L = pl.List(pl.String)


def frame(**cols):
    schema = {
        "chrom": pl.String, "start": pl.UInt32, "ref": pl.String, "alt": pl.String,
        "IMPACT": L, "Consequence": L, "SYMBOL": L, "CANONICAL": L, "MANE_SELECT": L,
        "SIFT": L, "PolyPhen": L, "Existing_variation": L, "CLIN_SIG": L,
        "MAX_AF": pl.Float32, "gnomADg_AF": pl.Float32, "CADD_PHRED": pl.String,
        "am_class": L, "ClinVar_CLNSIG": pl.String,
        **{f"SpliceAI_pred_DS_{k}": L for k in ("AG", "AL", "DG", "DL")},
    }
    n = len(next(iter(cols.values())))
    defaults = {"chrom": ["chr22"] * n, "start": list(range(1, n + 1)), "ref": ["A"] * n, "alt": ["G"] * n}
    data = {c: cols.get(c, defaults.get(c, [None] * n)) for c in schema}
    return pl.DataFrame(data, schema=schema)


def kept(qid, df):
    return df.filter(BY_ID[qid].expr())["start"].to_list()


def test_catalogue_has_18_unique_ids():
    ids = [q.id for q in QUERIES]
    assert len(ids) == 18 and len(set(ids)) == 18


def test_every_query_names_the_columns_it_reads():
    for q in QUERIES:
        assert q.columns, q.id


def test_region_bounds_are_inclusive():
    df = frame(start=[19_999_999, 20_000_000, 25_000_000, 25_000_001], IMPACT=[[]] * 4)
    assert kept("R1", df) == [20_000_000, 25_000_000]


def test_region_without_pushdown_keeps_the_same_rows():
    df = frame(start=[19_999_999, 20_000_000, 25_000_001], IMPACT=[[]] * 3)
    q = BY_ID["R1"]
    assert df.filter(q.expr_no_pushdown())["start"].to_list() == [20_000_000]


def test_rare_keeps_missing_and_is_strict_at_the_threshold():
    # 0.01 parsed to Float32 must not count as < 0.01 (filter_vep compares text as double).
    df = frame(start=[1, 2, 3], MAX_AF=[None, 0.01, 0.009])
    assert kept("Q1", df) == [1, 3]


def test_novel_is_null_or_empty_existing_variation():
    df = frame(start=[1, 2, 3], Existing_variation=[None, [], ["rs1"]])
    assert kept("Q3", df) == [1, 2]


def test_clin_sig_match_is_case_insensitive_substring():
    df = frame(start=[1, 2, 3], CLIN_SIG=[["likely_pathogenic"], ["benign"], None])
    assert kept("Q4", df) == [1]


def test_lof_needs_both_conditions_on_the_same_entry():
    # Variant 1: LoF on a non-canonical entry, canonical on a different entry -> not kept.
    # Variant 2: LoF and MANE on the same entry -> kept.
    df = frame(
        start=[1, 2],
        Consequence=[["stop_gained", "intron_variant"], ["frameshift_variant&splice_region_variant"]],
        CANONICAL=[[None, "YES"], [None]],
        MANE_SELECT=[[None, None], ["NM_1.1"]],
    )
    assert kept("Q7", df) == [2]


def test_splice_donor_does_not_match_the_5th_base_term():
    df = frame(start=[1], Consequence=[["splice_donor_5th_base_variant"]],
               CANONICAL=[["YES"]], MANE_SELECT=[[None]])
    assert kept("Q7", df) == []


def test_damaging_missense_is_per_entry():
    df = frame(
        start=[1, 2],
        Consequence=[["missense_variant", "missense_variant"], ["missense_variant"]],
        SIFT=[["deleterious(0.01)", "tolerated(0.3)"], ["deleterious_low_confidence(0.02)"]],
        PolyPhen=[["benign(0.1)", "probably_damaging(0.99)"], ["possibly_damaging(0.6)"]],
    )
    assert kept("Q8", df) == [2]


def test_composite_combines_variant_and_entry_levels():
    df = frame(
        start=[1, 2, 3],
        IMPACT=[["HIGH"], ["MODERATE"], ["HIGH"]],
        CANONICAL=[["YES"], [None], ["YES"]],
        MANE_SELECT=[[None], [None], [None]],
        MAX_AF=[None, 0.001, 0.5],
    )
    assert kept("Q10", df) == [1]


def test_cadd_phred_is_numeric_on_a_string_column():
    df = frame(start=[1, 2, 3], CADD_PHRED=["25.1", "3.2", None])
    assert kept("P1", df) == [1]


def test_spliceai_any_of_four_scores():
    df = frame(
        start=[1, 2],
        SpliceAI_pred_DS_AG=[["0.00"], ["0.10"]],
        SpliceAI_pred_DS_AL=[["0.00"], ["0.00"]],
        SpliceAI_pred_DS_DG=[["0.00"], ["0.00"]],
        SpliceAI_pred_DS_DL=[["0.50"], None],
    )
    assert kept("P3", df) == [1]


def test_p5_cadd_is_variant_level_and_am_is_per_entry():
    df = frame(
        start=[1, 2, 3],
        IMPACT=[["MODERATE", "MODIFIER"], ["MODERATE", "MODIFIER"], ["MODERATE"]],
        CANONICAL=[["YES", None], ["YES", None], [None]],
        MANE_SELECT=[[None, None], [None, None], [None]],
        MAX_AF=[None, None, None],
        CADD_PHRED=["30", None, "30"],
        am_class=[[None, None], [None, "likely_pathogenic"], [None]],
    )
    # 1: CADD high + canonical HIGH/MODERATE entry -> kept.
    # 2: AlphaMissense only on the non-canonical MODIFIER entry -> not kept.
    # 3: CADD high but no canonical/MANE entry -> not kept.
    assert kept("P5", df) == [1]


def test_panel_has_78_symbols_and_includes_nf2():
    genes = panel_genes()
    assert len(genes) == 78 and "NF2" in genes
```

- [ ] **Step 4: Run to see it fail**

Run: `env -u CONDA_PREFIX uv run pytest performance-tests/polars-integration/tests/test_pibench_queries.py -q`
Expected: collection error `ModuleNotFoundError: No module named 'pibench.queries'`.

- [ ] **Step 5: Write `queries.py`**

```python
"""The 18 queries: one hand-written filter_vep <-> Polars pair each.

filter_vep semantics mirrored here (ensembl-vep FilterSet.pm, release/115.2):
- the whole expression is evaluated against ONE CSQ entry at a time, with the
  variant's fixed columns and INFO merged in; a record is kept if any entry passes;
- `match` is a case-insensitive regex (`filter_re`);
- a value is split on `&`/`,` only when a digit precedes the separator (:487),
  so consequence terms are matched with `match`, never `is`/`in`;
- empty and `-` values are undefined, so `not FIELD` is `is_null()`.

Per-entry conjunctions use element-wise list arithmetic on Int8 flags (product
= and, sum = or, truthy = > 0), which is row-local and keeps streaming.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import reduce
from operator import add, mul
from typing import Callable

import polars as pl

from pibench.paths import PANELS

KEY_COLUMNS = ("chrom", "start", "ref", "alt")
# VEP's CSQ order: plugins in --plugin order, then --custom ClinVar last.
VEPYR_PLUGINS = ["spliceai", "cadd", "alphamissense", "dbnsfp", "clinvar"]
LOF_TERMS = (
    "stop_gained", "frameshift_variant", "splice_acceptor_variant",
    "splice_donor_variant", "start_lost", "stop_lost",
)
PANEL_TXT = PANELS / "acmg_sf_v3.2.txt"
PANEL_BED = PANELS / "acmg_sf_v3.2_chr22.bed"


def panel_genes() -> list[str]:
    return [g for g in PANEL_TXT.read_text().split() if g]


def panel_regions() -> list[tuple[str, int, int]]:
    """BED rows as 1-based closed (chrom, start, end)."""
    with PANEL_BED.open() as fh:
        rows = [r for r in csv.reader(fh, delimiter="\t") if r and not r[0].startswith("#")]
    return [(c, int(s) + 1, int(e)) for c, s, e, *_ in rows]


# ---- per-entry helpers ----------------------------------------------------
def entry(col: str, pred: Callable[[pl.Expr], pl.Expr]) -> pl.Expr:
    """List(Int8) flag per CSQ entry; a null entry value never passes."""
    return pl.col(col).list.eval(pred(pl.element()).fill_null(False).cast(pl.Int8))


def all_of(*flags: pl.Expr) -> pl.Expr:
    return reduce(mul, flags)


def any_of(*flags: pl.Expr) -> pl.Expr:
    return reduce(add, flags)


def any_entry(flags: pl.Expr) -> pl.Expr:
    return flags.list.max().fill_null(0) > 0


def imatch(pattern: str) -> Callable[[pl.Expr], pl.Expr]:
    return lambda x: x.str.contains(f"(?i){pattern}")


def num(x: pl.Expr) -> pl.Expr:
    return x.cast(pl.Float64, strict=False)


def af_below(col: str, t: float) -> pl.Expr:
    # Float32 column: compare against the Float32 literal so a value written
    # "0.01" is not below 0.01, as in filter_vep's text-to-double comparison.
    return pl.col(col).is_null() | (pl.col(col) < pl.lit(t, dtype=pl.Float32))


def variant_list_match(col: str, pattern: str) -> pl.Expr:
    return pl.col(col).list.eval(pl.element().str.contains(f"(?i){pattern}")).list.any().fill_null(False)


def region(regions: list[tuple[str, int, int]], pushable: bool = True) -> pl.Expr:
    start = pl.col("start") if pushable else pl.col("start").cast(pl.Int64)  # a cast is not pushed down
    groups = [(pl.col("chrom") == c) & (start >= s) & (start <= e) for c, s, e in regions]
    return reduce(lambda a, b: a | b, groups)


def region_filter_vep(regions: list[tuple[str, int, int]]) -> str:
    return " or ".join(f"(CHROM is {c} and POS >= {s} and POS <= {e})" for c, s, e in regions)


# ---- reused sub-expressions ------------------------------------------------
def _impact_hm() -> pl.Expr:
    return entry("IMPACT", lambda x: x.is_in(["HIGH", "MODERATE"]))


def _canon_or_mane() -> pl.Expr:
    return any_of(entry("CANONICAL", lambda x: x == "YES"), entry("MANE_SELECT", lambda x: x.is_not_null()))


def _q10() -> pl.Expr:
    return af_below("MAX_AF", 0.01) & any_entry(all_of(_impact_hm(), _canon_or_mane()))


_Q10_FV = "IMPACT in HIGH,MODERATE and (MAX_AF < 0.01 or not MAX_AF) and (CANONICAL is YES or MANE_SELECT)"
_LOF_FV = "(" + " or ".join(f"Consequence match {t}" for t in LOF_TERMS) + ")"
_SPLICE = ("AG", "AL", "DG", "DL")


@dataclass(frozen=True)
class Query:
    id: str
    tier: str
    cache: str
    plugins: bool
    filter_vep: str
    columns: tuple[str, ...]
    expr: Callable[[], pl.Expr]
    expr_no_pushdown: Callable[[], pl.Expr] | None = None
    note: str = ""


def _r(qid, regions, note):
    return Query(
        qid, "region", "ensembl", False, region_filter_vep(regions), ("chrom", "start"),
        lambda: region(regions), lambda: region(regions, pushable=False), note,
    )


QUERIES: list[Query] = [
    _r("R1", [("chr22", 20_000_000, 25_000_000)], "5 Mb window"),
    _r("R2", [("chr22", 30_000_000, 30_100_000)], "100 kb window"),
    Query("R3", "region", "ensembl", False, "", ("chrom", "start"),
          lambda: region(panel_regions()), lambda: region(panel_regions(), pushable=False),
          "ACMG SF v3.2 gene loci on chr22 (NF2 only)"),
    Query("Q1", "frequency", "ensembl", False, "MAX_AF < 0.01 or not MAX_AF",
          ("MAX_AF",), lambda: af_below("MAX_AF", 0.01)),
    Query("Q2", "frequency", "ensembl", False, "gnomADg_AF < 0.001 or not gnomADg_AF",
          ("gnomADg_AF",), lambda: af_below("gnomADg_AF", 0.001)),
    Query("Q3", "known", "ensembl", False, "not Existing_variation",
          ("Existing_variation",), lambda: pl.col("Existing_variation").list.len().fill_null(0) == 0),
    Query("Q4", "known", "ensembl", False, "CLIN_SIG match pathogenic",
          ("CLIN_SIG",), lambda: variant_list_match("CLIN_SIG", "pathogenic"),
          "case-insensitive substring: also matches likely_pathogenic and conflicting_*_pathogenicity"),
    Query("Q5", "consequence", "ensembl", False, "IMPACT is HIGH",
          ("IMPACT",), lambda: any_entry(entry("IMPACT", lambda x: x == "HIGH"))),
    Query("Q6", "consequence", "ensembl", False, "IMPACT in HIGH,MODERATE",
          ("IMPACT",), lambda: any_entry(_impact_hm())),
    Query("Q7", "consequence", "ensembl", False, f"{_LOF_FV} and (CANONICAL is YES or MANE_SELECT)",
          ("Consequence", "CANONICAL", "MANE_SELECT"),
          lambda: any_entry(all_of(entry("Consequence", imatch("|".join(LOF_TERMS))), _canon_or_mane()))),
    Query("Q8", "consequence", "ensembl", False,
          "Consequence match missense_variant and SIFT match deleterious and PolyPhen match damaging",
          ("Consequence", "SIFT", "PolyPhen"),
          lambda: any_entry(all_of(entry("Consequence", imatch("missense_variant")),
                                   entry("SIFT", imatch("deleterious")),
                                   entry("PolyPhen", imatch("damaging"))))),
    Query("Q9", "consequence", "ensembl", False, "SYMBOL in /panels/acmg_sf_v3.2.txt",
          ("SYMBOL",), lambda: any_entry(entry("SYMBOL", lambda x: x.is_in(panel_genes())))),
    Query("Q10", "composite", "ensembl", False, _Q10_FV,
          ("IMPACT", "MAX_AF", "CANONICAL", "MANE_SELECT"), _q10),
    Query("P1", "plugin", "merged", True, "CADD_PHRED > 20",
          ("CADD_PHRED",), lambda: (num(pl.col("CADD_PHRED")) > 20).fill_null(False)),
    Query("P2", "plugin", "merged", True, "am_class is likely_pathogenic",
          ("am_class",), lambda: any_entry(entry("am_class", lambda x: x == "likely_pathogenic"))),
    Query("P3", "plugin", "merged", True,
          " or ".join(f"SpliceAI_pred_DS_{k} >= 0.5" for k in _SPLICE),
          tuple(f"SpliceAI_pred_DS_{k}" for k in _SPLICE),
          # OR distributes over entries, so any-per-column is exact and
          # tolerates a column whose whole list is null.
          lambda: reduce(lambda a, b: a | b, [
              any_entry(entry(f"SpliceAI_pred_DS_{k}", lambda x: num(x) >= 0.5)) for k in _SPLICE])),
    Query("P4", "plugin", "merged", True, "ClinVar_CLNSIG match Pathogenic",
          ("ClinVar_CLNSIG",), lambda: pl.col("ClinVar_CLNSIG").str.contains("(?i)Pathogenic").fill_null(False)),
    Query("P5", "plugin", "merged", True,
          f"{_Q10_FV} and (CADD_PHRED > 20 or am_class is likely_pathogenic)",
          ("IMPACT", "MAX_AF", "CANONICAL", "MANE_SELECT", "CADD_PHRED", "am_class"),
          # CADD is per variant (the same value on every entry), so
          # any(e & (cadd | am)) == (cadd & any(e)) | any(e & am).
          lambda: af_below("MAX_AF", 0.01) & (
              ((num(pl.col("CADD_PHRED")) > 20).fill_null(False)
               & any_entry(all_of(_impact_hm(), _canon_or_mane())))
              | any_entry(all_of(_impact_hm(), _canon_or_mane(),
                                 entry("am_class", lambda x: x == "likely_pathogenic"))))),
]
BY_ID = {q.id: q for q in QUERIES}


def filter_vep_expression(q: Query) -> str:
    """R3's expression depends on the committed BED, so it is built on demand."""
    return region_filter_vep(panel_regions()) if q.id == "R3" else q.filter_vep
```

- [ ] **Step 6: Run the tests**

Run: `env -u CONDA_PREFIX uv run pytest performance-tests/polars-integration/tests/test_pibench_queries.py -q`
Expected: all pass except `test_panel_has_78_symbols_and_includes_nf2`, which needs Task 5's panel file. Mark it `@pytest.mark.skipif(not PANEL_TXT.exists(), reason="panel added in Task 5")` for now, then remove the marker in Task 5.

- [ ] **Step 7: Commit**

```bash
git add performance-tests/polars-integration/pibench performance-tests/polars-integration/tests
git commit -m "perf(polars-integration): filter_vep <-> Polars query catalogue"
```

---

### Task 3: CSQ adapter (VEP VCF → vepyr-shaped LazyFrame)

**Files:**
- Create: `performance-tests/polars-integration/pibench/csq.py`
- Test: `performance-tests/polars-integration/tests/test_pibench_csq.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `csq_fields(vcf: Path) -> list[str]`, `derive(fields: list[str], needed: Iterable[str]) -> dict[str, pl.Expr]` (expressions over a `CSQ: List(String)` column), `VARIANT_FLOAT`, `VARIANT_LIST`, `VARIANT_STRING` sets.

- [ ] **Step 1: Write the failing test**

```python
import gzip

import polars as pl

from pibench.csq import csq_fields, derive

HEADER = '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|CANONICAL|MAX_AF|Existing_variation|CADD_PHRED|am_class">\n'


def test_csq_fields_reads_plain_and_gzip(tmp_path):
    plain = tmp_path / "a.vcf"
    plain.write_text("##fileformat=VCFv4.2\n" + HEADER + "#CHROM\tPOS\n")
    gz = tmp_path / "a.vcf.gz"
    with gzip.open(gz, "wt") as fh:
        fh.write(plain.read_text())
    want = ["Allele", "Consequence", "IMPACT", "CANONICAL", "MAX_AF", "Existing_variation", "CADD_PHRED", "am_class"]
    assert csq_fields(plain) == want == csq_fields(gz)


def test_derive_gives_vepyr_shapes():
    fields = ["Allele", "Consequence", "IMPACT", "CANONICAL", "MAX_AF", "Existing_variation", "CADD_PHRED", "am_class"]
    df = pl.DataFrame({"CSQ": [[
        "T|missense_variant|MODERATE|YES|0.001|rs1&COSV2|25.1|likely_pathogenic",
        "T|upstream_gene_variant|MODIFIER||0.001|rs1&COSV2|25.1|",
    ], ["-|intergenic_variant|MODIFIER|||-||"]]})
    exprs = derive(fields, ["IMPACT", "CANONICAL", "MAX_AF", "Existing_variation", "CADD_PHRED", "am_class"])
    out = df.select(**exprs)
    assert out.schema["IMPACT"] == pl.List(pl.String)
    assert out.schema["MAX_AF"] == pl.Float32
    assert out.schema["Existing_variation"] == pl.List(pl.String)
    assert out.schema["CADD_PHRED"] == pl.String
    r0, r1 = out.to_dicts()
    assert r0["IMPACT"] == ["MODERATE", "MODIFIER"]
    assert r0["CANONICAL"] == ["YES", None]          # '' -> null
    assert r0["am_class"] == ["likely_pathogenic", None]
    assert r0["Existing_variation"] == ["rs1", "COSV2"]
    assert abs(r0["MAX_AF"] - 0.001) < 1e-9 and r0["CADD_PHRED"] == "25.1"
    assert r1["MAX_AF"] is None and r1["Existing_variation"] is None  # '-' -> null
```

- [ ] **Step 2: Run to see it fail**

Run: `env -u CONDA_PREFIX uv run pytest performance-tests/polars-integration/tests/test_pibench_csq.py -q`
Expected: `ModuleNotFoundError: No module named 'pibench.csq'`.

- [ ] **Step 3: Write `csq.py`**

```python
"""Give a VEP-annotated VCF the column shapes of vepyr's LazyFrame.

Only the fields a query reads are derived, and the parse is inside the timed
region, since filter_vep parses CSQ too. Empty and '-' become null, as
filter_vep's parse_line does. Variant-level fields are taken from the first
entry that carries them (co-located and per-variant plugin values repeat on
every entry).
"""

from __future__ import annotations

import gzip
import re
from pathlib import Path
from typing import Iterable

import polars as pl

VARIANT_FLOAT = {"MAX_AF", "gnomADg_AF", "gnomADe_AF", "AF"}
VARIANT_LIST = {"Existing_variation", "CLIN_SIG"}
VARIANT_STRING = {"CADD_PHRED", "ClinVar_CLNSIG"}
_FORMAT = re.compile(r'##INFO=<ID=CSQ,.*Format: ([^"]+)"')


def csq_fields(vcf: Path) -> list[str]:
    opener = gzip.open if str(vcf).endswith((".gz", ".bgz")) else open
    with opener(vcf, "rt") as fh:
        for line in fh:
            if not line.startswith("#"):
                break
            m = _FORMAT.match(line)
            if m:
                return m.group(1).split("|")
    raise ValueError(f"no CSQ Format in {vcf}")


def _clean(x: pl.Expr) -> pl.Expr:
    return pl.when(x.is_in(["", "-"])).then(None).otherwise(x)


def derive(fields: list[str], needed: Iterable[str]) -> dict[str, pl.Expr]:
    idx = {f: i for i, f in enumerate(fields)}
    out: dict[str, pl.Expr] = {}
    for name in needed:
        if name in ("chrom", "start", "ref", "alt"):
            continue
        per_entry = pl.col("CSQ").list.eval(_clean(pl.element().str.split("|").list.get(idx[name], null_on_oob=True)))
        first = per_entry.list.drop_nulls().list.first()
        if name in VARIANT_FLOAT:
            out[name] = first.cast(pl.Float32, strict=False)
        elif name in VARIANT_LIST:
            out[name] = first.str.split("&")
        elif name in VARIANT_STRING:
            out[name] = first
        else:
            out[name] = per_entry
    return out
```

- [ ] **Step 4: Run the tests**

Run: `env -u CONDA_PREFIX uv run pytest performance-tests/polars-integration/tests/test_pibench_csq.py -q`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add performance-tests/polars-integration/pibench/csq.py performance-tests/polars-integration/tests/test_pibench_csq.py
git commit -m "perf(polars-integration): CSQ adapter giving VEP VCFs vepyr's column shapes"
```

---

### Task 4: Measurement harness and parity helpers

**Files:**
- Create: `performance-tests/polars-integration/pibench/measure.py`
- Create: `performance-tests/polars-integration/pibench/parity.py`
- Test: `performance-tests/polars-integration/tests/test_pibench_measure.py`

**Interfaces:**
- Produces:
  - `measure.run_once(cmd: list[str], log_prefix: Path, env: dict | None = None) -> dict` with keys `cmd, exit, wall_s, user_s, sys_s, max_rss_bytes, load_1m_before`.
  - `measure.repeat(cmd, log_dir: Path, *, warmups=1, repeats=3, max_load=MAX_LOAD, env=None) -> dict` with keys `warmups, runs, median_wall_s, min_wall_s, max_wall_s, median_rss_bytes`. It raises `RuntimeError` when any run exits non-zero.
  - `measure.summarize(runs: list[dict]) -> dict`.
  - `measure.environment() -> dict`.
  - `measure.write_json(path: Path, obj: dict) -> None`.
  - `parity.vcf_body(path) -> list[str]` (non-header lines, newline-stripped).
  - `parity.vcf_keys(path) -> list[tuple[str, int, str, str]]`.
  - `parity.parquet_keys(path) -> list[tuple]`.
  - `parity.compare(a: list, b: list) -> dict` (`equal`, `n_a`, `n_b`, `first_diff`).

- [ ] **Step 1: Write the failing tests**

```python
import sys

import pytest

from pibench import measure, parity


def test_run_once_reports_wall_rss_and_exit(tmp_path):
    r = measure.run_once([sys.executable, "-c", "x = bytearray(50_000_000)"], tmp_path / "a")
    assert r["exit"] == 0
    assert r["wall_s"] > 0
    assert r["max_rss_bytes"] > 50_000_000
    assert (tmp_path / "a.stderr.txt").exists()


def test_repeat_discards_warmups_and_takes_the_median(tmp_path):
    out = measure.repeat([sys.executable, "-c", "pass"], tmp_path, warmups=1, repeats=3, max_load=1e9)
    assert len(out["warmups"]) == 1 and len(out["runs"]) == 3
    assert out["min_wall_s"] <= out["median_wall_s"] <= out["max_wall_s"]


def test_repeat_raises_on_a_failing_command(tmp_path):
    with pytest.raises(RuntimeError):
        measure.repeat([sys.executable, "-c", "raise SystemExit(3)"], tmp_path, warmups=0, repeats=1, max_load=1e9)


def test_summarize_median_of_three():
    runs = [{"wall_s": w, "max_rss_bytes": m} for w, m in ((3.0, 30), (1.0, 10), (2.0, 20))]
    s = measure.summarize(runs)
    assert s["median_wall_s"] == 2.0 and s["median_rss_bytes"] == 20


def test_vcf_keys_and_body(tmp_path):
    p = tmp_path / "a.vcf"
    p.write_text("##x\n#CHROM\tPOS\tID\tREF\tALT\nchr22\t10\t.\tA\tG\tq\nchr22\t12\t.\tC\tT\tq\n")
    assert parity.vcf_keys(p) == [("chr22", 10, "A", "G"), ("chr22", 12, "C", "T")]
    assert parity.vcf_body(p) == ["chr22\t10\t.\tA\tG\tq", "chr22\t12\t.\tC\tT\tq"]


def test_compare_reports_the_first_difference():
    c = parity.compare([1, 2, 3], [1, 9, 3])
    assert not c["equal"] and c["first_diff"]["index"] == 1
    assert parity.compare([1], [1])["equal"]
```

- [ ] **Step 2: Run to see it fail**

Run: `env -u CONDA_PREFIX uv run pytest performance-tests/polars-integration/tests/test_pibench_measure.py -q`
Expected: `ImportError: cannot import name 'measure'`.

- [ ] **Step 3: Write `measure.py`**

```python
"""One subprocess per run, measured with wait4 so RSS belongs to that run only.

Peak RSS is the child's own. For `docker run` that is the Docker client, not
the container, so VEP memory is not reported.
"""

from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pibench.paths import MAX_LOAD, REPO_ROOT, VEP_IMAGE

RSS_UNIT = 1 if sys.platform == "darwin" else 1024  # ru_maxrss: bytes on macOS, KiB on Linux


def wait_for_quiet(max_load: float, timeout_s: float = 900, poll_s: float = 15) -> float:
    deadline = time.monotonic() + timeout_s
    while True:
        load = os.getloadavg()[0]
        if load < max_load:
            return load
        if time.monotonic() > deadline:
            raise RuntimeError(f"load average {load:.2f} stayed >= {max_load} for {timeout_s:.0f}s")
        time.sleep(poll_s)


def run_once(cmd: list[str], log_prefix: Path, env: dict | None = None, load: float | None = None) -> dict:
    log_prefix.parent.mkdir(parents=True, exist_ok=True)
    with open(f"{log_prefix}.stdout.txt", "wb") as out, open(f"{log_prefix}.stderr.txt", "wb") as err:
        t0 = time.perf_counter()
        proc = subprocess.Popen(cmd, stdout=out, stderr=err, env=env)
        _, status, ru = os.wait4(proc.pid, 0)
        wall = time.perf_counter() - t0
    proc.returncode = os.waitstatus_to_exitcode(status)
    return {
        "cmd": cmd, "exit": proc.returncode, "wall_s": wall,
        "user_s": ru.ru_utime, "sys_s": ru.ru_stime,
        "max_rss_bytes": ru.ru_maxrss * RSS_UNIT, "load_1m_before": load,
    }


def summarize(runs: list[dict]) -> dict:
    walls = [r["wall_s"] for r in runs]
    return {
        "median_wall_s": statistics.median(walls), "min_wall_s": min(walls), "max_wall_s": max(walls),
        "median_rss_bytes": statistics.median(r["max_rss_bytes"] for r in runs),
    }


def repeat(cmd: list[str], log_dir: Path, *, warmups: int = 1, repeats: int = 3,
           max_load: float = MAX_LOAD, env: dict | None = None) -> dict:
    out: dict = {"warmups": [], "runs": []}
    for kind, n in (("warmups", warmups), ("runs", repeats)):
        for i in range(n):
            load = wait_for_quiet(max_load)
            r = run_once(cmd, log_dir / f"{kind[:-1]}{i}", env=env, load=load)
            if r["exit"] != 0:
                raise RuntimeError(f"exit {r['exit']}: {' '.join(cmd)} (see {log_dir})")
            out[kind].append(r)
    out.update(summarize(out["runs"]))
    return out


def _git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO_ROOT), *args], capture_output=True, text=True).stdout.strip()


def environment() -> dict:
    import importlib.metadata as md

    def version(pkg: str) -> str | None:
        try:
            return md.version(pkg)
        except md.PackageNotFoundError:
            return None

    cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip()
    engine = next((l for l in (REPO_ROOT / "Cargo.toml").read_text().splitlines()
                   if l.startswith("datafusion-bio-function-vep")), "")
    image = subprocess.run(["docker", "image", "inspect", VEP_IMAGE, "--format", "{{.Id}}"],
                           capture_output=True, text=True).stdout.strip()
    return {
        "date_utc": datetime.now(timezone.utc).isoformat(), "host": platform.node(),
        "os": platform.platform(), "cpu": cpu, "ncpu": os.cpu_count(),
        "vepyr_commit": _git("rev-parse", "HEAD"), "vepyr_dirty": bool(_git("status", "--porcelain", "--untracked-files=no")),
        "engine_pin": engine, "python": sys.version.split()[0],
        "polars": version("polars"), "polars_bio": version("polars-bio"), "vepyr": version("vepyr"),
        "vep_image": VEP_IMAGE, "vep_image_id": image,
    }


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str) + "\n")
```

- [ ] **Step 4: Write `parity.py`**

```python
from __future__ import annotations

import gzip
from pathlib import Path

import polars as pl


def _lines(path: Path):
    opener = gzip.open if str(path).endswith((".gz", ".bgz")) else open
    with opener(path, "rt") as fh:
        for line in fh:
            if not line.startswith("#"):
                yield line.rstrip("\n")


def vcf_body(path: Path) -> list[str]:
    return list(_lines(path))


def vcf_keys(path: Path) -> list[tuple[str, int, str, str]]:
    out = []
    for line in _lines(path):
        c, p, _, r, a = line.split("\t", 5)[:5]
        out.append((c, int(p), r, a))
    return out


def parquet_keys(path: Path) -> list[tuple]:
    df = pl.read_parquet(path, columns=["chrom", "start", "ref", "alt"])
    return [(c, int(s), r, a) for c, s, r, a in df.iter_rows()]


def compare(a: list, b: list) -> dict:
    first = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), None)
    if first is None and len(a) != len(b):
        first = min(len(a), len(b))
    diff = None if first is None else {
        "index": first, "a": a[first] if first < len(a) else None, "b": b[first] if first < len(b) else None,
    }
    return {"equal": first is None, "n_a": len(a), "n_b": len(b), "first_diff": diff}
```

- [ ] **Step 5: Run the tests**

Run: `env -u CONDA_PREFIX uv run pytest performance-tests/polars-integration/tests -q`
Expected: all pass (the panel test is still skipped).

- [ ] **Step 6: Commit**

```bash
git add performance-tests/polars-integration/pibench/measure.py performance-tests/polars-integration/pibench/parity.py performance-tests/polars-integration/tests/test_pibench_measure.py
git commit -m "perf(polars-integration): per-run subprocess measurement and parity helpers"
```

---

### Task 5: Gene panel and its chr22 loci

**Files:**
- Create: `performance-tests/polars-integration/panels/acmg_sf_v3.2.txt`
- Create: `performance-tests/polars-integration/panels/README.md`
- Create: `performance-tests/polars-integration/panels/make_panel_bed.py`
- Create: `performance-tests/polars-integration/panels/acmg_sf_v3.2_chr22.bed` (generated, committed)
- Modify: `performance-tests/polars-integration/tests/test_pibench_queries.py` (drop the skip marker)

- [ ] **Step 1: Write the panel, one symbol per line, no comments** (filter_vep reads every line of an `in` file as a value)

```
ACTA2
ACTC1
ACVRL1
APC
APOB
ATP7B
BAG3
BMPR1A
BRCA1
BRCA2
BTD
CACNA1S
CALM1
CALM2
CALM3
CASQ2
DES
DSC2
DSG2
DSP
ENG
FBN1
FLNC
GAA
GLA
HFE
HNF1A
KCNH2
KCNQ1
LDLR
LMNA
MAX
MEN1
MLH1
MSH2
MSH6
MUTYH
MYBPC3
MYH11
MYH7
MYL2
MYL3
NF2
OTC
PALB2
PCSK9
PKP2
PMS2
PRKAG2
PTEN
RB1
RBM20
RET
RPE65
RYR1
RYR2
SCN5A
SDHAF2
SDHB
SDHC
SDHD
SMAD3
SMAD4
STK11
TGFBR1
TGFBR2
TMEM127
TMEM43
TNNC1
TNNI3
TNNT2
TRDN
TSC1
TSC2
TTN
TTR
VHL
WT1
```

- [ ] **Step 2: Write `panels/README.md`**

```markdown
# Gene panel

`acmg_sf_v3.2.txt`: the 78 gene symbols in the "Gene via GTR" column of
https://www.ncbi.nlm.nih.gov/clinvar/docs/acmg/ (ACMG SF v3.2), fetched
2026-09-24. One symbol per line and no comments, because `filter_vep --filter
"SYMBOL in <file>"` reads every line as a value.

`acmg_sf_v3.2_chr22.bed`: the loci of those genes on chr22, from
`make_panel_bed.py` over the release-116 Ensembl transcript cache: the
min(start)-max(end) of each gene's transcripts, 0-based half-open, `chr`-prefixed.
Only NF2 is on chr22.
```

- [ ] **Step 3: Write `make_panel_bed.py`**

```python
"""Write the chr22 loci of the panel genes from the 116 Ensembl transcript cache."""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from pibench.paths import VEPYR_CACHE  # noqa: E402


def main() -> None:
    genes = (HERE / "acmg_sf_v3.2.txt").read_text().split()
    loci = (
        pl.scan_parquet(VEPYR_CACHE["ensembl"] / "transcript/chr22.parquet")
        .filter(pl.col("gene_symbol").is_in(genes))
        .group_by("chrom", "gene_symbol")
        .agg(pl.col("start").min(), pl.col("end").max())
        .sort("start")
        .collect()
    )
    rows = [f"chr{c}\t{s - 1}\t{e}\t{g}" for c, g, s, e in loci.select("chrom", "gene_symbol", "start", "end").iter_rows()]
    (HERE / "acmg_sf_v3.2_chr22.bed").write_text("\n".join(rows) + "\n")
    print("\n".join(rows))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Generate and check the BED**

Run: `env -u CONDA_PREFIX uv run python performance-tests/polars-integration/panels/make_panel_bed.py`
Expected output, exactly: `chr22	29603519	29698598	NF2`

- [ ] **Step 5: Remove the skip marker from `test_panel_has_78_symbols_and_includes_nf2`, then add:**

```python
def test_r3_is_the_nf2_locus():
    from pibench.queries import filter_vep_expression, panel_regions
    assert panel_regions() == [("chr22", 29_603_520, 29_698_598)]
    assert filter_vep_expression(BY_ID["R3"]) == "(CHROM is chr22 and POS >= 29603520 and POS <= 29698598)"
```

Run: `env -u CONDA_PREFIX uv run pytest performance-tests/polars-integration/tests -q`
Expected: all pass, none skipped.

- [ ] **Step 6: Commit**

```bash
git add performance-tests/polars-integration/panels performance-tests/polars-integration/tests/test_pibench_queries.py
git commit -m "perf(polars-integration): ACMG SF v3.2 panel and its chr22 loci"
```

---

### Task 6: chr22 inputs and plugin source slices

**Files:**
- Create: `performance-tests/polars-integration/scripts/prepare_chr22.sh`

- [ ] **Step 1: Write `prepare_chr22.sh`**

```bash
#!/usr/bin/env bash
# Build the chr22 work directory. Everything lives under $DATA_VEPYR_DIR so the
# VEP container, which mounts only that tree, can reach it.
set -euo pipefail
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
WORK="${PIBENCH_WORK:-$DATA/polars_integration/chr22}"
SRC="$DATA/input/HG002_normalized.vcf.gz"
FASTA="$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
mkdir -p "$WORK/input"
if [[ ! -s "$WORK/input/HG002_chr22.vcf.gz.tbi" ]]; then
  bcftools view -r chr22 -Oz -o "$WORK/input/HG002_chr22.vcf.gz" "$SRC"
  tabix -f -p vcf "$WORK/input/HG002_chr22.vcf.gz"
fi
n=$(bcftools view -H "$WORK/input/HG002_chr22.vcf.gz" | wc -l | tr -d ' ')
[[ "$n" == 50861 ]] || { echo "expected 50861 chr22 records, got $n" >&2; exit 1; }
# Hard links, not symlinks: a symlink target outside the mount is invisible in the container.
for f in "$FASTA" "$FASTA.fai"; do
  [[ -e "$WORK/input/$(basename "$f")" ]] || ln "$f" "$WORK/input/$(basename "$f")"
done
echo "chr22 input ready in $WORK/input ($n records)"
```

- [ ] **Step 2: Run it and check the records match the plugin reference input**

Run:
```bash
bash performance-tests/polars-integration/scripts/prepare_chr22.sh
W=~/workspace/data_vepyr/polars_integration/chr22/input/HG002_chr22.vcf.gz
cmp <(gzcat $W | grep -v '^#' | cut -f1,2,4,5) \
    <(gzcat ~/workspace/data_vepyr/output/116/plugins/HG002_chr22_5plugins_vep116_caddfix.vcf.gz | grep -v '^#' | cut -f1,2,4,5) \
  && echo SAME
```
Expected: `chr22 input ready … (50861 records)` then `SAME`.

- [ ] **Step 3: Serve the plugin sources and cut the chr22 slices**

The builder cuts the slices and runs a serial VEP with the five plugins. The Drive folder is served over HTTP; do not download the 168 GB corpus.

```bash
rclone serve http --read-only --addr 127.0.0.1:8765 \
  --drive-root-folder-id 1ZT3g31I0LXepORF_dusy47AuXOnbRlZ_ gdrive-mw: &
RCLONE_PID=$!
DATA_VEPYR_DIR=~/workspace/data_vepyr \
VEP_PLUGIN_SOURCE_URL=http://127.0.0.1:8765/ \
VEP_PLUGIN_DIR=~/workspace/data_vepyr/output/116/plugins/plugin_code \
VEP_OUTPUT_VCF=~/workspace/data_vepyr/polars_integration/chr22/vep_plugin_ref/output/HG002_chr22_5plugins.vcf \
  bash e2e-testing/scripts/build_vep_plugin_reference.sh 22 ~/workspace/data_vepyr/polars_integration/chr22/vep_plugin_ref
kill $RCLONE_PID
ls ~/workspace/data_vepyr/polars_integration/chr22/vep_plugin_ref/slices
```
Expected: slices `clinvar_chr22.vcf.gz`, `spliceai_chr22.vcf.gz`, `cadd_snv_chr22.tsv.gz`, `cadd_indel_chr22.tsv.gz`, `alphamissense_chr22.tsv.gz`, `dbNSFP5.3.1a_grch38_chr22.gz`, each with `.tbi`. The builder's own check reports 38 plugin CSQ fields.
If the builder rejects a remote path, read `fetch_slice` callers in `build_vep_plugin_reference.sh` (lines 137-200) for the paths it expects, and compare them with `rclone lsf --drive-root-folder-id 1ZT3g31I0LXepORF_dusy47AuXOnbRlZ_ -R gdrive-mw:`. Do not edit the builder. Report the mismatch.

- [ ] **Step 4: Check the new plugin reference against the existing one**

```bash
cmp <(grep -v '^#' ~/workspace/data_vepyr/polars_integration/chr22/vep_plugin_ref/output/HG002_chr22_5plugins.vcf) \
    <(gzcat ~/workspace/data_vepyr/output/116/plugins/HG002_chr22_5plugins_vep116_caddfix.vcf.gz | grep -v '^#') && echo BODY_SAME
```
Expected: `BODY_SAME`. If they differ, stop and report the first differing line. It means the sources changed since the reference was built.

- [ ] **Step 5: Commit**

```bash
git add performance-tests/polars-integration/scripts/prepare_chr22.sh
git commit -m "perf(polars-integration): chr22 input preparation"
```

---

### Task 7: VEP fork runs

**Files:**
- Create: `performance-tests/polars-integration/scripts/run_vep.py`

**Interfaces:**
- Consumes: `pibench.paths.*`, `pibench.measure.repeat/environment/write_json`.
- Produces: annotated VCFs at `WORK/vep/{mode}/fork{N}/rep{k}.vcf`, where `rep0.vcf` from the first timed run is the canonical output. Also `outputs/116/<run_dir>/vep/{mode}_fork{N}.json`. `mode` ∈ {"core", "plugins"}. Function `vep_command(mode: str, fork: int, out: Path) -> list[str]`.

- [ ] **Step 1: Write `run_vep.py`**

```python
"""Time Ensembl VEP 116 on chr22 at --fork 0/1/3/7, core (Ensembl cache) and plugins (merged + 5 plugins).

Core flags match PR #117's macOS runs. Plugin flags match
e2e-testing/scripts/build_vep_plugin_reference.sh, whose --plugin order fixes the CSQ layout.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import environment, repeat, write_json  # noqa: E402
from pibench.paths import DATA, FASTA, INPUT_VCF, PKG_ROOT, PLUGIN_CODE, PLUGIN_SLICES, VEP_CACHE, VEP_IMAGE, WORK  # noqa: E402

DBNSFP_COLS = (
    "SIFT4G_score,SIFT4G_pred,Polyphen2_HDIV_score,Polyphen2_HVAR_score,"
    "MutationTaster_score,MutationTaster_pred,PROVEAN_score,PROVEAN_pred,VEST4_score,"
    "MetaSVM_score,MetaSVM_pred,MetaLR_score,MetaLR_pred,REVEL_score,GERP++_RS,"
    "phyloP100way_vertebrate,phastCons100way_vertebrate,CADD_raw,CADD_phred"
)


def in_data(p: Path) -> str:
    return "/data/" + str(p.resolve().relative_to(DATA.resolve()))


def vep_command(mode: str, fork: int, out: Path) -> list[str]:
    docker = ["docker", "run", "--rm", "--user", f"{os.getuid()}:{os.getgid()}", "--env", "HOME=/tmp",
              "-v", f"{DATA}:/data"]
    common = ["--offline", "--vcf", "--no_stats", "--force_overwrite", "--everything",
              "--fasta", in_data(FASTA), "--input_file", in_data(INPUT_VCF), "--output_file", in_data(out)]
    fork_args = ["--fork", str(fork)] if fork else []
    if mode == "core":
        cache = VEP_CACHE["ensembl"]
        return [*docker, "-v", f"{cache}:/opt/vep/.vep/homo_sapiens/116_GRCh38:ro", VEP_IMAGE, "vep",
                "--dir", "/opt/vep/.vep", "--cache", "--assembly", "GRCh38", "--hgvs", *common, *fork_args]
    s = in_data(PLUGIN_SLICES)
    return [*docker, "-v", f"{PLUGIN_CODE}:/plugins:ro", VEP_IMAGE, "vep",
            "--cache", "--cache_version", "116", "--dir_cache", "/data", "--merged", *common,
            "--dir_plugins", "/plugins",
            "--custom", f"{s}/clinvar_chr22.vcf.gz,ClinVar,vcf,exact,0,CLNSIG,CLNREVSTAT,CLNDN,CLNVC,CLNVI",
            "--plugin", f"SpliceAI,snv={s}/spliceai_chr22.vcf.gz,indel={s}/spliceai_chr22.vcf.gz",
            "--plugin", f"CADD,snv={s}/cadd_snv_chr22.tsv.gz,indels={s}/cadd_indel_chr22.tsv.gz",
            "--plugin", f"AlphaMissense,file={s}/alphamissense_chr22.tsv.gz",
            "--plugin", f"dbNSFP,{s}/dbNSFP5.3.1a_grch38_chr22.gz,{DBNSFP_COLS}",
            *fork_args]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path, help="outputs/116/<name> under the package")
    ap.add_argument("--modes", nargs="+", default=["core", "plugins"])
    ap.add_argument("--forks", nargs="+", type=int, default=[0, 1, 3, 7])
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--warmups", type=int, default=1)
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    write_json(run_dir / "env.json", environment())
    for mode in a.modes:
        for fork in a.forks:
            out_dir = WORK / "vep" / mode / f"fork{fork}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out = out_dir / "rep0.vcf"  # every repeat overwrites it; the last run's file is kept
            res = repeat(vep_command(mode, fork, out), run_dir / "logs" / "vep" / f"{mode}_fork{fork}",
                         warmups=a.warmups, repeats=a.repeats)
            res.update(mode=mode, fork=fork, processes=fork + 1, output=str(out))
            write_json(run_dir / "vep" / f"{mode}_fork{fork}.json", res)
            print(f"{mode} fork{fork}: median {res['median_wall_s']:.1f}s", flush=True)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-test one serial core run without repeats**

Run:
```bash
RUN=outputs/116/macos_$(hostname -s)_chr22_$(date +%Y%m%d)
env -u CONDA_PREFIX uv run python performance-tests/polars-integration/scripts/run_vep.py \
  --run-dir $RUN --modes core --forks 0 --repeats 1 --warmups 0
grep -vc '^#' ~/workspace/data_vepyr/polars_integration/chr22/vep/core/fork0/rep0.vcf
```
Expected: a median wall time printed, then `50861`.

- [ ] **Step 3: Check the serial core output against vepyr before timing anything**

```bash
env -u CONDA_PREFIX uv run python - <<'EOF'
import vepyr
from pathlib import Path
W = Path.home() / "workspace/data_vepyr/polars_integration/chr22"
vepyr.annotate(str(W/"input/HG002_chr22.vcf.gz"), str(Path.home()/"workspace/data_vepyr/cache/116_GRCh38_ensembl"),
               everything=True, reference_fasta=str(W/"input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"),
               output_vcf=str(W/"vepyr_core_check.vcf"), show_progress=False)
EOF
cmp <(grep -v '^#' ~/workspace/data_vepyr/polars_integration/chr22/vep/core/fork0/rep0.vcf) \
    <(grep -v '^#' ~/workspace/data_vepyr/polars_integration/chr22/vepyr_core_check.vcf) && echo BODY_SAME
```
Expected: `BODY_SAME`. vepyr's output_vcf matches VEP 116 byte for byte on HG002. If this fails, stop: every later parity result depends on it.

- [ ] **Step 4: Run the full VEP matrix** (core and plugins × forks 0/1/3/7, 1 warm-up + 3 timed each; expect several hours)

Run: `env -u CONDA_PREFIX uv run python performance-tests/polars-integration/scripts/run_vep.py --run-dir $RUN`
Expected: 8 JSON files under `$RUN/vep/`.

- [ ] **Step 5: Record fork drift (a finding, not a gate)**

```bash
for m in core plugins; do for n in 1 3 7; do
  printf '%s fork%s vs fork0: ' $m $n
  diff <(grep -v '^#' ~/workspace/data_vepyr/polars_integration/chr22/vep/$m/fork0/rep0.vcf) \
       <(grep -v '^#' ~/workspace/data_vepyr/polars_integration/chr22/vep/$m/fork$n/rep0.vcf) | grep -c '^<' || true
done; done | tee performance-tests/polars-integration/$RUN/vep/fork_drift.txt
```
Expected: a count of differing lines per fork. Any value is acceptable; it is reported in the supplement.

- [ ] **Step 6: Commit**

```bash
git add performance-tests/polars-integration/scripts/run_vep.py performance-tests/polars-integration/$RUN/vep performance-tests/polars-integration/$RUN/env.json
git commit -m "perf(polars-integration): VEP 116 chr22 fork runs, core and five plugins"
```

---

### Task 8: One-run children (filter_vep, Polars over VEP, vepyr)

**Files:**
- Create: `performance-tests/polars-integration/scripts/filter_vep_once.sh`
- Create: `performance-tests/polars-integration/scripts/polars_filter_once.py`
- Create: `performance-tests/polars-integration/scripts/vepyr_once.py`

**Interfaces:**
- Consumes: `pibench.queries.BY_ID/filter_vep_expression/KEY_COLUMNS/VEPYR_PLUGINS`, `pibench.csq.csq_fields/derive`, `pibench.paths`.
- Produces three CLIs used by Tasks 9-12:
  - `filter_vep_once.sh <in.vcf> <out.vcf> <expression>`
  - `polars_filter_once.py --vcf IN --query ID --out OUT.vcf`
  - `vepyr_once.py --query ID --path {collect,vcf,parquet} --workers W [--no-pushdown] [--narrow] --out PATH [--keys-out KEYS.parquet]`

- [ ] **Step 1: Write `filter_vep_once.sh`**

```bash
#!/usr/bin/env bash
# One filter_vep run in the VEP 116 container. Paths must be under $DATA_VEPYR_DIR.
# No --only_matched: the full CSQ is kept, as on the vepyr side.
set -euo pipefail
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
IMAGE="${VEP_IMAGE:-ensemblorg/ensembl-vep:release_116.0}"
PANELS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../panels" && pwd)"
in_data() { local p; p="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"; echo "/data/${p#"$(cd "$DATA" && pwd)/"}"; }
exec docker run --rm --user "$(id -u):$(id -g)" --env HOME=/tmp \
  -v "$DATA:/data" -v "$PANELS:/panels:ro" "$IMAGE" \
  filter_vep --format vcf --force_overwrite \
    --input_file "$(in_data "$1")" --output_file "$(in_data "$2")" --filter "$3"
```

- [ ] **Step 2: Write `polars_filter_once.py`**

```python
"""Experiment A, Polars side: read a VEP-annotated VCF, filter with the catalogue expression, write VCF."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import polars_bio as pb  # noqa: E402

from pibench.csq import csq_fields, derive  # noqa: E402
from pibench.queries import BY_ID  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vcf", required=True, type=Path)
    ap.add_argument("--query", required=True)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args()
    q = BY_ID[a.query]
    derived = derive(csq_fields(a.vcf), q.columns)
    lf = pb.scan_vcf(str(a.vcf), preserve_record_layout=True)
    out = lf.with_columns(**derived).filter(q.expr()).drop(list(derived))
    pb.sink_vcf(out, str(a.out))


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Write `vepyr_once.py`**

```python
"""Experiments B and C, vepyr side: annotate -> filter -> one output path, in one process."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import polars as pl  # noqa: E402
import vepyr  # noqa: E402

from pibench.paths import FASTA, INPUT_VCF, PLUGIN_CACHE_ROOT, VEPYR_CACHE  # noqa: E402
from pibench.queries import BY_ID, KEY_COLUMNS, VEPYR_PLUGINS  # noqa: E402


def build(query: str, path: str, workers: int, pushdown: bool, narrow: bool) -> pl.LazyFrame:
    q = BY_ID[query]
    lf = vepyr.annotate(
        str(INPUT_VCF), str(VEPYR_CACHE[q.cache]),
        everything=True, reference_fasta=str(FASTA), workers=workers,
        skip_csq=(path != "vcf"),
        plugin_cache_root=str(PLUGIN_CACHE_ROOT) if q.plugins else None,
        plugins=VEPYR_PLUGINS if q.plugins else None,
        show_progress=False,
    )
    expr = q.expr() if pushdown or q.expr_no_pushdown is None else q.expr_no_pushdown()
    lf = lf.filter(expr)
    if narrow:
        lf = lf.select(list(dict.fromkeys([*KEY_COLUMNS, *q.columns])))
    return lf


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True)
    ap.add_argument("--path", required=True, choices=("collect", "vcf", "parquet"))
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--no-pushdown", action="store_true")
    ap.add_argument("--narrow", action="store_true")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--keys-out", type=Path, help="collect path only: write the kept keys (verification runs)")
    a = ap.parse_args()
    if a.narrow and a.path == "vcf":
        ap.error("--narrow does not apply to the VCF path: CSQ needs every flag")
    lf = build(a.query, a.path, a.workers, not a.no_pushdown, a.narrow)
    if a.path == "collect":
        df = lf.collect()
        a.out.write_text(f"{df.height}\n")
        if a.keys_out:
            df.select(KEY_COLUMNS).write_parquet(a.keys_out)
    elif a.path == "parquet":
        lf.sink_parquet(a.out, row_group_size=5000)
    else:
        import polars_bio as pb
        pb.sink_vcf(lf, str(a.out))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Smoke-test each child on Q5 (IMPACT is HIGH)**

```bash
W=~/workspace/data_vepyr/polars_integration/chr22
S=performance-tests/polars-integration/scripts
mkdir -p $W/smoke
bash $S/filter_vep_once.sh $W/vep/core/fork0/rep0.vcf $W/smoke/fv_Q5.vcf "IMPACT is HIGH"
env -u CONDA_PREFIX uv run python $S/polars_filter_once.py --vcf $W/vep/core/fork0/rep0.vcf --query Q5 --out $W/smoke/pl_Q5.vcf
env -u CONDA_PREFIX uv run python $S/vepyr_once.py --query Q5 --path vcf --out $W/smoke/vy_Q5.vcf
for f in fv pl vy; do printf '%s ' $f; grep -vc '^#' $W/smoke/${f}_Q5.vcf; done
```
Expected: three equal, non-zero record counts.

- [ ] **Step 5: Commit**

```bash
git add performance-tests/polars-integration/scripts/filter_vep_once.sh performance-tests/polars-integration/scripts/polars_filter_once.py performance-tests/polars-integration/scripts/vepyr_once.py
git commit -m "perf(polars-integration): filter_vep, Polars and vepyr single-run children"
```

---

### Task 9: The parity gate

**Files:**
- Create: `performance-tests/polars-integration/scripts/verify.py`

**Interfaces:**
- Consumes: the three children from Task 8, `pibench.parity`, `pibench.queries`.
- Produces: `outputs/116/<run_dir>/verify.json`, mapping `{query_id: {"reference_rows": int, "checks": {name: {"equal": bool, ...}}, "pass": {"A": bool, "B_collect": bool, "B_vcf": bool, "B_parquet": bool}}}`. Tasks 10-12 skip any (query, path) whose pass flag is false.

- [ ] **Step 1: Write `verify.py`**

```python
"""Parity gate: every query must give filter_vep's records before it is timed.

Reference = filter_vep over serial VEP (fork 0). Checks per query:
  A        Polars over the same VEP VCF: record keys equal (body reported)
  B_vcf    vepyr -> pb.sink_vcf at W in {1,2,4,8}: body line-for-line equal
  B_collect / B_parquet at W=1 and W=8: record keys equal
  C        pushdown off, and narrow select, give the same keys as the reference
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import write_json  # noqa: E402
from pibench.parity import compare, parquet_keys, vcf_body, vcf_keys  # noqa: E402
from pibench.paths import PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression  # noqa: E402

S = Path(__file__).resolve().parent
PY = [sys.executable]


def sh(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--queries", nargs="*", help="default: all")
    a = ap.parse_args()
    out_dir = WORK / "verify"
    out_dir.mkdir(parents=True, exist_ok=True)
    result: dict = {}
    for q in QUERIES:
        if a.queries and q.id not in a.queries:
            continue
        mode = "plugins" if q.plugins else "core"
        vep = WORK / "vep" / mode / "fork0" / "rep0.vcf"
        ref = out_dir / f"{q.id}.filter_vep.vcf"
        sh(["bash", str(S / "filter_vep_once.sh"), str(vep), str(ref), filter_vep_expression(q)])
        ref_keys, ref_body = vcf_keys(ref), vcf_body(ref)
        checks: dict = {}

        pa = out_dir / f"{q.id}.polars.vcf"
        sh([*PY, str(S / "polars_filter_once.py"), "--vcf", str(vep), "--query", q.id, "--out", str(pa)])
        checks["A_keys"] = compare(ref_keys, vcf_keys(pa))
        checks["A_body"] = compare(ref_body, vcf_body(pa))

        for w in (1, 2, 4, 8):
            v = out_dir / f"{q.id}.vepyr.w{w}.vcf"
            sh([*PY, str(S / "vepyr_once.py"), "--query", q.id, "--path", "vcf", "--workers", str(w), "--out", str(v)])
            checks[f"B_vcf_w{w}"] = compare(ref_body, vcf_body(v))
        for w in (1, 8):
            k = out_dir / f"{q.id}.collect.w{w}.parquet"
            sh([*PY, str(S / "vepyr_once.py"), "--query", q.id, "--path", "collect", "--workers", str(w),
                "--out", str(out_dir / "rows.txt"), "--keys-out", str(k)])
            checks[f"B_collect_w{w}"] = compare(ref_keys, parquet_keys(k))
            p = out_dir / f"{q.id}.sink.w{w}.parquet"
            sh([*PY, str(S / "vepyr_once.py"), "--query", q.id, "--path", "parquet", "--workers", str(w), "--out", str(p)])
            checks[f"B_parquet_w{w}"] = compare(ref_keys, parquet_keys(p))
        for flag, name in (("--narrow", "C_narrow"), ("--no-pushdown", "C_no_pushdown")):
            p = out_dir / f"{q.id}.{name}.parquet"
            sh([*PY, str(S / "vepyr_once.py"), "--query", q.id, "--path", "parquet", flag, "--out", str(p)])
            checks[name] = compare(ref_keys, parquet_keys(p))

        ok = lambda *names: all(checks[n]["equal"] for n in names)  # noqa: E731
        result[q.id] = {
            "reference_rows": len(ref_keys), "checks": checks,
            "pass": {
                "A": ok("A_keys"),
                "B_vcf": ok(*(f"B_vcf_w{w}" for w in (1, 2, 4, 8))),
                "B_collect": ok("B_collect_w1", "B_collect_w8"),
                "B_parquet": ok("B_parquet_w1", "B_parquet_w8"),
                "C": ok("C_narrow", "C_no_pushdown"),
            },
        }
        print(q.id, len(ref_keys), result[q.id]["pass"], flush=True)
    write_json(PKG_ROOT / a.run_dir / "verify.json", result)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the gate on one query first**

Run: `env -u CONDA_PREFIX uv run python performance-tests/polars-integration/scripts/verify.py --run-dir $RUN --queries Q5`
Expected: `Q5 <n> {'A': True, 'B_vcf': True, 'B_collect': True, 'B_parquet': True, 'C': True}`

- [ ] **Step 3: Run the gate on all 18**

Run: `env -u CONDA_PREFIX uv run python performance-tests/polars-integration/scripts/verify.py --run-dir $RUN`
Expected: 18 lines. For every failing flag, read `checks.<name>.first_diff` in `verify.json`.

- [ ] **Step 4: STOP and report to the user.** List:
  - each query's `reference_rows`;
  - every failing (query, check) with its first difference;
  - `A_body` results. Body differences there are expected where polars-bio respells a value, and are reported, not gated.

  Do not "fix" a query expression to match without explaining the semantic difference it papers over. A mismatch is either a bug in a Polars expression (fix it and re-run that query) or a real behavioural difference (report it; the query is excluded from timing and named in the supplement). The user decides which.

- [ ] **Step 5: Commit**

```bash
git add performance-tests/polars-integration/scripts/verify.py performance-tests/polars-integration/$RUN/verify.json
git commit -m "perf(polars-integration): parity gate against filter_vep on chr22"
```

---

### Task 10: Experiment A (filter only)

**Files:**
- Create: `performance-tests/polars-integration/scripts/run_filter_only.py`

- [ ] **Step 1: Write `run_filter_only.py`**

```python
"""Experiment A: filter_vep vs Polars over the same serial-VEP VCF, one process each."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import repeat, write_json  # noqa: E402
from pibench.parity import vcf_keys  # noqa: E402
from pibench.paths import PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression  # noqa: E402

S = Path(__file__).resolve().parent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    verify = json.loads((run_dir / "verify.json").read_text())
    tmp = WORK / "runs" / "A"
    tmp.mkdir(parents=True, exist_ok=True)
    for q in QUERIES:
        if not verify[q.id]["pass"]["A"]:
            print(f"{q.id}: excluded (parity)"); continue
        vep = WORK / "vep" / ("plugins" if q.plugins else "core") / "fork0" / "rep0.vcf"
        tools = {
            "filter_vep": ["bash", str(S / "filter_vep_once.sh"), str(vep), str(tmp / f"{q.id}.fv.vcf"), filter_vep_expression(q)],
            "polars": [sys.executable, str(S / "polars_filter_once.py"), "--vcf", str(vep), "--query", q.id, "--out", str(tmp / f"{q.id}.pl.vcf")],
        }
        for tool, cmd in tools.items():
            res = repeat(cmd, run_dir / "logs" / "A" / f"{q.id}_{tool}", repeats=a.repeats)
            out = tmp / f"{q.id}.{'fv' if tool == 'filter_vep' else 'pl'}.vcf"
            res.update(query=q.id, tool=tool, rows=len(vcf_keys(out)))
            write_json(run_dir / "A" / f"{q.id}_{tool}.json", res)
            print(f"{q.id} {tool}: {res['median_wall_s']:.2f}s rows={res['rows']}", flush=True)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it**

Run: `env -u CONDA_PREFIX uv run python performance-tests/polars-integration/scripts/run_filter_only.py --run-dir $RUN`
Expected: two lines per passing query. Within a query, both tools report the same `rows`.

- [ ] **Step 3: Commit**

```bash
git add performance-tests/polars-integration/scripts/run_filter_only.py performance-tests/polars-integration/$RUN/A
git commit -m "perf(polars-integration): experiment A, filter_vep vs Polars on chr22"
```

---

### Task 11: Experiment C (pushdown ablation, vepyr only)

**Files:**
- Create: `performance-tests/polars-integration/scripts/run_pushdown.py`

- [ ] **Step 1: Write `run_pushdown.py`**

```python
"""Experiment C: what pushdown saves. W=1 and W=8.

Region (R1-R3): pushdown on vs off, on all three output paths.
Projection (Q*, P*): full frame vs narrow select, on collect and parquet only
(sink_vcf needs CSQ, which needs every flag).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import repeat, write_json  # noqa: E402
from pibench.paths import PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES  # noqa: E402

S = Path(__file__).resolve().parent
EXT = {"collect": "rows.txt", "vcf": "vcf", "parquet": "parquet"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--workers", nargs="+", type=int, default=[1, 8])
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    verify = json.loads((run_dir / "verify.json").read_text())
    tmp = WORK / "runs" / "C"
    tmp.mkdir(parents=True, exist_ok=True)
    for q in QUERIES:
        if not verify[q.id]["pass"]["C"]:
            print(f"{q.id}: excluded (parity)"); continue
        if q.tier == "region":
            variants = [(p, v, flags) for p in ("collect", "vcf", "parquet")
                        for v, flags in (("pushdown", []), ("no_pushdown", ["--no-pushdown"]))]
        else:
            variants = [(p, v, flags) for p in ("collect", "parquet")
                        for v, flags in (("full", []), ("narrow", ["--narrow"]))]
        for w in a.workers:
            for path, variant, flags in variants:
                if not verify[q.id]["pass"][f"B_{path}"]:
                    continue
                out = tmp / f"{q.id}.{path}.{variant}.w{w}.{EXT[path]}"
                cmd = [sys.executable, str(S / "vepyr_once.py"), "--query", q.id, "--path", path,
                       "--workers", str(w), *flags, "--out", str(out)]
                res = repeat(cmd, run_dir / "logs" / "C" / f"{q.id}_{path}_{variant}_w{w}", repeats=a.repeats)
                res.update(query=q.id, path=path, variant=variant, workers=w)
                write_json(run_dir / "C" / f"{q.id}_{path}_{variant}_w{w}.json", res)
                print(f"{q.id} {path} {variant} w{w}: {res['median_wall_s']:.2f}s", flush=True)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it**

Run: `env -u CONDA_PREFIX uv run python performance-tests/polars-integration/scripts/run_pushdown.py --run-dir $RUN`
Expected: for R1-R3, `pushdown` well below `no_pushdown`. The docs measured chr22 R1 at 0.6 s against 2.6 s for a full `collect()`, and every process run here adds import and startup time on top.

- [ ] **Step 3: Commit**

```bash
git add performance-tests/polars-integration/scripts/run_pushdown.py performance-tests/polars-integration/$RUN/C
git commit -m "perf(polars-integration): experiment C, region and projection pushdown on chr22"
```

---

### Task 12: Experiment B (end to end)

**Files:**
- Create: `performance-tests/polars-integration/scripts/run_e2e.py`

- [ ] **Step 1: Write `run_e2e.py`**

```python
"""Experiment B: VEP --fork N + filter_vep vs vepyr workers=N+1 -> filter -> output.

VEP side = median VEP annotation (Task 7 JSON, same mode and fork) + median
filter_vep on that fork's output (timed here). vepyr side = one process per
(query, path, W).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import repeat, write_json  # noqa: E402
from pibench.paths import FORK_TO_WORKERS, PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression  # noqa: E402

S = Path(__file__).resolve().parent
EXT = {"collect": "rows.txt", "vcf": "vcf", "parquet": "parquet"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    verify = json.loads((run_dir / "verify.json").read_text())
    tmp = WORK / "runs" / "B"
    tmp.mkdir(parents=True, exist_ok=True)
    for q in QUERIES:
        mode = "plugins" if q.plugins else "core"
        for fork, w in FORK_TO_WORKERS.items():
            vep = WORK / "vep" / mode / f"fork{fork}" / "rep0.vcf"
            fv = repeat(["bash", str(S / "filter_vep_once.sh"), str(vep), str(tmp / f"{q.id}.fv.fork{fork}.vcf"),
                         filter_vep_expression(q)], run_dir / "logs" / "B" / f"{q.id}_filter_vep_fork{fork}",
                        repeats=a.repeats)
            annot = json.loads((run_dir / "vep" / f"{mode}_fork{fork}.json").read_text())
            write_json(run_dir / "B" / f"{q.id}_vep_fork{fork}.json", {
                "query": q.id, "tool": "vep", "fork": fork, "processes": fork + 1,
                "annotate_median_wall_s": annot["median_wall_s"], "filter": fv,
                "median_wall_s": annot["median_wall_s"] + fv["median_wall_s"],
            })
            for path in ("collect", "vcf", "parquet"):
                if not verify[q.id]["pass"][f"B_{path}"]:
                    continue
                out = tmp / f"{q.id}.{path}.w{w}.{EXT[path]}"
                cmd = [sys.executable, str(S / "vepyr_once.py"), "--query", q.id, "--path", path,
                       "--workers", str(w), "--out", str(out)]
                res = repeat(cmd, run_dir / "logs" / "B" / f"{q.id}_{path}_w{w}", repeats=a.repeats)
                res.update(query=q.id, tool="vepyr", path=path, workers=w, processes=w)
                write_json(run_dir / "B" / f"{q.id}_{path}_w{w}.json", res)
                print(f"{q.id} {path} w{w}: {res['median_wall_s']:.2f}s", flush=True)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it**

Run: `env -u CONDA_PREFIX uv run python performance-tests/polars-integration/scripts/run_e2e.py --run-dir $RUN`
Expected: per query, 4 VEP JSONs and up to 12 vepyr JSONs.

- [ ] **Step 3: Commit**

```bash
git add performance-tests/polars-integration/scripts/run_e2e.py performance-tests/polars-integration/$RUN/B
git commit -m "perf(polars-integration): experiment B, VEP+filter_vep vs vepyr end to end on chr22"
```

---

### Task 13: Tables, figures and README

**Files:**
- Create: `performance-tests/polars-integration/scripts/make_supplement.py`
- Test: `performance-tests/polars-integration/tests/test_pibench_make_supplement.py`
- Create: `performance-tests/polars-integration/README.md`

**Interfaces:**
- Produces: `make_supplement.py --run-dir RUN --dest DIR`, which writes `DIR/tables/{equivalence,filter_only,pushdown,e2e}.tex` and `DIR/figures/e2e_scaling.pdf` (tables carry A and C; one figure is enough for the scaling). Also pure functions `latex_escape(s) -> str`, `fmt_s(x) -> str`, `equivalence_table(queries) -> str`, `filter_only_table(rows: list[dict]) -> str`, `e2e_table(rows: list[dict]) -> str`, `pushdown_table(rows: list[dict]) -> str`, `load(run_dir, sub) -> list[dict]`.

- [ ] **Step 1: Write the failing tests**

```python
import importlib.util
from pathlib import Path

from pibench.queries import QUERIES

spec = importlib.util.spec_from_file_location(
    "make_supplement", Path(__file__).resolve().parents[1] / "scripts/make_supplement.py")
ms = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ms)


def test_latex_escape_handles_filter_vep_characters():
    assert ms.latex_escape("MAX_AF < 0.01 & x #1 {a}") == r"MAX\_AF $<$ 0.01 \& x \#1 \{a\}"


def test_fmt_s_rounds_by_magnitude():
    assert ms.fmt_s(0.123) == "0.12" and ms.fmt_s(12.34) == "12.3" and ms.fmt_s(1234.5) == "1235"


def test_equivalence_table_has_one_row_per_query():
    tex = ms.equivalence_table(QUERIES)
    assert tex.count(r"\\") >= len(QUERIES) and r"\begin{tabular}" in tex


def test_filter_only_table_reports_speedup():
    rows = [
        {"query": "Q5", "tool": "filter_vep", "median_wall_s": 10.0, "median_rss_bytes": 1, "rows": 7},
        {"query": "Q5", "tool": "polars", "median_wall_s": 2.0, "median_rss_bytes": 2 * 2**30, "rows": 7},
    ]
    tex = ms.filter_only_table(rows)
    assert "Q5" in tex and "5.0" in tex and "2.00" in tex
```

- [ ] **Step 2: Run to see it fail**

Run: `env -u CONDA_PREFIX uv run pytest performance-tests/polars-integration/tests/test_pibench_make_supplement.py -q`
Expected: `FileNotFoundError` for `make_supplement.py`.

- [ ] **Step 3: Write `make_supplement.py`**

```python
"""Raw results -> LaTeX tables and PDF figures for papers/vepyr/supplementary/."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from pibench.paths import FORK_TO_WORKERS, PKG_ROOT  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression  # noqa: E402

_ESC = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#", "_": r"\_",
        "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}",
        "<": r"$<$", ">": r"$>$"}


def latex_escape(s: str) -> str:
    return "".join(_ESC.get(c, c) for c in s)


def fmt_s(x: float) -> str:
    return f"{x:.2f}" if x < 10 else f"{x:.1f}" if x < 1000 else f"{x:.0f}"


def _gib(b: float) -> str:
    return f"{b / 2**30:.2f}"


def _tabular(cols: str, header: list[str], rows: list[list[str]]) -> str:
    body = "\n".join(" & ".join(r) + r" \\" for r in rows)
    return (f"\\begin{{tabular}}{{{cols}}}\n\\toprule\n" + " & ".join(header) + " \\\\\n\\midrule\n"
            + body + "\n\\bottomrule\n\\end{tabular}\n")


def equivalence_table(queries) -> str:
    rows = [[q.id, latex_escape(q.tier), r"\texttt{" + latex_escape(filter_vep_expression(q)) + "}",
             latex_escape(q.note)] for q in queries]
    return _tabular("llp{0.55\\linewidth}p{0.2\\linewidth}", ["Id", "Tier", r"\texttt{filter\_vep}", "Note"], rows)


def filter_only_table(rows: list[dict]) -> str:
    by = defaultdict(dict)
    for r in rows:
        by[r["query"]][r["tool"]] = r
    out = []
    for qid, t in by.items():
        fv, pl_ = t.get("filter_vep"), t.get("polars")
        if not (fv and pl_):
            continue
        out.append([qid, str(fv["rows"]), fmt_s(fv["median_wall_s"]), fmt_s(pl_["median_wall_s"]),
                    f"{fv['median_wall_s'] / pl_['median_wall_s']:.1f}", _gib(pl_["median_rss_bytes"])])
    return _tabular("lrrrrr", ["Query", "Rows", r"\texttt{filter\_vep} (s)", "Polars (s)", "Speed-up", "Polars RSS (GiB)"], out)


def e2e_table(rows: list[dict]) -> str:
    vep = {(r["query"], r["processes"]): r for r in rows if r["tool"] == "vep"}
    vy = {(r["query"], r["path"], r["processes"]): r for r in rows if r["tool"] == "vepyr"}
    out = []
    for q in QUERIES:
        for fork, w in FORK_TO_WORKERS.items():
            v = vep.get((q.id, fork + 1))
            if not v:
                continue
            cells = [q.id, f"{fork + 1} / {w}", fmt_s(v["median_wall_s"])]
            for path in ("collect", "vcf", "parquet"):
                r = vy.get((q.id, path, w))
                cells.append(f"{fmt_s(r['median_wall_s'])} ({v['median_wall_s'] / r['median_wall_s']:.0f}$\\times$)" if r else "--")
            out.append(cells)
    return _tabular("llrrrr", ["Query", "Processes VEP / vepyr", "VEP + \\texttt{filter\\_vep} (s)",
                               "collect (s)", "sink\\_vcf (s)", "sink\\_parquet (s)"], out)


def pushdown_table(rows: list[dict]) -> str:
    by = defaultdict(dict)
    for r in rows:
        by[(r["query"], r["path"], r["workers"])][r["variant"]] = r
    out = []
    for (qid, path, w), v in sorted(by.items()):
        on = v.get("pushdown") or v.get("narrow")
        off = v.get("no_pushdown") or v.get("full")
        if on and off:
            kind = "region" if "pushdown" in v else "projection"
            out.append([qid, kind, latex_escape(path), str(w), fmt_s(off["median_wall_s"]), fmt_s(on["median_wall_s"]),
                        f"{off['median_wall_s'] / on['median_wall_s']:.1f}"])
    return _tabular("lllrrrr", ["Query", "Pushdown", "Output", "Workers", "Without (s)", "With (s)", "Speed-up"], out)


def load(run_dir: Path, sub: str) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted((run_dir / sub).glob("*.json"))]


def _fig_e2e(rows: list[dict], dest: Path) -> None:
    fig, ax = plt.subplots(figsize=(6, 4))
    for tool, path, style in (("vep", None, "k-o"), ("vepyr", "collect", "C0-s"), ("vepyr", "vcf", "C1-^"), ("vepyr", "parquet", "C2-d")):
        pts = defaultdict(list)
        for r in rows:
            if r["tool"] == tool and (path is None or r.get("path") == path):
                pts[r["processes"]].append(r["median_wall_s"])
        xs = sorted(pts)
        ax.plot(xs, [sorted(pts[x])[len(pts[x]) // 2] for x in xs], style,
                label="VEP + filter_vep" if tool == "vep" else f"vepyr {path}")
    ax.set(xscale="log", yscale="log", xlabel="processes", ylabel="median wall time, median over queries (s)")
    ax.legend(); fig.tight_layout(); fig.savefig(dest); plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--dest", required=True, type=Path, help="papers/vepyr/supplementary")
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    (a.dest / "tables").mkdir(parents=True, exist_ok=True)
    (a.dest / "figures").mkdir(parents=True, exist_ok=True)
    (a.dest / "tables/equivalence.tex").write_text(equivalence_table(QUERIES))
    (a.dest / "tables/filter_only.tex").write_text(filter_only_table(load(run_dir, "A")))
    (a.dest / "tables/pushdown.tex").write_text(pushdown_table(load(run_dir, "C")))
    e2e = load(run_dir, "B")
    (a.dest / "tables/e2e.tex").write_text(e2e_table(e2e))
    _fig_e2e(e2e, a.dest / "figures/e2e_scaling.pdf")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests**

Run: `env -u CONDA_PREFIX uv run pytest performance-tests/polars-integration/tests -q`
Expected: all pass.

- [ ] **Step 5: Write `README.md`.** Include:
  - what each experiment measures;
  - the pairing rule;
  - a reproduction block: `prepare_chr22.sh` → rclone + builder → `run_vep.py` → `verify.py` → `run_filter_only.py` → `run_pushdown.py` → `run_e2e.py` → `make_supplement.py`, each with the exact flags used above;
  - the six deviations from the spec;
  - the parity summary from `verify.json`, including every excluded query and why;
  - a caveat that the host is shared and the load guard is 2.0.

- [ ] **Step 6: Commit**

```bash
git add performance-tests/polars-integration/scripts/make_supplement.py performance-tests/polars-integration/tests/test_pibench_make_supplement.py performance-tests/polars-integration/README.md
git commit -m "perf(polars-integration): LaTeX tables and figures from the chr22 results"
```

---

### Task 14: The LaTeX supplement (paper repo)

**Files (repo `~/research/git/papers/vepyr`, Overleaf-synced, commit to its current branch only after the user has read the draft):**
- Create: `supplementary/vepyr-supplementary.tex`
- Create: `supplementary/supplementary.bib`
- Create: `supplementary/tables/*.tex`, `supplementary/figures/*.pdf` (generated)

- [ ] **Step 1: Generate tables and figures into the paper repo**

Run: `env -u CONDA_PREFIX uv run python performance-tests/polars-integration/scripts/make_supplement.py --run-dir $RUN --dest ~/research/git/papers/vepyr/supplementary`
Expected: four `.tex` tables and `figures/e2e_scaling.pdf`.

- [ ] **Step 2: Write `supplementary.bib`.** Include only entries missing from `../references.bib`: Polars (Vink et al., `pola-rs/polars` Zenodo) and polars-bio (Wiewiórka et al. 2025; use the key `intro_wiewiorka2025` that the main text already cites). Copy `mclarenEnsemblVariantEffect2016` from `../references.bib`.

- [ ] **Step 3: Write `vepyr-supplementary.tex`**

```latex
\documentclass[11pt]{article}
\usepackage[margin=2.2cm]{geometry}
\usepackage{booktabs,graphicx,hyperref,array}
\title{vepyr: Supplementary Material}
\date{}
\begin{document}
\maketitle
\renewcommand{\thetable}{S\arabic{table}}
\renewcommand{\thefigure}{S\arabic{figure}}

\section{Polars integration}
% One paragraph: annotate() returns a LazyFrame backed by a Polars IO plugin;
% results are collected, or streamed to VCF (polars-bio sink_vcf) or Parquet.
% Setup: HG002 chr22 (50,861 records), release 116, host/CPU from env.json,
% process pairing VEP --fork N <-> vepyr workers N+1, medians of 3 after 1 warm-up.

\subsection{Filtering}
% filter_vep semantics (per-CSQ-entry evaluation, match = case-insensitive
% regex, & splitting rule) and how the Polars expressions mirror them;
% the parity gate and its result (verify.json), naming any excluded query.
\begin{table}[h]\centering\small
\input{tables/equivalence}
\caption{The 18 queries. Each is run as the \texttt{filter\_vep} expression shown and as the equivalent Polars expression.}
\end{table}
% Experiment A paragraph + Table S2 (tables/filter_only).
% Experiment B paragraph (annotate -> region + prioritisation filter -> collect / sink_vcf / sink_parquet),
% Table S3 (tables/e2e), Figure S1 (figures/e2e_scaling.pdf), VEP fork drift (fork_drift.txt).

\subsection{Pushdowns}
% What reaches the engine: region predicates -> input regions (indexed seek),
% narrowing select -> annotation flags, head(n) -> LIMIT; why projection
% cannot help sink_vcf (CSQ needs every flag). Table S4 (tables/pushdown).

\bibliographystyle{plain}
\bibliography{../references,supplementary}
\end{document}
```

Replace every `%` placeholder comment with prose written from the actual numbers in the generated tables and `verify.json`. Do not state a number that is not in those files.

- [ ] **Step 4: Compile**

Run: `cd ~/research/git/papers/vepyr/supplementary && latexmk -pdf -interaction=nonstopmode vepyr-supplementary.tex`
Expected: `vepyr-supplementary.pdf` builds with no undefined references (`grep -c 'undefined' vepyr-supplementary.log` → 0).

- [ ] **Step 5: STOP and hand the PDF to the user for review.** Do not commit or push in the paper repo until they approve: it syncs to Overleaf. Build products (`*.aux`, `*.log`, `*.pdf`, `*.fls`, `*.fdb_latexmk`, `*.bbl`, `*.blg`) are not committed. Check `.gitignore` there first.

---

### Task 15: Open the vepyr PR

- [ ] **Step 1: Run the whole unit suite and lint**

```bash
env -u CONDA_PREFIX uv run pytest performance-tests/polars-integration/tests -q
uv run pre-commit run ruff --all-files && uv run pre-commit run ruff-format --all-files
git status --short   # re-check: the hook's --fix may have removed imports
```

- [ ] **Step 2: Push and open a draft PR.** Title: `perf: Polars integration benchmarks vs filter_vep (chr22)`. The body should include:
  - the parity summary;
  - headline medians per experiment;
  - the pairing rule;
  - the deviations;
  - the note that the chr1 decision is pending.

  End the body with the Claude Code attribution line.

  ```bash
  git push -u origin perf/polars-supplement
  gh pr create --draft --base master --title "perf: Polars integration benchmarks vs filter_vep (chr22)" --body-file <file>
  ```

- [ ] **Step 3: Report to the user:**
  - the PR link;
  - the parity table;
  - the three headline results;
  - anything excluded;
  - the decision to make: whether to repeat on chr1.
