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
    "stop_gained",
    "frameshift_variant",
    "splice_acceptor_variant",
    "splice_donor_variant",
    "start_lost",
    "stop_lost",
)
PANEL_TXT = PANELS / "acmg_sf_v3.2.txt"
PANEL_BED = PANELS / "acmg_sf_v3.2_chr22.bed"


def panel_genes() -> list[str]:
    return [g for g in PANEL_TXT.read_text().split() if g]


def panel_regions() -> list[tuple[str, int, int]]:
    """BED rows as 1-based closed (chrom, start, end)."""
    with PANEL_BED.open() as fh:
        rows = [
            r for r in csv.reader(fh, delimiter="\t") if r and not r[0].startswith("#")
        ]
    return [(c, int(s) + 1, int(e)) for c, s, e, *_ in rows]


# ---- per-entry helpers ----------------------------------------------------
def entry(col: str, pred: Callable[[pl.Expr], pl.Expr]) -> pl.Expr:
    """List(Int8) flag per CSQ entry; a null entry value never passes."""
    return pl.col(col).list.eval(pred(pl.element()).fill_null(False).cast(pl.Int8))


def all_of(*flags: pl.Expr) -> pl.Expr:
    return reduce(mul, flags)


def _zero_like(flag: pl.Expr) -> pl.Expr:
    """A List(Int8) of zeros, one per entry of `flag`; null stays null (shape unknown)."""
    return flag.list.eval(pl.lit(0, dtype=pl.Int8))


def any_of(*flags: pl.Expr) -> pl.Expr:
    """Sum of List(Int8) flags, or-ing per entry without a null flag list vetoing the rest.

    A flag list is null when its source column is null for the whole row (e.g. no
    MANE_SELECT at all), not when individual entries are absent (`entry()` already
    turns those into 0). Summing directly would let that null propagate through
    `+` and null out every entry, hiding a true flag on another list. Instead,
    each null flag is filled from another flag's shape, zeroed out, before summing
    -- so a null flag contributes 0s, not null, unless every flag is null (then the
    row-level "nothing to check" case correctly stays null; `any_entry` treats it
    as False). The zero template is built with `list.eval` rather than `flag * 0`:
    the latter silently upcasts a null List(Int8) to List(Int32) in Polars 1.39,
    and adding mismatched-width list flags panics the engine.
    """
    zero_from_others = [
        reduce(
            lambda a, b: a.fill_null(b),
            [_zero_like(other) for j, other in enumerate(flags) if j != i],
        )
        for i in range(len(flags))
    ]
    filled = [f.fill_null(z) for f, z in zip(flags, zero_from_others)]
    return reduce(add, filled)


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
    return (
        pl.col(col)
        .list.eval(pl.element().str.contains(f"(?i){pattern}"))
        .list.any()
        .fill_null(False)
    )


def region(regions: list[tuple[str, int, int]], pushable: bool = True) -> pl.Expr:
    start = (
        pl.col("start") if pushable else pl.col("start").cast(pl.Int64)
    )  # a cast is not pushed down
    groups = [
        (pl.col("chrom") == c) & (start >= s) & (start <= e) for c, s, e in regions
    ]
    return reduce(lambda a, b: a | b, groups)


def region_filter_vep(regions: list[tuple[str, int, int]]) -> str:
    return " or ".join(
        f"(CHROM is {c} and POS >= {s} and POS <= {e})" for c, s, e in regions
    )


# ---- reused sub-expressions ------------------------------------------------
def _impact_hm() -> pl.Expr:
    return entry("IMPACT", lambda x: x.is_in(["HIGH", "MODERATE"]))


def _canon_or_mane() -> pl.Expr:
    return any_of(
        entry("CANONICAL", lambda x: x == "YES"),
        entry("MANE_SELECT", lambda x: x.is_not_null()),
    )


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
        qid,
        "region",
        "ensembl",
        False,
        region_filter_vep(regions),
        ("chrom", "start"),
        lambda: region(regions),
        lambda: region(regions, pushable=False),
        note,
    )


QUERIES: list[Query] = [
    _r("R1", [("chr22", 20_000_000, 25_000_000)], "5 Mb window"),
    _r("R2", [("chr22", 30_000_000, 30_100_000)], "100 kb window"),
    Query(
        "R3",
        "region",
        "ensembl",
        False,
        "",
        ("chrom", "start"),
        lambda: region(panel_regions()),
        lambda: region(panel_regions(), pushable=False),
        "ACMG SF v3.2 gene loci on chr22 (NF2 only)",
    ),
    Query(
        "Q1",
        "frequency",
        "ensembl",
        False,
        "MAX_AF < 0.01 or not MAX_AF",
        ("MAX_AF",),
        lambda: af_below("MAX_AF", 0.01),
    ),
    Query(
        "Q2",
        "frequency",
        "ensembl",
        False,
        "gnomADg_AF < 0.001 or not gnomADg_AF",
        ("gnomADg_AF",),
        lambda: af_below("gnomADg_AF", 0.001),
    ),
    Query(
        "Q3",
        "known",
        "ensembl",
        False,
        "not Existing_variation",
        ("Existing_variation",),
        lambda: pl.col("Existing_variation").list.len().fill_null(0) == 0,
    ),
    Query(
        "Q4",
        "known",
        "ensembl",
        False,
        "CLIN_SIG match pathogenic",
        ("CLIN_SIG",),
        lambda: variant_list_match("CLIN_SIG", "pathogenic"),
        "case-insensitive substring: also matches likely_pathogenic and conflicting_*_pathogenicity",
    ),
    Query(
        "Q5",
        "consequence",
        "ensembl",
        False,
        "IMPACT is HIGH",
        ("IMPACT",),
        lambda: any_entry(entry("IMPACT", lambda x: x == "HIGH")),
    ),
    Query(
        "Q6",
        "consequence",
        "ensembl",
        False,
        "IMPACT in HIGH,MODERATE",
        ("IMPACT",),
        lambda: any_entry(_impact_hm()),
    ),
    Query(
        "Q7",
        "consequence",
        "ensembl",
        False,
        f"{_LOF_FV} and (CANONICAL is YES or MANE_SELECT)",
        ("Consequence", "CANONICAL", "MANE_SELECT"),
        lambda: any_entry(
            all_of(entry("Consequence", imatch("|".join(LOF_TERMS))), _canon_or_mane())
        ),
    ),
    Query(
        "Q8",
        "consequence",
        "ensembl",
        False,
        "Consequence match missense_variant and SIFT match deleterious and PolyPhen match damaging",
        ("Consequence", "SIFT", "PolyPhen"),
        lambda: any_entry(
            all_of(
                entry("Consequence", imatch("missense_variant")),
                entry("SIFT", imatch("deleterious")),
                entry("PolyPhen", imatch("damaging")),
            )
        ),
    ),
    Query(
        "Q9",
        "consequence",
        "ensembl",
        False,
        "SYMBOL in /panels/acmg_sf_v3.2.txt",
        ("SYMBOL",),
        lambda: any_entry(entry("SYMBOL", lambda x: x.is_in(panel_genes()))),
    ),
    Query(
        "Q10",
        "composite",
        "ensembl",
        False,
        _Q10_FV,
        ("IMPACT", "MAX_AF", "CANONICAL", "MANE_SELECT"),
        _q10,
    ),
    Query(
        "P1",
        "plugin",
        "merged",
        True,
        "CADD_PHRED > 20",
        ("CADD_PHRED",),
        lambda: (num(pl.col("CADD_PHRED")) > 20).fill_null(False),
    ),
    Query(
        "P2",
        "plugin",
        "merged",
        True,
        "am_class is likely_pathogenic",
        ("am_class",),
        lambda: any_entry(entry("am_class", lambda x: x == "likely_pathogenic")),
    ),
    Query(
        "P3",
        "plugin",
        "merged",
        True,
        " or ".join(f"SpliceAI_pred_DS_{k} >= 0.5" for k in _SPLICE),
        tuple(f"SpliceAI_pred_DS_{k}" for k in _SPLICE),
        # OR distributes over entries, so any-per-column is exact and
        # tolerates a column whose whole list is null.
        lambda: reduce(
            lambda a, b: a | b,
            [
                any_entry(entry(f"SpliceAI_pred_DS_{k}", lambda x: num(x) >= 0.5))
                for k in _SPLICE
            ],
        ),
    ),
    Query(
        "P4",
        "plugin",
        "merged",
        True,
        "ClinVar_CLNSIG match Pathogenic",
        ("ClinVar_CLNSIG",),
        lambda: (
            pl.col("ClinVar_CLNSIG").str.contains("(?i)Pathogenic").fill_null(False)
        ),
    ),
    Query(
        "P5",
        "plugin",
        "merged",
        True,
        f"{_Q10_FV} and (CADD_PHRED > 20 or am_class is likely_pathogenic)",
        ("IMPACT", "MAX_AF", "CANONICAL", "MANE_SELECT", "CADD_PHRED", "am_class"),
        # CADD is per variant (the same value on every entry), so
        # any(e & (cadd | am)) == (cadd & any(e)) | any(e & am).
        lambda: (
            af_below("MAX_AF", 0.01)
            & (
                (
                    (num(pl.col("CADD_PHRED")) > 20).fill_null(False)
                    & any_entry(all_of(_impact_hm(), _canon_or_mane()))
                )
                | any_entry(
                    all_of(
                        _impact_hm(),
                        _canon_or_mane(),
                        entry("am_class", lambda x: x == "likely_pathogenic"),
                    )
                )
            )
        ),
    ),
]
BY_ID = {q.id: q for q in QUERIES}


def filter_vep_expression(q: Query) -> str:
    """R3's expression depends on the committed BED, so it is built on demand."""
    return region_filter_vep(panel_regions()) if q.id == "R3" else q.filter_vep
