"""Extraction of genomic regions from a Polars predicate (pure, no engine)."""

from __future__ import annotations

import polars as pl
import pytest

from vepyr._regions import GENOMIC_COLUMNS, extract_regions

CONTIGS = ["chr1", "chr2", "chr3", "chrX", "chrM"]


def region(chrom, start=None, end=None):
    return {"chrom": chrom, "start": start, "end": end}


def test_genomic_columns():
    assert GENOMIC_COLUMNS == frozenset({"chrom", "start", "end"})


@pytest.mark.parametrize(
    "predicate, expected",
    [
        (pl.col("chrom") == "chr2", [region("chr2")]),
        (pl.col("chrom") != "chr2", [region(c) for c in CONTIGS if c != "chr2"]),
        (pl.col("chrom").is_in(["chr3", "chr1"]), [region("chr1"), region("chr3")]),
        (pl.col("chrom").str.starts_with("chrX"), [region("chrX")]),
        (~(pl.col("chrom") == "chr1"), [region(c) for c in CONTIGS if c != "chr1"]),
        (
            (pl.col("chrom") == "chr1") | (pl.col("chrom") == "chr3"),
            [region("chr1"), region("chr3")],
        ),
        ((pl.col("chrom") == "chr1") & (pl.col("chrom") == "chr3"), []),
        (pl.col("chrom") == "1", []),
    ],
)
def test_chrom_conjuncts_are_evaluated_against_contigs(predicate, expected):
    assert extract_regions(predicate, CONTIGS) == expected


@pytest.mark.parametrize(
    "predicate, start, end",
    [
        (pl.col("start") >= 100, 100, None),
        (pl.col("start") > 100, 101, None),
        (pl.col("start") <= 200, None, 200),
        (pl.col("start") < 200, None, 199),
        (pl.col("start") == 150, 150, 150),
        (pl.lit(100) <= pl.col("start"), 100, None),
        (pl.lit(200) > pl.col("start"), None, 199),
        (pl.col("start").is_between(100, 200), 100, 200),
        (pl.col("start").is_between(100, 200, closed="left"), 100, 199),
        (pl.col("start").is_between(100, 200, closed="right"), 101, 200),
        (pl.col("start").is_between(100, 200, closed="none"), 101, 199),
        (pl.col("end") <= 200, None, 200),
        (pl.col("end") < 200, None, 199),
        (pl.col("end") == 200, None, 200),
        (pl.col("end") >= 100, None, None),
        (pl.col("end").is_between(100, 200), None, 200),
        (pl.col("start") >= pl.lit(100, dtype=pl.Int64), 100, None),
        (pl.col("start") >= pl.lit(100, dtype=pl.UInt32), 100, None),
    ],
)
def test_range_conjuncts_bound_start(predicate, start, end):
    got = extract_regions((pl.col("chrom") == "chr1") & predicate, CONTIGS)
    assert got == [region("chr1", start, end)]


def test_range_without_chrom_applies_to_every_contig():
    got = extract_regions(pl.col("start").is_between(5, 9), CONTIGS)
    assert got == [region(c, 5, 9) for c in CONTIGS]


def test_conjuncts_intersect():
    p = (
        (pl.col("chrom") == "chr1")
        & (pl.col("start") >= 100)
        & (pl.col("start") >= 150)
        & (pl.col("end") <= 900)
        & (pl.col("start") <= 500)
    )
    assert extract_regions(p, CONTIGS) == [region("chr1", 150, 500)]


def test_or_of_chrom_and_range_groups_is_a_superset():
    # Each disjunct is recognised on its own: chr1 unrestricted, every contig
    # from 6 on. The engine merges the two chr1 entries.
    p = (pl.col("chrom") == "chr1") | (pl.col("start") > 5)
    assert extract_regions(p, CONTIGS) == [region("chr1")] + [
        region(c, 6) for c in CONTIGS
    ]


def test_unsatisfiable_range_is_empty():
    p = (
        (pl.col("chrom") == "chr1")
        & (pl.col("start") >= 500)
        & (pl.col("start") <= 100)
    )
    assert extract_regions(p, CONTIGS) == []


def test_non_genomic_conjuncts_are_residual():
    p = (
        (pl.col("chrom") == "chr1")
        & (pl.col("AF") > 0.5)
        & (pl.col("SYMBOL").is_not_null())
    )
    assert extract_regions(p, CONTIGS) == [region("chr1")]


def test_top_level_or_yields_one_group_per_disjunct():
    p = (
        ((pl.col("chrom") == "chr1") & pl.col("start").is_between(10, 20))
        | ((pl.col("chrom") == "chr1") & pl.col("start").is_between(100, 200))
        | (pl.col("chrom") == "chr3")
    )
    assert extract_regions(p, CONTIGS) == [
        region("chr1", 10, 20),
        region("chr1", 100, 200),
        region("chr3"),
    ]


@pytest.mark.parametrize(
    "predicate",
    [
        pl.col("start") > 5.5,
        (pl.col("chrom") == "chr1") & (pl.col("start") > pl.col("end")),
        (pl.col("chrom") == "chr1") & ((pl.col("start") > 5) | (pl.col("start") < 3)),
        (pl.col("chrom") == "chr1") | (pl.col("AF") > 0.5),
        pl.col("chrom").rank() == 1,
        (pl.col("chrom") == "chr1") & (pl.col("start").cast(pl.Int64) > 5),
        pl.col("start") != 5,
    ],
)
def test_unrecognised_shapes_fail_open(predicate):
    assert extract_regions(predicate, CONTIGS) is None


def test_empty_contig_list_means_nothing_matches():
    assert extract_regions(pl.col("chrom") == "chr1", []) == []
