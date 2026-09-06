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


@pytest.mark.parametrize(
    "predicate",
    [
        pl.col("chrom").is_duplicated(),
        ~pl.col("chrom").is_unique(),
        pl.col("chrom").is_first_distinct(),
        pl.col("chrom").str.len_chars() > 3,
    ],
)
def test_set_dependent_chrom_functions_fail_open(predicate):
    # These would evaluate to something else on the one-row-per-contig frame
    # than on the data rows, so they must not be pushed down.
    assert extract_regions(predicate, CONTIGS) is None


def test_elementwise_string_functions_are_evaluated():
    assert extract_regions(pl.col("chrom").str.to_uppercase() == "CHRX", CONTIGS) == [
        region("chrX")
    ]
    assert extract_regions(pl.col("chrom").is_not_null(), CONTIGS) == [
        region(c) for c in CONTIGS
    ]


@pytest.mark.parametrize(
    "predicate, expected",
    [
        ((pl.col("chrom") == "chr1") & (pl.col("start") >= 0), [region("chr1")]),
        ((pl.col("chrom") == "chr1") & (pl.col("start") > -5), [region("chr1")]),
        ((pl.col("chrom") == "chr1") & (pl.col("start") >= 1), [region("chr1")]),
        ((pl.col("chrom") == "chr1") & (pl.col("start") > 1), [region("chr1", 2)]),
        ((pl.col("chrom") == "chr1") & (pl.col("start") < 1), []),
        ((pl.col("chrom") == "chr1") & (pl.col("end") <= 0), []),
        (
            (pl.col("chrom") == "chr1") & pl.col("start").is_between(0, 5),
            [region("chr1", None, 5)],
        ),
    ],
)
def test_bounds_below_one_are_normalised(predicate, expected):
    assert extract_regions(predicate, CONTIGS) == expected


@pytest.mark.parametrize(
    "predicate, expected",
    [
        (pl.col("end") <= pl.lit(2**63, dtype=pl.UInt64), [region("chr1")]),
        (pl.col("start") <= pl.lit(2**64 - 1, dtype=pl.UInt64), [region("chr1")]),
        (pl.col("start") > pl.lit(2**63 - 1, dtype=pl.UInt64), []),
        (pl.col("start") >= pl.lit(2**63, dtype=pl.UInt64), []),
    ],
)
def test_bounds_beyond_i64_are_normalised(predicate, expected):
    assert extract_regions((pl.col("chrom") == "chr1") & predicate, CONTIGS) == expected


def test_contigs_are_fetched_lazily_and_only_for_pushable_predicates():
    calls = []

    def contigs():
        calls.append(1)
        return list(CONTIGS)

    # Unsupported shapes never ask for the contigs (an unindexed input would
    # have to scan the file to produce them).
    assert extract_regions(pl.col("start").cast(pl.Int64) > 5, contigs) is None
    assert (
        extract_regions((pl.col("chrom") == "chr1") & (pl.col("start") > 5.5), contigs)
        is None
    )
    assert extract_regions(pl.col("chrom").is_duplicated(), contigs) is None
    assert calls == []
    # A mixed Or whose later group is unsupported never asks either.
    assert (
        extract_regions(
            (pl.col("chrom") == "chr1") | (pl.col("start").cast(pl.Int64) > 5), contigs
        )
        is None
    )
    assert calls == []
    # A pushable predicate asks exactly once, even with several groups.
    p = ((pl.col("chrom") == "chr1") & (pl.col("start") > 5)) | (
        pl.col("chrom") == "chr2"
    )
    assert extract_regions(p, contigs) == [region("chr1", 6), region("chr2")]
    assert calls == [1]


@pytest.mark.parametrize(
    "predicate",
    [
        pl.col("end") >= 100,
        pl.col("end") > 100,
        pl.col("start") >= 0,
        pl.col("start") >= 1,
        pl.col("start") > 0,
        pl.col("start").is_between(1, 2**63),
        (pl.col("chrom") == "chr1") | (pl.col("end") > 5),
    ],
)
def test_groups_that_narrow_nothing_fail_open(predicate):
    calls = []

    def contigs():
        calls.append(1)
        return list(CONTIGS)

    assert extract_regions(predicate, contigs) is None
    assert calls == []


def test_unknown_contigs_fail_open():
    # No ``##contig`` lines and no index: nothing can be proven, so no pushdown
    # rather than an empty result.
    assert extract_regions(pl.col("chrom") == "chr1", []) is None
    assert extract_regions(pl.col("start") > 5, []) is None
    assert extract_regions(pl.col("chrom") == "chr1", list) is None
