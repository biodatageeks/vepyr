"""Region pushdown parity on the golden fixtures (Ensembl and merged caches).

For every predicate, the pushed-down frame must equal the whole-file frame
filtered in Polars, row order included. ``buffer_size=7`` turns the
100-variant fixture into ~15 input buffers so ranges start and end mid-buffer
and cross several seams, which is what exercises the engine's warm-up on the
merged cache.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest

from tests.cache_metadata import copy_cache_with_source_metadata

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"
MERGED_GOLDEN_DIR = TESTS_DIR / "data" / "golden_merged"
CACHE_DIR = str(GOLDEN_DIR / "cache")
INPUT_VCF = str(GOLDEN_DIR / "input.vcf.gz")
REFERENCE_FASTA = str(GOLDEN_DIR / "reference.fa")


@pytest.fixture(scope="module")
def ensembl_cache_dir(tmp_path_factory):
    if not os.path.isdir(CACHE_DIR):
        pytest.skip("Golden test cache not available")
    target = tmp_path_factory.mktemp("ensembl_cache_with_metadata")
    return str(copy_cache_with_source_metadata(CACHE_DIR, target, "ensembl", "115"))


@pytest.fixture(scope="module")
def merged_cache_dir(tmp_path_factory):
    src = MERGED_GOLDEN_DIR / "cache"
    if not src.is_dir():
        pytest.skip("Merged golden test cache not available")
    target = tmp_path_factory.mktemp("merged_cache_with_metadata")
    return str(copy_cache_with_source_metadata(str(src), target, "merged", "115"))


def _lazy(cache_dir):
    import vepyr

    return vepyr.annotate(
        INPUT_VCF,
        cache_dir,
        everything=True,
        reference_fasta=REFERENCE_FASTA,
        buffer_size=7,
    )


def _predicates(full: pl.DataFrame) -> dict[str, pl.Expr]:
    s = full["start"].to_list()
    return {
        "chrom": pl.col("chrom") == "chr1",
        "chrom_is_in": pl.col("chrom").is_in(["chr1", "chr9"]),
        "range_mid_buffer": (pl.col("chrom") == "chr1")
        & pl.col("start").is_between(s[10], s[40]),
        "range_open_end": (pl.col("chrom") == "chr1") & (pl.col("start") >= s[73]),
        "end_upper": pl.col("end") <= s[23],
        "single_position": (pl.col("chrom") == "chr1") & (pl.col("start") == s[33]),
        "two_ranges_same_contig": (
            (pl.col("chrom") == "chr1") & pl.col("start").is_between(s[5], s[12])
        )
        | ((pl.col("chrom") == "chr1") & pl.col("start").is_between(s[60], s[71])),
        "adjacent_ranges": (
            (pl.col("chrom") == "chr1") & pl.col("start").is_between(s[20], s[27])
        )
        | ((pl.col("chrom") == "chr1") & pl.col("start").is_between(s[28], s[35])),
        "with_residual": (pl.col("chrom") == "chr1")
        & (pl.col("start") >= s[15])
        & (pl.col("most_severe_consequence") != "intron_variant"),
        "unknown_contig": pl.col("chrom") == "chr2",
        "unsatisfiable": (pl.col("chrom") == "chr1") & (pl.col("start") > s[-1]),
    }


def _assert_parity(cache_dir):
    lf = _lazy(cache_dir)
    full = lf.collect()
    assert full.height == 100
    predicates = _predicates(full)
    for name, predicate in predicates.items():
        pushed = lf.filter(predicate).collect()
        reference = full.filter(predicate)
        assert pushed.equals(reference), (
            f"{name}: pushed {pushed.height} rows, reference {reference.height}"
        )
    # a non-trivial case must actually select a strict subset
    assert 0 < lf.filter(predicates["range_mid_buffer"]).collect().height < 100


def test_pushdown_parity_ensembl(ensembl_cache_dir):
    _assert_parity(ensembl_cache_dir)


def test_pushdown_parity_merged(merged_cache_dir):
    _assert_parity(merged_cache_dir)


def test_pushdown_parity_with_limit(ensembl_cache_dir):
    lf = _lazy(ensembl_cache_dir)
    full = lf.collect()
    s = full["start"].to_list()
    predicate = (pl.col("chrom") == "chr1") & (pl.col("start") >= s[30])
    assert lf.filter(predicate).head(5).collect().equals(full.filter(predicate).head(5))


def test_pushdown_parity_with_projection(merged_cache_dir):
    lf = _lazy(merged_cache_dir)
    full = lf.collect()
    s = full["start"].to_list()
    predicate = (pl.col("chrom") == "chr1") & pl.col("start").is_between(s[10], s[40])
    cols = [
        "chrom",
        "start",
        "ref",
        "alt",
        "SYMBOL",
        "HGNC_ID",
        "most_severe_consequence",
    ]
    assert (
        lf.filter(predicate)
        .select(cols)
        .collect()
        .equals(full.filter(predicate).select(cols))
    )
