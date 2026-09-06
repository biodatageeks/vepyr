"""workers>1 on the LazyFrame path must equal workers=1 row for row, in order.

``buffer_size=7`` turns the 100-variant fixture into ~15 input buffers and
``VEP_STREAM_RUN_BUFFERS=1`` makes every buffer its own run on the Ensembl
cache, so the ordered release crosses a seam at every buffer. The merged cache
keeps its four-buffer floor (stateful warm-up), so it is also run with the
default run length.
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


@pytest.fixture(params=["ensembl", "merged"])
def cache_dir(request, ensembl_cache_dir, merged_cache_dir):
    return ensembl_cache_dir if request.param == "ensembl" else merged_cache_dir


def _lazy(cache_dir, workers, **kwargs):
    import vepyr

    return vepyr.annotate(
        INPUT_VCF,
        cache_dir,
        everything=True,
        reference_fasta=REFERENCE_FASTA,
        buffer_size=7,
        workers=workers,
        **kwargs,
    )


def _assert_same(actual: pl.DataFrame, expected: pl.DataFrame) -> None:
    assert actual.columns == expected.columns
    assert actual.height == expected.height, (actual.height, expected.height)
    assert actual.equals(expected), "frames differ (row order included)"


@pytest.mark.parametrize("workers", [2, 3])
def test_collect_equals_serial(cache_dir, workers, monkeypatch):
    monkeypatch.setenv("VEP_STREAM_RUN_BUFFERS", "1")
    serial = _lazy(cache_dir, 1).collect()
    assert serial.height == 100
    _assert_same(_lazy(cache_dir, workers).collect(), serial)


def test_collect_equals_serial_with_default_run_length(merged_cache_dir, monkeypatch):
    monkeypatch.delenv("VEP_STREAM_RUN_BUFFERS", raising=False)
    serial = _lazy(merged_cache_dir, 1).collect()
    _assert_same(_lazy(merged_cache_dir, 4).collect(), serial)


def test_csq_column_equals_serial(cache_dir, monkeypatch):
    monkeypatch.setenv("VEP_STREAM_RUN_BUFFERS", "1")
    serial = _lazy(cache_dir, 1, skip_csq=False).collect()
    parallel = _lazy(cache_dir, 3, skip_csq=False).collect()
    assert "CSQ" in parallel.columns
    _assert_same(parallel, serial)


def test_pushed_down_predicate_composes_with_workers(cache_dir, monkeypatch):
    monkeypatch.setenv("VEP_STREAM_RUN_BUFFERS", "1")
    serial = _lazy(cache_dir, 1).collect()
    starts = serial["start"].to_list()
    predicates = [
        (pl.col("chrom") == "chr1")
        & pl.col("start").is_between(starts[10], starts[40]),
        (pl.col("chrom") == "chr1") & (pl.col("start") >= starts[73]),
        (
            (pl.col("chrom") == "chr1")
            & pl.col("start").is_between(starts[5], starts[12])
        )
        | (
            (pl.col("chrom") == "chr1")
            & pl.col("start").is_between(starts[60], starts[71])
        ),
    ]
    for predicate in predicates:
        pushed = _lazy(cache_dir, 3).filter(predicate).collect()
        _assert_same(pushed, serial.filter(predicate))


def test_head_matches_serial_prefix(cache_dir, monkeypatch):
    monkeypatch.setenv("VEP_STREAM_RUN_BUFFERS", "1")
    serial = _lazy(cache_dir, 1).collect()
    for n in (1, 7, 23):
        _assert_same(_lazy(cache_dir, 3).head(n).collect(), serial.head(n))


def test_select_composes_with_workers(cache_dir, monkeypatch):
    monkeypatch.setenv("VEP_STREAM_RUN_BUFFERS", "1")
    columns = ["chrom", "start", "SYMBOL", "most_severe_consequence"]
    serial = _lazy(cache_dir, 1).select(columns).collect()
    _assert_same(_lazy(cache_dir, 2).select(columns).collect(), serial)


def test_lazyframe_is_rerunnable_at_workers_above_one(ensembl_cache_dir):
    lf = _lazy(ensembl_cache_dir, 2)
    first = lf.collect()
    second = lf.collect()
    _assert_same(second, first)
