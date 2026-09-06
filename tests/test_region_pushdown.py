"""Region predicate pushdown: wiring, warning, and fixture parity."""

from __future__ import annotations

import json
import os
import warnings
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"
MERGED_GOLDEN_DIR = TESTS_DIR / "data" / "golden_merged"
CACHE_DIR = str(GOLDEN_DIR / "cache")
INPUT_VCF = str(GOLDEN_DIR / "input.vcf.gz")
PLAIN_INPUT_VCF = str(GOLDEN_DIR / "input.vcf")
REFERENCE_FASTA = str(GOLDEN_DIR / "reference.fa")


def test_vcf_contigs_are_exact_with_and_without_an_index():
    from vepyr._core import vcf_contigs

    # Indexed input: the data-bearing contigs from the .tbi.
    assert vcf_contigs(INPUT_VCF) == ["chr1"]
    # Plain input: the header declares the whole assembly, but only the
    # contigs the records use count (found by one scan of the file).
    assert vcf_contigs(PLAIN_INPUT_VCF) == ["chr1"]


def test_vcf_contigs_missing_file_raises():
    from vepyr._core import vcf_contigs

    with pytest.raises(RuntimeError, match="Failed to open VCF"):
        vcf_contigs("/nonexistent/input.vcf")


class _FakeAnnotator:
    schema = pa.schema(
        [
            pa.field("chrom", pa.string()),
            pa.field("start", pa.uint32()),
            pa.field("end", pa.uint32()),
            pa.field("most_severe_consequence", pa.string()),
        ]
    )

    def __iter__(self):
        return iter(())


@pytest.fixture
def fake_engine(monkeypatch):
    """Capture the options every annotator creation receives."""
    import vepyr

    seen: list[dict] = []

    def fake_create(vcf_path, cache_dir, options_json, skip_csq=True, limit=None):
        seen.append(json.loads(options_json))
        return _FakeAnnotator()

    monkeypatch.setattr(vepyr, "_create_annotator", fake_create)
    monkeypatch.setattr(vepyr, "_vcf_contigs", lambda path: ["chr1", "chr2"])
    return seen


def _collect_opts(seen):
    # seen[0] is the schema probe at annotate() time; the rest are collects.
    return seen[1:]


def test_genomic_predicate_becomes_regions(fake_engine):
    import vepyr

    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    lf.filter((pl.col("chrom") == "chr1") & (pl.col("start") >= 100)).collect()
    (opts,) = _collect_opts(fake_engine)
    assert opts["regions"] == [{"chrom": "chr1", "start": 100, "end": None}]


def test_non_genomic_predicate_sends_no_regions(fake_engine):
    import vepyr

    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    lf.filter(pl.col("most_severe_consequence") == "missense_variant").collect()
    (opts,) = _collect_opts(fake_engine)
    assert "regions" not in opts


def test_unrecognised_genomic_predicate_sends_no_regions(fake_engine):
    import vepyr

    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    lf.filter((pl.col("chrom") == "chr1") & (pl.col("start") > pl.col("end"))).collect()
    (opts,) = _collect_opts(fake_engine)
    assert "regions" not in opts


def test_empty_regions_short_circuit_without_an_annotator(fake_engine):
    import vepyr

    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    df = lf.filter(pl.col("chrom") == "chr9").collect()
    assert df.height == 0
    assert _collect_opts(fake_engine) == []


def test_no_predicate_sends_no_regions_and_reads_no_contigs(monkeypatch, fake_engine):
    import vepyr

    def boom(path):
        raise AssertionError("contigs must not be read without a genomic predicate")

    monkeypatch.setattr(vepyr, "_vcf_contigs", boom)
    vepyr.annotate(INPUT_VCF, CACHE_DIR).collect()
    (opts,) = _collect_opts(fake_engine)
    assert "regions" not in opts


def test_unknown_header_contigs_disable_pushdown(monkeypatch, fake_engine):
    import vepyr

    monkeypatch.setattr(vepyr, "_vcf_contigs", lambda path: [])
    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    lf.filter(pl.col("chrom") == "chr1").collect()
    (opts,) = _collect_opts(fake_engine)
    assert "regions" not in opts


@pytest.mark.parametrize(
    "residual",
    [
        pl.col("most_severe_consequence").is_duplicated(),
        pl.col("most_severe_consequence").rank() > 1,
        pl.col("start").cum_count() > 1,
    ],
)
def test_set_dependent_residuals_never_reach_the_plugin(
    monkeypatch, fake_engine, residual
):
    """Region pushdown relies on Polars applying the pushed predicate to the
    narrowed rows, which is only sound for elementwise expressions. Polars
    keeps set-dependent predicates on its own side of the IO plugin; this
    pins that assumption so a change in its pushdown rules shows up here."""
    import vepyr

    handed_over = []
    original = vepyr.extract_regions
    monkeypatch.setattr(
        vepyr,
        "extract_regions",
        lambda predicate, contigs: (
            handed_over.append(predicate) or original(predicate, contigs)
        ),
    )
    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    lf.filter((pl.col("start") >= 100) & residual).collect()
    assert handed_over == [], "a set-dependent predicate reached the IO plugin"
    (opts,) = _collect_opts(fake_engine)
    assert "regions" not in opts


def test_unindexed_input_warns_once_per_annotate_call(fake_engine):
    import vepyr

    lf = vepyr.annotate(PLAIN_INPUT_VCF, CACHE_DIR)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        lf.filter(pl.col("chrom") == "chr1").collect()
        lf.filter(pl.col("chrom") == "chr1").collect()
    hits = [w for w in caught if "tabix/CSI index" in str(w.message)]
    assert len(hits) == 1
    assert issubclass(hits[0].category, RuntimeWarning)


def test_indexed_input_does_not_warn(fake_engine):
    import vepyr

    assert os.path.exists(INPUT_VCF + ".tbi")
    lf = vepyr.annotate(INPUT_VCF, CACHE_DIR)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        lf.filter(pl.col("chrom") == "chr1").collect()


def test_non_genomic_predicate_never_warns_even_without_index(fake_engine):
    import vepyr

    lf = vepyr.annotate(PLAIN_INPUT_VCF, CACHE_DIR)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        lf.filter(pl.col("most_severe_consequence") == "x").collect()
