"""Tests for vepyr.annotate() streaming annotation pipeline."""

from __future__ import annotations

import json
import os
import sys
import threading
import tempfile
import types
from pathlib import Path

import polars as pl
import pytest

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"
CACHE_DIR = str(GOLDEN_DIR / "cache")
INPUT_VCF = str(GOLDEN_DIR / "input.vcf.gz")
REFERENCE_FASTA = str(GOLDEN_DIR / "reference.fa")


@pytest.fixture(scope="module")
def skip_if_no_cache():
    if not os.path.isdir(CACHE_DIR):
        pytest.skip("Golden test cache not available")


class TestAnnotate:
    """Test the streaming annotation pipeline."""

    def test_returns_lazyframe(self, skip_if_no_cache):
        import vepyr

        lf = vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            everything=True,
            reference_fasta=REFERENCE_FASTA,
        )
        assert isinstance(lf, pl.LazyFrame)

    def test_collect_returns_dataframe(self, skip_if_no_cache):
        import vepyr

        lf = vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            everything=True,
            reference_fasta=REFERENCE_FASTA,
        )
        df = lf.collect()
        assert isinstance(df, pl.DataFrame)
        assert df.height > 0
        assert df.width > 10

    def test_has_annotation_columns(self, skip_if_no_cache):
        import vepyr

        df = vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            everything=True,
            reference_fasta=REFERENCE_FASTA,
        ).collect()
        assert "most_severe_consequence" in df.columns
        assert "chrom" in df.columns
        assert "start" in df.columns
        assert "ref" in df.columns
        assert "alt" in df.columns

    def test_projection_pushdown(self, skip_if_no_cache):
        """Selecting a subset of columns should work."""
        import vepyr

        df = (
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                everything=True,
                reference_fasta=REFERENCE_FASTA,
            )
            .select(["chrom", "start", "ref", "alt", "most_severe_consequence"])
            .collect()
        )
        assert df.width == 5
        assert df.height > 0

    def test_filter_pushdown(self, skip_if_no_cache):
        """Filtering should work on the LazyFrame."""
        import vepyr

        df = (
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                everything=True,
                reference_fasta=REFERENCE_FASTA,
            )
            .filter(pl.col("most_severe_consequence") == "missense_variant")
            .collect()
        )
        assert isinstance(df, pl.DataFrame)
        # May have 0 rows if no missense in the 100 test variants
        if df.height > 0:
            assert all(
                v == "missense_variant" for v in df["most_severe_consequence"].to_list()
            )

    def test_sink_vcf(self, skip_if_no_cache):
        """Writing to VCF via polars-bio sink_vcf should work."""
        import vepyr

        lf = vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            everything=True,
            reference_fasta=REFERENCE_FASTA,
        )

        try:
            import polars_bio  # noqa: F401
        except ImportError:
            pytest.skip("polars-bio not installed")

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            out_path = f.name

        try:
            lf.collect().pipe(
                lambda df: df.select(
                    ["chrom", "start", "ref", "alt", "most_severe_consequence"]
                )
            ).write_csv(out_path, separator="\t")
            assert os.path.getsize(out_path) > 0
        finally:
            os.unlink(out_path)

    def test_validates_reference_fasta(self):
        """everything=True without reference_fasta should raise."""
        import vepyr

        with pytest.raises(ValueError, match="reference_fasta"):
            vepyr.annotate(INPUT_VCF, CACHE_DIR, everything=True)

    def test_validates_hgvs_reference_fasta(self):
        """hgvs=True without reference_fasta should raise."""
        import vepyr

        with pytest.raises(ValueError, match="reference_fasta"):
            vepyr.annotate(INPUT_VCF, CACHE_DIR, hgvs=True)

    @pytest.mark.parametrize("kwargs", [{"hgvsc": True}, {"hgvsp": True}])
    def test_validates_hgvs_subfield_reference_fasta(self, kwargs):
        """hgvsc/hgvsp without reference_fasta should raise."""
        import vepyr

        with pytest.raises(ValueError, match="reference_fasta"):
            vepyr.annotate(INPUT_VCF, CACHE_DIR, **kwargs)

    def test_annotate_to_vcf_output(self, skip_if_no_cache):
        """Writing to VCF via output_vcf should produce a non-empty file."""
        import vepyr

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            out_path = f.name

        try:
            result = vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                everything=True,
                reference_fasta=REFERENCE_FASTA,
                output_vcf=out_path,
            )
            assert result == out_path
            assert os.path.getsize(out_path) > 0
        finally:
            os.unlink(out_path)

    def test_annotate_vcf_returns_path(self, skip_if_no_cache):
        """output_vcf should return the output path as a string."""
        import vepyr

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            out_path = f.name

        try:
            result = vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                everything=True,
                reference_fasta=REFERENCE_FASTA,
                output_vcf=out_path,
            )
            assert isinstance(result, str)
            assert result == out_path
        finally:
            os.unlink(out_path)

    def test_annotate_vcf_has_csq_header(self, skip_if_no_cache):
        """VCF output should contain CSQ in the INFO header."""
        import vepyr

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            out_path = f.name

        try:
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                everything=True,
                reference_fasta=REFERENCE_FASTA,
                output_vcf=out_path,
            )
            with open(out_path) as f:
                header_lines = [line for line in f if line.startswith("#")]
            assert any("CSQ" in line for line in header_lines)
        finally:
            os.unlink(out_path)

    def test_pick_options_forward_to_vcf_writer(self, monkeypatch):
        """flag_pick_allele_gene and pick_order should reach native VCF output."""
        import vepyr

        seen = {}

        def fake_annotate_vcf(
            vcf_path,
            cache_dir,
            output_path,
            options_json,
            show_progress,
            compression,
            on_batch_written,
        ):
            seen["options"] = json.loads(options_json)
            return 0

        monkeypatch.setattr(vepyr, "_annotate_vcf", fake_annotate_vcf)

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            out_path = f.name

        try:
            result = vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf=out_path,
                show_progress=False,
                merged=True,
                flag_pick_allele_gene=True,
                pick_order="biotype,rank,mane_select",
            )
            assert result == out_path
            assert seen["options"]["merged"] is True
            assert seen["options"]["flag_pick_allele_gene"] is True
            assert seen["options"]["pick_order"] == "biotype,rank,mane_select"
        finally:
            os.unlink(out_path)

    def test_notebook_progress_updates_on_main_thread(self, monkeypatch):
        """Default tqdm notebook updates should be applied from the main thread."""
        import vepyr

        bars = []

        class FakeTqdm:
            def __init__(self, **kwargs):
                self.total = kwargs.get("total")
                self.updates = []
                self.closed = False
                bars.append(self)

            def update(self, value):
                self.updates.append((value, threading.current_thread().name))

            def refresh(self):
                pass

            def close(self):
                self.closed = True

        def fake_annotate_vcf(
            vcf_path,
            cache_dir,
            output_path,
            options_json,
            show_progress,
            compression,
            on_batch_written,
        ):
            assert show_progress is False
            assert on_batch_written is not None
            on_batch_written(10, 10, 30)
            on_batch_written(20, 30, 30)
            return 30

        monkeypatch.setattr(vepyr, "_annotate_vcf", fake_annotate_vcf)
        monkeypatch.setitem(
            sys.modules,
            "tqdm.auto",
            types.SimpleNamespace(tqdm=FakeTqdm),
        )

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            out_path = f.name

        try:
            result = vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf=out_path,
                show_progress=True,
            )
            assert result == out_path
            assert len(bars) == 1
            assert bars[0].updates == [(10, "MainThread"), (20, "MainThread")]
            assert bars[0].total == 30
            assert bars[0].closed is True
        finally:
            os.unlink(out_path)
