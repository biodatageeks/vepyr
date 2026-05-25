"""Tests for vepyr.annotate() streaming annotation pipeline."""

from __future__ import annotations

import inspect
import json
import os
import sys
import threading
import tempfile
import types
from pathlib import Path

import polars as pl
import pytest
from tests.cache_metadata import copy_cache_with_source_metadata

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"
CACHE_DIR = str(GOLDEN_DIR / "cache")
INPUT_VCF = str(GOLDEN_DIR / "input.vcf.gz")
REFERENCE_FASTA = str(GOLDEN_DIR / "reference.fa")


@pytest.fixture(scope="module")
def skip_if_no_cache():
    if not os.path.isdir(CACHE_DIR):
        pytest.skip("Golden test cache not available")


@pytest.fixture(scope="module")
def metadata_cache_dir(skip_if_no_cache, tmp_path_factory):
    target = tmp_path_factory.mktemp("ensembl_cache_with_metadata")
    return str(copy_cache_with_source_metadata(CACHE_DIR, target, "ensembl"))


class TestAnnotate:
    """Test the streaming annotation pipeline."""

    def test_source_mode_flags_not_in_signature(self):
        import vepyr

        sig = inspect.signature(vepyr.annotate)
        assert "merged" not in sig.parameters
        assert "refseq" not in sig.parameters

    def test_has_forks_and_workers_params(self):
        import vepyr

        sig = inspect.signature(vepyr.annotate)
        assert "target_partitions" not in sig.parameters
        assert "chrom_parallelism" not in sig.parameters
        p = sig.parameters["forks"]
        assert p.default == 0
        workers = sig.parameters["workers"]
        assert workers.default == 1

    def test_returns_lazyframe(self, metadata_cache_dir):
        import vepyr

        lf = vepyr.annotate(
            INPUT_VCF,
            metadata_cache_dir,
            everything=True,
            reference_fasta=REFERENCE_FASTA,
        )
        assert isinstance(lf, pl.LazyFrame)

    def test_parallelism_forwards_to_streaming_annotator(self, monkeypatch):
        import pyarrow as pa
        import vepyr

        seen = []

        class FakeAnnotator:
            schema = pa.schema([pa.field("chrom", pa.string())])

            def __iter__(self):
                return iter(())

        def fake_create_annotator(
            vcf_path,
            cache_dir,
            options_json,
            skip_csq=True,
            limit=None,
            forks=0,
            workers=1,
        ):
            seen.append((json.loads(options_json), forks, workers, limit))
            return FakeAnnotator()

        monkeypatch.setattr(vepyr, "_create_annotator", fake_create_annotator)

        lf = vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            use_fjall=True,
            forks=2,
            workers=4,
        )

        assert isinstance(lf, pl.LazyFrame)
        assert seen[0][1] == 2
        assert seen[0][2] == 4
        assert seen[0][3] is None
        assert seen[0][0]["use_fjall"] is True
        assert seen[0][0]["forks"] == 4
        assert seen[0][0]["annotation_workers"] == 4
        assert seen[0][0]["inline_lookup"] is False
        assert seen[0][0]["contig_parallelism"] == 2
        assert seen[0][0]["chunked_buffer_lookup"] is True

    def test_single_chrom_workers_do_not_enable_chunked_lookup(self, monkeypatch):
        import pyarrow as pa
        import vepyr

        seen = []

        class FakeAnnotator:
            schema = pa.schema([pa.field("chrom", pa.string())])

            def __iter__(self):
                return iter(())

        def fake_create_annotator(
            vcf_path,
            cache_dir,
            options_json,
            skip_csq=True,
            limit=None,
            forks=0,
            workers=1,
        ):
            seen.append((json.loads(options_json), forks, workers, limit))
            return FakeAnnotator()

        monkeypatch.setattr(vepyr, "_create_annotator", fake_create_annotator)

        lf = vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            use_fjall=True,
            forks=1,
            workers=4,
        )

        assert isinstance(lf, pl.LazyFrame)
        assert seen[0][1] == 1
        assert seen[0][2] == 4
        assert seen[0][0]["forks"] == 4
        assert seen[0][0]["contig_parallelism"] == 1
        assert "chunked_buffer_lookup" not in seen[0][0]

    def test_collect_returns_dataframe(self, metadata_cache_dir):
        import vepyr

        lf = vepyr.annotate(
            INPUT_VCF,
            metadata_cache_dir,
            everything=True,
            reference_fasta=REFERENCE_FASTA,
        )
        df = lf.collect()
        assert isinstance(df, pl.DataFrame)
        assert df.height > 0
        assert df.width > 10

    def test_has_annotation_columns(self, metadata_cache_dir):
        import vepyr

        df = vepyr.annotate(
            INPUT_VCF,
            metadata_cache_dir,
            everything=True,
            reference_fasta=REFERENCE_FASTA,
        ).collect()
        assert "most_severe_consequence" in df.columns
        assert "chrom" in df.columns
        assert "start" in df.columns
        assert "ref" in df.columns
        assert "alt" in df.columns

    def test_projection_pushdown(self, metadata_cache_dir):
        """Selecting a subset of columns should work."""
        import vepyr

        df = (
            vepyr.annotate(
                INPUT_VCF,
                metadata_cache_dir,
                everything=True,
                reference_fasta=REFERENCE_FASTA,
            )
            .select(["chrom", "start", "ref", "alt", "most_severe_consequence"])
            .collect()
        )
        assert df.width == 5
        assert df.height > 0

    def test_filter_pushdown(self, metadata_cache_dir):
        """Filtering should work on the LazyFrame."""
        import vepyr

        df = (
            vepyr.annotate(
                INPUT_VCF,
                metadata_cache_dir,
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

    def test_sink_vcf(self, metadata_cache_dir):
        """Writing to VCF via polars-bio sink_vcf should work."""
        import vepyr

        lf = vepyr.annotate(
            INPUT_VCF,
            metadata_cache_dir,
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

    def test_annotate_to_vcf_output(self, metadata_cache_dir):
        """Writing to VCF via output_vcf should produce a non-empty file."""
        import vepyr

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            out_path = f.name

        try:
            result = vepyr.annotate(
                INPUT_VCF,
                metadata_cache_dir,
                everything=True,
                reference_fasta=REFERENCE_FASTA,
                output_vcf=out_path,
            )
            assert result == out_path
            assert os.path.getsize(out_path) > 0
        finally:
            os.unlink(out_path)

    def test_annotate_vcf_returns_path(self, metadata_cache_dir):
        """output_vcf should return the output path as a string."""
        import vepyr

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            out_path = f.name

        try:
            result = vepyr.annotate(
                INPUT_VCF,
                metadata_cache_dir,
                everything=True,
                reference_fasta=REFERENCE_FASTA,
                output_vcf=out_path,
            )
            assert isinstance(result, str)
            assert result == out_path
        finally:
            os.unlink(out_path)

    def test_annotate_vcf_has_csq_header(self, metadata_cache_dir):
        """VCF output should contain CSQ in the INFO header."""
        import vepyr

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as f:
            out_path = f.name

        try:
            vepyr.annotate(
                INPUT_VCF,
                metadata_cache_dir,
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
        """Pick mode options and pick_order should reach native VCF output."""
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
            forks,
            workers,
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
                pick=True,
                pick_allele=True,
                per_gene=True,
                pick_allele_gene=True,
                flag_pick=True,
                flag_pick_allele=True,
                flag_pick_allele_gene=True,
                pick_order="biotype,rank,mane_select",
            )
            assert result == out_path
            assert seen["options"]["pick"] is True
            assert seen["options"]["pick_allele"] is True
            assert seen["options"]["per_gene"] is True
            assert seen["options"]["pick_allele_gene"] is True
            assert seen["options"]["flag_pick"] is True
            assert seen["options"]["flag_pick_allele"] is True
            assert seen["options"]["flag_pick_allele_gene"] is True
            assert seen["options"]["pick_order"] == "biotype,rank,mane_select"
        finally:
            os.unlink(out_path)

    def test_buffer_size_forwards_to_vcf_writer(self, monkeypatch):
        """buffer_size should default to VEP's 5000 and allow override."""
        import vepyr

        seen = []

        def fake_annotate_vcf(
            vcf_path,
            cache_dir,
            output_path,
            options_json,
            show_progress,
            compression,
            on_batch_written,
            forks,
            workers,
        ):
            seen.append(json.loads(options_json))
            return 0

        monkeypatch.setattr(vepyr, "_annotate_vcf", fake_annotate_vcf)

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as default_file:
            default_out = default_file.name
        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as override_file:
            override_out = override_file.name

        try:
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf=default_out,
                show_progress=False,
            )
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf=override_out,
                show_progress=False,
                buffer_size=1234,
            )

            assert seen[0]["buffer_size"] == 5000
            assert seen[1]["buffer_size"] == 1234
        finally:
            os.unlink(default_out)
            os.unlink(override_out)

    def test_forks_forwards_to_vcf_writer(self, monkeypatch):
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
            forks,
            workers,
        ):
            seen["options"] = json.loads(options_json)
            seen["forks"] = forks
            seen["workers"] = workers
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
                use_fjall=True,
                forks=3,
                workers=4,
            )
            assert result == out_path
            assert seen["forks"] == 3
            assert seen["workers"] == 4
            assert seen["options"]["use_fjall"] is True
            assert seen["options"]["forks"] == 4
            assert seen["options"]["annotation_workers"] == 4
            assert seen["options"]["inline_lookup"] is False
            assert seen["options"]["contig_parallelism"] == 3
            assert seen["options"]["chunked_buffer_lookup"] is True
        finally:
            os.unlink(out_path)

    @pytest.mark.parametrize("source_flag", ["merged", "refseq"])
    def test_source_mode_flags_rejected(self, source_flag):
        """Source mode is selected by cache metadata, not annotate() flags."""
        import vepyr

        with pytest.raises(TypeError, match=f"{source_flag}"):
            vepyr.annotate(INPUT_VCF, CACHE_DIR, **{source_flag: True})

    def test_buffer_size_rejects_non_positive_values(self):
        """buffer_size mirrors VEP's positive integer buffer-size contract."""
        import vepyr

        for value in (0, True):
            with pytest.raises(
                ValueError, match="buffer_size must be a positive integer"
            ):
                vepyr.annotate(
                    INPUT_VCF,
                    CACHE_DIR,
                    output_vcf="unused.vcf",
                    show_progress=False,
                    buffer_size=value,
                )

    @pytest.mark.parametrize("value", [-1, True])
    def test_forks_rejects_invalid_values(self, value):
        import vepyr

        with pytest.raises(ValueError, match="forks must be a non-negative integer"):
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf="unused.vcf",
                show_progress=False,
                forks=value,
            )

    def test_forks_requires_fjall_when_nonzero(self):
        import vepyr

        with pytest.raises(ValueError, match="forks > 0 requires use_fjall=True"):
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf="unused.vcf",
                show_progress=False,
                forks=2,
            )

    @pytest.mark.parametrize("value", [0, -1, True])
    def test_workers_rejects_invalid_values(self, value):
        import vepyr

        with pytest.raises(ValueError, match="workers must be a positive integer"):
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf="unused.vcf",
                show_progress=False,
                workers=value,
            )

    def test_workers_gt_one_requires_forks(self):
        import vepyr

        with pytest.raises(ValueError, match="workers > 1 requires forks > 0"):
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf="unused.vcf",
                show_progress=False,
                use_fjall=True,
                workers=2,
            )

    def test_chrom_parallelism_removed_from_public_api(self):
        import vepyr

        with pytest.raises(TypeError, match="chrom_parallelism"):
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf="unused.vcf",
                show_progress=False,
                use_fjall=True,
                chrom_parallelism=2,
            )

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
            forks,
            workers,
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
