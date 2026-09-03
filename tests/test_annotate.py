"""Tests for vepyr.annotate() streaming annotation pipeline."""

from __future__ import annotations

import inspect
import json
import os
import shutil
import sys
import threading
import warnings
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
    return str(copy_cache_with_source_metadata(CACHE_DIR, target, "ensembl", "115"))


@pytest.fixture(scope="module")
def partial_cache_dir(metadata_cache_dir, tmp_path_factory):
    """A per-contig download: every shard on disk, manifests listing more.

    Mirrors ``snapshot_download(allow_patterns=["*/chr1.parquet",
    "*/chrom_manifest.json"])`` against a published cache, whose
    ``chrom_manifest.json`` files describe the whole cache. The entries
    prepended here name contigs that have no shard on disk, and the first of
    them is what a manifest-position probe would try to open.

    Shards are copied, not symlinked: Windows needs Developer Mode or elevated
    privileges for symlinks, and the suite runs there.
    """
    source = Path(metadata_cache_dir)
    target = tmp_path_factory.mktemp("partial_cache")
    absent = [
        {"chrom": chrom, "dataset": f"{chrom}.parquet", "rows": 1}
        for chrom in ("chr21", "chr22")
    ]
    for entity_dir in sorted(d for d in source.iterdir() if d.is_dir()):
        out = target / entity_dir.name
        out.mkdir()
        for shard in entity_dir.glob("*.parquet"):
            shutil.copy2(shard, out / shard.name)
        entries = json.loads((entity_dir / "chrom_manifest.json").read_text())
        (out / "chrom_manifest.json").write_text(json.dumps(absent + entries, indent=2))
    return str(target)


class TestPartialCache:
    """A cache whose shards are a subset of its manifests must annotate what it has.

    Regression test for sitekwb/vepyr-porting-tests#607: the engine used to read
    ``bio.vep.cache_source_type`` off the manifest's first shard unconditionally,
    so a partial download failed with ``failed to open Parquet cache shard
    '.../variation/chr21.parquet'`` before touching a requested contig.
    """

    def test_manifest_lists_more_than_is_on_disk(self, partial_cache_dir):
        variation = Path(partial_cache_dir) / "variation"
        entries = json.loads((variation / "chrom_manifest.json").read_text())
        assert [e["chrom"] for e in entries] == ["chr21", "chr22", "chr1"]
        assert not (variation / entries[0]["dataset"]).exists()
        assert (variation / "chr1.parquet").is_file()

    def test_annotates_the_contigs_it_has(self, partial_cache_dir, metadata_cache_dir):
        import vepyr

        def run(cache_dir):
            return vepyr.annotate(
                INPUT_VCF,
                cache_dir,
                everything=True,
                reference_fasta=REFERENCE_FASTA,
            ).collect()

        partial = run(partial_cache_dir)
        intact = run(metadata_cache_dir)
        assert partial.height > 0
        assert partial.height == intact.height
        assert partial.select("chrom", "start", "most_severe_consequence").equals(
            intact.select("chrom", "start", "most_severe_consequence")
        )

    def test_cache_without_manifest_still_fails_on_the_manifest(
        self, metadata_cache_dir, tmp_path
    ):
        """What the old dataset-card recipe produced: a shard and no manifest.

        Not repaired by the shard-selection fix, and must not be: without a
        manifest there is no partitioned cache to open.
        """
        import vepyr

        cache = tmp_path / "no_manifest"
        (cache / "variation").mkdir(parents=True)
        shutil.copy2(
            Path(metadata_cache_dir) / "variation" / "chr1.parquet",
            cache / "variation" / "chr1.parquet",
        )
        with pytest.raises(RuntimeError, match="chrom_manifest.json"):
            vepyr.annotate(
                INPUT_VCF,
                str(cache),
                everything=True,
                reference_fasta=REFERENCE_FASTA,
            ).collect()


@pytest.fixture(scope="module")
def demo_plugin_cache(metadata_cache_dir, tmp_path_factory):
    """A one-plugin cache built in-process against the golden variation shard.

    Two rows keyed to golden input variants, so the ``DEMO`` field is populated
    for them and the run is distinguishable from one that ignored the plugin.
    """
    from tests.test_build_plugin_cache import _init_full_repo

    import vepyr

    root = tmp_path_factory.mktemp("demo_plugin")
    repo = _init_full_repo(root)
    source = root / "demo.tsv"
    source.write_text("1\t604358\tG\tC\t0.5\n1\t604360\tT\tC\t0.25\n")
    plugin_root = root / "pc"
    built = vepyr.build_plugin_cache(
        "demo",
        "v0.1.0",
        source_path=str(source),
        cache_dir=metadata_cache_dir,
        plugin_cache_root=str(plugin_root),
        plugins_repo=str(repo),
        chroms=["1"],
    )
    assert built == [("chr1", 2, 0, 2)]
    return str(plugin_root)


@pytest.fixture(scope="module")
def partial_plugin_cache(demo_plugin_cache, tmp_path_factory):
    """The plugin-cache analogue of ``partial_cache_dir``: ``manifest.json``
    lists contigs whose shards were never downloaded, ahead of the one that was.
    """
    source = Path(demo_plugin_cache) / "plugin" / "demo"
    target = tmp_path_factory.mktemp("partial_plugin") / "plugin" / "demo"
    target.mkdir(parents=True)
    shutil.copy2(source / "chr1.parquet", target / "chr1.parquet")
    manifest = json.loads((source / "manifest.json").read_text())
    absent = [
        {"chrom": chrom, "file": f"{chrom}.parquet", "rows": 1, "warm": 0, "cold": 1}
        for chrom in ("chr21", "chr22")
    ]
    manifest["chroms"] = absent + manifest["chroms"]
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return str(target.parents[1])


class TestPartialPluginCache:
    """A plugin cache with a subset of its manifest's shards annotates what it has.

    Companion to ``TestPartialCache``: the plugin registry is opened per contig
    and reads the CSQ header from ``manifest.json`` without touching a shard, so
    a per-chromosome plugin download works without trimming the manifest. The
    one deliberate exception is a requested contig the manifest says has rows
    but whose shard is absent, which fails loudly rather than emitting nulls
    under a header that still advertises the plugin's fields.
    """

    @staticmethod
    def _records(path: Path) -> tuple[str, list[str]]:
        lines = path.read_text().splitlines()
        csq_header = next(line for line in lines if line.startswith("##INFO=<ID=CSQ"))
        return csq_header, [line for line in lines if not line.startswith("#")]

    def test_plugin_manifest_lists_more_than_is_on_disk(self, partial_plugin_cache):
        plugin_dir = Path(partial_plugin_cache) / "plugin" / "demo"
        manifest = json.loads((plugin_dir / "manifest.json").read_text())
        assert [c["chrom"] for c in manifest["chroms"]] == ["chr21", "chr22", "chr1"]
        assert not (plugin_dir / manifest["chroms"][0]["file"]).exists()
        assert (plugin_dir / "chr1.parquet").is_file()

    def test_annotates_the_contigs_it_has(
        self,
        partial_cache_dir,
        partial_plugin_cache,
        metadata_cache_dir,
        demo_plugin_cache,
        tmp_path,
    ):
        import vepyr

        def run(cache_dir, plugin_root, name):
            out = tmp_path / f"{name}.vcf"
            vepyr.annotate(
                INPUT_VCF,
                cache_dir,
                output_vcf=str(out),
                reference_fasta=REFERENCE_FASTA,
                skip_csq=False,
                plugin_cache_root=plugin_root,
                plugins=["demo"],
            )
            return self._records(out)

        partial_header, partial = run(
            partial_cache_dir, partial_plugin_cache, "partial"
        )
        intact_header, intact = run(metadata_cache_dir, demo_plugin_cache, "intact")
        assert "DEMO" in partial_header
        assert partial_header == intact_header
        assert len(partial) > 0
        assert partial == intact
        assert any("604358" in line and "|0.5" in line for line in partial)

    def test_listed_contig_with_missing_shard_fails_loudly(
        self, demo_plugin_cache, metadata_cache_dir, tmp_path
    ):
        import vepyr

        source = Path(demo_plugin_cache) / "plugin" / "demo"
        broken = tmp_path / "broken" / "plugin" / "demo"
        broken.mkdir(parents=True)
        shutil.copy2(source / "manifest.json", broken / "manifest.json")
        with pytest.raises(RuntimeError, match="shard is missing"):
            vepyr.annotate(
                INPUT_VCF,
                metadata_cache_dir,
                output_vcf=str(tmp_path / "out.vcf"),
                reference_fasta=REFERENCE_FASTA,
                skip_csq=False,
                plugin_cache_root=str(tmp_path / "broken"),
                plugins=["demo"],
            )


class TestAnnotate:
    """Test the streaming annotation pipeline."""

    def test_source_mode_flags_not_in_signature(self):
        import vepyr

        sig = inspect.signature(vepyr.annotate)
        assert "merged" not in sig.parameters
        assert "refseq" not in sig.parameters

    def test_has_workers_param_only(self):
        import vepyr

        sig = inspect.signature(vepyr.annotate)
        assert "forks" not in sig.parameters
        assert "threads" not in sig.parameters
        assert "target_partitions" not in sig.parameters
        assert "chrom_parallelism" not in sig.parameters
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

    def test_workers_forward_to_streaming_annotator(self, monkeypatch):
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
        ):
            seen.append((json.loads(options_json), limit))
            return FakeAnnotator()

        monkeypatch.setattr(vepyr, "_create_annotator", fake_create_annotator)

        lf = vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            workers=4,
        )

        assert isinstance(lf, pl.LazyFrame)
        assert seen[0][1] is None
        assert seen[0][0]["cache_format"] == "parquet"
        assert seen[0][0]["workers"] == 4
        assert "forks" not in seen[0][0]
        assert "contig_parallelism" not in seen[0][0]
        assert "annotation_workers" not in seen[0][0]

    def test_expected_cache_version_forwards_to_streaming_annotator(self, monkeypatch):
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
        ):
            seen.append(json.loads(options_json))
            return FakeAnnotator()

        monkeypatch.setattr(vepyr, "_create_annotator", fake_create_annotator)
        vepyr.annotate(INPUT_VCF, CACHE_DIR, expected_cache_version="116")
        assert seen[0]["expected_cache_version"] == "116"

    def test_workers_one_omits_workers_key(self, monkeypatch):
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
        ):
            seen.append(json.loads(options_json))
            return FakeAnnotator()

        monkeypatch.setattr(vepyr, "_create_annotator", fake_create_annotator)

        lf = vepyr.annotate(INPUT_VCF, CACHE_DIR, workers=1)

        assert isinstance(lf, pl.LazyFrame)
        assert "workers" not in seen[0]
        assert "forks" not in seen[0]

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

    def test_workers_forward_to_vcf_writer(self, monkeypatch):
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
                workers=4,
            )
            assert result == out_path
            assert seen["options"]["cache_format"] == "parquet"
            assert seen["options"]["workers"] == 4
            assert "forks" not in seen["options"]
            assert "contig_parallelism" not in seen["options"]
        finally:
            os.unlink(out_path)

    def test_expected_cache_version_forwards_to_vcf_writer(self, monkeypatch):
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
            seen.update(json.loads(options_json))

        monkeypatch.setattr(vepyr, "_annotate_vcf", fake_annotate_vcf)
        vepyr.annotate(
            INPUT_VCF,
            CACHE_DIR,
            output_vcf="unused.vcf",
            show_progress=False,
            expected_cache_version="115",
        )
        assert seen["expected_cache_version"] == "115"

    @pytest.mark.parametrize("value", [115, True, 115.2])
    def test_expected_cache_version_rejects_non_strings(self, value):
        import vepyr

        with pytest.raises(TypeError, match="expected_cache_version"):
            vepyr.annotate(INPUT_VCF, CACHE_DIR, expected_cache_version=value)

    @pytest.mark.parametrize("value", ["115.2", "117", "v116", ""])
    def test_expected_cache_version_rejects_unsupported_strings(self, value):
        import vepyr

        with pytest.raises(ValueError, match="Unsupported expected_cache_version"):
            vepyr.annotate(INPUT_VCF, CACHE_DIR, expected_cache_version=value)

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

    @pytest.mark.parametrize("removed", ["forks", "threads"])
    def test_removed_knobs_rejected(self, removed):
        import vepyr

        with pytest.raises(TypeError, match=removed):
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf="unused.vcf",
                show_progress=False,
                **{removed: 2},
            )

    def test_invalid_cache_format_rejected(self):
        import vepyr

        with pytest.raises(ValueError, match="cache_format"):
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf="unused.vcf",
                show_progress=False,
                cache_format="fjall",
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

    def test_chrom_parallelism_removed_from_public_api(self):
        import vepyr

        with pytest.raises(TypeError, match="chrom_parallelism"):
            vepyr.annotate(
                INPUT_VCF,
                CACHE_DIR,
                output_vcf="unused.vcf",
                show_progress=False,
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


class _Stop(Exception):
    """Abort annotate() once the options have been captured."""


# --- plugin subset selection ---------------------------------------------


def _fake_plugin_root(tmp_path: Path, names: list[str]) -> str:
    """A plugin cache root with `names` as manifest-bearing directories."""
    root = tmp_path / "plugin_cache"
    for name in names:
        d = root / "plugin" / name
        d.mkdir(parents=True)
        (d / "manifest.json").write_text("{}")
    # A directory without a manifest is not a plugin and must be ignored.
    (root / "plugin" / "not_a_plugin").mkdir(parents=True)
    return str(root)


def test_plugin_subset_root_links_only_selected(tmp_path):
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd", "clinvar", "spliceai"])

    with vepyr._plugin_subset_root(root, ["clinvar", "cadd"]) as subset:
        plugin_dir = Path(subset) / "plugin"
        assert sorted(p.name for p in plugin_dir.iterdir()) == ["cadd", "clinvar"]
        for name in ("cadd", "clinvar"):
            manifest = plugin_dir / name / "manifest.json"
            assert manifest.is_file()
            # Files are hard-linked, not copied and not symlinked: a directory
            # symlink would need target_is_directory plus a privilege that
            # non-elevated Windows sessions do not have.
            assert not (plugin_dir / name).is_symlink()
            assert not manifest.is_symlink()
            assert (
                manifest.stat().st_ino
                == (Path(root) / "plugin" / name / "manifest.json").stat().st_ino
            )


def test_plugin_subset_root_falls_back_when_link_unavailable(tmp_path, monkeypatch):
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd"])

    def no_hardlinks(src, dst):
        raise OSError("cross-device link")

    monkeypatch.setattr(os, "link", no_hardlinks)
    with vepyr._plugin_subset_root(root, ["cadd"]) as subset:
        manifest = Path(subset) / "plugin" / "cadd" / "manifest.json"
        assert manifest.is_symlink()
        assert manifest.read_text() == "{}"


def test_plugin_subset_root_reports_when_no_link_method_works(tmp_path, monkeypatch):
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd"])
    monkeypatch.setattr(os, "link", lambda s, d: (_ for _ in ()).throw(OSError("nope")))
    monkeypatch.setattr(
        os, "symlink", lambda s, d: (_ for _ in ()).throw(OSError("nope either"))
    )
    with pytest.raises(OSError, match="Developer Mode"):
        vepyr._plugin_subset_root(root, ["cadd"])


def test_plugin_subset_root_empty_list_selects_nothing(tmp_path):
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd"])
    with vepyr._plugin_subset_root(root, []) as subset:
        assert list((Path(subset) / "plugin").iterdir()) == []


def test_plugin_subset_root_deduplicates(tmp_path):
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd"])
    with vepyr._plugin_subset_root(root, ["cadd", "cadd"]) as subset:
        assert [p.name for p in (Path(subset) / "plugin").iterdir()] == ["cadd"]


def test_plugin_subset_root_rejects_unknown_name(tmp_path):
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd", "clinvar"])
    with pytest.raises(ValueError, match="Unknown plugin 'nope'") as exc:
        vepyr._plugin_subset_root(root, ["nope"])
    # The message must list the real plugins, not stray directories.
    assert "cadd, clinvar" in str(exc.value)
    assert "not_a_plugin" not in str(exc.value)


def test_plugin_subset_root_rejects_bare_string(tmp_path):
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd"])
    with pytest.raises(TypeError, match="must be a list"):
        vepyr._plugin_subset_root(root, "cadd")


def test_plugin_subset_root_rejects_non_string_element(tmp_path):
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd"])
    with pytest.raises(TypeError, match="must be strings"):
        vepyr._plugin_subset_root(root, [1])


def test_plugin_subset_root_missing_plugin_dir(tmp_path):
    import vepyr

    (tmp_path / "empty").mkdir()
    with pytest.raises(FileNotFoundError, match="No plugin directory"):
        vepyr._plugin_subset_root(str(tmp_path / "empty"), ["cadd"])


def test_annotate_plugins_requires_plugin_cache_root():
    import vepyr

    with pytest.raises(ValueError, match="plugins requires plugin_cache_root"):
        vepyr.annotate("input.vcf", CACHE_DIR, plugins=["cadd"])


def test_annotate_plugins_is_accepted_in_signature():
    import vepyr

    params = inspect.signature(vepyr.annotate).parameters
    assert "plugins" in params
    assert params["plugins"].default is None


def test_plugin_subset_root_rejects_a_nested_plugin_layout(tmp_path):
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd"])
    (Path(root) / "plugin" / "cadd" / "shards").mkdir()

    # Silently skipping the directory would drop shards from the subset and the
    # engine would only report missing files, never the reason.
    with pytest.raises(NotImplementedError, match="nested directory"):
        vepyr._plugin_subset_root(root, ["cadd"])


def test_annotate_empty_plugins_selects_no_plugin_root(tmp_path, monkeypatch):
    """plugins=[] must be a plugin-free run, not an empty subset tree."""
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd"])
    seen = {}

    def fake(vcf, cache_dir, options_json, skip_csq, limit):
        seen["opts"] = json.loads(options_json)
        raise _Stop()

    monkeypatch.setattr(vepyr, "_create_annotator", fake)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(_Stop):
            vepyr.annotate("in.vcf", CACHE_DIR, plugin_cache_root=root, plugins=[])

    assert "plugin_cache_root" not in seen["opts"]
    assert [w for w in caught if "skip_csq" in str(w.message)] == []


def test_annotate_nonempty_plugins_warns_only_when_csq_is_dropped(
    tmp_path, monkeypatch
):
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd"])

    def fake(vcf, cache_dir, options_json, skip_csq, limit):
        raise _Stop()

    monkeypatch.setattr(vepyr, "_create_annotator", fake)
    for skip_csq, expected in ((True, 1), (False, 0)):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(_Stop):
                vepyr.annotate(
                    "in.vcf",
                    CACHE_DIR,
                    plugin_cache_root=root,
                    plugins=["cadd"],
                    skip_csq=skip_csq,
                )
        hits = [w for w in caught if "skip_csq" in str(w.message)]
        assert len(hits) == expected, f"skip_csq={skip_csq}"


def test_annotate_releases_the_subset_when_the_probe_fails(tmp_path, monkeypatch):
    """A failure before the closure owns the tree must not leak it."""
    import vepyr

    root = _fake_plugin_root(tmp_path, ["cadd"])
    created = []
    real = vepyr._plugin_subset_root

    def spy(cache_root, names):
        sub = real(cache_root, names)
        created.append(sub.name)
        return sub

    monkeypatch.setattr(vepyr, "_plugin_subset_root", spy)
    monkeypatch.setattr(
        vepyr, "_create_annotator", lambda *a, **k: (_ for _ in ()).throw(_Stop())
    )
    with pytest.raises(_Stop):
        vepyr.annotate(
            "in.vcf",
            CACHE_DIR,
            plugin_cache_root=root,
            plugins=["cadd"],
            skip_csq=False,
        )

    assert created, "the subset root was never built"
    assert not os.path.exists(created[0]), "subset tree outlived the failed call"
