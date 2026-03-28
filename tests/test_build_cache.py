"""Tests for vepyr.build_cache() API and the native _core.build_cache binding."""

from __future__ import annotations

import inspect
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import vepyr
from vepyr._core import build_cache as _build_cache

TESTS_DIR = Path(__file__).parent
ENSEMBL_CACHE_DIR = TESTS_DIR / "data" / "ensembl_cache"


@pytest.fixture(scope="module")
def skip_if_no_ensembl_cache():
    if not (ENSEMBL_CACHE_DIR / "info.txt").exists():
        pytest.skip("Ensembl cache fixture not available")


class TestBuildCacheSignature:
    """Verify the Python build_cache() signature matches the documented API."""

    def test_has_release_param(self):
        sig = inspect.signature(vepyr.build_cache)
        assert "release" in sig.parameters

    def test_has_cache_dir_param(self):
        sig = inspect.signature(vepyr.build_cache)
        assert "cache_dir" in sig.parameters

    def test_has_build_fjall_param(self):
        sig = inspect.signature(vepyr.build_cache)
        p = sig.parameters["build_fjall"]
        assert p.default is True

    def test_has_fjall_zstd_level_param(self):
        sig = inspect.signature(vepyr.build_cache)
        p = sig.parameters["fjall_zstd_level"]
        assert p.default == 3

    def test_has_fjall_dict_size_kb_param(self):
        sig = inspect.signature(vepyr.build_cache)
        p = sig.parameters["fjall_dict_size_kb"]
        assert p.default == 112

    def test_has_show_progress_param(self):
        sig = inspect.signature(vepyr.build_cache)
        p = sig.parameters["show_progress"]
        assert p.default is True

    def test_has_on_progress_param(self):
        sig = inspect.signature(vepyr.build_cache)
        assert "on_progress" in sig.parameters

    def test_has_local_cache_param(self):
        sig = inspect.signature(vepyr.build_cache)
        p = sig.parameters["local_cache"]
        assert p.default is None

    def test_has_partitions_param(self):
        sig = inspect.signature(vepyr.build_cache)
        p = sig.parameters["partitions"]
        assert p.default == 1

    def test_no_memory_limit_gb_param(self):
        """memory_limit_gb was removed in the upstream migration."""
        sig = inspect.signature(vepyr.build_cache)
        assert "memory_limit_gb" not in sig.parameters


class TestNativeBuildCacheSignature:
    """Verify the native _core.build_cache function signature."""

    def test_callable(self):
        assert callable(_build_cache)

    def test_accepts_on_progress_none(self):
        """on_progress=None should be accepted (checked at signature level)."""
        sig = inspect.signature(_build_cache)
        assert "on_progress" in sig.parameters


class TestBuildCacheValidation:
    """Test input validation in the Python build_cache() wrapper."""

    def test_invalid_cache_type_raises(self):
        with pytest.raises(ValueError, match="Invalid cache_type"):
            vepyr.build_cache(115, "/tmp/fake", cache_type="invalid")

    def test_local_cache_not_found_raises(self):
        with pytest.raises(FileNotFoundError, match="Local cache directory not found"):
            vepyr.build_cache(115, "/tmp/fake", local_cache="/nonexistent/path")

    def test_valid_cache_types_accepted(self):
        """vep, merged, refseq should not raise ValueError."""
        for cache_type in ("vep", "merged", "refseq"):
            # Will fail at a later stage (no cache dir), not at cache_type validation
            with pytest.raises((FileNotFoundError, RuntimeError)):
                vepyr.build_cache(
                    115, "/tmp/fake", cache_type=cache_type, local_cache="/nonexistent"
                )


class TestBuildCacheProgressCallback:
    """Test that the progress callback is wired correctly."""

    @patch("vepyr._build_cache")
    def test_on_progress_forwarded_to_native(self, mock_native):
        """Custom on_progress callable should be passed to the native layer."""
        mock_native.return_value = []
        cb = MagicMock()

        # Use local_cache to skip download; mock native to skip actual build
        os.makedirs("/tmp/test_vepyr_cache_cb", exist_ok=True)
        try:
            vepyr.build_cache(
                115,
                "/tmp/test_vepyr_cache_cb_out",
                local_cache="/tmp/test_vepyr_cache_cb",
                on_progress=cb,
                show_progress=False,
            )
        finally:
            os.rmdir("/tmp/test_vepyr_cache_cb")

        mock_native.assert_called_once()
        # The 7th positional arg is the progress callback
        call_args = mock_native.call_args
        assert call_args[0][6] is cb

    @patch("vepyr._build_cache")
    def test_show_progress_false_no_tqdm(self, mock_native):
        """show_progress=False with no on_progress should pass None."""
        mock_native.return_value = []

        os.makedirs("/tmp/test_vepyr_cache_np", exist_ok=True)
        try:
            vepyr.build_cache(
                115,
                "/tmp/test_vepyr_cache_np_out",
                local_cache="/tmp/test_vepyr_cache_np",
                show_progress=False,
            )
        finally:
            os.rmdir("/tmp/test_vepyr_cache_np")

        call_args = mock_native.call_args
        assert call_args[0][6] is None

    @patch("vepyr._build_cache")
    def test_returns_flat_parquet_list(self, mock_native):
        """Return value should be flattened to [(path, rows)]."""
        mock_native.return_value = [
            (
                "variation",
                [("/out/variation/chr1.parquet", 1000)],
                (500, 400, 2048, 1.5),
            ),
            ("transcript", [("/out/transcript/chr1.parquet", 200)], None),
        ]

        os.makedirs("/tmp/test_vepyr_cache_ret", exist_ok=True)
        try:
            result = vepyr.build_cache(
                115,
                "/tmp/test_vepyr_cache_ret_out",
                local_cache="/tmp/test_vepyr_cache_ret",
                show_progress=False,
            )
        finally:
            os.rmdir("/tmp/test_vepyr_cache_ret")

        assert result == [
            ("/out/variation/chr1.parquet", 1000),
            ("/out/transcript/chr1.parquet", 200),
        ]

    @patch("vepyr._build_cache")
    def test_build_fjall_params_forwarded(self, mock_native):
        """build_fjall, zstd_level, dict_size_kb should be forwarded."""
        mock_native.return_value = []

        os.makedirs("/tmp/test_vepyr_cache_fj", exist_ok=True)
        try:
            vepyr.build_cache(
                115,
                "/tmp/test_vepyr_cache_fj_out",
                local_cache="/tmp/test_vepyr_cache_fj",
                build_fjall=False,
                fjall_zstd_level=5,
                fjall_dict_size_kb=256,
                show_progress=False,
            )
        finally:
            os.rmdir("/tmp/test_vepyr_cache_fj")

        call_args = mock_native.call_args[0]
        # (cache_root, output_dir, partitions, build_fjall, zstd_level, dict_size_kb, on_progress)
        assert call_args[3] is False  # build_fjall
        assert call_args[4] == 5  # zstd_level
        assert call_args[5] == 256  # dict_size_kb


@pytest.fixture(scope="module")
def built_cache(skip_if_no_ensembl_cache):
    """Build cache once; return (output_dir, flat_result, native_result) for all tests."""
    import pyarrow.parquet as pq

    _tmpdir = tempfile.mkdtemp()

    native_result = _build_cache(str(ENSEMBL_CACHE_DIR), _tmpdir, 2, True, 3, 112, None)
    flat_result = []
    for entity, files, fjall in native_result:
        flat_result.extend(files)

    # Pre-read all parquet tables keyed by entity name
    tables: dict = {}
    for entity, files, _ in native_result:
        for path, _ in files:
            tables[entity] = pq.read_table(path)

    yield _tmpdir, flat_result, native_result, tables

    import shutil

    shutil.rmtree(_tmpdir, ignore_errors=True)


class TestBuildCacheIntegration:
    """Integration tests using real Ensembl cache fixture data.

    Fixture data: chr22:16000001-16002000 from Ensembl 115 GRCh38.
    """

    # ── Overall structure ───────────────────────────────────────────

    def test_all_entities_present(self, built_cache):
        """All data-bearing entity directories should be produced."""
        _, _, native_result, tables = built_cache
        entities = {s[0] for s in native_result}
        for expected in (
            "variation",
            "transcript",
            "exon",
            "translation_core",
            "translation_sift",
            "regulatory",
        ):
            assert expected in entities, f"Missing entity: {expected}"

    def test_all_parquet_files_readable(self, built_cache):
        """Every reported parquet file must exist, be valid, and match row count."""
        import pyarrow.parquet as pq

        _, flat_result, _, _ = built_cache
        for path, expected_rows in flat_result:
            assert os.path.isfile(path), f"Missing: {path}"
            table = pq.read_table(path)
            assert table.num_rows == expected_rows
            assert table.num_columns > 0

    def test_total_row_count(self, built_cache):
        """763 + 106 + 396 + 7 + 7 + 43 = 1322."""
        _, flat_result, _, _ = built_cache
        assert sum(r for _, r in flat_result) == 1322

    # ── Variation (763 rows, 76 cols) ───────────────────────────────

    def test_variation_row_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["variation"].num_rows == 763

    def test_variation_column_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["variation"].num_columns == 76

    def test_variation_required_columns(self, built_cache):
        _, _, _, tables = built_cache
        cols = tables["variation"].column_names
        for c in (
            "chrom",
            "start",
            "end",
            "variation_name",
            "allele_string",
            "failed",
            "somatic",
            "strand",
            "clin_sig",
            "AF",
            "gnomADe",
            "gnomADg",
        ):
            assert c in cols, f"Missing variation column: {c}"

    def test_variation_chrom_is_22(self, built_cache):
        _, _, _, tables = built_cache
        chroms = set(tables["variation"].column("chrom").to_pylist())
        assert chroms == {"22"}

    def test_variation_start_range(self, built_cache):
        import pyarrow.compute as pc

        _, _, _, tables = built_cache
        starts = tables["variation"].column("start")
        assert pc.min(starts).as_py() == 16000001
        assert pc.max(starts).as_py() == 16001972

    def test_variation_sorted_by_start(self, built_cache):
        _, _, _, tables = built_cache
        starts = tables["variation"].column("start").to_pylist()
        assert starts == sorted(starts)

    def test_variation_no_null_keys(self, built_cache):
        _, _, _, tables = built_cache
        t = tables["variation"]
        for col in ("chrom", "start", "allele_string", "variation_name"):
            assert t.column(col).null_count == 0, f"Unexpected nulls in variation.{col}"

    def test_variation_name_format(self, built_cache):
        """All variation_name values should be rs-IDs."""
        _, _, _, tables = built_cache
        names = tables["variation"].column("variation_name").to_pylist()
        assert all(n.startswith("rs") for n in names)

    def test_variation_allele_string_format(self, built_cache):
        """allele_string should contain '/' separating ref/alt."""
        _, _, _, tables = built_cache
        alleles = tables["variation"].column("allele_string").to_pylist()
        assert all("/" in a for a in alleles)

    # ── Transcript (106 rows, 67 cols) ──────────────────────────────

    def test_transcript_row_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["transcript"].num_rows == 106

    def test_transcript_column_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["transcript"].num_columns == 67

    def test_transcript_required_columns(self, built_cache):
        _, _, _, tables = built_cache
        cols = tables["transcript"].column_names
        for c in (
            "chrom",
            "start",
            "end",
            "strand",
            "stable_id",
            "version",
            "biotype",
            "source",
            "is_canonical",
            "gene_stable_id",
            "gene_symbol",
            "gene_symbol_source",
            "gene_hgnc_id",
            "cds_start",
            "cds_end",
            "exon_count",
            "exons",
            "peptide_seq",
            "tsl",
            "appris",
            "mane_select",
        ):
            assert c in cols, f"Missing transcript column: {c}"

    def test_transcript_chrom_is_22(self, built_cache):
        _, _, _, tables = built_cache
        assert set(tables["transcript"].column("chrom").to_pylist()) == {"22"}

    def test_transcript_sorted_by_start(self, built_cache):
        _, _, _, tables = built_cache
        starts = tables["transcript"].column("start").to_pylist()
        assert starts == sorted(starts)

    def test_transcript_unique_stable_ids(self, built_cache):
        _, _, _, tables = built_cache
        ids = tables["transcript"].column("stable_id").to_pylist()
        assert len(ids) == len(set(ids)), "Duplicate transcript stable_ids"

    def test_transcript_no_null_keys(self, built_cache):
        _, _, _, tables = built_cache
        t = tables["transcript"]
        for col in ("chrom", "start", "stable_id", "biotype"):
            assert t.column(col).null_count == 0, (
                f"Unexpected nulls in transcript.{col}"
            )

    def test_transcript_stable_id_format(self, built_cache):
        _, _, _, tables = built_cache
        ids = tables["transcript"].column("stable_id").to_pylist()
        assert all(i.startswith("ENST") for i in ids)

    def test_transcript_biotypes(self, built_cache):
        _, _, _, tables = built_cache
        biotypes = sorted(set(tables["transcript"].column("biotype").to_pylist()))
        assert "protein_coding" in biotypes
        assert "lncRNA" in biotypes
        assert len(biotypes) == 10

    def test_transcript_gene_symbols(self, built_cache):
        _, _, _, tables = built_cache
        symbols = sorted(
            set(v for v in tables["transcript"].column("gene_symbol").to_pylist() if v)
        )
        assert "XKR3" in symbols
        assert "GAB4" in symbols
        assert len(symbols) == 27

    def test_transcript_canonical_count(self, built_cache):
        _, _, _, tables = built_cache
        canonical = sum(
            1 for v in tables["transcript"].column("is_canonical").to_pylist() if v
        )
        assert canonical == 44

    def test_transcript_exons_is_list_of_struct(self, built_cache):
        """exons column should be list<struct<start, end, phase>>."""
        _, _, _, tables = built_cache
        exons_type = tables["transcript"].schema.field("exons").type
        assert str(exons_type).startswith("list<")

    # ── Exon (396 rows, 34 cols) ────────────────────────────────────

    def test_exon_row_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["exon"].num_rows == 396

    def test_exon_column_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["exon"].num_columns == 34

    def test_exon_required_columns(self, built_cache):
        _, _, _, tables = built_cache
        cols = tables["exon"].column_names
        for c in (
            "chrom",
            "start",
            "end",
            "strand",
            "stable_id",
            "version",
            "phase",
            "end_phase",
            "transcript_id",
            "gene_stable_id",
            "exon_number",
        ):
            assert c in cols, f"Missing exon column: {c}"

    def test_exon_no_null_keys(self, built_cache):
        _, _, _, tables = built_cache
        t = tables["exon"]
        for col in ("transcript_id", "start", "stable_id"):
            assert t.column(col).null_count == 0, f"Unexpected nulls in exon.{col}"

    def test_exon_transcript_count(self, built_cache):
        """Exons should span exactly 106 unique transcripts."""
        _, _, _, tables = built_cache
        tx_ids = set(tables["exon"].column("transcript_id").to_pylist())
        assert len(tx_ids) == 106

    def test_exon_number_range(self, built_cache):
        _, _, _, tables = built_cache
        nums = tables["exon"].column("exon_number").to_pylist()
        assert min(nums) == 1
        assert max(nums) == 12

    def test_exon_stable_id_format(self, built_cache):
        _, _, _, tables = built_cache
        ids = tables["exon"].column("stable_id").to_pylist()
        assert all(i.startswith("ENSE") for i in ids)

    # ── Translation core (7 rows, 8 cols) ───────────────────────────

    def test_translation_core_row_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["translation_core"].num_rows == 7

    def test_translation_core_column_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["translation_core"].num_columns == 8

    def test_translation_core_required_columns(self, built_cache):
        _, _, _, tables = built_cache
        cols = tables["translation_core"].column_names
        for c in (
            "transcript_id",
            "stable_id",
            "version",
            "cds_len",
            "protein_len",
            "translation_seq",
            "cds_sequence",
            "protein_features",
        ):
            assert c in cols, f"Missing translation_core column: {c}"

    def test_translation_core_no_null_keys(self, built_cache):
        _, _, _, tables = built_cache
        t = tables["translation_core"]
        for col in ("transcript_id", "stable_id"):
            assert t.column(col).null_count == 0

    def test_translation_core_transcript_ids(self, built_cache):
        _, _, _, tables = built_cache
        tx_ids = sorted(tables["translation_core"].column("transcript_id").to_pylist())
        assert tx_ids == [
            "ENST00000331428",
            "ENST00000359963",
            "ENST00000400588",
            "ENST00000465611",
            "ENST00000643316",
            "ENST00000651146",
            "ENST00000684488",
        ]

    def test_translation_core_stable_ids(self, built_cache):
        _, _, _, tables = built_cache
        ids = sorted(tables["translation_core"].column("stable_id").to_pylist())
        assert ids == [
            "ENSP00000331704",
            "ENSP00000353048",
            "ENSP00000383431",
            "ENSP00000428584",
            "ENSP00000495950",
            "ENSP00000498845",
            "ENSP00000507478",
        ]

    def test_translation_core_cds_lens(self, built_cache):
        _, _, _, tables = built_cache
        cds_lens = sorted(tables["translation_core"].column("cds_len").to_pylist())
        assert cds_lens == [575, 626, 1380, 1380, 1674, 1725, 2055]

    def test_translation_core_all_have_sequences(self, built_cache):
        _, _, _, tables = built_cache
        seqs = tables["translation_core"].column("translation_seq").to_pylist()
        assert all(s is not None and len(s) > 0 for s in seqs)

    def test_translation_core_protein_features_is_list(self, built_cache):
        _, _, _, tables = built_cache
        pf_type = tables["translation_core"].schema.field("protein_features").type
        assert str(pf_type).startswith("list<")

    # ── Translation sift (7 rows, 6 cols) ───────────────────────────

    def test_translation_sift_row_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["translation_sift"].num_rows == 7

    def test_translation_sift_column_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["translation_sift"].num_columns == 6

    def test_translation_sift_required_columns(self, built_cache):
        _, _, _, tables = built_cache
        cols = tables["translation_sift"].column_names
        for c in (
            "transcript_id",
            "chrom",
            "start",
            "end",
            "sift_predictions",
            "polyphen_predictions",
        ):
            assert c in cols, f"Missing translation_sift column: {c}"

    def test_translation_sift_chrom_is_22(self, built_cache):
        _, _, _, tables = built_cache
        assert set(tables["translation_sift"].column("chrom").to_pylist()) == {"22"}

    def test_translation_sift_all_have_sift_predictions(self, built_cache):
        _, _, _, tables = built_cache
        preds = tables["translation_sift"].column("sift_predictions").to_pylist()
        assert all(p is not None and len(p) > 0 for p in preds)

    def test_translation_sift_all_have_polyphen_predictions(self, built_cache):
        _, _, _, tables = built_cache
        preds = tables["translation_sift"].column("polyphen_predictions").to_pylist()
        assert all(p is not None and len(p) > 0 for p in preds)

    def test_translation_sift_predictions_schema(self, built_cache):
        """sift_predictions should be list<struct<position, amino_acid, prediction, score>>."""
        _, _, _, tables = built_cache
        t = tables["translation_sift"]
        sift_type = t.schema.field("sift_predictions").type
        assert str(sift_type).startswith("list<")
        # Check struct fields
        first_row = t.column("sift_predictions")[0].as_py()
        assert "position" in first_row[0]
        assert "amino_acid" in first_row[0]
        assert "prediction" in first_row[0]
        assert "score" in first_row[0]

    # ── Regulatory (43 rows, 31 cols) ───────────────────────────────

    def test_regulatory_row_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["regulatory"].num_rows == 43

    def test_regulatory_column_count(self, built_cache):
        _, _, _, tables = built_cache
        assert tables["regulatory"].num_columns == 31

    def test_regulatory_required_columns(self, built_cache):
        _, _, _, tables = built_cache
        cols = tables["regulatory"].column_names
        for c in (
            "chrom",
            "start",
            "end",
            "strand",
            "stable_id",
            "feature_type",
            "cell_types",
        ):
            assert c in cols, f"Missing regulatory column: {c}"

    def test_regulatory_chrom_is_22(self, built_cache):
        _, _, _, tables = built_cache
        assert set(tables["regulatory"].column("chrom").to_pylist()) == {"22"}

    def test_regulatory_sorted_by_start(self, built_cache):
        _, _, _, tables = built_cache
        starts = tables["regulatory"].column("start").to_pylist()
        assert starts == sorted(starts)

    def test_regulatory_no_null_keys(self, built_cache):
        _, _, _, tables = built_cache
        t = tables["regulatory"]
        for col in ("chrom", "start", "stable_id", "feature_type"):
            assert t.column(col).null_count == 0

    def test_regulatory_feature_types(self, built_cache):
        _, _, _, tables = built_cache
        ftypes = sorted(set(tables["regulatory"].column("feature_type").to_pylist()))
        assert ftypes == [
            "CTCF_binding_site",
            "enhancer",
            "open_chromatin_region",
            "promoter",
        ]

    def test_regulatory_stable_id_format(self, built_cache):
        _, _, _, tables = built_cache
        ids = tables["regulatory"].column("stable_id").to_pylist()
        assert all(i.startswith("ENSR") for i in ids)

    # ── Fjall stores ────────────────────────────────────────────────

    def test_fjall_variation_stats(self, built_cache):
        out, _, native_result, _ = built_cache
        var = [s for s in native_result if s[0] == "variation"][0]
        _, _, fjall = var
        assert fjall is not None
        variants, positions, total_bytes, secs = fjall
        assert variants == 763
        assert positions == 743
        assert total_bytes > 0
        assert os.path.isdir(os.path.join(out, "variation.fjall"))

    def test_fjall_sift_stats(self, built_cache):
        out, _, native_result, _ = built_cache
        sift = [s for s in native_result if s[0] == "translation_sift"][0]
        _, _, fjall = sift
        assert fjall is not None
        variants, positions, total_bytes, secs = fjall
        assert variants == 7
        assert positions == 7
        assert os.path.isdir(os.path.join(out, "translation_sift.fjall"))

    def test_fjall_disabled(self, skip_if_no_ensembl_cache):
        """build_fjall=False should produce no fjall directories or stats."""
        with tempfile.TemporaryDirectory() as out:
            result = _build_cache(str(ENSEMBL_CACHE_DIR), out, 2, False, 3, 112, None)
            for entity, files, fjall in result:
                assert fjall is None, f"{entity} has fjall stats with build_fjall=False"
            assert not os.path.isdir(os.path.join(out, "variation.fjall"))

    # ── Progress callback ───────────────────────────────────────────

    def test_progress_callback_invoked(self, skip_if_no_ensembl_cache):
        events: list[tuple] = []

        def cb(entity, fmt, batch_rows, total_rows, total_expected):
            events.append((entity, fmt, batch_rows, total_rows, total_expected))

        with tempfile.TemporaryDirectory() as out:
            _build_cache(str(ENSEMBL_CACHE_DIR), out, 2, True, 3, 112, cb)

        assert len(events) > 0
        entities = {e[0] for e in events}
        formats = {e[1] for e in events}
        assert "variation" in entities
        assert "parquet" in formats
        for e in events:
            assert len(e) == 5
            assert isinstance(e[0], str)
            assert e[1] in ("parquet", "fjall")
            assert all(isinstance(v, int) for v in e[2:])

    # ── Python wrapper end-to-end ───────────────────────────────────

    def test_python_build_cache_end_to_end(self, skip_if_no_ensembl_cache):
        with tempfile.TemporaryDirectory() as out:
            result = vepyr.build_cache(
                115,
                out,
                local_cache=str(ENSEMBL_CACHE_DIR),
                build_fjall=True,
                show_progress=False,
            )
            assert all(isinstance(p, str) and isinstance(r, int) for p, r in result)
            assert sum(r for _, r in result) == 1322

    def test_output_directory_layout(self, skip_if_no_ensembl_cache):
        with tempfile.TemporaryDirectory() as out:
            vepyr.build_cache(
                115,
                out,
                local_cache=str(ENSEMBL_CACHE_DIR),
                build_fjall=True,
                show_progress=False,
            )
            parquet_dir = os.path.join(out, "parquet", "115_GRCh38_vep")
            assert os.path.isdir(parquet_dir)
            for entity in (
                "variation",
                "transcript",
                "exon",
                "regulatory",
                "translation_core",
                "translation_sift",
            ):
                entity_dir = os.path.join(parquet_dir, entity)
                assert os.path.isdir(entity_dir), f"Missing dir: {entity}"
                parquets = [f for f in os.listdir(entity_dir) if f.endswith(".parquet")]
                assert len(parquets) > 0, f"No parquet files in {entity}"
            assert os.path.isdir(os.path.join(parquet_dir, "variation.fjall"))
            assert os.path.isdir(os.path.join(parquet_dir, "translation_sift.fjall"))
