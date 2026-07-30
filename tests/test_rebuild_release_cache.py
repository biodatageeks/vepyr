from __future__ import annotations

import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

SCRIPTS_DIR = Path(__file__).parents[1] / "e2e-testing" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import rebuild_release_cache as rebuild  # noqa: E402


def _write_entity(
    root: Path,
    entity: str,
    release: str,
    *,
    source_type: str = "merged",
    include_identity: bool = True,
) -> None:
    entity_dir = root / entity
    entity_dir.mkdir(parents=True)
    metadata = {}
    if include_identity:
        metadata = {
            rebuild.CACHE_VERSION_METADATA_KEY: release.encode(),
            rebuild.CACHE_SOURCE_METADATA_KEY: source_type.encode(),
        }

    fields = [pa.field("value", pa.string(), nullable=True)]
    arrays = [pa.array(["x"])]
    if entity == "variation" and release == "116":
        fields.append(pa.field("clin_sig_ref_allele", pa.string(), nullable=True))
        arrays.append(pa.array(["A"]))
    if entity == "motif" and release == "116":
        fields.extend(
            [
                pa.field("binding_matrix", pa.string(), nullable=True),
                pa.field("transcription_factors", pa.string(), nullable=True),
            ]
        )
        arrays.extend([pa.array(["MATRIX"]), pa.array(["TF"])])

    schema = pa.schema(fields, metadata=metadata)
    pq.write_table(
        pa.Table.from_arrays(arrays, schema=schema),
        entity_dir / "chr1.parquet",
    )
    (entity_dir / "chrom_manifest.json").write_text(
        json.dumps([{"chrom": "chr1", "dataset": "chr1.parquet", "rows": 1}])
    )


def _write_cache(root: Path, release: str = "116") -> None:
    for entity in rebuild.ENTITIES:
        _write_entity(root, entity, release)


def _rewrite_transcript_distribution(root: Path, rows_by_chrom: dict[str, int]) -> None:
    entity_dir = root / "transcript"
    for shard in entity_dir.glob("*.parquet"):
        shard.unlink()
    schema = pa.schema(
        [pa.field("value", pa.string(), nullable=True)],
        metadata={
            rebuild.CACHE_VERSION_METADATA_KEY: b"116",
            rebuild.CACHE_SOURCE_METADATA_KEY: b"merged",
        },
    )
    entries = []
    for chrom, rows in rows_by_chrom.items():
        dataset = f"{chrom}.parquet"
        pq.write_table(
            pa.Table.from_arrays(
                [pa.array(["x"] * rows, type=pa.string())], schema=schema
            ),
            entity_dir / dataset,
        )
        entries.append({"chrom": chrom, "dataset": dataset, "rows": rows})
    (entity_dir / "chrom_manifest.json").write_text(json.dumps(entries))


def _write_raw_source(root: Path) -> None:
    root.mkdir(parents=True)
    (root / "info.txt").write_text("cache_version 116\n")
    for index in range(99):
        (root / f"source-{index}").write_text("")


def test_verify_cache_checks_every_entity_and_release_contract(tmp_path):
    cache = tmp_path / "116_GRCh38_merged"
    _write_cache(cache)

    report = rebuild.verify_cache(cache, "116", "merged")

    assert report.total_rows == len(rebuild.ENTITIES)
    assert report.rows_by_entity()["variation"] == 1
    assert report.motif_non_empty == {
        "binding_matrix": 1,
        "transcription_factors": 1,
    }


def test_verify_cache_rejects_missing_parquet_release_metadata(tmp_path):
    cache = tmp_path / "116_GRCh38_merged"
    _write_cache(cache)
    shard = cache / "exon" / "chr1.parquet"
    table = pq.read_table(shard)
    pq.write_table(table.replace_schema_metadata({}), shard)

    with pytest.raises(rebuild.VerificationError, match="cache_version"):
        rebuild.verify_cache(cache, "116", "merged")


def test_verify_cache_rejects_116_without_clinvar_reference_field(tmp_path):
    cache = tmp_path / "116_GRCh38_merged"
    _write_cache(cache)
    shard = cache / "variation" / "chr1.parquet"
    table = pq.read_table(shard, columns=["value"])
    table = table.replace_schema_metadata(
        {
            rebuild.CACHE_VERSION_METADATA_KEY: b"116",
            rebuild.CACHE_SOURCE_METADATA_KEY: b"merged",
        }
    )
    pq.write_table(table, shard)

    with pytest.raises(rebuild.VerificationError, match="clin_sig_ref_allele"):
        rebuild.verify_cache(cache, "116", "merged")


def test_verify_cache_rejects_populated_115_clinvar_reference_alleles(tmp_path):
    cache = tmp_path / "115_GRCh38_merged"
    _write_cache(cache, release="115")
    shard = cache / "variation" / "chr1.parquet"
    table = pq.read_table(shard).append_column(
        "clin_sig_ref_allele",
        pa.array(["A"]),
    )
    pq.write_table(table, shard)

    with pytest.raises(rebuild.VerificationError, match="populated row"):
        rebuild.verify_cache(cache, "115", "merged")


def test_verify_cache_rejects_manifest_footer_row_mismatch(tmp_path):
    cache = tmp_path / "115_GRCh38_merged"
    _write_cache(cache, release="115")
    manifest = cache / "transcript" / "chrom_manifest.json"
    manifest.write_text(
        json.dumps([{"chrom": "chr1", "dataset": "chr1.parquet", "rows": 2}])
    )

    with pytest.raises(rebuild.VerificationError, match="footer has 1"):
        rebuild.verify_cache(cache, "115", "merged")


def test_verify_cache_rejects_empty_116_motif_entity(tmp_path):
    cache = tmp_path / "116_GRCh38_merged"
    _write_cache(cache)
    motif_dir = cache / "motif"
    (motif_dir / "chr1.parquet").unlink()
    (motif_dir / "chrom_manifest.json").write_text("[]")

    with pytest.raises(rebuild.VerificationError, match="at least one row"):
        rebuild.verify_cache(cache, "116", "merged")


def test_verify_cache_rejects_an_empty_required_entity(tmp_path):
    cache = tmp_path / "115_GRCh38_merged"
    _write_cache(cache, release="115")
    transcript_dir = cache / "transcript"
    (transcript_dir / "chr1.parquet").unlink()
    (transcript_dir / "chrom_manifest.json").write_text("[]")

    with pytest.raises(rebuild.VerificationError, match="transcript.*at least one row"):
        rebuild.verify_cache(cache, "115", "merged")


def test_verify_cache_allows_the_release_115_empty_motif_contract(tmp_path):
    cache = tmp_path / "115_GRCh38_merged"
    _write_cache(cache, release="115")
    motif_dir = cache / "motif"
    (motif_dir / "chr1.parquet").unlink()
    (motif_dir / "chrom_manifest.json").write_text("[]")

    report = rebuild.verify_cache(cache, "115", "merged")

    assert report.rows_by_entity()["motif"] == 0


def test_verify_cache_rejects_nonempty_release_115_motif_data(tmp_path):
    cache = tmp_path / "115_GRCh38_merged"
    _write_cache(cache, release="115")

    with pytest.raises(rebuild.VerificationError, match="motif cache must be empty"):
        rebuild.verify_cache(cache, "115", "merged")


@pytest.mark.parametrize(
    ("column", "values"),
    [
        ("binding_matrix", ["MATRIX", None]),
        ("transcription_factors", ["TF", ""]),
    ],
)
def test_verify_cache_requires_every_116_motif_value(column, values, tmp_path):
    cache = tmp_path / "116_GRCh38_merged"
    _write_cache(cache)
    motif_dir = cache / "motif"
    populated = {
        "binding_matrix": ["MATRIX1", "MATRIX2"],
        "transcription_factors": ["TF1", "TF2"],
    }
    populated[column] = values
    schema = pa.schema(
        [
            pa.field("value", pa.string(), nullable=True),
            pa.field("binding_matrix", pa.string(), nullable=True),
            pa.field("transcription_factors", pa.string(), nullable=True),
        ],
        metadata={
            rebuild.CACHE_VERSION_METADATA_KEY: b"116",
            rebuild.CACHE_SOURCE_METADATA_KEY: b"merged",
        },
    )
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array(["x", "y"]),
                pa.array(populated["binding_matrix"]),
                pa.array(populated["transcription_factors"]),
            ],
            schema=schema,
        ),
        motif_dir / "chr1.parquet",
    )
    (motif_dir / "chrom_manifest.json").write_text(
        json.dumps([{"chrom": "chr1", "dataset": "chr1.parquet", "rows": 2}])
    )

    with pytest.raises(rebuild.VerificationError, match=f"{column} in 1 of 2 rows"):
        rebuild.verify_cache(cache, "116", "merged")


def test_print_report_formats_entity_name_and_counts(capsys):
    report = rebuild.CacheReport(
        cache_dir=Path("/cache"),
        release="115",
        source_type="merged",
        entities=(
            rebuild.EntityReport(
                "variation",
                463,
                1_332_332_652,
                {"chr1": 1_332_332_652},
            ),
        ),
        motif_non_empty={"binding_matrix": 0, "transcription_factors": 0},
    )

    rebuild._print_report(report)

    output = capsys.readouterr().out
    assert "variation" in output
    assert "463 shards" in output
    assert "1,332,332,652 rows" in output


def test_swap_rolls_back_if_replacement_rename_fails(tmp_path, monkeypatch):
    target = tmp_path / "cache"
    target.mkdir()
    (target / "old").write_text("old")
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "new").write_text("new")
    original_rename = Path.rename

    def fail_staging_rename(path: Path, destination: Path):
        if path == staging:
            raise OSError("injected replacement failure")
        return original_rename(path, destination)

    monkeypatch.setattr(Path, "rename", fail_staging_rename)

    with pytest.raises(OSError, match="injected"):
        rebuild._swap_with_rollback(staging, target, "STAMP")

    assert (target / "old").read_text() == "old"
    assert (staging / "new").read_text() == "new"
    assert not (tmp_path / "cache.backup-STAMP").exists()


def test_main_blocks_when_staging_space_is_insufficient(tmp_path, monkeypatch, capsys):
    source = tmp_path / "raw" / "116_GRCh38"
    _write_raw_source(source)
    target = tmp_path / "116_GRCh38_merged"
    _write_cache(target)
    monkeypatch.setattr(rebuild, "dir_size", lambda _path: 1_000)
    monkeypatch.setattr(
        rebuild.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=1),
    )

    with patch("vepyr.build_cache") as public_builder:
        result = rebuild.main(
            [
                "--run",
                "--release",
                "116",
                "--target",
                str(target),
                "--local-cache",
                str(source),
            ]
        )

    assert result == 2
    public_builder.assert_not_called()
    assert "insufficient free space" in capsys.readouterr().err


def test_main_blocks_when_live_manifest_disagrees_with_footer(tmp_path, capsys):
    source = tmp_path / "raw" / "116_GRCh38"
    _write_raw_source(source)
    target = tmp_path / "116_GRCh38_merged"
    _write_cache(target)
    (target / "transcript" / "chrom_manifest.json").write_text(
        json.dumps([{"chrom": "chr1", "dataset": "chr1.parquet", "rows": 2}])
    )

    with patch("vepyr.build_cache") as public_builder:
        result = rebuild.main(
            [
                "--run",
                "--release",
                "116",
                "--target",
                str(target),
                "--local-cache",
                str(source),
            ]
        )

    assert result == 2
    public_builder.assert_not_called()
    error = capsys.readouterr().err
    assert "cannot inventory existing target" in error
    assert "manifest declares 2 rows, footer has 1" in error


def test_main_rejects_row_count_drift_before_swap(tmp_path, monkeypatch, capsys):
    source = tmp_path / "raw" / "116_GRCh38"
    _write_raw_source(source)
    target = tmp_path / "116_GRCh38_merged"
    _write_cache(target)
    monkeypatch.setattr(
        rebuild.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=10**12),
    )

    def fake_build_cache(
        release: int,
        cache_dir: str,
        *,
        cache_type: str,
        **_kwargs,
    ) -> None:
        assert release == 116
        staged = Path(cache_dir) / f"116_GRCh38_{cache_type}"
        _write_cache(staged)
        transcript = staged / "transcript" / "chr1.parquet"
        schema = pq.read_schema(transcript)
        pq.write_table(
            pa.Table.from_arrays([pa.array(["x", "y"])], schema=schema),
            transcript,
        )
        (staged / "transcript" / "chrom_manifest.json").write_text(
            json.dumps([{"chrom": "chr1", "dataset": "chr1.parquet", "rows": 2}])
        )

    with patch("vepyr.build_cache", side_effect=fake_build_cache):
        result = rebuild.main(
            [
                "--run",
                "--release",
                "116",
                "--target",
                str(target),
                "--local-cache",
                str(source),
            ]
        )

    assert result == 1
    assert rebuild._footer_rows_by_entity_chrom(target)["transcript"] == {"chr1": 1}
    assert list(tmp_path.glob(".116_GRCh38_merged.rebuild-*"))
    assert "row-count reconciliation failed" in capsys.readouterr().err


def test_main_rejects_cross_chrom_row_redistribution_with_equal_total(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "raw" / "116_GRCh38"
    _write_raw_source(source)
    target = tmp_path / "116_GRCh38_merged"
    _write_cache(target)
    _rewrite_transcript_distribution(target, {"chr1": 1, "chr2": 1})
    monkeypatch.setattr(
        rebuild.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=10**12),
    )

    def fake_build_cache(
        _release: int,
        cache_dir: str,
        *,
        cache_type: str,
        **_kwargs,
    ) -> None:
        staged = Path(cache_dir) / f"116_GRCh38_{cache_type}"
        _write_cache(staged)
        _rewrite_transcript_distribution(staged, {"chr1": 0, "chr2": 2})

    with patch("vepyr.build_cache", side_effect=fake_build_cache):
        result = rebuild.main(
            [
                "--run",
                "--release",
                "116",
                "--target",
                str(target),
                "--local-cache",
                str(source),
            ]
        )

    assert result == 1
    assert rebuild._footer_rows_by_entity_chrom(target)["transcript"] == {
        "chr1": 1,
        "chr2": 1,
    }
    error = capsys.readouterr().err
    assert "transcript/chr1: old=1, new=0" in error
    assert "transcript/chr2: old=1, new=2" in error
