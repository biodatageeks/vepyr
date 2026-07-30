from __future__ import annotations

import json
from pathlib import Path
import sys
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

SCRIPTS_DIR = Path(__file__).parents[1] / "e2e-testing" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import rebuild_cache_entity as rebuild  # noqa: E402


def _motif_table(
    release: str = "116",
    source_type: str = "merged",
    *,
    transcription_factors: list[str | None] | None = None,
) -> pa.Table:
    transcription_factors = transcription_factors or ["TF1", "TF2"]
    schema = pa.schema(
        [
            pa.field("motif_id", pa.string()),
            pa.field("binding_matrix", pa.string()),
            pa.field("binding_matrix_length", pa.int64()),
            pa.field("binding_matrix_elements", pa.list_(pa.float64())),
            pa.field("binding_matrix_unit", pa.string()),
            pa.field("motif_seq", pa.string()),
            pa.field("transcription_factors", pa.string()),
        ],
        metadata={
            rebuild.CACHE_VERSION_METADATA_KEY: release.encode(),
            rebuild.CACHE_SOURCE_METADATA_KEY: source_type.encode(),
        },
    )
    return pa.Table.from_arrays(
        [
            pa.array(["MF1", "MF2"]),
            pa.array(["MA1", "MA2"]),
            pa.array([2, 2]),
            pa.array([[0.1, 0.9], [0.2, 0.8]]),
            pa.array(["bits", "bits"]),
            pa.array(["AC", "GT"]),
            pa.array(transcription_factors),
        ],
        schema=schema,
    )


def _write_motif(
    motif_dir: Path,
    *,
    release: str = "116",
    source_type: str = "merged",
    shard_count: int = 2,
) -> list[Path]:
    motif_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    shards = []
    for index in range(1, shard_count + 1):
        shard = motif_dir / f"chr{index}.parquet"
        pq.write_table(_motif_table(release, source_type), shard)
        entries.append(
            {
                "chrom": f"chr{index}",
                "dataset": shard.name,
                "rows": 2,
            }
        )
        shards.append(shard)
    (motif_dir / "chrom_manifest.json").write_text(json.dumps(entries))
    return shards


def _generic_table(
    entity: str,
    release: str = "116",
    source_type: str = "merged",
) -> pa.Table:
    fields = [pa.field("stable_id", pa.string())]
    arrays: list[pa.Array] = [pa.array([f"{entity}-1", f"{entity}-2"])]
    if entity == "variation" and release == "116":
        fields.append(pa.field("clin_sig_ref_allele", pa.string(), nullable=True))
        arrays.append(pa.array(["A", None]))
    schema = pa.schema(
        fields,
        metadata={
            rebuild.CACHE_VERSION_METADATA_KEY: release.encode(),
            rebuild.CACHE_SOURCE_METADATA_KEY: source_type.encode(),
        },
    )
    return pa.Table.from_arrays(arrays, schema=schema)


def _write_generic_entity(
    entity_dir: Path,
    entity: str,
    *,
    release: str = "116",
    source_type: str = "merged",
    shard_count: int = 1,
) -> list[Path]:
    entity_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    shards = []
    for index in range(1, shard_count + 1):
        shard = entity_dir / f"chr{index}.parquet"
        pq.write_table(_generic_table(entity, release, source_type), shard)
        entries.append(
            {
                "chrom": f"chr{index}",
                "dataset": shard.name,
                "rows": 2,
            }
        )
        shards.append(shard)
    (entity_dir / "chrom_manifest.json").write_text(json.dumps(entries))
    return shards


@pytest.mark.parametrize(
    "entity",
    ["transcript", "exon", "translation_core", "translation_sift", "regulatory"],
)
def test_verify_generic_entity_checks_all_shards(entity, tmp_path):
    entity_dir = tmp_path / entity
    _write_generic_entity(entity_dir, entity, shard_count=2)

    report = rebuild.verify_entity_dir(entity_dir, entity, "116", "merged")

    assert report.entity == entity
    assert report.shards == 2
    assert report.rows == 4
    assert report.non_empty == {}


def test_verify_variation_requires_release_116_clin_sig_ref_allele(tmp_path):
    variation = tmp_path / "variation"
    shards = _write_generic_entity(variation, "variation", release="115")
    table = pq.read_table(shards[0]).replace_schema_metadata(
        {
            rebuild.CACHE_VERSION_METADATA_KEY: b"116",
            rebuild.CACHE_SOURCE_METADATA_KEY: b"merged",
        }
    )
    pq.write_table(table, shards[0])

    with pytest.raises(
        rebuild.VerificationError,
        match="lacks clin_sig_ref_allele",
    ):
        rebuild.verify_entity_dir(variation, "variation", "116", "merged")


def test_verify_translation_covers_both_generated_entities(tmp_path):
    _write_generic_entity(tmp_path / "translation_core", "translation_core")
    _write_generic_entity(tmp_path / "translation_sift", "translation_sift")

    reports = rebuild.verify_selected_entity(
        tmp_path,
        "translation",
        "116",
        "merged",
    )

    assert [report.entity for report in reports] == [
        "translation_core",
        "translation_sift",
    ]
    assert [report.rows for report in reports] == [2, 2]


def test_verify_motif_checks_every_manifest_shard(tmp_path):
    motif = tmp_path / "motif"
    shards = _write_motif(motif)
    second = pq.read_table(shards[1]).drop(["transcription_factors"])
    pq.write_table(second, shards[1])

    with pytest.raises(
        rebuild.VerificationError,
        match=r"chr2\.parquet: missing motif columns transcription_factors",
    ):
        rebuild.verify_entity_dir(motif, "motif", "116", "merged")


def test_verify_motif_checks_identity_on_every_shard(tmp_path):
    motif = tmp_path / "motif"
    shards = _write_motif(motif)
    second = pq.read_table(shards[1]).replace_schema_metadata(
        {
            rebuild.CACHE_VERSION_METADATA_KEY: b"115",
            rebuild.CACHE_SOURCE_METADATA_KEY: b"merged",
        }
    )
    pq.write_table(second, shards[1])

    with pytest.raises(rebuild.VerificationError, match="cache release '115'"):
        rebuild.verify_entity_dir(motif, "motif", "116", "merged")


def test_verify_motif_reconciles_manifest_and_footer_rows(tmp_path):
    motif = tmp_path / "motif"
    _write_motif(motif, shard_count=1)
    (motif / "chrom_manifest.json").write_text(
        json.dumps([{"chrom": "chr1", "dataset": "chr1.parquet", "rows": 3}])
    )

    with pytest.raises(rebuild.VerificationError, match="footer has 2"):
        rebuild.verify_entity_dir(motif, "motif", "116", "merged")


def test_verify_motif_rejects_unreferenced_shards(tmp_path):
    motif = tmp_path / "motif"
    _write_motif(motif, shard_count=1)
    pq.write_table(_motif_table(), motif / "chr2.parquet")

    with pytest.raises(rebuild.VerificationError, match="1 unreferenced"):
        rebuild.verify_entity_dir(motif, "motif", "116", "merged")


def test_verify_motif_requires_complete_116_identity_values(tmp_path):
    motif = tmp_path / "motif"
    shards = _write_motif(motif, shard_count=1)
    pq.write_table(
        _motif_table(transcription_factors=["TF1", None]),
        shards[0],
    )

    with pytest.raises(
        rebuild.VerificationError,
        match="transcription_factors=1/2",
    ):
        rebuild.verify_entity_dir(motif, "motif", "116", "merged")


def test_verify_motif_accepts_empty_115_manifest(tmp_path):
    motif = tmp_path / "motif"
    motif.mkdir()
    (motif / "chrom_manifest.json").write_text("[]")

    report = rebuild.verify_entity_dir(motif, "motif", "115", "ensembl")

    assert report.shards == 0
    assert report.rows == 0


def test_verify_motif_reports_complete_multi_shard_totals(tmp_path):
    motif = tmp_path / "motif"
    _write_motif(motif)

    report = rebuild.verify_entity_dir(motif, "motif", "116", "merged")

    assert report.shards == 2
    assert report.rows == 4
    assert report.non_empty["binding_matrix"] == 4
    assert report.non_empty["transcription_factors"] == 4


def test_main_uses_public_release_aware_builder_and_retains_backup(tmp_path):
    source = tmp_path / "raw" / "116_GRCh38"
    source.mkdir(parents=True)
    (source / "info.txt").write_text("cache_version 116\n")
    target = tmp_path / "116_GRCh38_merged"
    live_motif = target / "motif"
    _write_motif(live_motif, shard_count=1)

    def fake_build_cache_entity(
        release: int,
        cache_dir: str,
        entity: str,
        **kwargs,
    ) -> list[tuple[str, int]]:
        assert release == 116
        assert entity == "motif"
        assert kwargs == {
            "cache_type": "merged",
            "partitions": 3,
            "local_cache": str(source),
            "overwrite": True,
        }
        staged_motif = Path(cache_dir) / "116_GRCh38_merged" / "motif"
        shards = _write_motif(staged_motif, shard_count=1)
        return [(str(shard), 2) for shard in shards]

    with patch(
        "vepyr.build_cache_entity",
        side_effect=fake_build_cache_entity,
    ) as public_builder:
        result = rebuild.main(
            [
                "--run",
                "--release",
                "116",
                "--entity",
                "motif",
                "--cache-type",
                "merged",
                "--partitions",
                "3",
                "--target",
                str(target),
                "--local-cache",
                str(source),
            ]
        )

    assert result == 0
    public_builder.assert_called_once()
    assert rebuild.verify_entity_dir(live_motif, "motif", "116", "merged").rows == 2
    backups = list(tmp_path.glob(".116_GRCh38_merged.motif-backup-*"))
    assert len(backups) == 1
    assert rebuild.verify_entity_dir(backups[0], "motif", "116", "merged").rows == 2
    assert not list(tmp_path.glob(".116_GRCh38_merged.motif-rebuild-*"))


def test_main_rebuilds_requested_non_motif_entity(tmp_path):
    source = tmp_path / "raw" / "116_GRCh38"
    source.mkdir(parents=True)
    (source / "info.txt").write_text("cache_version 116\n")
    target = tmp_path / "116_GRCh38_merged"
    live_variation = target / "variation"
    _write_generic_entity(live_variation, "variation")

    def fake_build_cache_entity(
        release: int,
        cache_dir: str,
        entity: str,
        **kwargs,
    ) -> list[tuple[str, int]]:
        assert release == 116
        assert entity == "variation"
        assert kwargs["cache_type"] == "merged"
        staged = Path(cache_dir) / "116_GRCh38_merged" / "variation"
        shards = _write_generic_entity(staged, "variation", shard_count=1)
        return [(str(shard), 2) for shard in shards]

    with patch(
        "vepyr.build_cache_entity",
        side_effect=fake_build_cache_entity,
    ):
        result = rebuild.main(
            [
                "--run",
                "--release",
                "116",
                "--entity",
                "variation",
                "--target",
                str(target),
                "--local-cache",
                str(source),
            ]
        )

    assert result == 0
    assert (
        rebuild.verify_entity_dir(
            live_variation,
            "variation",
            "116",
            "merged",
        ).rows
        == 2
    )
    backups = list(tmp_path.glob(".116_GRCh38_merged.variation-backup-*"))
    assert len(backups) == 1
    assert (
        rebuild.verify_entity_dir(
            backups[0],
            "variation",
            "116",
            "merged",
        ).rows
        == 2
    )


def test_main_dry_run_performs_preflight_without_building(tmp_path, capsys):
    source = tmp_path / "raw" / "115_GRCh38"
    source.mkdir(parents=True)
    (source / "info.txt").write_text("cache_version 115\n")
    target = tmp_path / "115_GRCh38_merged"
    target.mkdir()

    with patch("vepyr.build_cache_entity") as public_builder:
        result = rebuild.main(
            [
                "--release",
                "115",
                "--entity",
                "motif",
                "--target",
                str(target),
                "--local-cache",
                str(source),
            ]
        )

    assert result == 0
    public_builder.assert_not_called()
    assert "Dry run complete" in capsys.readouterr().out


def test_main_blocked_preflight_does_not_build(tmp_path, capsys):
    with patch("vepyr.build_cache_entity") as public_builder:
        result = rebuild.main(
            [
                "--run",
                "--release",
                "116",
                "--entity",
                "variation",
                "--target",
                str(tmp_path / "missing-target"),
                "--local-cache",
                str(tmp_path / "missing-source"),
            ]
        )

    assert result == 2
    public_builder.assert_not_called()
    assert "BLOCKED" in capsys.readouterr().err


def test_main_rejects_empty_targeted_rebuild_before_swap(tmp_path, capsys):
    source = tmp_path / "raw" / "116_GRCh38"
    source.mkdir(parents=True)
    (source / "info.txt").write_text("cache_version 116\n")
    target = tmp_path / "116_GRCh38_merged"
    live_transcript = target / "transcript"
    _write_generic_entity(live_transcript, "transcript")

    def fake_build_cache_entity(
        _release: int,
        cache_dir: str,
        _entity: str,
        **_kwargs,
    ) -> list[tuple[str, int]]:
        staged = Path(cache_dir) / "116_GRCh38_merged" / "transcript"
        _write_generic_entity(staged, "transcript", shard_count=0)
        return []

    with patch("vepyr.build_cache_entity", side_effect=fake_build_cache_entity):
        result = rebuild.main(
            [
                "--run",
                "--release",
                "116",
                "--entity",
                "transcript",
                "--target",
                str(target),
                "--local-cache",
                str(source),
            ]
        )

    assert result == 1
    assert (
        rebuild.verify_entity_dir(
            live_transcript,
            "transcript",
            "116",
            "merged",
        ).rows
        == 2
    )
    assert not list(tmp_path.glob(".116_GRCh38_merged.transcript-backup-*"))
    assert list(tmp_path.glob(".116_GRCh38_merged.transcript-rebuild-*"))
    assert "row-count reconciliation failed" in capsys.readouterr().err


def test_main_restores_live_motif_when_swap_fails(tmp_path, monkeypatch):
    source = tmp_path / "raw" / "116_GRCh38"
    source.mkdir(parents=True)
    (source / "info.txt").write_text("cache_version 116\n")
    target = tmp_path / "116_GRCh38_merged"
    live_motif = target / "motif"
    _write_motif(live_motif, shard_count=1)

    def fake_build_cache_entity(
        _release: int,
        cache_dir: str,
        _entity: str,
        **_kwargs,
    ) -> list[tuple[str, int]]:
        staged_motif = Path(cache_dir) / "116_GRCh38_merged" / "motif"
        shards = _write_motif(staged_motif, shard_count=1)
        return [(str(shard), 2) for shard in shards]

    original_rename = Path.rename

    def fail_staged_swap(path: Path, destination: Path):
        if ".motif-rebuild-" in str(path):
            raise OSError("injected motif swap failure")
        return original_rename(path, destination)

    monkeypatch.setattr(Path, "rename", fail_staged_swap)

    with (
        patch(
            "vepyr.build_cache_entity",
            side_effect=fake_build_cache_entity,
        ),
        pytest.raises(OSError, match="injected motif swap failure"),
    ):
        rebuild.main(
            [
                "--run",
                "--release",
                "116",
                "--entity",
                "motif",
                "--cache-type",
                "merged",
                "--target",
                str(target),
                "--local-cache",
                str(source),
            ]
        )

    assert rebuild.verify_entity_dir(live_motif, "motif", "116", "merged").rows == 2
    assert not list(tmp_path.glob(".116_GRCh38_merged.motif-backup-*"))
    assert list(tmp_path.glob(".116_GRCh38_merged.motif-rebuild-*"))


def test_translation_swap_restores_both_outputs_after_partial_failure(
    tmp_path,
    monkeypatch,
):
    target = tmp_path / "116_GRCh38_merged"
    staged = tmp_path / "staged"
    entities = ("translation_core", "translation_sift")
    backups = {
        entity: tmp_path / f".116_GRCh38_merged.{entity}-backup" for entity in entities
    }
    for entity in entities:
        (target / entity).mkdir(parents=True)
        (target / entity / "old").write_text(entity)
        (staged / entity).mkdir(parents=True)
        (staged / entity / "new").write_text(entity)

    original_rename = Path.rename

    def fail_second_install(path: Path, destination: Path):
        if path == staged / "translation_sift":
            raise OSError("injected second-output swap failure")
        return original_rename(path, destination)

    monkeypatch.setattr(Path, "rename", fail_second_install)

    with pytest.raises(OSError, match="second-output"):
        rebuild._swap_with_rollback(target, staged, entities, backups)

    for entity in entities:
        assert (target / entity / "old").read_text() == entity
        assert (staged / entity / "new").read_text() == entity
        assert not backups[entity].exists()
