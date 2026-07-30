#!/usr/bin/env python3
"""Safely rebuild one raw entity of a converted VEP cache.

The command is a dry run unless ``--run`` is supplied. A real rebuild uses
the public, release-aware :func:`vepyr.build_cache_entity` API, writes into a
sibling staging directory, validates every manifest-referenced Parquet shard,
and swaps only after release/source metadata, schemas, and row counts pass.
Entity-specific checks cover the release-116 variation and motif contracts.
The previous generated entity directories are retained as timestamped backups.

The raw ``translation`` entity produces two generated entities,
``translation_core`` and ``translation_sift``. They are verified and swapped
as one transaction.

Examples:
    uv run python e2e-testing/scripts/rebuild_cache_entity.py \
        --release 116 --entity motif
    uv run python e2e-testing/scripts/rebuild_cache_entity.py \
        --release 115 --cache-type merged --entity variation --run
    uv run python e2e-testing/scripts/rebuild_cache_entity.py \
        --release 116 --cache-type ensembl --entity translation --run
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from comparison import profiles  # noqa: E402

CACHE_VERSION_METADATA_KEY = b"bio.vep.cache_version"
CACHE_SOURCE_METADATA_KEY = b"bio.vep.cache_source_type"
RAW_ENTITIES = (
    "variation",
    "transcript",
    "exon",
    "translation",
    "regulatory",
    "motif",
)
GENERATED_ENTITIES = {
    "variation": ("variation",),
    "transcript": ("transcript",),
    "exon": ("exon",),
    "translation": ("translation_core", "translation_sift"),
    "regulatory": ("regulatory",),
    "motif": ("motif",),
}
MOTIF_REQUIRED_COLUMNS = (
    "motif_id",
    "binding_matrix",
    "binding_matrix_length",
    "binding_matrix_elements",
    "binding_matrix_unit",
    "motif_seq",
    "transcription_factors",
)
MOTIF_FULLY_POPULATED_116_COLUMNS = (
    "motif_id",
    "binding_matrix",
    "transcription_factors",
)


class VerificationError(RuntimeError):
    """A staged generated entity does not satisfy the release contract."""


@dataclass(frozen=True)
class EntityReport:
    entity: str
    shards: int
    rows: int
    non_empty: dict[str, int]

    def detail(self) -> str:
        detail = f"{self.shards:,} shards, {self.rows:,} rows"
        if self.non_empty:
            values = ", ".join(
                f"{name}={self.non_empty[name]:,}/{self.rows:,}"
                for name in self.non_empty
            )
            detail += f"; {values}"
        return detail


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--run", action="store_true", help="Build, verify, and swap")
    parser.add_argument("--release", required=True, choices=profiles.RELEASES)
    parser.add_argument("--entity", required=True, choices=RAW_ENTITIES)
    parser.add_argument(
        "--cache-type",
        default="merged",
        choices=["merged", "refseq", "ensembl"],
    )
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--target", default=None, help="Converted cache directory")
    parser.add_argument(
        "--local-cache",
        default=None,
        help="Extracted raw cache release directory containing info.txt",
    )
    args = parser.parse_args(argv)
    if args.partitions <= 0:
        parser.error("--partitions must be a positive integer")
    return args


def _read_manifest(entity_dir: Path) -> list[dict[str, Any]]:
    manifest_path = entity_dir / "chrom_manifest.json"
    try:
        value = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise VerificationError(f"cannot read {manifest_path}: {exc}") from exc
    if not isinstance(value, list):
        raise VerificationError(f"{manifest_path} must contain a JSON array")
    return value


def _metadata_value(metadata: dict[bytes, bytes], key: bytes, shard: Path) -> str:
    raw = metadata.get(key)
    if raw is None:
        raise VerificationError(
            f"{shard}: missing required Parquet metadata {key.decode()!r}"
        )
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise VerificationError(
            f"{shard}: metadata {key.decode()!r} is not UTF-8"
        ) from exc


def _non_empty_count(values: pa.ChunkedArray) -> int:
    if pa.types.is_string(values.type) or pa.types.is_large_string(values.type):
        present = pc.and_(
            pc.is_valid(values),
            pc.greater(pc.utf8_length(values), 0),
        )
        return int(pc.sum(pc.cast(present, pa.int64())).as_py() or 0)
    return sum(value not in (None, "", []) for value in values.to_pylist())


def _check_variation_schema(schema: pa.Schema, release: str, shard: Path) -> None:
    field = (
        schema.field("clin_sig_ref_allele")
        if "clin_sig_ref_allele" in schema.names
        else None
    )
    if release == "116" and field is None:
        raise VerificationError(
            f"{shard}: VEP 116 variation shard lacks clin_sig_ref_allele"
        )
    if field is not None and (field.type != pa.string() or not field.nullable):
        raise VerificationError(
            f"{shard}: clin_sig_ref_allele must be nullable UTF-8, got {field}"
        )


def verify_entity_dir(
    entity_dir: str | Path,
    entity: str,
    release: str,
    source_type: str,
) -> EntityReport:
    """Validate every manifest-referenced shard for one generated entity."""
    root = Path(entity_dir)
    if entity not in {
        value for values in GENERATED_ENTITIES.values() for value in values
    }:
        raise VerificationError(f"unsupported generated entity {entity!r}")
    if not root.is_dir():
        raise VerificationError(f"no {entity} directory at {root}")

    entries = _read_manifest(root)
    seen_chroms: set[str] = set()
    seen_datasets: set[str] = set()
    baseline_schema: pa.Schema | None = None
    total_rows = 0
    non_empty = (
        {name: 0 for name in MOTIF_REQUIRED_COLUMNS} if entity == "motif" else {}
    )

    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise VerificationError(f"manifest entry {index} is not an object")
        chrom = entry.get("chrom")
        dataset = entry.get("dataset")
        rows = entry.get("rows")
        if not isinstance(chrom, str) or not chrom:
            raise VerificationError(f"manifest entry {index} has invalid chrom")
        if (
            not isinstance(dataset, str)
            or not dataset
            or Path(dataset).name != dataset
            or Path(dataset).suffix != ".parquet"
        ):
            raise VerificationError(
                f"manifest entry {index} has unsafe dataset {dataset!r}"
            )
        if not isinstance(rows, int) or isinstance(rows, bool) or rows < 0:
            raise VerificationError(f"manifest entry {index} has invalid rows {rows!r}")
        if chrom in seen_chroms:
            raise VerificationError(f"manifest repeats chromosome {chrom!r}")
        if dataset in seen_datasets:
            raise VerificationError(f"manifest repeats dataset {dataset!r}")
        seen_chroms.add(chrom)
        seen_datasets.add(dataset)

        shard = root / dataset
        if not shard.is_file():
            raise VerificationError(f"manifest-referenced shard is missing: {shard}")
        try:
            parquet_file = pq.ParquetFile(shard)
        except Exception as exc:
            raise VerificationError(
                f"cannot read Parquet footer {shard}: {exc}"
            ) from exc
        actual_rows = parquet_file.metadata.num_rows
        if actual_rows != rows:
            raise VerificationError(
                f"{shard}: manifest declares {rows:,} rows, footer has {actual_rows:,}"
            )

        schema = parquet_file.schema_arrow
        if entity == "motif":
            missing = [
                name for name in MOTIF_REQUIRED_COLUMNS if name not in schema.names
            ]
            if missing:
                raise VerificationError(
                    f"{shard}: missing motif columns {', '.join(missing)}"
                )
        if baseline_schema is None:
            baseline_schema = schema.remove_metadata()
        elif not schema.remove_metadata().equals(baseline_schema):
            raise VerificationError(
                f"{shard}: schema differs from other {entity} shards"
            )

        metadata = schema.metadata or {}
        actual_release = _metadata_value(metadata, CACHE_VERSION_METADATA_KEY, shard)
        actual_source = _metadata_value(metadata, CACHE_SOURCE_METADATA_KEY, shard)
        if actual_release != release:
            raise VerificationError(
                f"{shard}: cache release {actual_release!r}, expected {release!r}"
            )
        if actual_source != source_type:
            raise VerificationError(
                f"{shard}: cache source {actual_source!r}, expected {source_type!r}"
            )

        if entity == "variation":
            _check_variation_schema(schema, release, shard)
        if entity == "motif":
            if actual_rows:
                table = pq.read_table(shard, columns=list(MOTIF_REQUIRED_COLUMNS))
                for name in MOTIF_REQUIRED_COLUMNS:
                    non_empty[name] += _non_empty_count(table[name])
        total_rows += actual_rows

    unreferenced = sorted(
        path.name for path in root.glob("*.parquet") if path.name not in seen_datasets
    )
    if unreferenced:
        preview = ", ".join(unreferenced[:3])
        raise VerificationError(
            f"{root}: {len(unreferenced)} unreferenced Parquet shard(s), "
            f"including {preview}"
        )

    if entity == "variation" and not entries:
        raise VerificationError("variation manifest must contain at least one shard")
    if entity == "motif":
        if release == "116" and total_rows == 0:
            raise VerificationError("VEP 116 motif entity contains no rows")
        if total_rows:
            empty = [name for name, count in non_empty.items() if count == 0]
            if empty:
                raise VerificationError(
                    f"motif columns contain no populated values: {', '.join(empty)}"
                )
        if release == "116":
            incomplete = [
                name
                for name in MOTIF_FULLY_POPULATED_116_COLUMNS
                if non_empty[name] != total_rows
            ]
            if incomplete:
                detail = ", ".join(
                    f"{name}={non_empty[name]:,}/{total_rows:,}" for name in incomplete
                )
                raise VerificationError(
                    f"VEP 116 motif identity columns are not fully populated: {detail}"
                )

    return EntityReport(entity, len(entries), total_rows, non_empty)


def verify_selected_entity(
    cache_dir: str | Path,
    raw_entity: str,
    release: str,
    source_type: str,
) -> tuple[EntityReport, ...]:
    """Verify every generated output produced by one raw cache entity."""
    if raw_entity not in GENERATED_ENTITIES:
        raise VerificationError(f"unsupported raw entity {raw_entity!r}")
    root = Path(cache_dir)
    return tuple(
        verify_entity_dir(root / entity, entity, release, source_type)
        for entity in GENERATED_ENTITIES[raw_entity]
    )


def _reports_detail(reports: tuple[EntityReport, ...]) -> str:
    return "; ".join(f"{report.entity}: {report.detail()}" for report in reports)


def _parquet_row_total(entity_dir: Path) -> int:
    """Inventory all current Parquet rows when stricter verification fails."""
    total = 0
    for shard in sorted(entity_dir.glob("*.parquet")):
        try:
            total += pq.ParquetFile(shard).metadata.num_rows
        except Exception as exc:
            raise VerificationError(
                f"cannot read current Parquet footer {shard}: {exc}"
            ) from exc
    return total


def verify(
    cache_dir: str | Path,
    raw_entity: str,
    release: str,
    source_type: str,
) -> tuple[bool, str]:
    """Return a concise CLI-compatible verification result."""
    try:
        reports = verify_selected_entity(cache_dir, raw_entity, release, source_type)
    except VerificationError as exc:
        return False, str(exc)
    return True, _reports_detail(reports)


def _swap_with_rollback(
    target: Path,
    staged_cache: Path,
    generated_entities: tuple[str, ...],
    backups: dict[str, Path],
) -> list[Path]:
    backed_up: list[str] = []
    installed: list[str] = []
    try:
        for entity in generated_entities:
            live = target / entity
            if live.exists():
                live.rename(backups[entity])
                backed_up.append(entity)
        for entity in generated_entities:
            staged = staged_cache / entity
            staged.rename(target / entity)
            installed.append(entity)
    except Exception:
        for entity in reversed(installed):
            live = target / entity
            staged = staged_cache / entity
            if live.exists() and not staged.exists():
                live.rename(staged)
        for entity in reversed(backed_up):
            live = target / entity
            backup = backups[entity]
            if backup.exists() and not live.exists():
                backup.rename(live)
        raise
    return [backups[entity] for entity in backed_up]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    release = args.release
    cache_type = args.cache_type
    raw_entity = args.entity
    generated_entities = GENERATED_ENTITIES[raw_entity]
    target = Path(
        args.target or profiles.cache_dir_for(cache_type, release, warn=False)
    ).expanduser()
    source = Path(
        args.local_cache or profiles.raw_cache_dir_for(cache_type, release)
    ).expanduser()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    staging_parent = target.with_name(
        f".{target.name}.{raw_entity}-rebuild-{stamp}-{os.getpid()}"
    )
    staging_cache = staging_parent / f"{release}_GRCh38_{cache_type}"
    backups = {
        entity: target.with_name(f".{target.name}.{entity}-backup-{stamp}")
        for entity in generated_entities
    }

    print(f"release:      {release}")
    print(f"cache type:   {cache_type}")
    print(f"raw entity:   {raw_entity}")
    print(f"outputs:      {', '.join(generated_entities)}")
    print(f"raw source:   {source}")
    print(f"staging:      {staging_cache}")
    print(f"target:       {target}")
    print(f"partitions:   {args.partitions}")

    problems: list[str] = []
    if not source.is_dir():
        problems.append(f"raw source cache not found: {source}")
    elif not (source / "info.txt").is_file():
        problems.append(f"raw source has no info.txt: {source}")
    if not target.is_dir():
        problems.append(f"converted cache not found: {target}")
    if staging_parent.exists():
        problems.append(f"staging path already exists: {staging_parent}")
    for entity, backup in backups.items():
        if backup.exists():
            problems.append(f"{entity} backup path already exists: {backup}")

    existing_outputs = [
        entity for entity in generated_entities if (target / entity).is_dir()
    ]
    live_totals: dict[str, int] = {}
    live_reports: list[EntityReport] = []
    live_errors: list[str] = []
    if existing_outputs:
        if len(existing_outputs) != len(generated_entities):
            missing = sorted(set(generated_entities) - set(existing_outputs))
            print(
                "current entity: NEEDS REBUILD — missing generated outputs "
                f"{', '.join(missing)}"
            )
        for entity in existing_outputs:
            try:
                entity_report = verify_entity_dir(
                    target / entity,
                    entity,
                    release,
                    cache_type,
                )
                live_totals[entity] = entity_report.rows
                live_reports.append(entity_report)
            except VerificationError as exc:
                live_errors.append(str(exc))
                print(f"current {entity}: NEEDS REBUILD — {exc}")
                try:
                    live_totals[entity] = _parquet_row_total(target / entity)
                except VerificationError as inventory_exc:
                    problems.append(
                        f"cannot inventory current {entity}: {inventory_exc}"
                    )
        if len(existing_outputs) == len(generated_entities) and not live_errors:
            detail = _reports_detail(tuple(live_reports))
            state = "OK"
            print(f"current entity: {state} — {detail}")

    if problems:
        print("BLOCKED:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 2
    if not args.run:
        print("Dry run complete. Re-run with --run to build, verify, and swap.")
        return 0

    import vepyr

    staging_parent.mkdir()
    print(f"building {raw_entity} in {staging_parent} ...")
    started = time.monotonic()
    try:
        results = vepyr.build_cache_entity(
            int(release),
            str(staging_parent),
            raw_entity,
            cache_type=cache_type,
            partitions=args.partitions,
            local_cache=str(source),
            overwrite=True,
        )
    except Exception:
        print(f"build failed; staging retained at {staging_parent}", file=sys.stderr)
        raise
    elapsed = time.monotonic() - started
    built_rows = sum(rows for _path, rows in results)
    print(f"builder reported {built_rows:,} rows in {elapsed / 60:.1f} min")

    try:
        staged_reports = verify_selected_entity(
            staging_cache,
            raw_entity,
            release,
            cache_type,
        )
    except VerificationError as exc:
        print(f"staged verification: FAILED — {exc}")
        print(f"staging retained for inspection: {staging_parent}", file=sys.stderr)
        return 1
    print(f"staged verification: OK — {_reports_detail(staged_reports)}")

    staged_totals = {report.entity: report.rows for report in staged_reports}
    changed = {
        entity: (live_totals[entity], staged_totals[entity])
        for entity in live_totals
        if live_totals[entity] != staged_totals[entity]
    }
    if changed:
        print(
            "row-count reconciliation failed; staging retained and target unchanged:",
            file=sys.stderr,
        )
        for entity, (old, new) in changed.items():
            print(f"  {entity}: old={old:,}, new={new:,}", file=sys.stderr)
        return 1

    try:
        retained_backups = _swap_with_rollback(
            target,
            staging_cache,
            generated_entities,
            backups,
        )
    except Exception:
        print(
            "swap failed; all previous generated entities restored",
            file=sys.stderr,
        )
        raise

    shutil.rmtree(staging_parent)
    print(
        "swapped verified generated entities into target: "
        f"{', '.join(str(target / entity) for entity in generated_entities)}"
    )
    for backup in retained_backups:
        print(f"previous generated entity retained at {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
