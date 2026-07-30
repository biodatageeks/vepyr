#!/usr/bin/env python3
"""Safely rebuild and verify a release-qualified vepyr Parquet cache.

The command is a dry run unless ``--run`` is supplied. A real rebuild is
written beside the target, every manifest-referenced Parquet footer is
validated, and the existing target is retained as a timestamped backup. The
replacement rename is rolled back if it fails.

Examples:
    uv run python e2e-testing/scripts/rebuild_release_cache.py --release 115
    uv run python e2e-testing/scripts/rebuild_release_cache.py --release 116 --run
    uv run python e2e-testing/scripts/rebuild_release_cache.py \
        --release 116 --verify-only ~/workspace/data_vepyr/116_GRCh38_merged
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
ENTITIES = (
    "variation",
    "transcript",
    "exon",
    "translation_core",
    "translation_sift",
    "regulatory",
    "motif",
)
SUPPORTED_RELEASES = ("115", "116")
MOTIF_VALUE_COLUMNS = ("binding_matrix", "transcription_factors")


class VerificationError(RuntimeError):
    """The staged cache does not satisfy the release contract."""


@dataclass(frozen=True)
class EntityReport:
    entity: str
    shards: int
    rows: int


@dataclass(frozen=True)
class CacheReport:
    cache_dir: Path
    release: str
    source_type: str
    entities: tuple[EntityReport, ...]
    motif_non_empty: dict[str, int]

    @property
    def total_rows(self) -> int:
        return sum(entity.rows for entity in self.entities)

    def rows_by_entity(self) -> dict[str, int]:
        return {entity.entity: entity.rows for entity in self.entities}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--release", required=True, choices=SUPPORTED_RELEASES)
    parser.add_argument(
        "--cache-type",
        default="merged",
        choices=["merged", "refseq", "ensembl"],
        help="Ensembl cache flavour (default: %(default)s)",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=8,
        help="DataFusion partitions during conversion (default: %(default)s)",
    )
    parser.add_argument(
        "--target",
        default=None,
        help="Final generated cache directory (default: comparison profile path)",
    )
    parser.add_argument(
        "--local-cache",
        default=None,
        help="Extracted raw cache release directory containing info.txt",
    )
    parser.add_argument(
        "--verify-only",
        metavar="CACHE_DIR",
        help="Validate an already-built cache without rebuilding it",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Build and swap. Without this flag the command only performs preflight.",
    )
    args = parser.parse_args(argv)
    if args.partitions <= 0:
        parser.error("--partitions must be a positive integer")
    if args.verify_only and args.run:
        parser.error("--verify-only and --run are mutually exclusive")
    return args


def human(n_bytes: int) -> str:
    value = float(n_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}PiB"


def dir_size(path: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except OSError:
                pass
    return total


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
            f"{shard}: missing required Parquet schema metadata {key.decode()!r}"
        )
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise VerificationError(
            f"{shard}: metadata {key.decode()!r} is not UTF-8"
        ) from exc


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


def _non_empty_string_count(shard: Path, columns: tuple[str, ...]) -> dict[str, int]:
    table = pq.read_table(shard, columns=list(columns))
    counts: dict[str, int] = {}
    for name in columns:
        values = table[name]
        present = pc.and_(pc.is_valid(values), pc.greater(pc.utf8_length(values), 0))
        counts[name] = int(pc.sum(pc.cast(present, pa.int64())).as_py() or 0)
    return counts


def verify_cache(
    cache_dir: str | Path,
    release: str,
    source_type: str,
    *,
    require_identity: bool = True,
    verify_motif_values: bool = True,
) -> CacheReport:
    """Validate every manifest-referenced shard and return footer-derived totals."""
    root = Path(cache_dir).expanduser().resolve()
    if release not in SUPPORTED_RELEASES:
        raise VerificationError(f"unsupported cache release {release!r}")
    if not root.is_dir():
        raise VerificationError(f"cache directory does not exist: {root}")

    reports: list[EntityReport] = []
    motif_non_empty = {name: 0 for name in MOTIF_VALUE_COLUMNS}
    for entity in ENTITIES:
        entity_dir = root / entity
        if not entity_dir.is_dir():
            raise VerificationError(
                f"required entity directory is missing: {entity_dir}"
            )
        entries = _read_manifest(entity_dir)
        seen_chroms: set[str] = set()
        seen_datasets: set[str] = set()
        baseline_schema: pa.Schema | None = None
        entity_rows = 0

        for index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                raise VerificationError(
                    f"{entity_dir}/chrom_manifest.json entry {index} is not an object"
                )
            chrom = entry.get("chrom")
            dataset = entry.get("dataset")
            rows = entry.get("rows")
            if not isinstance(chrom, str) or not chrom:
                raise VerificationError(
                    f"{entity} manifest entry {index} has invalid chrom"
                )
            if (
                not isinstance(dataset, str)
                or not dataset
                or Path(dataset).name != dataset
                or Path(dataset).suffix != ".parquet"
            ):
                raise VerificationError(
                    f"{entity} manifest entry {index} has unsafe dataset {dataset!r}"
                )
            if not isinstance(rows, int) or isinstance(rows, bool) or rows < 0:
                raise VerificationError(
                    f"{entity} manifest entry {index} has invalid rows {rows!r}"
                )
            if chrom in seen_chroms:
                raise VerificationError(
                    f"{entity} manifest repeats chromosome {chrom!r}"
                )
            if dataset in seen_datasets:
                raise VerificationError(
                    f"{entity} manifest repeats dataset {dataset!r}"
                )
            seen_chroms.add(chrom)
            seen_datasets.add(dataset)

            shard = entity_dir / dataset
            if not shard.is_file():
                raise VerificationError(
                    f"manifest-referenced shard is missing: {shard}"
                )
            try:
                parquet_file = pq.ParquetFile(shard)
            except Exception as exc:
                raise VerificationError(
                    f"cannot read Parquet footer {shard}: {exc}"
                ) from exc
            actual_rows = parquet_file.metadata.num_rows
            if actual_rows != rows:
                raise VerificationError(
                    f"{shard}: manifest declares {rows:,} rows, footer has "
                    f"{actual_rows:,}"
                )

            schema = parquet_file.schema_arrow
            if baseline_schema is None:
                baseline_schema = schema.remove_metadata()
            elif not schema.remove_metadata().equals(baseline_schema):
                raise VerificationError(
                    f"{shard}: schema differs from other {entity} shards"
                )
            if require_identity:
                metadata = schema.metadata or {}
                actual_release = _metadata_value(
                    metadata, CACHE_VERSION_METADATA_KEY, shard
                )
                actual_source = _metadata_value(
                    metadata, CACHE_SOURCE_METADATA_KEY, shard
                )
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
            if (
                entity == "motif"
                and release == "116"
                and verify_motif_values
                and actual_rows
            ):
                missing = [
                    name for name in MOTIF_VALUE_COLUMNS if name not in schema.names
                ]
                if missing:
                    raise VerificationError(
                        f"{shard}: missing VEP 116 motif columns {', '.join(missing)}"
                    )
                counts = _non_empty_string_count(shard, MOTIF_VALUE_COLUMNS)
                for name, count in counts.items():
                    motif_non_empty[name] += count
            entity_rows += actual_rows

        on_disk = {path.name for path in entity_dir.glob("*.parquet")}
        unreferenced = sorted(on_disk - seen_datasets)
        if unreferenced:
            preview = ", ".join(unreferenced[:3])
            raise VerificationError(
                f"{entity_dir}: {len(unreferenced)} unreferenced Parquet shard(s), "
                f"including {preview}"
            )
        if entity == "variation" and not entries:
            raise VerificationError(
                "variation manifest must contain at least one shard"
            )
        reports.append(EntityReport(entity, len(entries), entity_rows))

    if release == "116" and verify_motif_values:
        motif_rows = next(report.rows for report in reports if report.entity == "motif")
        for name, count in motif_non_empty.items():
            if motif_rows and count == 0:
                raise VerificationError(
                    f"VEP 116 motif cache has {motif_rows:,} rows but no non-empty {name}"
                )

    return CacheReport(root, release, source_type, tuple(reports), motif_non_empty)


def _manifest_totals(cache_dir: Path) -> dict[str, int]:
    """Read old-cache totals without accepting it as release-identified."""
    totals: dict[str, int] = {}
    for entity in ENTITIES:
        entries = _read_manifest(cache_dir / entity)
        total = 0
        for index, entry in enumerate(entries):
            rows = entry.get("rows") if isinstance(entry, dict) else None
            if not isinstance(rows, int) or isinstance(rows, bool) or rows < 0:
                raise VerificationError(
                    f"{entity} old-cache manifest entry {index} has invalid rows"
                )
            total += rows
        totals[entity] = total
    return totals


def _print_report(report: CacheReport) -> None:
    print(
        f"verified {report.cache_dir}: release={report.release}, "
        f"source={report.source_type}, rows={report.total_rows:,}"
    )
    for entity in report.entities:
        print(
            f"  {entity.entity:<20} {entity.shards:>5,} shards {entity.rows:>15,} rows"
        )
    if report.release == "116" and any(report.motif_non_empty.values()):
        detail = ", ".join(
            f"{name}={count:,}" for name, count in report.motif_non_empty.items()
        )
        print(f"  non-empty motif values: {detail}")


def _preflight_source(source: Path) -> list[str]:
    problems: list[str] = []
    if not source.is_dir():
        return [f"raw source cache not found: {source}"]
    try:
        entries = list(source.iterdir())
    except OSError as exc:
        return [f"cannot list raw source cache {source}: {exc}"]
    if not (source / "info.txt").is_file():
        problems.append(f"raw source has no info.txt: {source}")
    if len(entries) < 100:
        problems.append(
            f"raw source has only {len(entries)} entries; expected an extracted release "
            "directory rather than its parent"
        )
    return problems


def _swap_with_rollback(staging: Path, target: Path, stamp: str) -> Path | None:
    backup: Path | None = None
    if target.exists():
        backup = target.with_name(f"{target.name}.backup-{stamp}")
        if backup.exists():
            raise RuntimeError(f"refusing to overwrite existing backup {backup}")
        target.rename(backup)
    try:
        staging.rename(target)
    except Exception:
        if backup is not None and not target.exists():
            backup.rename(target)
        raise
    return backup


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    release = args.release
    cache_type = args.cache_type

    if args.verify_only:
        try:
            report = verify_cache(args.verify_only, release, cache_type)
        except VerificationError as exc:
            print(f"VERIFY FAILED: {exc}", file=sys.stderr)
            return 1
        _print_report(report)
        return 0

    target = (
        Path(args.target or profiles.cache_dir_for(cache_type, release, warn=False))
        .expanduser()
        .resolve()
    )
    source = (
        Path(args.local_cache or profiles.raw_cache_dir_for(cache_type, release))
        .expanduser()
        .resolve()
    )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    staging_parent = target.with_name(f".{target.name}.rebuild-{stamp}-{os.getpid()}")
    builder_name = f"{release}_GRCh38_{cache_type}"
    staging = staging_parent / builder_name

    print(f"release:      {release}")
    print(f"cache type:   {cache_type}")
    print(f"raw source:   {source}")
    print(f"staging:      {staging}")
    print(f"target:       {target}")
    print(f"partitions:   {args.partitions}")

    problems = _preflight_source(source)
    if staging_parent.exists():
        problems.append(f"staging path already exists: {staging_parent}")
    old_totals: dict[str, int] | None = None
    existing_size = 0
    if target.exists():
        if not target.is_dir():
            problems.append(f"target exists but is not a directory: {target}")
        else:
            existing_size = dir_size(target)
            try:
                old_totals = _manifest_totals(target)
            except VerificationError as exc:
                problems.append(f"cannot inventory existing target: {exc}")

    disk_parent = target.parent
    while not disk_parent.exists():
        if disk_parent == disk_parent.parent:
            problems.append(f"cannot resolve an existing parent for target {target}")
            break
        disk_parent = disk_parent.parent
    free = shutil.disk_usage(disk_parent).free
    needed = int(existing_size * 1.15) if existing_size else 0
    print(f"existing size: {human(existing_size)}")
    print(f"free space:    {human(free)}")
    if needed:
        print(f"estimated staging requirement: {human(needed)}")
    if needed and free < needed:
        problems.append(
            f"insufficient free space: {human(free)} available, "
            f"approximately {human(needed)} required"
        )
    if problems:
        print("BLOCKED:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 2
    if not args.run:
        print("Dry run complete. Re-run with --run to build, verify, and swap.")
        return 0

    import vepyr

    target.parent.mkdir(parents=True, exist_ok=True)
    staging_parent.mkdir()
    print(f"building fresh cache in {staging_parent} ...")
    started = time.monotonic()
    try:
        vepyr.build_cache(
            int(release),
            str(staging_parent),
            cache_type=cache_type,
            partitions=args.partitions,
            local_cache=str(source),
            overwrite=True,
        )
    except Exception:
        print(f"build failed; staging retained at {staging_parent}", file=sys.stderr)
        raise
    if not staging.is_dir():
        raise RuntimeError(
            f"builder returned without creating expected cache directory {staging}"
        )

    try:
        report = verify_cache(staging, release, cache_type)
    except VerificationError as exc:
        print(
            f"verification failed; staging retained at {staging_parent}",
            file=sys.stderr,
        )
        print(f"VERIFY FAILED: {exc}", file=sys.stderr)
        return 1
    _print_report(report)

    if old_totals is not None:
        new_totals = report.rows_by_entity()
        changed = {
            entity: (old_totals[entity], new_totals[entity])
            for entity in ENTITIES
            if old_totals[entity] != new_totals[entity]
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
        backup = _swap_with_rollback(staging, target, stamp)
    except Exception:
        print(
            "replacement rename failed; the previous target was restored and staging "
            "was retained",
            file=sys.stderr,
        )
        raise
    try:
        staging_parent.rmdir()
    except OSError:
        pass

    elapsed = time.monotonic() - started
    print(f"DONE: {target} in {elapsed / 60:.1f} minutes")
    if backup is not None:
        print(f"previous cache retained at {backup}")
    print("Next: run the release-qualified chromosome 1-22 comparison gate.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
