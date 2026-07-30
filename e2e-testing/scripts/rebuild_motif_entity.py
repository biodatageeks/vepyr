#!/usr/bin/env python3
"""Safely rebuild only the ``motif`` entity of a converted VEP cache.

The command is a dry run unless ``--run`` is supplied. A real rebuild uses
the public, release-aware :func:`vepyr.build_cache_entity` API, writes into a
sibling staging directory, validates every manifest-referenced Parquet shard,
and swaps only after release/source metadata, schemas, row counts, and motif
values pass. The previous motif directory is retained as a timestamped backup.

Examples:
    uv run python e2e-testing/scripts/rebuild_motif_entity.py --release 116
    uv run python e2e-testing/scripts/rebuild_motif_entity.py \
        --release 116 --cache-type merged --run
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
REQUIRED_COLUMNS = (
    "motif_id",
    "binding_matrix",
    "binding_matrix_length",
    "binding_matrix_elements",
    "binding_matrix_unit",
    "motif_seq",
    "transcription_factors",
)
FULLY_POPULATED_116_COLUMNS = (
    "motif_id",
    "binding_matrix",
    "transcription_factors",
)


class VerificationError(RuntimeError):
    """The staged motif entity does not satisfy the release contract."""


@dataclass(frozen=True)
class MotifReport:
    shards: int
    rows: int
    non_empty: dict[str, int]

    def detail(self) -> str:
        values = ", ".join(
            f"{name}={self.non_empty[name]:,}/{self.rows:,}"
            for name in REQUIRED_COLUMNS
        )
        return f"{self.shards:,} shards, {self.rows:,} rows; {values}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--run", action="store_true", help="Build, verify, and swap")
    parser.add_argument("--release", default="116", choices=profiles.RELEASES)
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


def _read_manifest(motif_dir: Path) -> list[dict[str, Any]]:
    manifest_path = motif_dir / "chrom_manifest.json"
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


def verify_motif(
    motif_dir: str | Path,
    release: str,
    source_type: str,
) -> MotifReport:
    """Validate every manifest-referenced motif shard."""
    root = Path(motif_dir)
    if not root.is_dir():
        raise VerificationError(f"no motif directory at {root}")

    entries = _read_manifest(root)
    seen_chroms: set[str] = set()
    seen_datasets: set[str] = set()
    baseline_schema: pa.Schema | None = None
    total_rows = 0
    non_empty = {name: 0 for name in REQUIRED_COLUMNS}

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
        missing = [name for name in REQUIRED_COLUMNS if name not in schema.names]
        if missing:
            raise VerificationError(
                f"{shard}: missing motif columns {', '.join(missing)}"
            )
        if baseline_schema is None:
            baseline_schema = schema.remove_metadata()
        elif not schema.remove_metadata().equals(baseline_schema):
            raise VerificationError(f"{shard}: schema differs from other motif shards")

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

        if actual_rows:
            table = pq.read_table(shard, columns=list(REQUIRED_COLUMNS))
            for name in REQUIRED_COLUMNS:
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
            for name in FULLY_POPULATED_116_COLUMNS
            if non_empty[name] != total_rows
        ]
        if incomplete:
            detail = ", ".join(
                f"{name}={non_empty[name]:,}/{total_rows:,}" for name in incomplete
            )
            raise VerificationError(
                f"VEP 116 motif identity columns are not fully populated: {detail}"
            )

    return MotifReport(len(entries), total_rows, non_empty)


def verify(
    motif_dir: str | Path,
    release: str = "116",
    source_type: str = "merged",
) -> tuple[bool, str]:
    """Compatibility result shape for callers and concise CLI reporting."""
    try:
        report = verify_motif(motif_dir, release, source_type)
    except VerificationError as exc:
        return False, str(exc)
    return True, report.detail()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    release = args.release
    cache_type = args.cache_type
    target = Path(
        args.target or profiles.cache_dir_for(cache_type, release, warn=False)
    ).expanduser()
    source = Path(
        args.local_cache or profiles.raw_cache_dir_for(cache_type, release)
    ).expanduser()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    staging_parent = target.with_name(
        f".{target.name}.motif-rebuild-{stamp}-{os.getpid()}"
    )
    staging_cache = staging_parent / f"{release}_GRCh38_{cache_type}"
    live_motif = target / "motif"
    staged_motif = staging_cache / "motif"
    backup = target.with_name(f".{target.name}.motif-backup-{stamp}")

    print(f"release:      {release}")
    print(f"cache type:   {cache_type}")
    print(f"raw source:   {source}")
    print(f"staging:      {staged_motif}")
    print(f"target:       {live_motif}")
    print(f"backup:       {backup}")
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
    if backup.exists():
        problems.append(f"backup path already exists: {backup}")

    if live_motif.is_dir():
        ok, detail = verify(live_motif, release, cache_type)
        state = "OK" if ok else "NEEDS REBUILD"
        print(f"current motif entity: {state} — {detail}")

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
    print(f"building motif entity in {staging_parent} ...")
    started = time.monotonic()
    try:
        results = vepyr.build_cache_entity(
            int(release),
            str(staging_parent),
            "motif",
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
    print(f"built {built_rows:,} motif rows in {elapsed / 60:.1f} min")

    ok, detail = verify(staged_motif, release, cache_type)
    print(f"staged verification: {'OK' if ok else 'FAILED'} — {detail}")
    if not ok:
        print(f"staging retained for inspection: {staging_parent}", file=sys.stderr)
        return 1

    backup_created = False
    try:
        if live_motif.is_dir():
            live_motif.rename(backup)
            backup_created = True
        staged_motif.rename(live_motif)
    except Exception:
        if backup_created and not live_motif.exists():
            backup.rename(live_motif)
        print("swap failed; previous motif entity restored", file=sys.stderr)
        raise

    shutil.rmtree(staging_parent)
    print(f"swapped verified motif entity into {live_motif}")
    if backup_created:
        print(f"previous motif entity retained at {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
