"""Helpers for adding VEP cache-source metadata to checked-in fixtures."""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow.parquet as pq

CACHE_SOURCE_METADATA_KEY = b"bio.vep.cache_source_type"
VALID_CACHE_SOURCE_TYPES = {"ensembl", "merged", "refseq"}


def copy_cache_with_source_metadata(
    source_dir: str | Path,
    target_dir: str | Path,
    cache_source_type: str,
) -> Path:
    """Copy a parquet cache fixture and add cache-source schema metadata."""
    if cache_source_type not in VALID_CACHE_SOURCE_TYPES:
        allowed = ", ".join(sorted(VALID_CACHE_SOURCE_TYPES))
        raise ValueError(
            f"Invalid cache_source_type '{cache_source_type}'. Must be one of: {allowed}."
        )

    source = Path(source_dir)
    target = Path(target_dir)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)

    for path in source.rglob("*"):
        relative = path.relative_to(source)
        destination = target / relative
        if path.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            continue

        destination.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".parquet":
            table = pq.read_table(path)
            metadata = dict(table.schema.metadata or {})
            metadata[CACHE_SOURCE_METADATA_KEY] = cache_source_type.encode("ascii")
            pq.write_table(table.replace_schema_metadata(metadata), destination)
        else:
            shutil.copy2(path, destination)

    return target
