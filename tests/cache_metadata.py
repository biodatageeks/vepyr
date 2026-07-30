"""Helpers for adding VEP cache-source metadata to checked-in fixtures."""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow.parquet as pq
from datafusion import SessionContext

CACHE_SOURCE_METADATA_KEY = b"bio.vep.cache_source_type"
CACHE_VERSION_METADATA_KEY = b"bio.vep.cache_version"
VALID_CACHE_SOURCE_TYPES = {"ensembl", "merged", "refseq"}


def copy_cache_with_source_metadata(
    source_dir: str | Path,
    target_dir: str | Path,
    cache_source_type: str,
    cache_version: str,
) -> Path:
    """Copy a parquet cache fixture and add strict VEP cache identity metadata.

    Parquet shards are rewritten with DataFusion (parquet-rs — the same writer
    the engine reads with) using page-level statistics, so the footer keeps a
    ColumnIndex/OffsetIndex the engine's point-lookup reader accepts. pyarrow's
    page index is not loaded by that reader, so it cannot be used here.
    ``skip_arrow_metadata`` is disabled so the added ``bio.vep.cache_source_type``
    key survives in the Arrow schema the engine reads off the variation shard.
    """
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

    ctx = SessionContext()
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
            metadata[CACHE_VERSION_METADATA_KEY] = cache_version.encode("ascii")
            table = table.replace_schema_metadata(metadata)
            if table.num_rows == 0:
                # Empty shard: no point-lookup pages, and DataFusion's writer
                # panics on a zero-batch input, so use pyarrow here.
                pq.write_table(table, destination)
            else:
                ctx.register_record_batches(
                    "shard", [table.combine_chunks().to_batches()]
                )
                if destination.exists():
                    destination.unlink()
                ctx.sql(
                    f"COPY shard TO '{destination}' STORED AS PARQUET OPTIONS "
                    "('statistics_enabled' 'page', 'skip_arrow_metadata' 'false')"
                ).collect()
                ctx.deregister_table("shard")
        else:
            shutil.copy2(path, destination)

    return target
