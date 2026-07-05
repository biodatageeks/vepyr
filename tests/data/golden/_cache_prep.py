"""Shared builder for trimmed golden Parquet caches (v0.12.1 layout).

Both ``tests/data/golden/prepare.py`` and ``tests/data/golden_merged/prepare.py``
use this to slice a full per-chromosome Parquet cache down to the golden test
region. Shards are written with DataFusion (parquet-rs — the same writer the
engine reads with) and page-level statistics, so the footer carries the
ColumnIndex/OffsetIndex the engine's point-lookup reader needs; pyarrow's page
index is not loaded by that reader. Each entity also gets a
``chrom_manifest.json`` (the v0.12.1 partitioned-Parquet layout).
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from datafusion import SessionContext

# Genomic-interval entities, trimmed by `start` in [0, end].
_INTERVAL_ENTITIES = ("variation", "transcript", "exon", "regulatory")

_COPY_OPTIONS = (
    "('compression' 'zstd(3)', 'dictionary_enabled' 'false', "
    "'statistics_enabled' 'page', 'data_pagesize_limit' '4096', "
    "'data_page_row_count_limit' '512')"
)


def _chrom_label(cache_src: Path) -> str:
    """The chrom string the engine uses for the chr1 shard (from the source manifest)."""
    manifest = json.loads((cache_src / "variation" / "chrom_manifest.json").read_text())
    return next(e["chrom"] for e in manifest if e["dataset"] == "chr1.parquet")


def write_trimmed_cache(cache_src, cache_dir, end: int) -> None:
    """Build a trimmed chr1 Parquet cache under ``cache_dir`` from ``cache_src``."""
    cache_src = Path(cache_src)
    cache_dir = Path(cache_dir)
    chrom_label = _chrom_label(cache_src)
    ctx = SessionContext()

    def write_manifest(dst_dir: Path, rows: int) -> None:
        dst_dir.joinpath("chrom_manifest.json").write_text(
            json.dumps(
                [{"chrom": chrom_label, "dataset": "chr1.parquet", "rows": rows}],
                indent=2,
            )
            + "\n"
        )

    def write_entity(entity: str, table) -> None:
        dst_dir = cache_dir / entity
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "chr1.parquet"
        if dst.exists():
            dst.unlink()
        if table.num_rows == 0:
            # Empty shard: no point-lookup pages, and DataFusion's writer panics
            # on a zero-batch input, so use pyarrow here.
            pq.write_table(table, str(dst))
        else:
            ctx.register_record_batches("shard", [table.combine_chunks().to_batches()])
            ctx.sql(
                f"COPY shard TO '{dst}' STORED AS PARQUET OPTIONS {_COPY_OPTIONS}"
            ).collect()
            ctx.deregister_table("shard")
        write_manifest(dst_dir, table.num_rows)
        print(f"  {entity}: {table.num_rows} rows ({dst.stat().st_size // 1024} KB)")

    def write_empty_entity(entity: str) -> None:
        dst_dir = cache_dir / entity
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst_dir.joinpath("chrom_manifest.json").write_text("[]\n")
        print(f"  {entity}: empty (manifest only)")

    for entity in _INTERVAL_ENTITIES:
        src = cache_src / entity / "chr1.parquet"
        if not src.exists():
            write_empty_entity(entity)
            continue
        table = pq.read_table(str(src))
        mask = pc.and_(
            pc.greater_equal(table["start"], 0), pc.less_equal(table["start"], end)
        )
        write_entity(entity, table.filter(mask))

    # motif: empty on chr1 in the full cache — manifest-only.
    write_empty_entity("motif")

    # translation_core: filter by transcript IDs in the trimmed transcript table.
    tx_table = pq.read_table(str(cache_dir / "transcript" / "chr1.parquet"))
    tx_ids = pa.array(tx_table.column("stable_id").to_pylist())
    tc = pq.read_table(str(cache_src / "translation_core" / "chr1.parquet"))
    write_entity(
        "translation_core", tc.filter(pc.is_in(tc["transcript_id"], value_set=tx_ids))
    )

    # translation_sift: a flat (key, sift, poly) point-lookup where
    # key = (transcript_uid << 32) | protein_position. Trim to the uids of the
    # transcripts kept above so the fixture stays small.
    ts = pq.read_table(str(cache_src / "translation_sift" / "chr1.parquet"))
    ts_uid = pc.shift_right(ts["key"], 32)
    tx_uids = pa.array(tx_table.column("transcript_uid").to_pylist()).cast(ts_uid.type)
    write_entity("translation_sift", ts.filter(pc.is_in(ts_uid, value_set=tx_uids)))
