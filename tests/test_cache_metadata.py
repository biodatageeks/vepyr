from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from tests.cache_metadata import (
    CACHE_SOURCE_METADATA_KEY,
    copy_cache_with_source_metadata,
)


def test_copy_cache_with_source_metadata_adds_metadata(tmp_path):
    source = tmp_path / "source"
    variation_source = source / "variation"
    variation_source.mkdir(parents=True)
    pq.write_table(
        pa.table({"chrom": ["1"], "start": [1], "end": [2]}),
        variation_source / "chr1.parquet",
    )

    target = copy_cache_with_source_metadata(source, tmp_path / "cache", "ensembl")
    variation = next((target / "variation").glob("*.parquet"))
    metadata = pq.read_schema(variation).metadata or {}
    assert metadata[CACHE_SOURCE_METADATA_KEY] == b"ensembl"
