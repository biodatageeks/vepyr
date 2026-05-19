from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from tests.cache_metadata import (
    CACHE_SOURCE_METADATA_KEY,
    copy_cache_with_source_metadata,
)


def test_copy_cache_with_source_metadata_adds_metadata(tmp_path):
    source = Path("tests/data/golden/cache")
    target = copy_cache_with_source_metadata(source, tmp_path / "cache", "ensembl")
    variation = next((target / "variation").glob("*.parquet"))
    metadata = pq.read_schema(variation).metadata or {}
    assert metadata[CACHE_SOURCE_METADATA_KEY] == b"ensembl"
