from vepyr._core import cache_to_parquet
from vepyr import build_cache


def test_cache_to_parquet_importable():
    assert callable(cache_to_parquet)


def test_build_cache_importable():
    assert callable(build_cache)
