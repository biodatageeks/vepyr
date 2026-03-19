from vepyr._core import convert_entity
from vepyr import build_cache


def test_convert_entity_importable():
    assert callable(convert_entity)


def test_build_cache_importable():
    assert callable(build_cache)
