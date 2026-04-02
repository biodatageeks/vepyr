from vepyr._core import build_cache as _build_cache
from vepyr import build_cache, annotate


def test_build_cache_native_importable():
    """The native _core.build_cache function is callable."""
    assert callable(_build_cache)


def test_build_cache_importable():
    """The public vepyr.build_cache function is callable."""
    assert callable(build_cache)


def test_annotate_importable():
    """The public vepyr.annotate function is callable."""
    assert callable(annotate)
