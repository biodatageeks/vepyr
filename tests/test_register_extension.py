"""Tests for VEP function registration into polars-bio's session."""

import pytest

import polars_bio as pb

from vepyr._core import _register_vep, _version_info


def test_register_extension_calls_callback():
    """register_extension passes a non-zero pointer and version string."""
    received = {}

    def callback(ctx_ptr, datafusion_version):
        received["ptr"] = ctx_ptr
        received["version"] = datafusion_version

    pb.ctx.register_extension(callback)

    assert isinstance(received["ptr"], int)
    assert received["ptr"] != 0
    assert isinstance(received["version"], str)
    assert len(received["version"]) > 0


def test_datafusion_version_is_semver():
    """The version string should be a valid semver."""
    version = None

    def callback(ctx_ptr, datafusion_version):
        nonlocal version
        version = datafusion_version

    pb.ctx.register_extension(callback)

    parts = version.split(".")
    assert len(parts) == 3, f"Expected semver, got {version}"
    assert all(p.isdigit() for p in parts)


def test_version_info_matches_polars_bio():
    """vepyr and polars-bio must use the same DataFusion version."""
    vepyr_df_version, vepyr_rustc = _version_info()

    pb_version = None

    def callback(ctx_ptr, datafusion_version):
        nonlocal pb_version
        pb_version = datafusion_version

    pb.ctx.register_extension(callback)

    assert vepyr_df_version == pb_version, (
        f"DataFusion version mismatch: vepyr={vepyr_df_version}, polars-bio={pb_version}"
    )


def test_register_vep_functions():
    """_register_vep should register VEP functions without error."""
    pb.ctx.register_extension(_register_vep)

    # Session should still be functional
    result = pb.sql("SELECT 1 AS x").collect()
    assert result.shape == (1, 1)
    assert result["x"][0] == 1


def test_pointer_stable_across_calls():
    """Multiple calls should return the same pointer."""
    ptrs = []

    def callback(ctx_ptr, datafusion_version):
        ptrs.append(ctx_ptr)

    pb.ctx.register_extension(callback)
    pb.ctx.register_extension(callback)

    assert ptrs[0] == ptrs[1]


def test_callback_error_propagates():
    """Errors in the callback should propagate."""

    def bad_callback(ctx_ptr, datafusion_version):
        raise ValueError("test error")

    with pytest.raises(ValueError, match="test error"):
        pb.ctx.register_extension(bad_callback)


def test_register_vep_importable():
    """vepyr.register_vep should be importable and callable."""
    from vepyr import _ensure_vep_registered

    _ensure_vep_registered()
