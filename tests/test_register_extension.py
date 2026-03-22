"""Tests for VEP function registration into polars-bio's session."""

import pytest
import polars_bio as pb
from vepyr._core import _register_vep


def test_register_extension_calls_callback():
    received = {}
    def callback(ctx_ptr, datafusion_version):
        received["ptr"] = ctx_ptr
        received["version"] = datafusion_version
    pb.ctx.register_extension(callback)
    assert isinstance(received["ptr"], int)
    assert received["ptr"] != 0
    assert isinstance(received["version"], str)


def test_datafusion_version_is_semver():
    version = None
    def callback(ctx_ptr, datafusion_version):
        nonlocal version
        version = datafusion_version
    pb.ctx.register_extension(callback)
    parts = version.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)


def test_register_vep_functions():
    pb.ctx.register_extension(_register_vep)
    result = pb.sql("SELECT 1 AS x").collect()
    assert result.shape == (1, 1)


def test_pointer_stable_across_calls():
    ptrs = []
    def callback(ctx_ptr, datafusion_version):
        ptrs.append(ctx_ptr)
    pb.ctx.register_extension(callback)
    pb.ctx.register_extension(callback)
    assert ptrs[0] == ptrs[1]


def test_callback_error_propagates():
    def bad_callback(ctx_ptr, datafusion_version):
        raise ValueError("test error")
    with pytest.raises(ValueError, match="test error"):
        pb.ctx.register_extension(bad_callback)
