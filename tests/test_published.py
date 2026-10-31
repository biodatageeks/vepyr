"""Tests for the published-cache pin table."""

from __future__ import annotations

import re

import pytest

from vepyr import _published

RELEASE_116 = {
    "ensembl",
    "merged",
    "refseq",
    "plugin_cadd",
    "plugin_spliceai",
    "plugin_alphamissense",
    "plugin_clinvar",
    "plugin_phenotypeorthologous",
}


def test_every_pin_is_a_full_lowercase_commit_hash():
    for key, revision in _published.PINS.items():
        assert re.fullmatch(r"[0-9a-f]{40}", revision), key


def test_release_116_pins_every_published_cache_and_plugin():
    names = {
        name
        for (release, assembly, name) in _published.PINS
        if (release, assembly) == (116, "GRCh38")
    }
    assert names == RELEASE_116


def test_repository_ids():
    assert (
        _published.cache_repo_id(116, "GRCh38", "merged")
        == "biodatageeks/vepyr_116_GRCh38_merged"
    )
    assert (
        _published.plugin_repo_id(116, "GRCh38", "clinvar")
        == "biodatageeks/vepyr_116_GRCh38_plugin_clinvar"
    )


def test_pinned_revision_is_the_default():
    assert (
        _published.cache_revision(116, "GRCh38", "merged", None)
        == _published.PINS[(116, "GRCh38", "merged")]
    )
    assert (
        _published.plugin_revision(116, "GRCh38", "clinvar", None)
        == "44b0029ed830bd5d4666b5575610e6f3ae56a2ba"
    )


@pytest.mark.parametrize("revision", ["main", "v1.0", "0123456"])
def test_explicit_revision_is_passed_through_even_when_unpinned(revision):
    assert _published.cache_revision(116, "GRCh38", "merged", revision) == revision
    assert _published.cache_revision(999, "GRCh37", "merged", revision) == revision
    assert _published.plugin_revision(999, "GRCh38", "revel", revision) == revision


def test_unpinned_cache_lists_what_is_published():
    with pytest.raises(ValueError) as excinfo:
        _published.cache_revision(115, "GRCh38", "merged", None)
    message = str(excinfo.value)
    assert "biodatageeks/vepyr_115_GRCh38_merged" in message
    assert "116 GRCh38: ensembl, merged, refseq" in message
    assert "build_cache" in message


def test_unpinned_plugin_lists_what_is_published():
    with pytest.raises(ValueError) as excinfo:
        _published.plugin_revision(116, "GRCh38", "revel", None)
    message = str(excinfo.value)
    assert "alphamissense, cadd, clinvar, phenotypeorthologous, spliceai" in message
    assert "build_plugin_cache" in message


@pytest.mark.parametrize("revision", [None, "main"])
def test_dbnsfp_explains_why_it_is_not_published(revision):
    with pytest.raises(ValueError, match="redistribution"):
        _published.plugin_revision(116, "GRCh38", "dbnsfp", revision)
