"""Source URLs distinguish the converted dataset from native VEP inputs."""

import tomllib

import pytest
from fixture_sources import ROOT, cache_source, is_download_url, vep_cache


@pytest.mark.parametrize("flavour", ["ensembl", "merged", "refseq"])
def test_dataset_url_uses_the_dataset_revision(flavour):
    pins = tomllib.loads((ROOT / "PINS.toml").read_text())
    pin = pins[f"hf_cache_{flavour}"]
    assert cache_source(flavour) == f"{pin['repo']}/tree/{pin['sha']}"
    assert vep_cache(flavour).startswith("https://ftp.ensembl.org/")
    suffix = "" if flavour == "ensembl" else f"_{flavour}"
    assert vep_cache(flavour).endswith(f"homo_sapiens{suffix}_vep_116_GRCh38.tar.gz")


@pytest.mark.parametrize(
    "url",
    [
        "local:/tmp/cache",
        "/tmp/reference.fa",
        "file:///tmp/x",
        "https:///missing-host",
        "https://user:password@example.org/x",
        "https://example.org/a b",
    ],
)
def test_local_or_malformed_urls_are_rejected(url):
    assert not is_download_url(url)


def test_download_urls_are_accepted():
    assert is_download_url(cache_source("merged"))
    assert is_download_url(vep_cache("merged"))
    assert is_download_url("https://ftp.ensembl.org/pub/reference.fa.gz")
