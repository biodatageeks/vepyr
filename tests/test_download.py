"""Tests for vepyr.download_cache and vepyr.download_plugin_cache."""

from __future__ import annotations

import fnmatch
import os

import pytest
from huggingface_hub import errors
from huggingface_hub import utils as hub_utils

import vepyr
from vepyr import _download


def hub_error(cls, message="fake"):
    """An instance of a huggingface_hub HTTP error without an HTTP response."""
    error = cls.__new__(cls)
    OSError.__init__(error, message)
    return error


# A transport error as httpx (huggingface_hub 1.x) or httpx2 (2.x) raises it.
TransportError = type(
    "ConnectError", (Exception,), {"__module__": "httpx2._exceptions"}
)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (hub_error(errors.RepositoryNotFoundError), ValueError),
        (hub_error(errors.RevisionNotFoundError), ValueError),
        (hub_error(errors.HfHubHTTPError), RuntimeError),
        (errors.LocalEntryNotFoundError("offline"), RuntimeError),
        (ConnectionError("refused"), RuntimeError),
        (TransportError("refused"), RuntimeError),
    ],
)
def test_hub_errors_are_translated_and_name_the_dataset(error, expected):
    with pytest.raises(expected) as excinfo:
        with _download._hub_errors("biodatageeks/x", "abc123"):
            raise error
    assert "biodatageeks/x" in str(excinfo.value)
    assert "abc123" in str(excinfo.value)


def test_other_errors_pass_through():
    with pytest.raises(KeyError):
        with _download._hub_errors("biodatageeks/x", "abc123"):
            raise KeyError("bug")


def test_progress_false_disables_and_restores_even_on_error():
    hub_utils.enable_progress_bars()
    with pytest.raises(RuntimeError):
        with _download._progress(False):
            assert hub_utils.are_progress_bars_disabled()
            raise RuntimeError("boom")
    assert not hub_utils.are_progress_bars_disabled()


def test_progress_false_keeps_bars_disabled_if_they_already_were():
    hub_utils.disable_progress_bars()
    try:
        with _download._progress(False):
            pass
        assert hub_utils.are_progress_bars_disabled()
    finally:
        hub_utils.enable_progress_bars()


def test_progress_true_leaves_the_state_alone():
    hub_utils.enable_progress_bars()
    with _download._progress(True):
        assert not hub_utils.are_progress_bars_disabled()


CORE_FILES = [
    ".gitattributes",
    "README.md",
    "chr_synonyms.txt",
    "reference_policy.json",
    *(
        f"{entity}/{contig}.parquet"
        for entity in ("exon", "variation")
        for contig in ("chr1", "chr22", "chrX", "chrMT", "GL000008.2")
    ),
    "exon/chrom_manifest.json",
    "variation/chrom_manifest.json",
]
CORE_SIDECARS = ["*/chrom_manifest.json", "reference_policy.json", "chr_synonyms.txt"]
MERGED_PIN = "5b83dd8d249106c6cc3f1c04c522b4bec716cc97"


class FakeHub:
    """Records calls and writes the files the patterns select into local_dir."""

    def __init__(self, files, error=None):
        self.files = files
        self.error = error
        self.listed = []
        self.snapshots = []

    def list_repo_files(self, repo_id, *, revision):
        self.listed.append((repo_id, revision))
        if self.error is not None:
            raise self.error
        return list(self.files)

    def snapshot_download(self, repo_id, *, revision, local_dir, allow_patterns):
        self.snapshots.append(
            {
                "repo_id": repo_id,
                "revision": revision,
                "local_dir": local_dir,
                "allow_patterns": allow_patterns,
            }
        )
        if self.error is not None:
            raise self.error
        for name in self.files:
            if any(fnmatch.fnmatch(name, p) for p in allow_patterns):
                path = os.path.join(local_dir, name)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as f:
                    f.write("x")
        return local_dir


@pytest.fixture
def hub(monkeypatch):
    def install(files, error=None):
        fake = FakeHub(files, error)
        monkeypatch.setattr(_download, "_list_repo_files", fake.list_repo_files)
        monkeypatch.setattr(_download, "_snapshot_download", fake.snapshot_download)
        return fake

    return install


def test_full_cache_uses_the_pin_and_skips_readme(hub, tmp_path):
    fake = hub(CORE_FILES)
    path = vepyr.download_cache(
        116, str(tmp_path), cache_type="merged", show_progress=False
    )
    assert path == str(tmp_path / "116_GRCh38_merged")
    (call,) = fake.snapshots
    assert call["repo_id"] == "biodatageeks/vepyr_116_GRCh38_merged"
    assert call["revision"] == MERGED_PIN
    assert call["local_dir"] == path
    assert call["allow_patterns"] == ["*/*.parquet", *CORE_SIDECARS]
    assert fake.listed == []  # no listing without chroms
    assert not os.path.exists(os.path.join(path, "README.md"))
    assert os.path.isfile(os.path.join(path, "variation", "chr22.parquet"))


@pytest.mark.parametrize(
    ("chroms", "contigs"),
    [
        (["22"], ["chr22"]),
        (["chr22"], ["chr22"]),
        (["X", "chrX"], ["chrX"]),
        (["MT"], ["chrMT"]),
        (["GL000008.2"], ["GL000008.2"]),
        (["22", "1"], ["chr22", "chr1"]),
    ],
)
def test_chroms_select_shards_plus_sidecars(hub, tmp_path, chroms, contigs):
    fake = hub(CORE_FILES)
    vepyr.download_cache(
        116, str(tmp_path), cache_type="merged", chroms=chroms, show_progress=False
    )
    assert fake.listed == [("biodatageeks/vepyr_116_GRCh38_merged", MERGED_PIN)]
    patterns = fake.snapshots[0]["allow_patterns"]
    assert patterns == [*(f"*/{c}.parquet" for c in contigs), *CORE_SIDECARS]


def test_unknown_contig_fails_before_downloading_and_lists_main_contigs(hub, tmp_path):
    fake = hub(CORE_FILES)
    with pytest.raises(ValueError) as excinfo:
        vepyr.download_cache(
            116, str(tmp_path / "c"), cache_type="merged", chroms=["chrM", "22"]
        )
    message = str(excinfo.value)
    assert "chrM" in message and "chrMT" in message
    assert "biodatageeks/vepyr_116_GRCh38_merged" in message and MERGED_PIN in message
    assert fake.snapshots == []
    assert not (tmp_path / "c").exists()


def test_empty_chroms_is_rejected(hub, tmp_path):
    fake = hub(CORE_FILES)
    with pytest.raises(ValueError, match="at least one"):
        vepyr.download_cache(116, str(tmp_path), cache_type="merged", chroms=[])
    assert fake.snapshots == []


def test_explicit_revision_is_passed_through(hub, tmp_path):
    fake = hub(CORE_FILES)
    vepyr.download_cache(
        116, str(tmp_path), cache_type="ensembl", revision="main", show_progress=False
    )
    assert fake.snapshots[0]["revision"] == "main"


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"cache_type": "vep"}, "Invalid cache_type"),
        (
            {"cache_type": "merged", "assembly": "GRCh37"},
            "116 GRCh38: ensembl, merged, refseq",
        ),
    ],
)
def test_rejected_requests_create_nothing(hub, tmp_path, kwargs, match):
    fake = hub(CORE_FILES)
    with pytest.raises(ValueError, match=match):
        vepyr.download_cache(116, str(tmp_path / "c"), **kwargs)
    assert fake.snapshots == [] and not (tmp_path / "c").exists()


def test_tilde_is_expanded(hub, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    hub(CORE_FILES)
    path = vepyr.download_cache(
        116, "~/caches", cache_type="merged", show_progress=False
    )
    assert path == str(tmp_path / "caches" / "116_GRCh38_merged")


def test_download_errors_are_translated(hub, tmp_path):
    hub(CORE_FILES, error=hub_error(errors.RepositoryNotFoundError))
    with pytest.raises(ValueError, match="vepyr_116_GRCh38_merged at revision"):
        vepyr.download_cache(
            116, str(tmp_path), cache_type="merged", show_progress=False
        )


def test_listing_errors_are_translated(hub, tmp_path):
    hub(CORE_FILES, error=ConnectionError("refused"))
    with pytest.raises(RuntimeError, match="could not download"):
        vepyr.download_cache(116, str(tmp_path), cache_type="merged", chroms=["22"])
