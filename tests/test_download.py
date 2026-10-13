"""Tests for vepyr.download_cache and vepyr.download_plugin_cache."""

from __future__ import annotations

import fnmatch
import json
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
MAIN_COMMIT = "f" * 40


class FakeHub:
    """Records calls and writes the files the patterns select into local_dir."""

    def __init__(self, files, error=None):
        self.files = files
        self.error = error
        self.listed = []
        self.snapshots = []
        self.resolved = []

    def list_repo_files(self, repo_id, *, revision):
        self.listed.append((repo_id, revision))
        if self.error is not None:
            raise self.error
        return list(self.files)

    def resolve_commit(self, repo_id, *, revision):
        self.resolved.append((repo_id, revision))
        return MAIN_COMMIT

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
        monkeypatch.setattr(_download, "_resolve_commit", fake.resolve_commit)
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
    # Listed once, after the download, to check nothing is missing.
    assert fake.listed == [("biodatageeks/vepyr_116_GRCh38_merged", MERGED_PIN)]
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


def test_a_branch_revision_is_resolved_to_its_commit(hub, tmp_path):
    fake = hub(CORE_FILES)
    vepyr.download_cache(
        116, str(tmp_path), cache_type="ensembl", revision="main", show_progress=False
    )
    assert fake.resolved == [("biodatageeks/vepyr_116_GRCh38_ensembl", "main")]
    assert fake.snapshots[0]["revision"] == MAIN_COMMIT


def test_a_pinned_commit_needs_no_resolution(hub, tmp_path):
    fake = hub(CORE_FILES)
    vepyr.download_cache(116, str(tmp_path), cache_type="merged", show_progress=False)
    assert fake.resolved == []


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
    # expanduser reads HOME on POSIX and USERPROFILE on Windows.
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
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


PLUGIN_FILES = [
    ".gitattributes",
    "README.md",
    "manifest.json",
    "qa_profile.json",
    *(f"chr{c}.parquet" for c in ("1", "22", "X", "MT")),
]
CLINVAR_PIN = "44b0029ed830bd5d4666b5575610e6f3ae56a2ba"


def test_full_plugin_cache(hub, tmp_path):
    fake = hub(PLUGIN_FILES)
    path = vepyr.download_plugin_cache(
        "clinvar", 116, str(tmp_path), show_progress=False
    )
    assert path == str(tmp_path / "plugin" / "clinvar")
    (call,) = fake.snapshots
    assert call["repo_id"] == "biodatageeks/vepyr_116_GRCh38_plugin_clinvar"
    assert call["revision"] == CLINVAR_PIN
    assert call["allow_patterns"] == ["*.parquet", "*.json"]
    assert os.path.isfile(os.path.join(path, "manifest.json"))
    assert not os.path.exists(os.path.join(path, "README.md"))


def test_plugin_chroms(hub, tmp_path):
    fake = hub(PLUGIN_FILES)
    vepyr.download_plugin_cache(
        "clinvar", 116, str(tmp_path), chroms=["22", "MT"], show_progress=False
    )
    assert fake.snapshots[0]["allow_patterns"] == [
        "chr22.parquet",
        "chrMT.parquet",
        "*.json",
    ]


@pytest.mark.parametrize("plugin", ["../x", "a/b", "", "clin var"])
def test_plugin_names_that_are_not_plain_are_rejected(hub, tmp_path, plugin):
    fake = hub(PLUGIN_FILES)
    with pytest.raises(ValueError, match="plugin name"):
        vepyr.download_plugin_cache(plugin, 116, str(tmp_path / "pc"))
    assert fake.snapshots == [] and not (tmp_path / "pc").exists()


@pytest.mark.parametrize(
    ("plugin", "match"),
    [("dbnsfp", "redistribution"), ("revel", "alphamissense, cadd, clinvar")],
)
def test_unpublished_plugins_create_nothing(hub, tmp_path, plugin, match):
    fake = hub(PLUGIN_FILES)
    with pytest.raises(ValueError, match=match):
        vepyr.download_plugin_cache(plugin, 116, str(tmp_path / "pc"))
    assert fake.snapshots == [] and not (tmp_path / "pc").exists()


def test_unknown_plugin_contig(hub, tmp_path):
    fake = hub(PLUGIN_FILES)
    with pytest.raises(
        ValueError, match="no contig chrM; available: chr1, chr22, chrMT, chrX"
    ):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path), chroms=["chrM"])
    assert fake.snapshots == []


network = pytest.mark.skipif(
    os.environ.get("VEPYR_NETWORK_TESTS") != "1",
    reason="set VEPYR_NETWORK_TESTS=1 to download from Hugging Face",
)


@network
def test_network_download_opens_in_the_engine(tmp_path):
    """Real downloads of one small contig; the engine opens the result."""
    cache = vepyr.download_cache(
        116,
        str(tmp_path / "c"),
        cache_type="merged",
        chroms=["MT"],
        show_progress=False,
    )
    assert os.path.isfile(os.path.join(cache, "variation", "chrMT.parquet"))
    assert os.path.isfile(os.path.join(cache, "reference_policy.json"))
    # The resume metadata stays inside the cache and must not disturb the engine.
    assert os.path.isdir(os.path.join(cache, ".cache", "huggingface"))
    identity = vepyr.cache_contig_identity(cache, "chrMT")
    assert identity

    plugin = vepyr.download_plugin_cache(
        "phenotypeorthologous",
        116,
        str(tmp_path / "pc"),
        chroms=["22"],
        show_progress=False,
    )
    assert sorted(os.listdir(plugin)) == [".cache", "chr22.parquet", "manifest.json"]


def _files(path):
    """Files under ``path``, ignoring the lock (filelock >= 3.22 removes it)."""
    return sorted(
        rel
        for root, _dirs, names in os.walk(path)
        for name in names
        if not (rel := os.path.relpath(os.path.join(root, name), path)).endswith(
            "download.lock"
        )
    )


@pytest.mark.parametrize("kind", ["cache", "plugin"])
def test_a_failed_download_leaves_no_record_and_no_data(hub, tmp_path, kind):
    """Directories stay (other downloads may share them); no files do."""
    hub(CORE_FILES + PLUGIN_FILES, error=ConnectionError("refused"))
    with pytest.raises(RuntimeError):
        if kind == "cache":
            vepyr.download_cache(116, str(tmp_path / "c"), cache_type="merged")
        else:
            vepyr.download_plugin_cache("clinvar", 116, str(tmp_path / "c"))
    assert _files(tmp_path / "c") == []


def test_a_failed_download_never_deletes_a_shared_root(hub, tmp_path, monkeypatch):
    fake = hub(PLUGIN_FILES)
    root = tmp_path / "pc"

    def other_plugin_then_fail(repo_id, *, revision, local_dir, allow_patterns):
        other = root / "plugin" / "alphamissense"
        other.mkdir(parents=True)
        (other / "manifest.json").write_text("{}")
        raise ConnectionError("refused")

    monkeypatch.setattr(_download, "_snapshot_download", other_plugin_then_fail)
    with pytest.raises(RuntimeError):
        vepyr.download_plugin_cache("clinvar", 116, str(root))
    assert (root / "plugin" / "alphamissense" / "manifest.json").is_file()
    assert fake.snapshots == []


@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0, reason="POSIX permissions")
def test_an_unwritable_root_is_a_runtime_error(hub, tmp_path):
    hub(PLUGIN_FILES)
    root = tmp_path / "ro"
    root.mkdir()
    root.chmod(0o500)
    try:
        with pytest.raises(RuntimeError, match="could not write"):
            vepyr.download_plugin_cache("clinvar", 116, str(root), show_progress=False)
    finally:
        root.chmod(0o700)


def test_translated_errors_are_one_line():
    multi = hub_error(
        errors.RepositoryNotFoundError,
        "404 Client Error.\n\nRepository Not Found\nhint",
    )
    with pytest.raises(ValueError) as excinfo:
        with _download._hub_errors("biodatageeks/x", "main"):
            raise multi
    assert "\n" not in str(excinfo.value)
    assert "404 Client Error. Repository Not Found hint" in str(excinfo.value)


def test_the_downloaded_commit_is_recorded(hub, tmp_path):
    hub(CORE_FILES)
    path = vepyr.download_cache(
        116, str(tmp_path), cache_type="merged", show_progress=False
    )
    assert _download._recorded(path) == {
        "repo_id": "biodatageeks/vepyr_116_GRCh38_merged",
        "commit": MERGED_PIN,
    }


def test_another_revision_is_not_mixed_into_an_existing_cache(hub, tmp_path):
    fake = hub(CORE_FILES)
    vepyr.download_cache(116, str(tmp_path), cache_type="merged", show_progress=False)
    with pytest.raises(ValueError) as excinfo:
        vepyr.download_cache(
            116, str(tmp_path), cache_type="merged", chroms=["22"], revision="main"
        )
    message = str(excinfo.value)
    assert MERGED_PIN in message and MAIN_COMMIT in message
    assert len(fake.snapshots) == 1


def test_the_same_revision_can_be_downloaded_again(hub, tmp_path):
    fake = hub(CORE_FILES)
    for chroms in (["22"], ["1"]):
        vepyr.download_cache(
            116, str(tmp_path), cache_type="merged", chroms=chroms, show_progress=False
        )
    assert len(fake.snapshots) == 2


def test_plugin_caches_do_not_mix_revisions(hub, tmp_path):
    hub(PLUGIN_FILES)
    vepyr.download_plugin_cache("clinvar", 116, str(tmp_path), show_progress=False)
    with pytest.raises(ValueError, match="already holds"):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path), revision="main")


def test_generic_hub_http_errors_become_runtime_errors(monkeypatch):
    """huggingface_hub 1.x: HfHubHTTPError derives from httpx, not OSError."""
    fake = type(
        "HfHubHTTPError", (Exception,), {"__module__": "huggingface_hub.errors"}
    )
    monkeypatch.setattr(errors, "HfHubHTTPError", fake)
    with pytest.raises(RuntimeError, match="429"):
        with _download._hub_errors("biodatageeks/x", "abc123"):
            raise fake("429 Too Many Requests")


@pytest.mark.parametrize("http_subclass", [False, True])
def test_offline_errors_say_so(monkeypatch, http_subclass):
    """In some releases LocalEntryNotFoundError derives from HfHubHTTPError."""
    if http_subclass:
        offline = type("LocalEntryNotFoundError", (errors.HfHubHTTPError,), {})
        monkeypatch.setattr(errors, "LocalEntryNotFoundError", offline)
        error = hub_error(offline, "offline")
    else:
        error = errors.LocalEntryNotFoundError("offline")
    with pytest.raises(RuntimeError, match="not available offline"):
        with _download._hub_errors("biodatageeks/x", "abc123"):
            raise error


def test_a_corrupt_record_names_the_file(hub, tmp_path):
    hub(PLUGIN_FILES)
    record = tmp_path / "plugin" / "clinvar" / ".cache" / "vepyr" / "download.json"
    record.parent.mkdir(parents=True)
    record.write_text('{"repo_id": "biodatageeks/vep')
    with pytest.raises(ValueError, match="download.json"):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path))


def test_the_refusal_names_both_repositories(hub, tmp_path):
    hub(PLUGIN_FILES)
    vepyr.download_plugin_cache("clinvar", 116, str(tmp_path), show_progress=False)
    with pytest.raises(ValueError) as excinfo:
        vepyr.download_plugin_cache(
            "clinvar", 115, str(tmp_path), revision="main", show_progress=False
        )
    message = str(excinfo.value)
    assert "vepyr_116_GRCh38_plugin_clinvar" in message
    assert "vepyr_115_GRCh38_plugin_clinvar" in message


def _populate(target, files, commit=None):
    """Files as `hf download` (with commit) or build_cache (without) leaves them."""
    for name in files:
        path = target / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")
        if commit is not None:
            meta = target / ".cache" / "huggingface" / "download" / f"{name}.metadata"
            meta.parent.mkdir(parents=True, exist_ok=True)
            meta.write_text(f"{commit}\netag\n{path.stat().st_mtime}\n")


def test_an_unrecorded_cache_from_another_source_is_refused(hub, tmp_path):
    fake = hub(CORE_FILES)
    _populate(tmp_path / "116_GRCh38_merged", ["variation/chr1.parquet"])
    with pytest.raises(ValueError, match="revision is unknown"):
        vepyr.download_cache(116, str(tmp_path), cache_type="merged", chroms=["22"])
    assert fake.snapshots == []


def test_an_hf_download_at_another_commit_is_refused(hub, tmp_path):
    fake = hub(CORE_FILES)
    _populate(tmp_path / "116_GRCh38_merged", ["variation/chr1.parquet"], "a" * 40)
    with pytest.raises(ValueError, match="a" * 40):
        vepyr.download_cache(116, str(tmp_path), cache_type="merged", chroms=["22"])
    assert fake.snapshots == []


def test_an_hf_download_at_the_same_commit_is_adopted(hub, tmp_path):
    fake = hub(CORE_FILES)
    target = tmp_path / "116_GRCh38_merged"
    _populate(target, ["variation/chr1.parquet", "chr_synonyms.txt"], MERGED_PIN)
    vepyr.download_cache(
        116, str(tmp_path), cache_type="merged", chroms=["22"], show_progress=False
    )
    assert len(fake.snapshots) == 1
    assert _download._recorded(str(target))["commit"] == MERGED_PIN


@pytest.mark.parametrize("content", ["[]", '"x"', "123", "null"])
def test_a_record_that_is_not_an_object_is_unreadable(hub, tmp_path, content):
    hub(PLUGIN_FILES)
    record = tmp_path / "plugin" / "clinvar" / ".cache" / "vepyr" / "download.json"
    record.parent.mkdir(parents=True)
    record.write_text(content)
    with pytest.raises(ValueError, match="download.json"):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path))


def test_a_target_under_a_regular_file_is_a_runtime_error(hub, tmp_path):
    hub(PLUGIN_FILES)
    (tmp_path / "plugin").write_text("not a directory")
    with pytest.raises(RuntimeError, match="could not (read|write)"):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path))


def test_a_failed_first_download_into_an_existing_directory_leaves_no_record(
    hub, tmp_path
):
    hub(PLUGIN_FILES, error=ConnectionError("refused"))
    target = tmp_path / "plugin" / "clinvar"
    target.mkdir(parents=True)
    with pytest.raises(RuntimeError):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path))
    assert target.is_dir()
    assert _download._recorded(str(target)) is None
    assert _files(target) == []


def test_the_target_is_locked_while_downloading(hub, tmp_path, monkeypatch):
    from filelock import FileLock, Timeout

    fake = hub(PLUGIN_FILES)
    seen = []
    download = fake.snapshot_download

    def check_lock(repo_id, *, revision, local_dir, allow_patterns):
        lock = FileLock(os.path.join(local_dir, _download._LOCK), timeout=0)
        try:
            lock.acquire()
            lock.release()
            seen.append("free")
        except Timeout:
            seen.append("held")
        return download(
            repo_id,
            revision=revision,
            local_dir=local_dir,
            allow_patterns=allow_patterns,
        )

    monkeypatch.setattr(_download, "_snapshot_download", check_lock)
    vepyr.download_plugin_cache("clinvar", 116, str(tmp_path), show_progress=False)
    assert seen == ["held"]


def test_a_lock_error_is_a_runtime_error(hub, tmp_path, monkeypatch):
    import filelock

    class BrokenLock:
        def __init__(self, path):
            self.path = path

        def acquire(self, timeout=-1):
            raise FileNotFoundError(self.path)

    hub(PLUGIN_FILES)
    monkeypatch.setattr(filelock, "FileLock", BrokenLock)
    with pytest.raises(RuntimeError, match="could not lock"):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path))


def test_waiting_for_another_download_is_reported(hub, tmp_path, monkeypatch, caplog):
    import filelock

    class BusyOnce:
        calls = 0

        def __init__(self, path):
            self.path = path

        def acquire(self, timeout=-1):
            BusyOnce.calls += 1
            if timeout == 0 and BusyOnce.calls == 1:
                raise filelock.Timeout(self.path)

        def release(self):
            pass

    hub(PLUGIN_FILES)
    monkeypatch.setattr(filelock, "FileLock", BusyOnce)
    with caplog.at_level("WARNING", logger="vepyr"):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path), show_progress=False)
    assert "waiting for another download" in caplog.text
    assert BusyOnce.calls == 2


@pytest.mark.skipif(os.name == "nt", reason="symlinks need privileges on Windows")
def test_a_symlinked_directory_counts_as_unknown_content(hub, tmp_path):
    fake = hub(CORE_FILES)
    elsewhere = tmp_path / "elsewhere"
    _populate(elsewhere, ["chr1.parquet"])
    target = tmp_path / "c" / "116_GRCh38_merged"
    target.mkdir(parents=True)
    (target / "variation").symlink_to(elsewhere, target_is_directory=True)
    with pytest.raises(ValueError, match="revision is unknown"):
        vepyr.download_cache(116, str(tmp_path / "c"), cache_type="merged")
    assert fake.snapshots == []


@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0, reason="POSIX permissions")
def test_an_unlistable_directory_aborts_the_download(hub, tmp_path):
    fake = hub(CORE_FILES)
    target = tmp_path / "116_GRCh38_merged"
    _populate(target, ["variation/chr1.parquet"])
    (target / "variation").chmod(0o300)
    try:
        with pytest.raises(RuntimeError, match="could not read"):
            vepyr.download_cache(116, str(tmp_path), cache_type="merged")
    finally:
        (target / "variation").chmod(0o700)
    assert fake.snapshots == []


def test_an_interrupted_record_write_leaves_no_partial_record(
    hub, tmp_path, monkeypatch
):
    fake = hub(PLUGIN_FILES)

    def half_written(obj, f):
        f.write('{"repo_id": "biodatageeks/vep')
        raise OSError("disk full")

    monkeypatch.setattr(_download.json, "dump", half_written)
    with pytest.raises(RuntimeError, match="could not write"):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path))
    assert _download._recorded(str(tmp_path / "plugin" / "clinvar")) is None
    assert fake.snapshots == []


@pytest.mark.parametrize("kind", ["cache", "plugin"])
def test_a_download_that_returns_no_data_fails(hub, tmp_path, monkeypatch, kind):
    """Offline, huggingface_hub may return a non-empty local_dir as is."""
    hub(CORE_FILES + PLUGIN_FILES)
    monkeypatch.setattr(
        _download, "_snapshot_download", lambda repo_id, **kw: kw["local_dir"]
    )
    with pytest.raises(RuntimeError, match="missing"):
        if kind == "cache":
            vepyr.download_cache(116, str(tmp_path), cache_type="merged", chroms=["22"])
        else:
            vepyr.download_plugin_cache("clinvar", 116, str(tmp_path), chroms=["22"])
    target = (
        tmp_path / "116_GRCh38_merged"
        if kind == "cache"
        else tmp_path / "plugin" / "clinvar"
    )
    assert _download._recorded(str(target)) is None


def test_a_file_changed_after_hf_download_has_unknown_revision(hub, tmp_path):
    fake = hub(CORE_FILES)
    target = tmp_path / "116_GRCh38_merged"
    _populate(target, ["variation/chr1.parquet"], MERGED_PIN)
    shard = target / "variation" / "chr1.parquet"
    later = os.path.getmtime(shard) + 3600
    os.utime(shard, (later, later))
    with pytest.raises(ValueError, match="revision is unknown"):
        vepyr.download_cache(116, str(tmp_path), cache_type="merged", chroms=["22"])
    assert fake.snapshots == []


def test_a_full_download_over_a_partial_cache_must_complete(hub, tmp_path, monkeypatch):
    """Offline, a full request may return an earlier chr22-only download as is."""
    hub(CORE_FILES)
    vepyr.download_cache(
        116, str(tmp_path), cache_type="merged", chroms=["22"], show_progress=False
    )
    monkeypatch.setattr(
        _download, "_snapshot_download", lambda repo_id, **kw: kw["local_dir"]
    )
    with pytest.raises(RuntimeError, match="missing"):
        vepyr.download_cache(116, str(tmp_path), cache_type="merged")


def test_the_cached_tree_listing_verifies_without_the_hub(hub, tmp_path):
    fake = hub(CORE_FILES)
    target = tmp_path / "116_GRCh38_merged"
    tree = target / ".cache" / "huggingface" / "trees" / f"{MERGED_PIN}.json"
    tree.parent.mkdir(parents=True)
    tree.write_text(
        json.dumps({"format_version": 1, "files": {n: {} for n in CORE_FILES}})
    )
    vepyr.download_cache(116, str(tmp_path), cache_type="merged", show_progress=False)
    assert fake.listed == []


@pytest.mark.parametrize(
    ("kind", "files"),
    [
        ("cache", [n for n in CORE_FILES if n != "variation/chrom_manifest.json"]),
        ("cache", [n for n in CORE_FILES if not n.endswith(".parquet")]),
        ("plugin", [n for n in PLUGIN_FILES if n != "manifest.json"]),
        ("plugin", [n for n in PLUGIN_FILES if not n.endswith(".parquet")]),
    ],
)
def test_a_revision_without_the_required_files_fails(hub, tmp_path, kind, files):
    hub(files)
    with pytest.raises(RuntimeError, match="missing"):
        if kind == "cache":
            vepyr.download_cache(116, str(tmp_path), cache_type="merged")
        else:
            vepyr.download_plugin_cache("clinvar", 116, str(tmp_path))


def test_local_paths_are_extended_only_on_windows(tmp_path):
    path = str(tmp_path / "a" / "b.parquet")
    extended = _download._local_path(path)
    if os.name == "nt":
        assert extended.startswith("\\\\?\\")
    else:
        assert extended == path


def test_a_full_core_download_needs_a_variation_shard(hub, tmp_path):
    hub(
        [
            n
            for n in CORE_FILES
            if not (n.startswith("variation/") and n.endswith(".parquet"))
        ]
    )
    with pytest.raises(RuntimeError, match="variation shard"):
        vepyr.download_cache(116, str(tmp_path), cache_type="merged")


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("C:\\cache\\f.parquet", "\\\\?\\C:\\cache\\f.parquet"),
        ("\\\\server\\share\\f.parquet", "\\\\?\\UNC\\server\\share\\f.parquet"),
        ("\\\\?\\C:\\f.parquet", "\\\\?\\C:\\f.parquet"),
    ],
)
def test_windows_extended_paths(monkeypatch, path, expected):
    import ntpath

    monkeypatch.setattr(_download.os, "name", "nt")
    monkeypatch.setattr(_download.os.path, "abspath", ntpath.abspath)
    assert _download._local_path(path) == expected


def test_hub_bookkeeping_alone_does_not_keep_the_record(hub, tmp_path, monkeypatch):
    hub(PLUGIN_FILES)
    target = tmp_path / "plugin" / "clinvar"

    def tree_only(repo_id, *, revision, local_dir, allow_patterns):
        tree = os.path.join(local_dir, ".cache", "huggingface", "trees", "x.json")
        os.makedirs(os.path.dirname(tree), exist_ok=True)
        with open(tree, "w") as f:
            f.write("{}")
        return local_dir

    monkeypatch.setattr(_download, "_snapshot_download", tree_only)
    with pytest.raises(RuntimeError, match="missing"):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path))
    assert _download._recorded(str(target)) is None


def test_a_partial_hub_file_keeps_the_record_for_resuming(hub, tmp_path, monkeypatch):
    hub(PLUGIN_FILES)
    target = tmp_path / "plugin" / "clinvar"

    def partial(repo_id, *, revision, local_dir, allow_patterns):
        part = os.path.join(
            local_dir, ".cache", "huggingface", "download", "x.incomplete"
        )
        os.makedirs(os.path.dirname(part), exist_ok=True)
        with open(part, "w") as f:
            f.write("x")
        raise ConnectionError("dropped")

    monkeypatch.setattr(_download, "_snapshot_download", partial)
    with pytest.raises(RuntimeError):
        vepyr.download_plugin_cache("clinvar", 116, str(tmp_path))
    assert _download._recorded(str(target)) is not None


def test_the_tree_listing_is_read_through_the_local_path(hub, tmp_path, monkeypatch):
    seen = []
    real = _download._local_path
    monkeypatch.setattr(_download, "_local_path", lambda p: seen.append(p) or real(p))
    hub(PLUGIN_FILES)
    vepyr.download_plugin_cache("clinvar", 116, str(tmp_path), show_progress=False)
    assert any(os.path.join("trees", "") in p for p in seen)
