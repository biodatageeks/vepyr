import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

import download_chr22 as download


@pytest.fixture
def lfs_fixture_repo(tmp_path, monkeypatch):
    if shutil.which("git-lfs") is None:
        pytest.skip("Git LFS is required for the recovery integration test")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    repo = tmp_path / "repo"
    remote = tmp_path / "remote.git"
    repo.mkdir()

    def git(*args):
        return subprocess.run(
            ["git", *map(str, args)], cwd=repo, check=True, capture_output=True
        )

    git("init", "--bare", remote)
    git("init", "-b", "main")
    git("config", "user.email", "test@example.invalid")
    git("config", "user.name", "Fixture test")
    git("lfs", "install", "--local")
    (repo / ".gitattributes").write_text("*.gz filter=lfs diff=lfs merge=lfs -text\n")
    content = b"original fixture bytes\n"
    metadata = {"size": len(content), "sha256": hashlib.sha256(content).hexdigest()}
    for name in ("input.vcf", "chr22.fa.gz", "merged.vcf.gz"):
        (repo / name).write_bytes(content)
    git("add", ".")
    git("commit", "--no-gpg-sign", "-m", "test fixtures")
    git("remote", "add", "origin", remote)
    git("push", "-u", "origin", "main")
    # Require an actual transfer from the local remote, not just a cache hit.
    shutil.rmtree(repo / ".git/lfs/objects")
    manifest = {
        "inputs": {"input.vcf": metadata, "chr22.fa.gz": metadata},
        "profiles": {"merged": {"path": "merged.vcf.gz", **metadata}},
    }
    monkeypatch.setattr(download, "ROOT", repo)
    return repo, remote, manifest, content


@pytest.mark.parametrize("state", ["corrupt", "pointer", "missing"])
def test_lfs_files_are_recovered_and_originals_preserved(lfs_fixture_repo, state):
    repo, _, manifest, content = lfs_fixture_repo
    originals = {}
    for name in ("chr22.fa.gz", "merged.vcf.gz"):
        if state == "missing":
            (repo / name).unlink()
        else:
            original = (
                b"x" * len(content)
                if state == "corrupt"
                else subprocess.check_output(["git", "show", f"HEAD:{name}"], cwd=repo)
            )
            (repo / name).write_bytes(original)
            originals[name] = original

    download.prepare_fixtures(manifest, ["merged"], False)

    for name in ("chr22.fa.gz", "merged.vcf.gz"):
        assert (repo / name).read_bytes() == content
        backups = list((repo / "e2e-testing/results/lfs-recovery").glob(f"*/{name}"))
        assert len(backups) == (0 if state == "missing" else 1)
        if backups:
            assert backups[0].read_bytes() == originals[name]
    assert (repo / "input.vcf").read_bytes() == content


def test_offline_fixture_failure_does_not_modify_files(lfs_fixture_repo, monkeypatch):
    repo, _, manifest, _ = lfs_fixture_repo
    corrupt = b"changed fixture"
    (repo / "chr22.fa.gz").write_bytes(corrupt)

    def no_commands(*args, **kwargs):
        pytest.fail("Offline fixture verification must not invoke Git")

    monkeypatch.setattr(download, "run", no_commands)
    with pytest.raises(RuntimeError, match="--offline"):
        download.prepare_fixtures(manifest, ["merged"], True)
    assert (repo / "chr22.fa.gz").read_bytes() == corrupt
    assert not (repo / "e2e-testing/results/lfs-recovery").exists()


def test_regular_git_fixture_is_not_moved_or_overwritten(lfs_fixture_repo):
    repo, _, manifest, _ = lfs_fixture_repo
    (repo / "input.vcf").write_bytes(b"user edit")
    with pytest.raises(RuntimeError, match="regular Git fixture"):
        download.prepare_fixtures(manifest, ["merged"], False)
    assert (repo / "input.vcf").read_bytes() == b"user edit"
    assert not (repo / "e2e-testing/results/lfs-recovery").exists()


def test_failed_lfs_pull_preserves_corrupt_original(lfs_fixture_repo):
    repo, remote, manifest, _ = lfs_fixture_repo
    (repo / "merged.vcf.gz").write_bytes(b"corrupt")
    remote.rename(remote.with_name("unavailable.git"))
    with pytest.raises(RuntimeError, match="Git LFS pull failed"):
        download.prepare_fixtures(manifest, ["merged"], False)
    backups = list((repo / "e2e-testing/results/lfs-recovery").glob("*/merged.vcf.gz"))
    assert len(backups) == 1
    assert backups[0].read_bytes() == b"corrupt"


def test_cache_download_is_pinned_and_recovers_corrupt_files(tmp_path, monkeypatch):
    content = b"parquet test bytes"
    metadata = {"size": len(content), "sha256": hashlib.sha256(content).hexdigest()}
    files = {
        "variation/chr22.parquet": metadata,
        "variation/chrom_manifest.json": metadata,
    }
    manifest = {
        "caches": {
            "merged": {"repo_id": "owner/cache", "revision": "b" * 40, "files": files}
        }
    }
    destination = tmp_path / "cache/116_GRCh38_merged"
    (destination / "variation").mkdir(parents=True)
    # Same size, different bytes: existence/size checks alone are insufficient.
    (destination / "variation/chr22.parquet").write_bytes(b"x" * len(content))
    (destination / "variation/chrom_manifest.json").write_bytes(content)
    calls = []

    def fetch(command):
        calls.append(command)
        (destination / "variation/chr22.parquet").write_bytes(content)

    monkeypatch.setattr(download, "run", fetch)
    download.prepare_caches(manifest, ["merged"], tmp_path, False)
    assert len(calls) == 1
    assert calls[0][3] == "variation/chr22.parquet"
    assert "variation/chrom_manifest.json" not in calls[0]
    assert calls[0][calls[0].index("--revision") + 1] == "b" * 40
    download.prepare_caches(manifest, ["merged"], tmp_path, True)
    assert len(calls) == 1
    (destination / "variation/chr22.parquet").unlink()
    with pytest.raises(RuntimeError, match="Missing or corrupt"):
        download.prepare_caches(manifest, ["merged"], tmp_path, True)
    assert len(calls) == 1


def test_git_blob_checksum(tmp_path):
    path = tmp_path / "file"
    path.write_bytes(b"hello\n")
    assert (
        download.checksum(path, "git_blob")
        == "ce013625030ba8dba906f756967f9e9ca394464a"
    )


def test_lfs_fetch_is_limited_to_selected_fixtures(tmp_path, monkeypatch):
    monkeypatch.setattr(download, "ROOT", tmp_path)
    data = b"fixture"
    meta = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    manifest = {
        "inputs": {"input.vcf.gz": meta},
        "profiles": {"merged": {"path": "golden/merged.vcf.gz", **meta}},
    }
    calls = []

    def fetch(command, **kwargs):
        calls.append(command)
        if "check-attr" in command:
            paths = command[command.index("--") + 1 :]
            return subprocess.CompletedProcess(
                command, 0, stdout="".join(f"{path}\0filter\0lfs\0" for path in paths)
            )
        for name in ("input.vcf.gz", "golden/merged.vcf.gz"):
            path = tmp_path / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

    monkeypatch.setattr(download, "run", fetch)
    download.prepare_fixtures(manifest, ["merged"], False)
    assert calls[-1][-2:] == [
        "--include=input.vcf.gz,golden/merged.vcf.gz",
        "--exclude=",
    ]


def test_cache_download_fetches_missing_root_files(tmp_path, monkeypatch):
    content = b"{}"
    metadata = {"size": len(content), "sha256": hashlib.sha256(content).hexdigest()}
    manifest = {
        "caches": {
            "merged": {
                "repo_id": "owner/cache",
                "revision": "c" * 40,
                "files": {"reference_policy.json": metadata},
            }
        }
    }
    destination = tmp_path / "cache/116_GRCh38_merged"
    destination.mkdir(parents=True)
    calls = []

    def fetch(command):
        calls.append(command)
        (destination / "reference_policy.json").write_bytes(content)

    monkeypatch.setattr(download, "run", fetch)
    download.prepare_caches(manifest, ["merged"], tmp_path, False)
    assert calls[0][3] == "reference_policy.json"
    assert (destination / "reference_policy.json").read_bytes() == content


def test_manifest_pins_only_chr22_for_ten_profiles():
    manifest = json.loads(download.MANIFEST.read_text())
    assert set(manifest["profiles"]) == set(download.CORE_PROFILES)
    for cache in manifest["caches"].values():
        assert len(cache["revision"]) == 40
        assert len(cache["files"]) == 16
        assert {p for p in cache["files"] if "/" not in p} == {
            "reference_policy.json",
            "chr_synonyms.txt",
        }
        assert all(
            "/" not in path
            or Path(path).name in ("chr22.parquet", "chrom_manifest.json")
            for path in cache["files"]
        )
    for name, reference in manifest["profiles"].items():
        assert reference["records"] == 50861
        assert reference["ignore_csq_order"] == download.PROFILES[name].ignore_csq_order


def test_workspace_uses_existing_profile_resolution_and_preserves_sources(
    tmp_path, monkeypatch
):
    from comparison import profiles

    root = tmp_path / "repo"
    root.mkdir()
    work = tmp_path / "work"
    monkeypatch.setattr(download, "ROOT", root)
    monkeypatch.setenv("DATA_VEPYR_DIR", str(work))
    content = b"fixture bytes"
    metadata = {"size": len(content), "sha256": hashlib.sha256(content).hexdigest()}
    manifest = {"inputs": {"input_chr22.vcf.gz": metadata}, "profiles": {}}
    (root / "input_chr22.vcf.gz").write_bytes(content)
    for name in download.CORE_PROFILES:
        path = f"{name}.vcf.gz"
        (root / path).write_bytes(content)
        manifest["profiles"][name] = {"path": path, **metadata}
        (work / "cache" / f"116_GRCh38_{profiles.PROFILES[name].flavour}").mkdir(
            parents=True, exist_ok=True
        )

    indexed = []

    def index(command):
        path = command[-1]
        assert path.is_relative_to(work)
        indexed.append(path)
        Path(str(path) + ".tbi").write_bytes(b"index")

    monkeypatch.setattr(download, "run", index)
    download.prepare_workspace(manifest, download.CORE_PROFILES, work)
    assert len(indexed) == 10
    for name in download.CORE_PROFILES:
        resolved = profiles.resolve(name, "116")
        assert Path(resolved.vep_vcf) in indexed
        assert Path(resolved.vep_vcf).read_bytes() == content
        assert (root / f"{name}.vcf.gz").read_bytes() == content
    assert (work / "input/input_chr22.vcf.gz").read_bytes() == content
