import hashlib
import json
from pathlib import Path

import pytest

import download_chr22 as download


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
        for name in ("input.vcf.gz", "golden/merged.vcf.gz"):
            path = tmp_path / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

    monkeypatch.setattr(download, "run", fetch)
    download.prepare_fixtures(manifest, ["merged"], False)
    assert calls[0][-2:] == [
        "--include=input.vcf.gz,golden/merged.vcf.gz",
        "--exclude=",
    ]


def test_manifest_pins_only_chr22_for_ten_profiles():
    manifest = json.loads(download.MANIFEST.read_text())
    assert set(manifest["profiles"]) == set(download.CORE_PROFILES)
    for cache in manifest["caches"].values():
        assert len(cache["revision"]) == 40
        assert len(cache["files"]) == 14
        assert all(
            Path(path).name in ("chr22.parquet", "chrom_manifest.json")
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
