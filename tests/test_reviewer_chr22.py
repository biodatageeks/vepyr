import copy
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from comparison import cli
import reviewer_chr22 as reviewer


def evidence():
    entry = {
        "body_match": True,
        "count_match": True,
        "vep_body_md5": "a" * 32,
        "vepyr_body_md5": "a" * 32,
        "vep_records": 2,
        "vepyr_records": 2,
        "csq_order_ignored": False,
    }
    report = {
        "profile": "merged",
        "release": "116",
        "chrom": "chr22",
        "md5": {mode: dict(entry) for mode in ("strict", "canonical")},
    }
    expected = {
        "records": 2,
        "strict_body_md5": "a" * 32,
        "canonical_body_md5": "a" * 32,
    }
    return report, expected


@pytest.mark.parametrize(
    "defect",
    [
        "none",
        "missing_mode",
        "empty",
        "wrong_truth",
        "mismatch",
        "wrong_release",
        "wrong_order",
        "failed_process",
        "missing_report",
    ],
)
def test_verdict_requires_complete_nonempty_current_evidence(tmp_path, defect):
    report, expected = evidence()
    returncode = 0
    if defect == "missing_mode":
        del report["md5"]["canonical"]
    elif defect == "empty":
        expected["records"] = 0
        for entry in report["md5"].values():
            entry["vep_records"] = entry["vepyr_records"] = 0
    elif defect == "wrong_truth":
        expected["strict_body_md5"] = "b" * 32
    elif defect == "mismatch":
        report["md5"]["strict"]["vepyr_body_md5"] = "b" * 32
    elif defect == "wrong_release":
        report["release"] = "115"
    elif defect == "wrong_order":
        report["md5"]["strict"]["csq_order_ignored"] = True
    elif defect == "failed_process":
        returncode = -9
    path = tmp_path / "report.json"
    if defect != "missing_report":
        path.write_text(json.dumps(report))
    result = reviewer.profile_result("merged", path, returncode, expected)
    assert (result["status"] == "PASS") == (defect == "none")


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

    def download(command):
        calls.append(command)
        (destination / "variation/chr22.parquet").write_bytes(content)

    monkeypatch.setattr(reviewer, "run", download)
    reviewer.prepare_caches(manifest, ["merged"], tmp_path, False)
    assert len(calls) == 1
    assert calls[0][3] == "variation/chr22.parquet"
    assert "variation/chrom_manifest.json" not in calls[0]
    assert calls[0][calls[0].index("--revision") + 1] == "b" * 40
    reviewer.prepare_caches(manifest, ["merged"], tmp_path, True)
    assert len(calls) == 1
    (destination / "variation/chr22.parquet").unlink()
    with pytest.raises(RuntimeError, match="Missing or corrupt"):
        reviewer.prepare_caches(manifest, ["merged"], tmp_path, True)
    assert len(calls) == 1


def test_git_blob_checksum(tmp_path):
    path = tmp_path / "file"
    path.write_bytes(b"hello\n")
    assert (
        reviewer.checksum(path, "git_blob")
        == "ce013625030ba8dba906f756967f9e9ca394464a"
    )


def test_lfs_fetch_is_limited_to_selected_fixtures(tmp_path, monkeypatch):
    monkeypatch.setattr(reviewer, "ROOT", tmp_path)
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

    monkeypatch.setattr(reviewer, "run", fetch)
    reviewer.prepare_fixtures(manifest, ["merged"], False)
    assert calls[0][-2:] == [
        "--include=input.vcf.gz,golden/merged.vcf.gz",
        "--exclude=",
    ]


def test_all_profiles_run_after_failure_and_stale_report_cannot_pass(
    tmp_path, monkeypatch
):
    args = reviewer.parse_args(["--work-dir", str(tmp_path)])
    report, expected = evidence()
    golden = tmp_path / "golden.vcf.gz"
    golden.write_bytes(b"test")
    manifest = {
        "inputs": {},
        "input_vcf": "input.vcf.gz",
        "fasta": "chr22.fa.gz",
        "profiles": {
            name: {**expected, "path": str(golden)} for name in reviewer.CORE_PROFILES
        },
    }
    reports = tmp_path / "reports"
    reports.mkdir()
    stale = reports / "fast_chr22_merged_116_report.json"
    stale.write_text(json.dumps(report))
    calls = []
    monkeypatch.setattr(reviewer, "verified", lambda *args: True)
    monkeypatch.setattr(reviewer, "run", lambda *args: None)

    def annotate(command, **kwargs):
        name = command[command.index("--profile") + 1]
        calls.append(name)
        if name == "merged":
            assert not stale.exists()
            return subprocess.CompletedProcess(command, 1)
        current = copy.deepcopy(report)
        current["profile"] = name
        for entry in current["md5"].values():
            entry["csq_order_ignored"] = reviewer.PROFILES[name].ignore_csq_order
        (reports / f"fast_chr22_{name}_116_report.json").write_text(json.dumps(current))
        assert command[command.index("--output-dir") + 1] == str(tmp_path)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(reviewer.subprocess, "run", annotate)
    assert reviewer.compare_profiles(args, manifest) == 1
    assert calls == list(reviewer.CORE_PROFILES)
    assert len((tmp_path / "summary.tsv").read_text().splitlines()) == 11


def test_isolated_comparison_forwards_output_dir(monkeypatch, tmp_path):
    args = cli.parse_args(["--release", "116", "--output-dir", str(tmp_path)])
    args.comparison_mode = "md5"
    seen = []
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda cmd: seen.append(cmd) or subprocess.CompletedProcess(cmd, 0),
    )
    assert cli._run_contig_isolated("chr22", args)
    assert seen[0][seen[0].index("--output-dir") + 1] == str(tmp_path)


def test_manifest_pins_only_chr22_for_ten_profiles():
    manifest = json.loads(reviewer.MANIFEST.read_text())
    assert set(manifest["profiles"]) == set(reviewer.CORE_PROFILES)
    for cache in manifest["caches"].values():
        assert len(cache["revision"]) == 40
        assert len(cache["files"]) == 14
        assert all(
            Path(path).name in ("chr22.parquet", "chrom_manifest.json")
            for path in cache["files"]
        )
    for name, reference in manifest["profiles"].items():
        assert reference["records"] == 50861
        assert reference["ignore_csq_order"] == reviewer.PROFILES[name].ignore_csq_order
