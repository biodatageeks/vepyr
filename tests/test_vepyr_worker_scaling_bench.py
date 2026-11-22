"""The worker-scaling runner must time only the annotation process.

The published "process wall" is compared with the engine-comparison harness,
which times a process that starts Python, imports vepyr, annotates and writes
the VCF. Reading the output back to count its records (29.4 GB for HG002)
therefore has to happen after the measurement, and the run must still fail
when the count does not match. Every run also records the power source,
because laptop runs on battery are slower and were not distinguishable.
"""

from __future__ import annotations

import csv
import importlib.metadata
import importlib.util
import json
import os
import subprocess
import sys
import textwrap
import time
import types
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).parents[1] / "performance-tests/vepyr/scripts"


def _load(name: str, monkeypatch):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolve their module through sys.modules.
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def runner(monkeypatch):
    return _load("run_vepyr_worker_scaling", monkeypatch)


@pytest.fixture
def once(monkeypatch):
    return _load("benchmark_vepyr_once", monkeypatch)


# A stand-in for benchmark_vepyr_once.py: it accepts the same arguments,
# writes a small VCF and a metrics file the way the real child does, and
# sleeps briefly, so the measured wall is short and predictable.
STUB_CHILD = textwrap.dedent(
    """
    import argparse, json, sys, time
    from pathlib import Path

    parser = argparse.ArgumentParser()
    for flag in ("--input-vcf", "--cache-dir", "--cache-type",
                 "--reference-fasta", "--output-vcf", "--metrics-json",
                 "--workers", "--expected-records", "--compression",
                 "--preserve-record-layout"):
        parser.add_argument(flag)
    args = parser.parse_args()
    records = int(Path(args.output_vcf).with_name("stub_records.txt").read_text())
    lines = ["##fileformat=VCFv4.2", "#CHROM\\tPOS\\tID\\tREF\\tALT"]
    lines += [f"chr1\\t{100 + i}\\t.\\tA\\tC" for i in range(records)]
    Path(args.output_vcf).write_text("\\n".join(lines) + "\\n")
    time.sleep(0.1)
    Path(args.metrics_json).write_text(json.dumps({
        "status": "annotated",
        "annotation_seconds": 0.1,
        "cache_type": args.cache_type,
        "workers": int(args.workers),
        "compression": args.compression,
        "preserve_record_layout": args.preserve_record_layout == "on",
        "vepyr_version": "0.0.0-stub",
        "vep_expected_records": int(args.expected_records),
    }))
    """
)


def _setup_run(tmp_path, monkeypatch, runner, records, expected, extra=()):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    for name in ("in.vcf", "ref.fa"):
        (inputs / name).write_text("x")
    (inputs / "cache").mkdir()
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    (ssd / "stub_records.txt").write_text(str(records))
    archive = tmp_path / "archive"

    stub = tmp_path / "stub_child.py"
    stub.write_text(STUB_CHILD)
    monkeypatch.setattr(runner, "ONCE_SCRIPT", stub)
    monkeypatch.setattr(runner, "detect_power_source", lambda: "AC Power")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_vepyr_worker_scaling.py",
            "--input-vcf",
            str(inputs / "in.vcf"),
            "--cache-dir",
            str(inputs / "cache"),
            "--cache-type",
            "merged",
            "--reference-fasta",
            str(inputs / "ref.fa"),
            "--ssd-output-dir",
            str(ssd),
            "--archive-dir",
            str(archive),
            "--expected-records",
            str(expected),
            "--minimum-free-gib",
            "0",
            *extra,
        ],
    )
    return archive


def _summary(archive: Path) -> list[dict[str, str]]:
    with (archive / "summary.tsv").open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _seconds(elapsed: str) -> float:
    parts = [float(part) for part in elapsed.split(":")]
    seconds = 0.0
    for part in parts:
        seconds = seconds * 60 + part
    return seconds


def test_child_does_not_count_or_read_back_the_output(once, tmp_path, monkeypatch):
    assert not hasattr(once, "count_vcf_records")

    output = tmp_path / "out.vcf"
    metrics_path = tmp_path / "metrics.json"
    for name in ("in.vcf", "ref.fa"):
        (tmp_path / name).write_text("x")
    (tmp_path / "cache").mkdir()

    def annotate(**kwargs):
        Path(kwargs["output_vcf"]).write_text("##fileformat=VCFv4.2\n")
        # Write-only: any attempt to read the output back now fails the run.
        os.chmod(kwargs["output_vcf"], 0o200)
        return kwargs["output_vcf"]

    fake = types.ModuleType("vepyr")
    fake.__file__ = str(tmp_path / "vepyr" / "__init__.py")
    fake.annotate = annotate
    monkeypatch.setitem(sys.modules, "vepyr", fake)
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "9.9.9")
    monkeypatch.setattr(once, "REQUIRED_VEPYR_VERSION", "9.9.9")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark_vepyr_once.py",
            "--input-vcf",
            str(tmp_path / "in.vcf"),
            "--cache-dir",
            str(tmp_path / "cache"),
            "--cache-type",
            "merged",
            "--reference-fasta",
            str(tmp_path / "ref.fa"),
            "--output-vcf",
            str(output),
            "--metrics-json",
            str(metrics_path),
            "--workers",
            "2",
            "--expected-records",
            "5",
        ],
    )

    assert once.main() == 0
    metrics = json.loads(metrics_path.read_text())
    assert metrics["status"] == "annotated", metrics.get("error")
    assert "annotation_seconds" in metrics
    for key in ("output_records", "records_match_vep", "output_bytes"):
        assert key not in metrics


def test_record_count_runs_after_the_measurement(runner, tmp_path, monkeypatch):
    archive = _setup_run(tmp_path, monkeypatch, runner, records=4, expected=4)
    counted = []

    def slow_count(path):
        counted.append(path)
        time.sleep(2.0)
        return 4

    monkeypatch.setattr(runner, "count_vcf_records", slow_count)
    monkeypatch.setattr(sys, "argv", [*sys.argv, "--workers", "1"])

    assert runner.main() == 0
    assert len(counted) == 1

    (row,) = _summary(archive)
    assert _seconds(row["process_elapsed_wall"]) < 1.5
    assert row["status"] == "ok"
    assert row["output_records"] == "4"
    assert row["records_match_vep"] == "True"
    assert int(row["output_bytes"]) > 0
    assert row["power_source"] == "AC Power"

    metrics = json.loads(Path(row["metrics_file"]).read_text())
    assert metrics["status"] == "ok"
    assert metrics["output_records"] == 4
    assert metrics["records_match_vep"] is True
    assert metrics["output_bytes"] == int(row["output_bytes"])
    assert metrics["power_source"] == "AC Power"
    assert str(metrics["annotation_seconds"]) == row["annotation_seconds"]
    for key in (
        "vepyr_version",
        "cache_type",
        "workers",
        "compression",
        "preserve_record_layout",
    ):
        assert key in metrics


def test_record_count_mismatch_fails_the_run(runner, tmp_path, monkeypatch):
    archive = _setup_run(tmp_path, monkeypatch, runner, records=3, expected=5)
    monkeypatch.setattr(sys, "argv", [*sys.argv, "--workers", "2", "1"])

    assert runner.main() == 1

    # The sweep stops at the first failing worker count.
    (row,) = _summary(archive)
    assert row["workers"] == "2"
    assert row["status"] == "record_count_mismatch"
    assert row["output_records"] == "3"
    assert row["records_match_vep"] == "False"

    metrics = json.loads(Path(row["metrics_file"]).read_text())
    assert metrics["status"] == "record_count_mismatch"
    assert metrics["records_match_vep"] is False
    assert "3 records" in metrics["error"]


def test_require_ac_power_refuses_on_battery(runner, tmp_path, monkeypatch):
    archive = _setup_run(
        tmp_path,
        monkeypatch,
        runner,
        records=1,
        expected=1,
        extra=("--require-ac-power", "--workers", "1"),
    )
    monkeypatch.setattr(runner, "detect_power_source", lambda: "Battery Power")

    with pytest.raises(RuntimeError, match="(?i)battery"):
        runner.main()
    assert not (archive / "summary.tsv").exists()
    assert not any((tmp_path / "ssd").glob("*.vcf"))


def test_battery_run_without_the_flag_is_recorded(runner, tmp_path, monkeypatch):
    archive = _setup_run(
        tmp_path, monkeypatch, runner, records=1, expected=1, extra=("--workers", "1")
    )
    monkeypatch.setattr(runner, "detect_power_source", lambda: "Battery Power")

    assert runner.main() == 0
    (row,) = _summary(archive)
    assert row["power_source"] == "Battery Power"


@pytest.mark.parametrize(
    ("first_line", "expected"),
    [
        ("Now drawing from 'AC Power'", "AC Power"),
        ("Now drawing from 'Battery Power'", "Battery Power"),
        ("something else", "unknown"),
    ],
)
def test_power_source_on_macos(runner, first_line, expected):
    def run(command, **kwargs):
        assert command == ["pmset", "-g", "batt"]
        return subprocess.CompletedProcess(
            command, 0, stdout=f"{first_line}\n -InternalBattery-0 (id=1)\t80%\n"
        )

    assert runner.detect_power_source(platform="darwin", run=run) == expected


def test_power_source_on_macos_without_pmset(runner):
    def run(command, **kwargs):
        raise FileNotFoundError(command[0])

    assert runner.detect_power_source(platform="darwin", run=run) == "unknown"


def test_power_source_on_linux(runner, tmp_path):
    root = tmp_path / "power_supply"
    assert runner.detect_power_source(platform="linux", sysfs_root=root) == "unknown"

    (root / "AC").mkdir(parents=True)
    (root / "AC" / "online").write_text("0\n")
    assert (
        runner.detect_power_source(platform="linux", sysfs_root=root) == "Battery Power"
    )

    (root / "AC" / "online").write_text("1\n")
    assert runner.detect_power_source(platform="linux", sysfs_root=root) == "AC Power"
