#!/usr/bin/env python3
"""Freeze Figure 1 performance data from a specified Git revision.

Reads committed TSVs, original GNU time logs and vepyr metrics without changing
the checkout. No benchmarks are executed. Run with --ref origin/master after
fetching; chart generation subsequently uses the frozen JSON without Git.
"""

import argparse
import csv
import hashlib
import io
import json
import math
import re
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
VEP = "performance-tests/vep/outputs/116"
VEPYR = "performance-tests/vepyr/outputs/116"
RECORDS = 4_096_123
PAIRS = ((1, "none"), (2, "1"), (4, "3"), (8, "7"))


def seconds(value):
    result = 0.0
    for part in value.split(":"):
        result = result * 60 + float(part)
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"Invalid elapsed time: {value}")
    return result


class Archive:
    def __init__(self, ref):
        self.commit = subprocess.check_output(
            ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"], cwd=REPO, text=True
        ).strip()
        self.sources = {}

    def read(self, path):
        data = subprocess.check_output(
            ["git", "show", f"{self.commit}:{path}"], cwd=REPO
        )
        self.sources[path] = hashlib.sha256(data).hexdigest()
        return data.decode()

    def rows(self, path):
        return list(csv.DictReader(io.StringIO(self.read(path)), delimiter="\t"))

    def check_time(self, path, expected):
        log = self.read(path)
        match = re.search(
            r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*(\S+)", log
        )
        if not match or not math.isclose(seconds(match[1]), expected, abs_tol=0.001):
            raise ValueError(f"Summary and original GNU time log disagree: {path}")
        if not re.search(r"Exit status:\s*0\s*$", log):
            raise ValueError(f"Unsuccessful measurement: {path}")
        return log


def collect(archive, platform, cache_type="merged"):
    linux = platform == "linux"
    version = "0.9.0" if linux else "0.7.0"
    vep_base = (
        f"{VEP}/{cache_type}_fork_scaling"
        if linux
        else f"{VEP}/macos_mwiewior_wgs_20260914"
    )
    vepyr_base = (
        f"{VEPYR}/linux_vepyr_0.9.0_20261002/vepyr_{cache_type}_worker_scaling"
        if linux
        else f"{VEPYR}/macos_mwiewior_wgs_20260914/raw"
    )
    vep_paths = [f"{vep_base}/summary.tsv"]
    if not linux:
        vep_paths.append(f"{vep_base}/merged_fork_scaling_summary_7_3.tsv")
    vep_rows = {}
    for path in vep_paths:
        for row in archive.rows(path):
            level = "none" if row["fork"] == "0" else row["fork"]
            if level in vep_rows:
                raise ValueError(f"Duplicate VEP setting: {path}: {level}")
            vep_rows[level] = (row, path)
    vepyr_summary = f"{vepyr_base}/summary.tsv"
    vepyr_rows = {}
    for row in archive.rows(vepyr_summary):
        level = int(row["workers"])
        if level in vepyr_rows:
            raise ValueError(f"Duplicate vepyr setting: {level}")
        vepyr_rows[level] = row

    runs = []
    for workers, fork in PAIRS:
        row, summary = vep_rows[fork]
        if row.get("status", "OK") != "OK" or row["exit_status"] != "0":
            raise ValueError(f"VEP run did not succeed: {platform}, {fork}")
        if row.get("cache_type", cache_type) != cache_type:
            raise ValueError(f"Unexpected VEP cache type: {row}")
        wall = seconds(row["elapsed_wall"])
        raw_dir = vep_base if not linux and fork in ("3", "7") else f"{vep_base}/raw"
        time_path = f"{raw_dir}/{cache_type}_fork{fork}.time.txt"
        log = archive.check_time(time_path, wall)
        for flag in (
            "release_116.0",
            f"--{cache_type}",
            "--everything",
            "--hgvs",
            "--fasta",
            "HG002_normalized.vcf.gz",
        ):
            if flag not in log:
                raise ValueError(f"Missing VEP setting {flag}: {time_path}")
        actual_fork = re.search(r"--fork\s+(\d+)", log)
        if (actual_fork[1] if actual_fork else "none") != fork:
            raise ValueError(f"VEP fork setting disagrees: {time_path}")
        runs.append(
            dict(
                tool="Ensembl VEP",
                version="116.0",
                parallelism=workers,
                fork=fork,
                wall_seconds=wall,
                summary=summary,
                time_log=time_path,
            )
        )

        row = vepyr_rows[workers]
        expected = {
            "cache_type": cache_type,
            "status": "ok",
            "exit_status": "0",
            "compression": "plain",
            "preserve_record_layout": "True",
            "records_match_vep": "True",
            "output_records": str(RECORDS),
        }
        for key, value in expected.items():
            if row[key] != value:
                raise ValueError(
                    f"Unexpected {platform} vepyr summary {key}: {row[key]}"
                )
        metrics_path = f"{vepyr_base}/{cache_type}_workers{workers}.metrics.json"
        metrics = json.loads(archive.read(metrics_path))
        expected = {
            "vepyr_version": version,
            "cache_type": cache_type,
            "workers": workers,
            "status": "ok",
            "output_records": RECORDS,
            "compression": "plain",
            "preserve_record_layout": True,
            "records_match_vep": True,
        }
        for key, value in expected.items():
            if metrics[key] != value:
                raise ValueError(f"Unexpected metrics {key}: {metrics_path}")
        if not math.isclose(
            metrics["annotation_seconds"],
            float(row["annotation_seconds"]),
            abs_tol=0.001,
        ):
            raise ValueError(f"Annotation duration disagrees: {metrics_path}")
        wall = seconds(row["process_elapsed_wall"])
        time_path = f"{vepyr_base}/{cache_type}_workers{workers}.time.txt"
        archive.check_time(time_path, wall)
        runs.append(
            dict(
                tool="vepyr",
                version=version,
                parallelism=workers,
                workers=workers,
                wall_seconds=wall,
                output_records=RECORDS,
                measured_at=metrics["started_at_utc"],
                summary=vepyr_summary,
                time_log=time_path,
                metrics=metrics_path,
            )
        )
    return dict(
        platform=platform, cache_type=cache_type, vepyr_version=version, runs=runs
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref", required=True)
    args = parser.parse_args()
    archive = Archive(args.ref)
    panels = [collect(archive, platform) for platform in ("linux", "macos")]
    for path in (
        "performance-tests/README.md",
        f"{VEPYR}/linux_vepyr_0.9.0_20261002/README.md",
    ):
        archive.read(path)
    snapshot = dict(
        source_commit=archive.commit,
        dataset="HG002 GRCh38 chr1–22",
        records=RECORDS,
        cache_release=116,
        cache_type="merged",
        metric="Whole-process elapsed wall time, including plain VCF writing",
        repetitions_per_setting=1,
        parallelism="VEP processes (fork children + parent) / vepyr workers; not a CPU-thread cap",
        panels=panels,
        source_sha256=archive.sources,
    )
    path = HERE / "data/performance-wgs.json"
    path.write_text(json.dumps(snapshot, indent=2) + "\n")
    print(
        f"Saved {path}; verified 16 plotted runs against raw logs at {archive.commit}"
    )
    for panel in panels:
        for workers, _ in PAIRS:
            paired = {
                r["tool"]: r["wall_seconds"]
                for r in panel["runs"]
                if r["parallelism"] == workers
            }
            print(
                panel["platform"],
                workers,
                paired,
                f"{paired['Ensembl VEP'] / paired['vepyr']:.2f}x",
            )


if __name__ == "__main__":
    main()
