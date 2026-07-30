#!/usr/bin/env python3
"""Run each Vepyr worker setting on SSD, then archive artifacts on HDD."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

SUMMARY_FIELDS = [
    "cache_type",
    "workers",
    "status",
    "exit_status",
    "annotation_seconds",
    "process_elapsed_wall",
    "max_rss_kb",
    "output_records",
    "vep_expected_records",
    "records_match_vep",
    "output_bytes",
    "output_file",
    "metrics_file",
    "time_file",
    "stderr_file",
    "stdout_file",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-vcf", required=True, type=Path)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument(
        "--cache-type", required=True, choices=("merged", "refseq")
    )
    parser.add_argument("--reference-fasta", required=True, type=Path)
    parser.add_argument("--ssd-output-dir", required=True, type=Path)
    parser.add_argument("--archive-dir", required=True, type=Path)
    parser.add_argument("--expected-records", required=True, type=int)
    parser.add_argument("--workers", nargs="+", type=int, default=[16, 8, 4, 2, 1])
    parser.add_argument("--name-prefix", default="HG002")
    parser.add_argument("--minimum-free-gib", type=float, default=40.0)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def parse_time_file(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    if not path.exists():
        return result

    patterns = {
        "process_elapsed_wall": re.compile(
            r"Elapsed \(wall clock\) time .*: (.+)$"
        ),
        "max_rss_kb": re.compile(r"Maximum resident set size \(kbytes\): (.+)$"),
    }
    for line in path.read_text(encoding="utf-8").splitlines():
        for key, pattern in patterns.items():
            if match := pattern.search(line):
                result[key] = match.group(1).strip()
    return result


def move_artifact(source: Path, destination: Path, force: bool) -> Path | None:
    if not source.exists():
        return None
    if destination.exists():
        if not force:
            raise FileExistsError(destination)
        destination.unlink()
    shutil.move(str(source), str(destination))
    return destination


def update_summary(path: Path, row: dict[str, object]) -> None:
    rows: list[dict[str, str]] = []
    if path.exists():
        with path.open(newline="", encoding="utf-8") as handle:
            rows = [
                existing
                for existing in csv.DictReader(handle, delimiter="\t")
                if existing["workers"] != str(row["workers"])
            ]

    rows.append({field: str(row.get(field, "")) for field in SUMMARY_FIELDS})
    rows.sort(key=lambda existing: int(existing["workers"]), reverse=True)

    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def validate_storage_paths(args: argparse.Namespace) -> None:
    for path in (args.input_vcf, args.cache_dir, args.reference_fasta):
        if not path.exists():
            raise FileNotFoundError(path)

    args.ssd_output_dir.mkdir(parents=True, exist_ok=True)
    args.archive_dir.mkdir(parents=True, exist_ok=True)

    ssd_device = args.ssd_output_dir.stat().st_dev
    for path in (args.input_vcf, args.cache_dir, args.reference_fasta):
        if path.stat().st_dev != ssd_device:
            raise RuntimeError(
                f"Measured path is not on the SSD output filesystem: {path}"
            )

    if args.archive_dir.stat().st_dev == ssd_device:
        raise RuntimeError(
            "Archive and measured output directories are on the same filesystem"
        )


def main() -> int:
    args = parse_args()
    validate_storage_paths(args)

    script_dir = Path(__file__).resolve().parent
    once_script = script_dir / "benchmark_vepyr_once.py"
    summary_path = args.archive_dir / "summary.tsv"
    failures = 0

    for workers in args.workers:
        if workers < 1:
            raise ValueError("workers must be positive integers")

        free_bytes = shutil.disk_usage(args.ssd_output_dir).free
        minimum_free_bytes = int(args.minimum_free_gib * 1024**3)
        if free_bytes < minimum_free_bytes:
            raise RuntimeError(
                f"Only {free_bytes / 1024**3:.1f} GiB is free on the SSD; "
                f"{args.minimum_free_gib:.1f} GiB is required before a run"
            )

        stem = (
            f"{args.name_prefix}_annotated_wgs_everything_hgvs_"
            f"vepyr_{args.cache_type}_workers{workers}"
        )
        names = {
            "output": f"{stem}.vcf",
            "metrics": f"{args.cache_type}_workers{workers}.metrics.json",
            "time": f"{args.cache_type}_workers{workers}.time.txt",
            "stderr": f"{args.cache_type}_workers{workers}.stderr.txt",
            "stdout": f"{args.cache_type}_workers{workers}.stdout.txt",
        }
        ssd_paths = {
            key: args.ssd_output_dir / name for key, name in names.items()
        }
        archive_paths = {
            key: args.archive_dir / name for key, name in names.items()
        }

        for path in (*ssd_paths.values(), *archive_paths.values()):
            if path.exists() and not args.force:
                raise FileExistsError(
                    f"{path} already exists; select unfinished workers or use --force"
                )
        if args.force:
            for path in ssd_paths.values():
                if path.exists():
                    path.unlink()

        command = [
            "/usr/bin/time",
            "-v",
            "-o",
            str(ssd_paths["time"]),
            sys.executable,
            "-P",
            str(once_script),
            "--input-vcf",
            str(args.input_vcf),
            "--cache-dir",
            str(args.cache_dir),
            "--cache-type",
            args.cache_type,
            "--reference-fasta",
            str(args.reference_fasta),
            "--output-vcf",
            str(ssd_paths["output"]),
            "--metrics-json",
            str(ssd_paths["metrics"]),
            "--workers",
            str(workers),
            "--expected-records",
            str(args.expected_records),
        ]

        print(
            f"cache={args.cache_type} workers={workers}: "
            f"measuring Vepyr on SSD ({free_bytes / 1024**3:.1f} GiB free)",
            flush=True,
        )
        with (
            ssd_paths["stdout"].open("w", encoding="utf-8") as stdout_handle,
            ssd_paths["stderr"].open("w", encoding="utf-8") as stderr_handle,
        ):
            completed = subprocess.run(
                command,
                stdout=stdout_handle,
                stderr=stderr_handle,
                check=False,
            )

        metrics: dict[str, object] = {}
        if ssd_paths["metrics"].exists():
            metrics = json.loads(
                ssd_paths["metrics"].read_text(encoding="utf-8")
            )
        process_metrics = parse_time_file(ssd_paths["time"])

        moved: dict[str, Path | None] = {}
        print(
            f"cache={args.cache_type} workers={workers}: "
            "archiving artifacts on HDD",
            flush=True,
        )
        for key in ("output", "metrics", "time", "stderr", "stdout"):
            moved[key] = move_artifact(
                ssd_paths[key], archive_paths[key], args.force
            )

        row: dict[str, object] = {
            "cache_type": args.cache_type,
            "workers": workers,
            "status": metrics.get("status", "failed"),
            "exit_status": completed.returncode,
            "annotation_seconds": metrics.get("annotation_seconds", ""),
            "process_elapsed_wall": process_metrics.get(
                "process_elapsed_wall", ""
            ),
            "max_rss_kb": process_metrics.get(
                "max_rss_kb", metrics.get("max_rss_kb", "")
            ),
            "output_records": metrics.get("output_records", ""),
            "vep_expected_records": args.expected_records,
            "records_match_vep": metrics.get("records_match_vep", ""),
            "output_bytes": metrics.get("output_bytes", ""),
            "output_file": moved["output"] or "",
            "metrics_file": moved["metrics"] or "",
            "time_file": moved["time"] or "",
            "stderr_file": moved["stderr"] or "",
            "stdout_file": moved["stdout"] or "",
        }
        update_summary(summary_path, row)

        if completed.returncode != 0:
            failures += 1
            print(
                f"workers={workers}: failed; see {archive_paths['stderr']}",
                file=sys.stderr,
            )
            break

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
