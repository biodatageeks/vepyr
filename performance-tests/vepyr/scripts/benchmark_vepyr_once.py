#!/usr/bin/env python3
"""Run and measure one Vepyr annotation call."""

from __future__ import annotations

import argparse
import gzip
import importlib.metadata
import json
import os
import resource
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

COMPRESSED_SUFFIXES = (".gz", ".bgz", ".bgzf")

# The published numbers pin the engine build they were measured against.
# Declare a different one to benchmark another release.
REQUIRED_VEPYR_VERSION = os.environ.get("VEPYR_EXPECTED_VERSION", "0.3.0")

# getrusage reports the peak resident set in bytes on macOS and in kilobytes
# on Linux, so the benchmark numbers are only comparable after normalising.
RSS_BYTES_PER_UNIT = 1024 if sys.platform == "darwin" else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-vcf", required=True, type=Path)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("--cache-type", required=True, choices=("merged", "refseq"))
    parser.add_argument("--reference-fasta", required=True, type=Path)
    parser.add_argument("--output-vcf", required=True, type=Path)
    parser.add_argument("--metrics-json", required=True, type=Path)
    parser.add_argument("--workers", required=True, type=int)
    parser.add_argument("--expected-records", required=True, type=int)
    parser.add_argument(
        "--compression",
        choices=("plain", "bgzf", "gzip"),
        default="plain",
        help=(
            "VCF output compression. Compressed runs are not time-comparable "
            "with the published plain-output numbers"
        ),
    )
    return parser.parse_args()


def open_vcf(path: Path):
    """Open a VCF for reading, transparently for bgzf, gzip and plain files."""
    # BGZF is a series of gzip members, so the stdlib reader handles it.
    if path.suffix in COMPRESSED_SUFFIXES:
        return gzip.open(path, "rb")
    return path.open("rb")


def count_vcf_records(path: Path) -> int:
    count = 0
    with open_vcf(path) as handle:
        while True:
            line = handle.readline()
            if not line:
                return 0
            if not line.startswith(b"#"):
                count = 1
                break

        while chunk := handle.read(8 * 1024 * 1024):
            count += chunk.count(b"\n")
    return count


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main() -> int:
    args = parse_args()
    started_at = datetime.now(timezone.utc)
    metrics: dict[str, object] = {
        "status": "failed",
        "started_at_utc": started_at.isoformat(),
        "input_vcf": str(args.input_vcf),
        "cache_dir": str(args.cache_dir),
        "cache_type": args.cache_type,
        "reference_fasta": str(args.reference_fasta),
        "compression": args.compression,
        "output_vcf": str(args.output_vcf),
        "workers": args.workers,
        "vep_expected_records": args.expected_records,
    }

    try:
        installed_version = importlib.metadata.version("vepyr")
        if installed_version != REQUIRED_VEPYR_VERSION:
            raise RuntimeError(
                f"vepyr=={REQUIRED_VEPYR_VERSION} is required; "
                f"found {installed_version}. Set VEPYR_EXPECTED_VERSION to "
                "benchmark a different release."
            )

        import vepyr

        metrics["vepyr_version"] = installed_version
        metrics["vepyr_module"] = str(Path(vepyr.__file__).resolve())

        if args.workers < 1:
            raise ValueError("workers must be a positive integer")
        for path in (args.input_vcf, args.cache_dir, args.reference_fasta):
            if not path.exists():
                raise FileNotFoundError(path)
        if args.output_vcf.exists():
            raise FileExistsError(args.output_vcf)

        args.output_vcf.parent.mkdir(parents=True, exist_ok=True)
        args.metrics_json.parent.mkdir(parents=True, exist_ok=True)

        user_cpu_before = resource.getrusage(resource.RUSAGE_SELF).ru_utime
        system_cpu_before = resource.getrusage(resource.RUSAGE_SELF).ru_stime
        annotation_started = time.perf_counter()

        lf = vepyr.annotate(
            vcf=str(args.input_vcf),
            cache_dir=str(args.cache_dir),
            everything=True,
            reference_fasta=str(args.reference_fasta),
            compression=args.compression,
            workers=args.workers,
            hgvs=True,
            output_vcf=str(args.output_vcf),
        )

        annotation_seconds = time.perf_counter() - annotation_started
        usage = resource.getrusage(resource.RUSAGE_SELF)

        metrics["annotation_seconds"] = annotation_seconds
        metrics["user_cpu_seconds"] = usage.ru_utime - user_cpu_before
        metrics["system_cpu_seconds"] = usage.ru_stime - system_cpu_before
        metrics["max_rss_kb"] = usage.ru_maxrss // RSS_BYTES_PER_UNIT
        metrics["returned_output_vcf"] = str(lf)

        output_records = count_vcf_records(args.output_vcf)
        records_match = output_records == args.expected_records
        metrics["output_records"] = output_records
        metrics["records_match_vep"] = records_match
        metrics["output_bytes"] = args.output_vcf.stat().st_size
        metrics["status"] = "ok" if records_match else "record_count_mismatch"

        if not records_match:
            raise RuntimeError(
                f"Vepyr wrote {output_records} records; "
                f"VEP wrote {args.expected_records}"
            )
    except Exception as exc:
        metrics["error"] = f"{type(exc).__name__}: {exc}"
        metrics["traceback"] = traceback.format_exc()
        return_code = 1
    else:
        return_code = 0
    finally:
        finished_at = datetime.now(timezone.utc)
        metrics["finished_at_utc"] = finished_at.isoformat()
        metrics["total_script_seconds"] = (finished_at - started_at).total_seconds()
        args.metrics_json.parent.mkdir(parents=True, exist_ok=True)
        write_json_atomic(args.metrics_json, metrics)

    return return_code


if __name__ == "__main__":
    sys.exit(main())
