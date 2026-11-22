#!/usr/bin/env python3
"""Run each Vepyr worker setting on SSD, then archive artifacts on HDD.

Timing definition: the process wall (``process_elapsed_wall``) covers one
benchmark_vepyr_once.py process, that is Python start-up, importing vepyr,
the annotation and writing the output VCF -- the same work the
engine-comparison harness times. The record count that verifies the output
reads the whole VCF back, so it runs here, after os.wait4 has returned, and
is not part of any reported time.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

# getrusage reports the peak resident set in bytes on macOS and in kilobytes
# on Linux, so the benchmark numbers are only comparable after normalising.
RSS_BYTES_PER_UNIT = 1024 if sys.platform == "darwin" else 1

WORKERS_FILE = Path(__file__).resolve().parent / "benchmark_workers.txt"
ONCE_SCRIPT = Path(__file__).resolve().parent / "benchmark_vepyr_once.py"

COMPRESSED_SUFFIXES = (".gz", ".bgz", ".bgzf")
POWER_SUPPLY_ROOT = Path("/sys/class/power_supply")
AC_POWER = "AC Power"
BATTERY_POWER = "Battery Power"
UNKNOWN_POWER = "unknown"


def read_worker_counts(path: Path = WORKERS_FILE) -> list[int]:
    """Read the sweep's worker counts from their single declaration."""
    return [
        int(field)
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
        for field in line.split()
    ]


SUMMARY_FIELDS = [
    "cache_type",
    "workers",
    "compression",
    "preserve_record_layout",
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
    # Appended last so existing summaries keep their column order.
    "power_source",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-vcf", required=True, type=Path)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("--cache-type", required=True, choices=("merged", "refseq"))
    parser.add_argument("--reference-fasta", required=True, type=Path)
    parser.add_argument("--ssd-output-dir", required=True, type=Path)
    parser.add_argument("--archive-dir", required=True, type=Path)
    parser.add_argument("--expected-records", required=True, type=int)
    parser.add_argument("--workers", nargs="+", type=int, default=read_worker_counts())
    parser.add_argument("--name-prefix", default="HG002")
    parser.add_argument("--minimum-free-gib", type=float, default=40.0)
    parser.add_argument(
        "--preserve-record-layout",
        choices=("on", "off"),
        default="on",
        help="Passed through to each measured run; 'off' is the ablation arm",
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--require-ac-power",
        action="store_true",
        help=(
            "refuse to start a run while the host is on battery power; the "
            "power source is recorded for every run either way"
        ),
    )
    parser.add_argument(
        "--compression",
        choices=("plain", "bgzf", "gzip"),
        default="plain",
        help=(
            "VCF output compression. bgzf shrinks the output roughly 17x, but "
            "the compression cost lands inside the measured annotation, so "
            "such runs are not comparable with the published plain numbers"
        ),
    )
    parser.add_argument(
        "--require-separate-filesystems",
        action="store_true",
        help=(
            "fail unless the measured inputs and output share one filesystem "
            "and the archive directory sits on another one"
        ),
    )
    return parser.parse_args()


@dataclass(frozen=True)
class ProcessMeasurement:
    """Wall clock and resource usage of one annotation subprocess."""

    exit_status: int
    elapsed_seconds: float
    user_cpu_seconds: float
    system_cpu_seconds: float
    max_rss_kb: int


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


def detect_power_source(
    platform: str = sys.platform,
    sysfs_root: Path = POWER_SUPPLY_ROOT,
    run=subprocess.run,
) -> str:
    """Return "AC Power", "Battery Power" or "unknown" for this host.

    A laptop on battery throttles, so runs on battery are not comparable with
    runs on mains power and the source is recorded with every measurement.
    """
    if platform == "darwin":
        try:
            result = run(
                ["pmset", "-g", "batt"],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError:
            return UNKNOWN_POWER
        first_line = (result.stdout or "").splitlines()[:1]
        for source in (AC_POWER, BATTERY_POWER):
            if first_line and f"'{source}'" in first_line[0]:
                return source
        return UNKNOWN_POWER

    # Linux: mains adapters expose `online`; batteries do not. A server
    # without any power_supply entry reports "unknown".
    states = []
    for online in sorted(sysfs_root.glob("*/online")):
        try:
            states.append(online.read_text(encoding="utf-8").strip())
        except OSError:
            continue
    if not states:
        return UNKNOWN_POWER
    return AC_POWER if "1" in states else BATTERY_POWER


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def verify_output(
    metrics: dict[str, object], output_vcf: Path, expected_records: int
) -> bool:
    """Count the output's records and set the verification fields in place.

    Runs after the measured process has exited, so reading the whole output
    back is not part of the process wall. Returns whether the count matches.
    """
    output_records = count_vcf_records(output_vcf)
    records_match = output_records == expected_records
    metrics["output_records"] = output_records
    metrics["records_match_vep"] = records_match
    metrics["output_bytes"] = output_vcf.stat().st_size
    metrics["status"] = "ok" if records_match else "record_count_mismatch"
    if not records_match:
        metrics["error"] = (
            f"RuntimeError: Vepyr wrote {output_records} records; "
            f"VEP wrote {expected_records}"
        )
    return records_match


def format_elapsed(seconds: float) -> str:
    """Render seconds the way GNU time does, so archived runs stay comparable."""
    whole = int(seconds)
    hours, remainder = divmod(whole, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs + seconds - whole:05.2f}"


def run_measured(command: list[str], stdout, stderr) -> ProcessMeasurement:
    """Run a command and collect its resource usage.

    os.wait4 reports the usage of this one child on both Linux and macOS,
    unlike RUSAGE_CHILDREN, which would carry the peak of every earlier worker
    count into later runs.
    """
    started = time.monotonic()
    process = subprocess.Popen(command, stdout=stdout, stderr=stderr)
    _, status, usage = os.wait4(process.pid, 0)
    elapsed = time.monotonic() - started
    process.returncode = os.waitstatus_to_exitcode(status)

    return ProcessMeasurement(
        exit_status=process.returncode,
        elapsed_seconds=elapsed,
        user_cpu_seconds=usage.ru_utime,
        system_cpu_seconds=usage.ru_stime,
        max_rss_kb=usage.ru_maxrss // RSS_BYTES_PER_UNIT,
    )


def write_time_report(
    path: Path, command: list[str], measurement: ProcessMeasurement
) -> None:
    """Write the resource record using GNU time's labels and layout."""
    cpu_seconds = measurement.user_cpu_seconds + measurement.system_cpu_seconds
    cpu_percent = (
        round(100 * cpu_seconds / measurement.elapsed_seconds)
        if measurement.elapsed_seconds > 0
        else 0
    )
    lines = [
        f'\tCommand being timed: "{" ".join(command)}"',
        f"\tUser time (seconds): {measurement.user_cpu_seconds:.2f}",
        f"\tSystem time (seconds): {measurement.system_cpu_seconds:.2f}",
        f"\tPercent of CPU this job got: {cpu_percent}%",
        (
            "\tElapsed (wall clock) time (h:mm:ss or m:ss): "
            f"{format_elapsed(measurement.elapsed_seconds)}"
        ),
        f"\tMaximum resident set size (kbytes): {measurement.max_rss_kb}",
        f"\tExit status: {measurement.exit_status}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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


def prepare_storage_directory(path: Path, option: str) -> None:
    """Create a run directory without materialising its parents.

    Both run directories live on volumes selected for the benchmark, so the
    parent must already exist: creating a whole ancestor chain would either
    fail deep inside pathlib with no mention of the offending option, or
    quietly write hundreds of gigabytes under an unmounted mount point.
    """
    if path.is_dir():
        return

    parent = path.parent
    if not parent.is_dir():
        raise NotADirectoryError(
            f"{option} {path} cannot be created because {parent} does not "
            "exist; mount or create the storage root first"
        )

    try:
        path.mkdir()
    except OSError as error:
        raise RuntimeError(
            f"{option} {path} could not be created: {error.strerror}"
        ) from error


def validate_storage_paths(args: argparse.Namespace) -> None:
    for path in (args.input_vcf, args.cache_dir, args.reference_fasta):
        if not path.exists():
            raise FileNotFoundError(path)

    prepare_storage_directory(args.ssd_output_dir, "--ssd-output-dir")
    prepare_storage_directory(args.archive_dir, "--archive-dir")

    ssd_device = args.ssd_output_dir.stat().st_dev
    if args.require_separate_filesystems:
        for path in (args.input_vcf, args.cache_dir, args.reference_fasta):
            if path.stat().st_dev != ssd_device:
                raise RuntimeError(
                    f"Measured path is not on the SSD output filesystem: {path}"
                )

        if args.archive_dir.stat().st_dev == ssd_device:
            raise RuntimeError(
                "Archive and measured output directories are on the same filesystem"
            )
    elif args.archive_dir.stat().st_dev == ssd_device:
        print(
            "note: archiving to the same filesystem that is being measured; "
            "pass --require-separate-filesystems for the SSD/HDD layout the "
            "published WGS numbers use",
            file=sys.stderr,
        )


def main() -> int:
    args = parse_args()
    validate_storage_paths(args)

    once_script = ONCE_SCRIPT
    summary_path = args.archive_dir / "summary.tsv"
    # The engine picks bgzf from the extension, so the name has to agree.
    vcf_suffix = "" if args.compression == "plain" else ".gz"
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

        power_source = detect_power_source()
        if power_source == BATTERY_POWER and args.require_ac_power:
            raise RuntimeError(
                f"workers={workers}: the host is on battery power and "
                "--require-ac-power is set; connect the charger and rerun"
            )

        stem = (
            f"{args.name_prefix}_annotated_wgs_everything_hgvs_"
            f"vepyr_{args.cache_type}_workers{workers}"
        )
        names = {
            "output": f"{stem}.vcf{vcf_suffix}",
            "metrics": f"{args.cache_type}_workers{workers}.metrics.json",
            "time": f"{args.cache_type}_workers{workers}.time.txt",
            "stderr": f"{args.cache_type}_workers{workers}.stderr.txt",
            "stdout": f"{args.cache_type}_workers{workers}.stdout.txt",
        }
        ssd_paths = {key: args.ssd_output_dir / name for key, name in names.items()}
        archive_paths = {key: args.archive_dir / name for key, name in names.items()}

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
            "--compression",
            args.compression,
            "--preserve-record-layout",
            args.preserve_record_layout,
        ]

        print(
            f"cache={args.cache_type} workers={workers}: "
            f"measuring Vepyr on SSD ({free_bytes / 1024**3:.1f} GiB free, "
            f"power: {power_source})",
            flush=True,
        )
        with (
            ssd_paths["stdout"].open("w", encoding="utf-8") as stdout_handle,
            ssd_paths["stderr"].open("w", encoding="utf-8") as stderr_handle,
        ):
            measurement = run_measured(command, stdout_handle, stderr_handle)

        write_time_report(ssd_paths["time"], command, measurement)

        # Everything below happens after the measurement has stopped.
        metrics: dict[str, object] = {}
        if ssd_paths["metrics"].exists():
            metrics = json.loads(ssd_paths["metrics"].read_text(encoding="utf-8"))
            metrics["power_source"] = power_source
            if (
                measurement.exit_status == 0
                and metrics.get("status") == "annotated"
                and ssd_paths["output"].exists()
            ):
                verify_output(metrics, ssd_paths["output"], args.expected_records)
            write_json_atomic(ssd_paths["metrics"], metrics)

        moved: dict[str, Path | None] = {}
        print(
            f"cache={args.cache_type} workers={workers}: "
            f"archiving artifacts to {args.archive_dir}",
            flush=True,
        )
        for key in ("output", "metrics", "time", "stderr", "stdout"):
            moved[key] = move_artifact(ssd_paths[key], archive_paths[key], args.force)

        row: dict[str, object] = {
            "cache_type": args.cache_type,
            "workers": workers,
            "compression": args.compression,
            "preserve_record_layout": metrics.get("preserve_record_layout", ""),
            "status": metrics.get("status", "failed"),
            "exit_status": measurement.exit_status,
            "annotation_seconds": metrics.get("annotation_seconds", ""),
            "process_elapsed_wall": format_elapsed(measurement.elapsed_seconds),
            "max_rss_kb": measurement.max_rss_kb,
            "output_records": metrics.get("output_records", ""),
            "vep_expected_records": args.expected_records,
            "records_match_vep": metrics.get("records_match_vep", ""),
            "output_bytes": metrics.get("output_bytes", ""),
            "output_file": moved["output"] or "",
            "metrics_file": moved["metrics"] or "",
            "time_file": moved["time"] or "",
            "stderr_file": moved["stderr"] or "",
            "stdout_file": moved["stdout"] or "",
            "power_source": power_source,
        }
        update_summary(summary_path, row)

        if measurement.exit_status != 0:
            failures += 1
            print(
                f"workers={workers}: failed; see {archive_paths['stderr']}",
                file=sys.stderr,
            )
            break
        # A count mismatch, or a run that never got verified, stops the sweep
        # exactly as a failing child process does.
        if row["status"] != "ok":
            failures += 1
            print(
                f"workers={workers}: {row['status']}: "
                f"{metrics.get('error', 'output not verified')}; "
                f"see {archive_paths['metrics']}",
                file=sys.stderr,
            )
            break

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
