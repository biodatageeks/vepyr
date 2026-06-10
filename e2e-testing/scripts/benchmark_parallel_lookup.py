#!/usr/bin/env python3
"""Benchmark VEPyR indexed-Parquet lookup parallelism."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import resource
import statistics
import subprocess
import sys
import tempfile
import time

SMOKE_PAIRS = "1:1,2:2,1:8,1:12,1:16,2:10,6:10,10:10"
REGRESSION_VALUES = (1, 2, 4, 6, 8, 10, 12, 16)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-vcf", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--reference-fasta", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", default="1,2,4,8")
    parser.add_argument("--target-partitions", type=int, default=1)
    parser.add_argument(
        "--matrix-max",
        type=int,
        help="Benchmark every workers/target_partitions pair up to this value.",
    )
    parser.add_argument(
        "--pairs",
        help="Comma-separated workers:target_partitions pairs.",
    )
    parser.add_argument(
        "--regression-suite",
        action="store_true",
        help="Run the full canonical workers/target_partitions matrix.",
    )
    parser.add_argument(
        "--smoke-suite",
        action="store_true",
        help="Run the shortened representative regression configuration set.",
    )
    parser.add_argument("--shuffle-seed", type=int, default=20260609)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--skip-serial", action="store_true")
    parser.add_argument("--single-run", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--forks", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--run-workers", type=int, default=1, help=argparse.SUPPRESS)
    parser.add_argument("--output-vcf", type=Path, help=argparse.SUPPRESS)
    return parser.parse_args()


def body_stats(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    records = 0
    with path.open("rb") as handle:
        for line in handle:
            if line.startswith(b"#"):
                continue
            digest.update(line)
            records += 1
    return digest.hexdigest(), records


def max_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return int(value)
    return int(value) * 1024


def run_once(args: argparse.Namespace) -> int:
    import vepyr

    if args.output_vcf is None:
        raise ValueError("--output-vcf is required for --single-run")

    started = time.perf_counter()
    vepyr.annotate(
        str(args.input_vcf),
        str(args.cache_dir),
        everything=True,
        reference_fasta=str(args.reference_fasta),
        output_vcf=str(args.output_vcf),
        show_progress=False,
        forks=args.forks,
        workers=args.run_workers,
        target_partitions=args.target_partitions,
    )
    elapsed = time.perf_counter() - started
    body_sha256, records = body_stats(args.output_vcf)
    print(
        json.dumps(
            {
                "elapsed_seconds": round(elapsed, 6),
                "max_rss_bytes": max_rss_bytes(),
                "records": records,
                "body_sha256": body_sha256,
            },
            sort_keys=True,
        )
    )
    return 0


def parse_workers(value: str) -> list[int]:
    workers = [int(item) for item in value.split(",")]
    if not workers or any(item <= 0 for item in workers):
        raise ValueError("--workers must contain positive integers")
    return workers


def parse_pairs(value: str) -> list[tuple[int, int]]:
    pairs = []
    for item in value.split(","):
        worker_value, separator, target_value = item.partition(":")
        if not separator:
            raise ValueError("--pairs entries must use workers:target_partitions")
        pair = (int(worker_value), int(target_value))
        if pair[0] <= 0 or pair[1] <= 0:
            raise ValueError("--pairs entries must contain positive integers")
        pairs.append(pair)
    if not pairs:
        raise ValueError("--pairs must not be empty")
    return pairs


def git_value(*args: str) -> str:
    result = subprocess.run(
        ["rtk", "git", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def write_csv(path: Path, rows: list[dict]) -> None:
    fields = [
        "mode",
        "workers",
        "repeat",
        "forks",
        "target_partitions",
        "elapsed_seconds",
        "max_rss_bytes",
        "records",
        "body_sha256",
        "matches_reference",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows({field: row[field] for field in fields} for row in rows)


def write_matrix_csv(path: Path, summaries: list[dict], value_field: str) -> None:
    workers = sorted(
        {row["workers"] for row in summaries if row["mode"] == "parallel"}
    )
    targets = sorted(
        {
            row["target_partitions"]
            for row in summaries
            if row["mode"] == "parallel"
        }
    )
    indexed = {
        (row["workers"], row["target_partitions"]): row
        for row in summaries
        if row["mode"] == "parallel"
    }
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["workers\\target_partitions", *targets])
        for worker_value in workers:
            writer.writerow(
                [
                    worker_value,
                    *[
                        indexed.get((worker_value, target_value), {}).get(
                            value_field, ""
                        )
                        for target_value in targets
                    ],
                ]
            )


def summarize(rows: list[dict]) -> list[dict]:
    summaries = []
    modes = sorted(
        {
            (row["mode"], row["workers"], row["target_partitions"])
            for row in rows
        }
    )
    for mode, workers, target_partitions in modes:
        selected = [
            row
            for row in rows
            if row["mode"] == mode
            and row["workers"] == workers
            and row["target_partitions"] == target_partitions
        ]
        elapsed = [row["elapsed_seconds"] for row in selected]
        rss = [row["max_rss_bytes"] for row in selected]
        summaries.append(
            {
                "mode": mode,
                "workers": workers,
                "target_partitions": target_partitions,
                "runs": len(selected),
                "median_seconds": round(statistics.median(elapsed), 6),
                "min_seconds": round(min(elapsed), 6),
                "max_seconds": round(max(elapsed), 6),
                "median_max_rss_bytes": int(statistics.median(rss)),
                "all_outputs_match": all(row["matches_reference"] for row in selected),
            }
        )
    return summaries


def orchestrate(args: argparse.Namespace) -> int:
    workers = parse_workers(args.workers)
    if args.repeats <= 0:
        raise ValueError("--repeats must be positive")
    if args.target_partitions <= 0:
        raise ValueError("--target-partitions must be positive")
    if args.matrix_max is not None and args.matrix_max <= 0:
        raise ValueError("--matrix-max must be positive")
    selected_modes = sum(
        value is not None
        for value in (
            args.matrix_max,
            args.pairs,
            "regression" if args.regression_suite else None,
            SMOKE_PAIRS if args.smoke_suite else None,
        )
    )
    if selected_modes > 1:
        raise ValueError(
            "--matrix-max, --pairs, --regression-suite, and --smoke-suite "
            "are mutually exclusive"
        )

    for path in (args.input_vcf, args.cache_dir, args.reference_fasta):
        if not path.exists():
            raise FileNotFoundError(path)

    args.results_dir.mkdir(parents=True, exist_ok=True)
    raw_path = args.results_dir / "runs.jsonl"
    csv_path = args.results_dir / "runs.csv"
    summary_path = args.results_dir / "summary.json"

    configurations: list[tuple[str, int, int, int, int]] = []
    if not args.skip_serial:
        configurations.append(("serial", 1, 0, 1, 1))
    selected_pairs = SMOKE_PAIRS if args.smoke_suite else args.pairs
    if selected_pairs is not None:
        configurations.extend(
            ("parallel", run_workers, 1, run_workers, target_partitions)
            for run_workers, target_partitions in parse_pairs(selected_pairs)
        )
    elif args.regression_suite:
        configurations.extend(
            ("parallel", run_workers, 1, run_workers, target_partitions)
            for run_workers in REGRESSION_VALUES
            for target_partitions in REGRESSION_VALUES
        )
    elif args.matrix_max is None:
        configurations.extend(
            ("parallel", item, 1, item, args.target_partitions)
            for item in workers
        )
    else:
        configurations.extend(
            ("parallel", run_workers, 1, run_workers, target_partitions)
            for run_workers in range(1, args.matrix_max + 1)
            for target_partitions in range(1, run_workers + 1)
        )

    scheduled_runs = [
        (*configuration, repeat)
        for configuration in configurations
        for repeat in range(1, args.repeats + 1)
    ]
    if (
        args.matrix_max is not None
        or selected_pairs is not None
        or args.regression_suite
    ):
        random.Random(args.shuffle_seed).shuffle(scheduled_runs)

    rows: list[dict] = []
    reference_hash = (
        "16cc94c0afee4553d18f37b8a0083b8f5011d32cab753c72f75e24a125664b30"
    )

    with raw_path.open("w") as raw_handle:
        for (
            mode,
            worker_label,
            forks,
            run_workers,
            target_partitions,
            repeat,
        ) in scheduled_runs:
            with tempfile.NamedTemporaryFile(
                prefix=f"vepyr-{mode}-w{worker_label}-p{target_partitions}-",
                suffix=".vcf",
                delete=False,
            ) as output:
                output_path = Path(output.name)

            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--input-vcf",
                str(args.input_vcf),
                "--cache-dir",
                str(args.cache_dir),
                "--reference-fasta",
                str(args.reference_fasta),
                "--results-dir",
                str(args.results_dir),
                "--single-run",
                "--forks",
                str(forks),
                "--run-workers",
                str(run_workers),
                "--target-partitions",
                str(target_partitions),
                "--output-vcf",
                str(output_path),
            ]
            try:
                child_env = os.environ.copy()
                if args.profile:
                    child_env["VEP_PROFILE"] = "1"
                completed = subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    text=True,
                    env=child_env,
                )
                measurement = json.loads(completed.stdout.strip().splitlines()[-1])
                if args.profile:
                    profile_path = (
                        args.results_dir
                        / (
                            f"profile-{mode}-workers{worker_label}"
                            f"-target{target_partitions}-repeat{repeat}.log"
                        )
                    )
                    profile_path.write_text(completed.stderr)
            finally:
                output_path.unlink(missing_ok=True)

            row = {
                "mode": mode,
                "workers": worker_label,
                "repeat": repeat,
                "forks": forks,
                "target_partitions": target_partitions,
                **measurement,
                "matches_reference": measurement["body_sha256"] == reference_hash,
            }
            rows.append(row)
            raw_handle.write(json.dumps(row, sort_keys=True) + "\n")
            raw_handle.flush()
            write_csv(csv_path, rows)
            print(
                f"{mode} workers={worker_label} target={target_partitions} "
                f"repeat={repeat}: {measurement['elapsed_seconds']:.3f}s, "
                f"rss={measurement['max_rss_bytes'] / 1024**3:.2f}GiB, "
                f"match={row['matches_reference']}",
                flush=True,
            )

    summaries = summarize(rows)
    summary = {
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "platform": platform.platform(),
        "python": sys.version,
        "git_commit": git_value("rev-parse", "HEAD"),
        "git_branch": git_value("branch", "--show-current"),
        "input_vcf": str(args.input_vcf.resolve()),
        "cache_dir": str(args.cache_dir.resolve()),
        "reference_fasta": str(args.reference_fasta.resolve()),
        "repeats": args.repeats,
        "profile": args.profile,
        "matrix_max": args.matrix_max,
        "pairs": selected_pairs,
        "regression_suite": args.regression_suite,
        "smoke_suite": args.smoke_suite,
        "regression_values": list(REGRESSION_VALUES)
        if args.regression_suite
        else None,
        "shuffle_seed": args.shuffle_seed,
        "target_partitions": None
        if args.matrix_max is not None
        or selected_pairs is not None
        or args.regression_suite
        else args.target_partitions,
        "reference_body_sha256": reference_hash,
        "results": summaries,
    }
    write_csv(csv_path, rows)
    write_matrix_csv(
        args.results_dir / "median_seconds_matrix.csv",
        summaries,
        "median_seconds",
    )
    write_matrix_csv(
        args.results_dir / "median_rss_bytes_matrix.csv",
        summaries,
        "median_max_rss_bytes",
    )
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    return 0


def main() -> int:
    args = parse_args()
    return run_once(args) if args.single_run else orchestrate(args)


if __name__ == "__main__":
    raise SystemExit(main())
