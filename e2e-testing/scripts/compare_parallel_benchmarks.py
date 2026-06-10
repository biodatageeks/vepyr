#!/usr/bin/env python3
"""Compare two VEPyR parallelism benchmark summary files."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--regression-threshold-percent", type=float, default=5.0)
    return parser.parse_args()


def summary_path(path: Path) -> Path:
    return path / "summary.json" if path.is_dir() else path


def load_results(path: Path) -> tuple[dict, dict[tuple[int, int], dict]]:
    summary = json.loads(summary_path(path).read_text())
    results = {
        (row["workers"], row["target_partitions"]): row
        for row in summary["results"]
        if row["mode"] == "parallel"
    }
    return summary, results


def percent_change(before: float, after: float) -> float:
    return ((after - before) / before) * 100.0


def main() -> int:
    args = parse_args()
    baseline_summary, baseline = load_results(args.baseline)
    candidate_summary, candidate = load_results(args.candidate)
    keys = sorted(set(baseline) | set(candidate))
    rows = []

    for workers, target_partitions in keys:
        before = baseline.get((workers, target_partitions))
        after = candidate.get((workers, target_partitions))
        if before is None or after is None:
            rows.append(
                {
                    "workers": workers,
                    "target_partitions": target_partitions,
                    "status": "missing",
                }
            )
            continue

        elapsed_change = percent_change(
            before["median_seconds"], after["median_seconds"]
        )
        rss_change = percent_change(
            before["median_max_rss_bytes"], after["median_max_rss_bytes"]
        )
        if elapsed_change >= args.regression_threshold_percent:
            status = "regression"
        elif elapsed_change <= -args.regression_threshold_percent:
            status = "improvement"
        else:
            status = "neutral"
        if not after["all_outputs_match"]:
            status = "incorrect"

        rows.append(
            {
                "workers": workers,
                "target_partitions": target_partitions,
                "baseline_median_seconds": before["median_seconds"],
                "candidate_median_seconds": after["median_seconds"],
                "elapsed_change_percent": round(elapsed_change, 3),
                "baseline_median_rss_bytes": before["median_max_rss_bytes"],
                "candidate_median_rss_bytes": after["median_max_rss_bytes"],
                "rss_change_percent": round(rss_change, 3),
                "candidate_outputs_match": after["all_outputs_match"],
                "status": status,
            }
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.output_dir / "comparison.csv"
    fields = [
        "workers",
        "target_partitions",
        "baseline_median_seconds",
        "candidate_median_seconds",
        "elapsed_change_percent",
        "baseline_median_rss_bytes",
        "candidate_median_rss_bytes",
        "rss_change_percent",
        "candidate_outputs_match",
        "status",
    ]
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(
            {field: row.get(field, "") for field in fields} for row in rows
        )

    counts = {
        status: sum(row["status"] == status for row in rows)
        for status in ("improvement", "neutral", "regression", "incorrect", "missing")
    }
    report = [
        "# Parallel benchmark comparison",
        "",
        f"- baseline: `{summary_path(args.baseline)}`",
        f"- candidate: `{summary_path(args.candidate)}`",
        f"- threshold: `{args.regression_threshold_percent:.1f}%`",
        f"- baseline hash: `{baseline_summary['reference_body_sha256']}`",
        f"- candidate hash: `{candidate_summary['reference_body_sha256']}`",
        "",
        "| status | count |",
        "|---|---:|",
        *[f"| {status} | {count} |" for status, count in counts.items()],
        "",
        "| workers | target | baseline s | candidate s | change | RSS change | status |",
        "|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        if row["status"] == "missing":
            report.append(
                f"| {row['workers']} | {row['target_partitions']} | - | - | - | - | missing |"
            )
            continue
        report.append(
            f"| {row['workers']} | {row['target_partitions']} "
            f"| {row['baseline_median_seconds']:.3f} "
            f"| {row['candidate_median_seconds']:.3f} "
            f"| {row['elapsed_change_percent']:+.1f}% "
            f"| {row['rss_change_percent']:+.1f}% | {row['status']} |"
        )
    (args.output_dir / "comparison.md").write_text("\n".join(report) + "\n")
    return 1 if counts["incorrect"] or counts["missing"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
