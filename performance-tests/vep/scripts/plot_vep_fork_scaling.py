#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
import tempfile
import textwrap
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", os.path.join(tempfile.gettempdir(), "matplotlib"))

import matplotlib.pyplot as plt


DEFAULT_RECORDS = 4_096_123
FORK_RE = re.compile(r"fork(?P<fork>none|\d+)\.time\.txt$")


@dataclass(frozen=True)
class RunRow:
    cache_type: str
    fork: str
    status: str
    exit_status: str
    elapsed_wall: str
    elapsed_seconds: int
    max_rss_kb: str
    time_file: Path
    stderr_file: Path
    warnings_file: Path


def elapsed_to_seconds(value: str) -> int:
    parts = value.strip().split(":")
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return int(hours) * 3600 + int(minutes) * 60 + int(float(seconds))
    if len(parts) == 2:
        minutes, seconds = parts
        return int(minutes) * 60 + int(float(seconds))
    raise ValueError(f"Unsupported elapsed value: {value}")


def seconds_to_label(seconds: int) -> str:
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    return f"{minutes}m {secs:02d}s"


def fork_sort_key(fork: str) -> tuple[int, int]:
    if fork == "none":
        return (0, 0)
    return (1, int(fork))


def parse_time_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if ": " not in line:
            continue
        key, value = line.split(": ", 1)
        values[key.strip()] = value.strip()
    return values


def fork_from_path(path: Path) -> str:
    match = FORK_RE.search(path.name)
    if not match:
        raise ValueError(f"Cannot infer fork from {path}")
    return match.group("fork")


def discover_rows(input_dir: Path, cache_type: str) -> list[RunRow]:
    rows: list[RunRow] = []
    for time_file in sorted(input_dir.glob(f"{cache_type}_fork*.time.txt"), key=lambda item: fork_sort_key(fork_from_path(item))):
        fork = fork_from_path(time_file)
        values = parse_time_file(time_file)
        elapsed_wall = values.get("Elapsed (wall clock) time (h:mm:ss or m:ss)", "")
        exit_status = values.get("Exit status", "")
        max_rss_kb = values.get("Maximum resident set size (kbytes)", "")
        status = "OK" if elapsed_wall and exit_status in {"", "0"} else "INCOMPLETE_OR_FAILED"
        if not elapsed_wall:
            elapsed_seconds = 0
        else:
            elapsed_seconds = elapsed_to_seconds(elapsed_wall)
        rows.append(
            RunRow(
                cache_type=cache_type,
                fork=fork,
                status=status,
                exit_status=exit_status,
                elapsed_wall=elapsed_wall,
                elapsed_seconds=elapsed_seconds,
                max_rss_kb=max_rss_kb,
                time_file=time_file,
                stderr_file=input_dir / f"{cache_type}_fork{fork}.stderr.txt",
                warnings_file=input_dir / f"HG002_annotated_wgs_everything_hgvs_{cache_type}_fork{fork}.vcf_warnings.txt",
            )
        )
    return rows


def write_summary(rows: list[RunRow], summary: Path, source_dir: Path) -> None:
    summary.parent.mkdir(parents=True, exist_ok=True)
    with summary.open("w", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            [
                "cache_type",
                "fork",
                "status",
                "exit_status",
                "elapsed_wall",
                "elapsed_seconds",
                "max_rss_kb",
                "time_file",
                "stderr_file",
                "warnings_file",
                "source_dir",
            ]
        )
        for row in sorted(rows, key=lambda item: fork_sort_key(item.fork)):
            writer.writerow(
                [
                    row.cache_type,
                    row.fork,
                    row.status,
                    row.exit_status,
                    row.elapsed_wall,
                    row.elapsed_seconds,
                    row.max_rss_kb,
                    row.time_file,
                    row.stderr_file if row.stderr_file.exists() else "",
                    row.warnings_file if row.warnings_file.exists() else "",
                    source_dir,
                ]
            )


def command_text(cache_type: str, forks: list[str]) -> str:
    cache_flag = f"--{cache_type}"
    ordered_forks = sorted((fork for fork in forks if fork != "none"), key=int, reverse=True)
    if "none" in forks:
        ordered_forks.append("none")
    fork_values = " ".join(ordered_forks)
    return textwrap.dedent(
        f"""\
        Kod uruchomienia VEP 116 benchmark:
        # FORK=none means no --fork argument is passed
        for FORK in {fork_values}; do
          fork_args=(); if [ "$FORK" != "none" ]; then fork_args=(--fork "$FORK"); fi
          /usr/bin/time -v -o "$time_file" docker run --rm \\
            --user "$(id -u):$(id -g)" --env HOME=/tmp \\
            -v "$DATA_VEPYR_DIR/homo_sapiens_{cache_type}/${{RELEASE}}_GRCh38:/opt/vep/.vep/homo_sapiens_{cache_type}/${{RELEASE}}_GRCh38:ro" \\
            -v "$DATA_VEPYR_DIR/input:/input:ro" -v "$OUT_DIR:/output" \\
            "$VEP_IMAGE" vep --dir /opt/vep/.vep --cache {cache_flag} --offline --assembly GRCh38 \\
            --input_file /input/HG002_normalized.vcf.gz --output_file "/output/$out_name" --vcf \\
            --force_overwrite --no_stats --everything --hgvs \\
            --fasta /input/Homo_sapiens.GRCh38.dna.primary_assembly.fa "${{fork_args[@]}}"
        done"""
    )


def select_baseline(rows: list[RunRow], baseline_fork: str) -> RunRow:
    for row in rows:
        if row.fork == baseline_fork:
            return row
    for row in rows:
        if row.fork == "1":
            return row
    return rows[0]


def plot_rows(rows: list[RunRow], args: argparse.Namespace) -> None:
    ok_rows = [row for row in rows if row.status == "OK" and row.elapsed_seconds > 0]
    ok_rows = sorted(ok_rows, key=lambda item: fork_sort_key(item.fork))
    if not ok_rows:
        raise SystemExit(f"No successful runs found in {args.input_dir}")

    labels = [row.fork for row in ok_rows]
    minutes = [row.elapsed_seconds / 60 for row in ok_rows]
    baseline = select_baseline(ok_rows, args.baseline_fork)
    record_label = f"{args.records:,}".replace(",", " ")

    fig = plt.figure(figsize=(18, 10), dpi=160)
    fig.text(
        0.06,
        0.965,
        command_text(args.cache_type, labels).replace("$", r"\$"),
        ha="left",
        va="top",
        family="monospace",
        fontsize=7,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "#f8f8f8", "edgecolor": "#bbbbbb"},
    )

    ax = fig.add_axes([0.06, 0.10, 0.90, 0.48])
    colors = ["#999999", "#7a68a6", "#4c78a8", "#59a14f", "#f28e2b", "#e15759"]
    bars = ax.bar(labels, minutes, color=colors[: len(labels)], width=0.62)

    for bar, row in zip(bars, ok_rows):
        speedup = baseline.elapsed_seconds / row.elapsed_seconds
        if row.fork == baseline.fork:
            suffix = "baseline"
        else:
            suffix = f"{speedup:.2f}x faster"
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(minutes) * 0.02,
            f"{seconds_to_label(row.elapsed_seconds)}\n{suffix}\n{record_label} variants",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    ax.set_title(args.title, fontsize=16, pad=16)
    ax.set_xlabel("fork")
    ax.set_ylabel("elapsed time [minutes]")
    ax.grid(axis="y", alpha=0.25)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(
        0.06,
        0.035,
        f"Summary: {args.summary} | records: {args.records} | baseline: fork={baseline.fork}",
        ha="left",
        fontsize=9,
        color="#444444",
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, bbox_inches="tight")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-type", choices=["merged", "refseq"], required=True)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--records", type=int, default=DEFAULT_RECORDS)
    parser.add_argument("--baseline-fork", default="none")
    args = parser.parse_args()

    rows = discover_rows(args.input_dir, args.cache_type)
    write_summary(rows, args.summary, args.input_dir)
    plot_rows(rows, args)


if __name__ == "__main__":
    main()
