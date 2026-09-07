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
    elapsed_seconds: float
    max_rss_kb: str
    time_file: Path
    stderr_file: Path
    warnings_file: Path


def elapsed_to_seconds(value: str, *, fractional: bool = False) -> float:
    parts = value.strip().split(":")
    if len(parts) == 3:
        hours, minutes, seconds = parts
        elapsed = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    elif len(parts) == 2:
        minutes, seconds = parts
        elapsed = int(minutes) * 60 + float(seconds)
    else:
        raise ValueError(f"Unsupported elapsed value: {value}")
    # Keep historical Linux summaries and labels unchanged in the default mode.
    return elapsed if fractional else int(elapsed)


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


def discover_rows(input_dir: Path, cache_type: str, *, fractional: bool = False) -> list[RunRow]:
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
            elapsed_seconds = elapsed_to_seconds(elapsed_wall, fractional=fractional)
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
                    int(row.elapsed_seconds) if float(row.elapsed_seconds).is_integer() else row.elapsed_seconds,
                    row.max_rss_kb,
                    row.time_file,
                    row.stderr_file if row.stderr_file.exists() else "",
                    row.warnings_file if row.warnings_file.exists() else "",
                    source_dir,
                ]
            )


def command_text(cache_type: str, forks: list[str], release: str = "116") -> str:
    cache_flag = f"--{cache_type}"
    ordered_forks = sorted((fork for fork in forks if fork != "none"), key=int, reverse=True)
    if "none" in forks:
        ordered_forks.append("none")
    fork_values = " ".join(ordered_forks)
    return textwrap.dedent(
        f"""\
        Kod uruchomienia VEP {release} benchmark:
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

    if args.scaling_panels:
        plot_scaling_panels(ok_rows, args)
        return

    labels = [row.fork for row in ok_rows]
    minutes = [row.elapsed_seconds / 60 for row in ok_rows]
    baseline = select_baseline(ok_rows, args.baseline_fork)
    record_label = f"{args.records:,}".replace(",", " ")

    fig = plt.figure(figsize=(18, 10), dpi=160)
    fig.text(
        0.06,
        0.965,
        (args.command_file.read_text() if args.command_file else command_text(args.cache_type, labels, args.release)).replace("$", r"\$"),
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
    plt.close(fig)


def plot_scaling_panels(rows: list[RunRow], args: argparse.Namespace) -> None:
    """Monochrome runtime + speedup figure, with the actual invocation embedded."""
    baseline = select_baseline(rows, args.baseline_fork)
    labels = ["no fork" if row.fork == "none" else row.fork for row in rows]
    times = [row.elapsed_seconds for row in rows]
    speedups = [baseline.elapsed_seconds / seconds for seconds in times]
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "pdf.fonttype": 42, "svg.fonttype": "none"})
    fig = plt.figure(figsize=(14, 10), dpi=180)
    fig.suptitle(args.title, x=0.07, y=0.97, ha="left", fontsize=18, weight="bold")
    fig.text(0.07, 0.922,
             f"{args.dataset} | {args.records:,} normalized variants | {args.cache_type} cache {args.release} | --everything --hgvs",
             fontsize=11)
    if args.environment:
        fig.text(0.07, 0.896, args.environment, fontsize=10, color="0.3")

    runtime_ax = fig.add_axes([0.075, 0.51, 0.40, 0.31])
    speedup_ax = fig.add_axes([0.57, 0.51, 0.36, 0.31])
    positions = list(range(len(rows)))
    bars = runtime_ax.bar(positions, times, color=["0.8" if row.fork == "none" else "0.3" for row in rows],
                          edgecolor="black", linewidth=0.7, width=0.62)
    runtime_ax.set_title("A  Wall time", loc="left", weight="bold", pad=12)
    runtime_ax.set_ylabel("Elapsed time (s)")
    runtime_ax.set_ylim(0, max(times) * 1.20)
    for bar, seconds in zip(bars, times):
        runtime_ax.text(bar.get_x() + bar.get_width() / 2, seconds + max(times) * 0.025,
                        f"{seconds:.2f} s", ha="center", fontsize=10)

    speedup_ax.plot(positions, speedups, "o-", color="black", linewidth=1.6, markersize=6)
    speedup_ax.axhline(1, color="0.6", linewidth=0.8, linestyle="--")
    speedup_ax.set_title("B  Speedup", loc="left", weight="bold", pad=12)
    speedup_ax.set_ylabel(f"Speedup relative to {'no --fork' if baseline.fork == 'none' else '--fork ' + baseline.fork}")
    speedup_ax.set_ylim(0, max(speedups) * 1.25)
    for xx, speedup in zip(positions, speedups):
        speedup_ax.annotate(f"{speedup:.2f}×", (xx, speedup), xytext=(0, 10),
                            textcoords="offset points", ha="center", fontsize=10)
    for ax in (runtime_ax, speedup_ax):
        ax.set_xticks(positions, labels)
        ax.set_xlabel("VEP --fork")
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", color="0.90", linewidth=0.7)
        ax.set_axisbelow(True)

    fig.text(0.07, 0.443, args.run_note, fontsize=10, color="0.25")
    fig.text(0.07, 0.393, "Exact benchmark command", fontsize=11, weight="bold")
    if args.command_file:
        invocation = args.command_file.read_text().strip()
        invocation = "\n".join(line for line in invocation.splitlines()
                               if not line.startswith("#!") and line != "set -euo pipefail").strip()
    else:
        invocation = command_text(args.cache_type, [row.fork for row in rows], args.release)
    fig.text(0.07, 0.365, invocation, va="top", family="monospace", fontsize=8,
             linespacing=1.5, parse_math=False,
             bbox={"facecolor": "0.97", "edgecolor": "0.7", "linewidth": 0.7, "pad": 10})
    fig.text(0.07, 0.06, "Clock: GNU time around docker run; startup and plain-VCF writing included.",
             fontsize=9, color="0.3")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for suffix in (".png", ".pdf", ".svg"):
        fig.savefig(args.output.with_suffix(suffix), facecolor="white")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-type", choices=["merged", "refseq"], required=True)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--records", type=int, default=DEFAULT_RECORDS)
    parser.add_argument("--baseline-fork", default="none")
    parser.add_argument("--release", default="116")
    parser.add_argument("--command-file", type=Path, help="Embed the exact saved invocation instead of a generic example")
    parser.add_argument("--scaling-panels", action="store_true", help="Monochrome wall-time and speedup panels; exports PNG/PDF/SVG")
    parser.add_argument("--environment", default="")
    parser.add_argument("--dataset", default="HG002 GRCh38")
    parser.add_argument("--run-note", default="One run per setting; exploratory benchmark.")
    args = parser.parse_args()

    rows = discover_rows(args.input_dir, args.cache_type, fractional=args.scaling_panels)
    write_summary(rows, args.summary, args.input_dir)
    plot_rows(rows, args)


if __name__ == "__main__":
    main()
