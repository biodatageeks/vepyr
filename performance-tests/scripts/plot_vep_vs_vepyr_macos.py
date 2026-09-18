#!/usr/bin/env python3
"""Plot Ensembl VEP against vepyr on the same Mac, chr22 and whole genome.

Reads the per-run summaries that the two benchmark plotters already write
(`plot_vep_fork_scaling.py` for VEP, `run_vepyr_worker_scaling.py` for vepyr)
and draws one panel per input: wall time on a log axis against the process
count, VEP `--fork N` children plus the parent next to vepyr `workers`, with
the VEP/vepyr ratio written between the matched pairs. Wall time is the whole process for both
tools: `docker run` for VEP and the Python benchmark process for vepyr, so
start-up and VCF writing are inside both numbers.
"""

from __future__ import annotations

import argparse
import csv
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", os.path.join(tempfile.gettempdir(), "matplotlib"))

import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, NullLocator

REPO = Path(__file__).resolve().parents[2]
VEP_OUT = REPO / "performance-tests" / "vep" / "outputs" / "116"
VEPYR_OUT = REPO / "performance-tests" / "vepyr" / "outputs" / "116"

# Categorical slots 1 and 2 of the validated default palette; the pair passes
# the CVD and normal-vision checks on the light surface.
VEP_COLOR = "#2a78d6"
VEPYR_COLOR = "#eb6834"
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"

# VEP --fork N runs N annotation children plus the parent that reads and
# writes, so --fork 1 is already two processes; vepyr workers=1 is one
# pipeline. Both series are drawn against their process count.
VEP_LEVELS = ["none", "1", "2", "4", "8"]
VEP_PROCESSES = {"none": 1, "1": 2, "2": 3, "4": 5, "8": 9}
VEPYR_LEVELS = ["1", "2", "4", "8"]
PAIRS = ((1, "none", "1"), (2, "1", "2"), (8.5, "8", "8"))


@dataclass(frozen=True)
class Panel:
    title: str
    records: int
    vep_summary: Path
    vepyr_summary: Path


PANELS = (
    Panel(
        "chr22 · 50,861 variants",
        50_861,
        VEP_OUT / "macos_mwiewior_chr22_20260913" / "summary.tsv",
        VEPYR_OUT / "macos_mwiewior_chr22_20260914" / "raw" / "summary.tsv",
    ),
    Panel(
        "whole genome · 4,096,123 variants",
        4_096_123,
        VEP_OUT / "macos_mwiewior_wgs_20260914" / "summary.tsv",
        VEPYR_OUT / "macos_mwiewior_wgs_20260914" / "raw" / "summary.tsv",
    ),
)


def parse_wall(value: str) -> float:
    """GNU time's elapsed field: h:mm:ss or m:ss.ss."""
    parts = value.strip().split(":")
    seconds = 0.0
    for part in parts:
        seconds = seconds * 60 + float(part)
    return seconds


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def vep_seconds(path: Path) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in read_tsv(path):
        if row["status"] != "OK" or row["exit_status"] != "0":
            raise SystemExit(f"{path}: fork={row['fork']} did not succeed")
        out[row["fork"]] = float(row["elapsed_seconds"])
    return out


def vepyr_seconds(path: Path, records: int) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in read_tsv(path):
        if row["status"] != "ok" or row["exit_status"] != "0":
            raise SystemExit(f"{path}: workers={row['workers']} did not succeed")
        if int(row["output_records"]) != records:
            raise SystemExit(
                f"{path}: workers={row['workers']} wrote {row['output_records']} "
                f"records, expected {records}"
            )
        out[row["workers"]] = parse_wall(row["process_elapsed_wall"])
    return out


def fmt_seconds(seconds: float) -> str:
    if seconds >= 3600:
        hours, rest = divmod(round(seconds), 3600)
        return f"{hours}h {rest // 60:02d}m"
    if seconds >= 60:
        minutes, secs = divmod(round(seconds), 60)
        return f"{minutes}m {secs:02d}s"
    return f"{seconds:.1f}s"


def draw_panel(ax, panel: Panel) -> None:
    vep = vep_seconds(panel.vep_summary)
    vepyr = vepyr_seconds(panel.vepyr_summary, panel.records)
    if any(level not in vep for level in VEP_LEVELS) or any(
        level not in vepyr for level in VEPYR_LEVELS
    ):
        raise SystemExit(
            f"{panel.title}: incomplete sweep, vep={sorted(vep)} vepyr={sorted(vepyr)}"
        )

    vep_x = [VEP_PROCESSES[level] for level in VEP_LEVELS]
    vep_y = [vep[level] for level in VEP_LEVELS]
    vepyr_x = [int(level) for level in VEPYR_LEVELS]
    vepyr_y = [vepyr[level] for level in VEPYR_LEVELS]

    ax.set_facecolor(SURFACE)
    ax.set_yscale("log")
    ax.plot(
        vep_x,
        vep_y,
        color=VEP_COLOR,
        linewidth=2,
        marker="o",
        markersize=8,
        markeredgecolor=SURFACE,
        markeredgewidth=2,
        solid_capstyle="round",
        label="Ensembl VEP 116.0 (Docker)",
        zorder=3,
    )
    ax.plot(
        vepyr_x,
        vepyr_y,
        color=VEPYR_COLOR,
        linewidth=2,
        marker="o",
        markersize=8,
        markeredgecolor=SURFACE,
        markeredgewidth=2,
        solid_capstyle="round",
        label="vepyr 0.7.0",
        zorder=3,
    )

    # The story is the gap at equal process count, so the ratio is the one
    # label the matched pairs carry: serial vs serial, --fork 1 vs workers=2,
    # and the two eight-way runs (VEP has one process more there).
    for x, vep_level, vepyr_level in PAIRS:
        ratio = vep[vep_level] / vepyr[vepyr_level]
        mid = (vep[vep_level] * vepyr[vepyr_level]) ** 0.5
        ax.annotate(
            f"{ratio:.0f}×",
            (x, mid),
            ha="center",
            va="center",
            fontsize=11,
            color=INK_SECONDARY,
            fontweight="bold",
            bbox={
                "boxstyle": "round,pad=0.2",
                "facecolor": SURFACE,
                "edgecolor": "none",
            },
        )

    # Endpoint labels only: the serial runs and the eight-way runs.
    ax.annotate(
        fmt_seconds(vep["none"]),
        (1, vep["none"]),
        xytext=(0, 10),
        textcoords="offset points",
        ha="center",
        fontsize=10,
        color=INK_SECONDARY,
    )
    ax.annotate(
        fmt_seconds(vep["8"]),
        (9, vep["8"]),
        xytext=(0, 10),
        textcoords="offset points",
        ha="center",
        fontsize=10,
        color=INK_SECONDARY,
    )
    ax.annotate(
        fmt_seconds(vepyr["1"]),
        (1, vepyr["1"]),
        xytext=(0, -16),
        textcoords="offset points",
        ha="center",
        fontsize=10,
        color=INK_SECONDARY,
    )
    ax.annotate(
        fmt_seconds(vepyr["8"]),
        (8, vepyr["8"]),
        xytext=(0, -16),
        textcoords="offset points",
        ha="center",
        fontsize=10,
        color=INK_SECONDARY,
    )

    ax.set_title(panel.title, fontsize=13, color=INK, loc="left", pad=12)
    ax.set_xticks(list(range(1, 10)))
    ax.set_xticklabels(
        [
            "1\nnone / w1",
            "2\nfork 1 / w2",
            "3\nfork 2",
            "4\nw4",
            "5\nfork 4",
            "6",
            "7",
            "8\nw8",
            "9\nfork 8",
        ],
        fontsize=8.5,
        color=INK_SECONDARY,
    )
    ax.set_xlabel(
        "processes: VEP --fork N children + parent, vepyr w = workers",
        fontsize=10,
        color=INK_SECONDARY,
    )
    ax.set_xlim(0.4, 9.6)

    ticks = [1, 10, 60, 600, 3600, 36000]
    labels = ["1 s", "10 s", "1 min", "10 min", "1 h", "10 h"]
    ax.yaxis.set_major_locator(FixedLocator(ticks))
    ax.yaxis.set_minor_locator(NullLocator())
    ax.set_yticklabels(labels, fontsize=10, color=INK_SECONDARY)
    ax.set_ylim(1, 60000)
    ax.tick_params(axis="both", length=0, colors=INK_SECONDARY)
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO / "performance-tests" / "figures" / "vep_vs_vepyr_macos_116.png",
        help="PNG path; an SVG is written next to it",
    )
    args = parser.parse_args()

    fig, axes = plt.subplots(1, 2, figsize=(13, 6), dpi=160, sharey=True)
    fig.patch.set_facecolor(SURFACE)
    for ax, panel in zip(axes, PANELS):
        draw_panel(ax, panel)
    axes[0].set_ylabel("wall time (log scale)", fontsize=10, color=INK_SECONDARY)

    fig.suptitle(
        "Ensembl VEP vs vepyr on one MacBook Pro (M3 Max), release 116, merged cache",
        fontsize=15,
        color=INK,
        x=0.06,
        ha="left",
        y=0.99,
    )
    fig.text(
        0.06,
        0.94,
        "HG002 GRCh38, --everything --hgvs with a reference FASTA, plain VCF output. "
        "Whole process wall time: docker run for VEP, the benchmark process for vepyr.",
        fontsize=9.5,
        color=INK_MUTED,
        ha="left",
    )
    fig.text(
        0.06,
        0.905,
        "One run per setting. Ratios at equal process count; the last one is "
        "--fork 8 (nine processes) against workers=8.",
        fontsize=9.5,
        color=INK_MUTED,
        ha="left",
    )
    axes[0].legend(
        loc="upper right", frameon=False, fontsize=10, labelcolor=INK_SECONDARY
    )
    fig.tight_layout(rect=(0, 0, 1, 0.9))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, facecolor=SURFACE)
    fig.savefig(args.output.with_suffix(".svg"), facecolor=SURFACE)
    plt.close(fig)
    print(args.output)


if __name__ == "__main__":
    main()
