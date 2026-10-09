#!/usr/bin/env python3
"""Plot Linux WGS timings for Ensembl VEP 116 and vepyr 0.9.0.

Adapted from plot_vep_vs_vepyr_macos.py on perf/macos-116-vep-vs-vepyr
(2e28399d1d6492d7dd55df2b858dcff9a8ea45d1), retaining its style and its
whole-process wall-time comparison. The two panels show merged and RefSeq.
VEP fork 0 (stored as "none"), 1, 3, 7 map to vepyr workers 1, 2, 4, 8.
HDD archiving is outside both measurements.
Run from any directory; the default inputs are the published repository data:
    python performance-tests/scripts/plot_vep_vs_vepyr_linux.py
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", os.path.join(tempfile.gettempdir(), "matplotlib"))

import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, NullLocator

REPO = Path(__file__).resolve().parents[2]
VEP_OUT = REPO / "performance-tests" / "vep" / "outputs" / "116"
VEPYR_OUT = (
    REPO
    / "performance-tests"
    / "vepyr"
    / "outputs"
    / "116"
    / "linux_vepyr_0.9.0_20261002"
)
VEPYR_VERSION = "0.9.0"

# Same palette and typography as the macOS comparison.
VEP_COLOR = "#2a78d6"
VEPYR_COLOR = "#eb6834"
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"

# VEP uses N annotation children plus the parent when forking is enabled.
# Fork 0 is the no-fork run, called "none" in the Linux benchmark summaries.
VEP_LEVELS = ["none", "1", "3", "7"]
VEP_PROCESSES = {"none": 1, "1": 2, "3": 4, "7": 8}
VEPYR_LEVELS = ["1", "2", "4", "8"]
PAIRS = ((1, "none", "1"), (2, "1", "2"), (4, "3", "4"), (8, "7", "8"))


@dataclass(frozen=True)
class Panel:
    title: str
    cache_type: str
    records: int
    vep_summary: Path
    vepyr_summary: Path


def parse_wall(value: str) -> float:
    """GNU time's elapsed field: h:mm:ss or m:ss.ss."""
    seconds = 0.0
    for part in value.strip().split(":"):
        seconds = seconds * 60 + float(part)
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError(f"invalid wall time: {value!r}")
    return seconds


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def vep_seconds(path: Path, cache_type: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in read_tsv(path):
        level = "none" if row["fork"] == "0" else row["fork"]
        if level not in VEP_LEVELS:
            continue
        if level in out:
            raise SystemExit(f"{path}: duplicate fork={level}")
        if row["cache_type"] != cache_type:
            raise SystemExit(f"{path}: expected cache_type={cache_type}")
        if row["status"] != "OK" or row["exit_status"] != "0":
            raise SystemExit(f"{path}: fork={level} did not succeed")
        # elapsed_seconds in the VEP summaries is rounded to integer seconds;
        # elapsed_wall retains the original GNU time precision.
        out[level] = parse_wall(row["elapsed_wall"])
    return out


def vepyr_seconds(path: Path, records: int, cache_type: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in read_tsv(path):
        level = row["workers"]
        if level not in VEPYR_LEVELS:
            continue
        if level in out:
            raise SystemExit(f"{path}: duplicate workers={level}")
        if row["cache_type"] != cache_type:
            raise SystemExit(f"{path}: expected cache_type={cache_type}")
        if row["status"] != "ok" or row["exit_status"] != "0":
            raise SystemExit(f"{path}: workers={level} did not succeed")
        if int(row["output_records"]) != records:
            raise SystemExit(
                f"{path}: workers={level} wrote {row['output_records']} "
                f"records, expected {records}"
            )
        if row["compression"] != "plain" or row["preserve_record_layout"] != "True":
            raise SystemExit(f"{path}: expected plain VCF and preserve_record_layout")
        # Check the release from the archived metrics, not from the plotting
        # interpreter or the old vepyr summaries in the checkout.
        metrics_path = path.parent / f"{cache_type}_workers{level}.metrics.json"
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        if metrics["vepyr_version"] != VEPYR_VERSION:
            raise SystemExit(f"{metrics_path}: expected vepyr {VEPYR_VERSION}")
        if (
            metrics["cache_type"] != cache_type
            or metrics["workers"] != int(level)
            or metrics["status"] != "ok"
            or metrics["output_records"] != records
            or metrics["compression"] != "plain"
            or metrics["preserve_record_layout"] is not True
        ):
            raise SystemExit(f"{metrics_path}: metrics do not match the summary")
        out[level] = parse_wall(row["process_elapsed_wall"])
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
    vep = vep_seconds(panel.vep_summary, panel.cache_type)
    vepyr = vepyr_seconds(panel.vepyr_summary, panel.records, panel.cache_type)
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
        label=f"vepyr {VEPYR_VERSION} (PyPI)",
        zorder=3,
    )

    # Ratios use the matched process/worker settings at all four positions.
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

    # Endpoint labels only, as in the macOS comparison.
    for x, seconds, offset in (
        (1, vep["none"], 10),
        (8, vep["7"], 10),
        (1, vepyr["1"], -16),
        (8, vepyr["8"], -16),
    ):
        ax.annotate(
            fmt_seconds(seconds),
            (x, seconds),
            xytext=(0, offset),
            textcoords="offset points",
            ha="center",
            fontsize=10,
            color=INK_SECONDARY,
        )

    ax.set_title(panel.title, fontsize=13, color=INK, loc="left", pad=12)
    ax.set_xticks([1, 2, 4, 8])
    ax.set_xticklabels(
        ["1\nfork 0 / w1", "2\nfork 1 / w2", "4\nfork 3 / w4", "8\nfork 7 / w8"],
        fontsize=8.5,
        color=INK_SECONDARY,
    )
    ax.set_xlabel(
        "VEP processes (children + parent) / vepyr workers",
        fontsize=10,
        color=INK_SECONDARY,
    )
    ax.set_xlim(0.4, 8.6)

    ticks = [60, 600, 3600, 36000]
    labels = ["1 min", "10 min", "1 h", "10 h"]
    ax.yaxis.set_major_locator(FixedLocator(ticks))
    ax.yaxis.set_minor_locator(NullLocator())
    ax.set_yticklabels(labels, fontsize=10, color=INK_SECONDARY)
    ax.set_ylim(30, 80000)
    ax.tick_params(axis="both", length=0, colors=INK_SECONDARY)
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--vep-root",
        type=Path,
        default=VEP_OUT,
        help="Directory containing merged_fork_scaling and refseq_fork_scaling",
    )
    parser.add_argument(
        "--vepyr-root",
        type=Path,
        default=VEPYR_OUT,
        help=(
            "Release directory containing vepyr_{merged,refseq}_worker_scaling "
            "summaries and metrics for vepyr 0.9.0"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO / "performance-tests" / "figures" / "vep_vs_vepyr_linux_116.png",
        help="PNG path; an SVG is written next to it",
    )
    args = parser.parse_args()
    panels = tuple(
        Panel(
            title=f"{title} cache · whole genome",
            cache_type=cache_type,
            records=4_096_123,
            vep_summary=args.vep_root / f"{cache_type}_fork_scaling" / "summary.tsv",
            vepyr_summary=args.vepyr_root
            / f"vepyr_{cache_type}_worker_scaling"
            / "summary.tsv",
        )
        for cache_type, title in (("merged", "Merged"), ("refseq", "RefSeq"))
    )

    fig, axes = plt.subplots(1, 2, figsize=(13, 6), dpi=160, sharey=True)
    fig.patch.set_facecolor(SURFACE)
    for ax, panel in zip(axes, panels):
        draw_panel(ax, panel)
    axes[0].set_ylabel("wall time (log scale)", fontsize=10, color=INK_SECONDARY)

    fig.suptitle(
        "Ensembl VEP vs vepyr on Linux · release 116 · whole genome",
        fontsize=15,
        color=INK,
        x=0.06,
        ha="left",
        y=0.99,
    )
    fig.text(
        0.06,
        0.94,
        "HG002 GRCh38 · 4,096,123 variants · --everything --hgvs · reference FASTA · "
        "plain VCF output · same Linux machine",
        fontsize=9.5,
        color=INK_MUTED,
        ha="left",
    )
    fig.text(
        0.06,
        0.905,
        "Whole-process wall time, excluding HDD archiving. One measurement shown per setting; "
        "ratios show VEP / vepyr. Fork 0 = no forking.",
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
