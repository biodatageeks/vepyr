#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import tempfile
import textwrap
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", os.path.join(tempfile.gettempdir(), "matplotlib"))

import matplotlib.pyplot as plt


DEFAULT_RECORDS = 4_096_123
EXPECTED_WORKERS = (1, 2, 4, 8, 16)


@dataclass(frozen=True)
class RunRow:
    cache_type: str
    workers: int
    status: str
    exit_status: int
    annotation_seconds: float
    output_records: int
    records_match_vep: bool


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes"}


def read_rows(summary: Path, cache_type: str) -> list[RunRow]:
    with summary.open(newline="", encoding="utf-8") as handle:
        raw_rows = list(csv.DictReader(handle, delimiter="\t"))

    rows = [
        RunRow(
            cache_type=row["cache_type"],
            workers=int(row["workers"]),
            status=row["status"],
            exit_status=int(row["exit_status"]),
            annotation_seconds=float(row["annotation_seconds"]),
            output_records=int(row["output_records"]),
            records_match_vep=parse_bool(row["records_match_vep"]),
        )
        for row in raw_rows
        if row["cache_type"] == cache_type
    ]
    rows.sort(key=lambda row: row.workers)
    return rows


def validate_rows(rows: list[RunRow], records: int) -> None:
    workers = tuple(row.workers for row in rows)
    if workers != EXPECTED_WORKERS:
        raise SystemExit(
            f"Expected workers {EXPECTED_WORKERS}, found {workers}"
        )

    for row in rows:
        if (
            row.status != "ok"
            or row.exit_status != 0
            or not row.records_match_vep
            or row.output_records != records
            or row.annotation_seconds <= 0
        ):
            raise SystemExit(
                f"Invalid benchmark row for workers={row.workers}: {row}"
            )


def seconds_to_label(seconds: float) -> str:
    rounded = int(round(seconds))
    hours = rounded // 3600
    minutes = (rounded % 3600) // 60
    secs = rounded % 60
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    return f"{minutes}m {secs:02d}s"


def command_text(cache_type: str) -> str:
    return textwrap.dedent(
        f"""\
        Kod uruchomienia Vepyr 0.3.0 benchmark:
        for workers in (16, 8, 4, 2, 1):
            lf = vepyr.annotate(
                vcf="/home/tgambin/workspace/vep_data/input/HG002_normalized.vcf.gz",
                cache_dir="/home/tgambin/workspace/vep_data/cache/116_GRCh38_{cache_type}",
                everything=True,
                reference_fasta="/home/tgambin/workspace/vep_data/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa",
                workers=workers,
                hgvs=True,
                output_vcf=f"HG002_annotated_vepyr_{cache_type}_workers{{workers}}.vcf",
            )"""
    )


def plot_rows(rows: list[RunRow], args: argparse.Namespace) -> None:
    baseline = next(row for row in rows if row.workers == 1)
    labels = [str(row.workers) for row in rows]
    minutes = [row.annotation_seconds / 60 for row in rows]
    record_label = f"{args.records:,}".replace(",", " ")

    fig = plt.figure(figsize=(18, 10), dpi=160)
    fig.text(
        0.06,
        0.965,
        command_text(args.cache_type),
        ha="left",
        va="top",
        family="monospace",
        fontsize=7,
        bbox={
            "boxstyle": "round,pad=0.35",
            "facecolor": "#f8f8f8",
            "edgecolor": "#bbbbbb",
        },
    )

    ax = fig.add_axes([0.06, 0.10, 0.90, 0.48])
    colors = ["#999999", "#7a68a6", "#4c78a8", "#59a14f", "#e15759"]
    bars = ax.bar(labels, minutes, color=colors, width=0.62)

    for bar, row in zip(bars, rows):
        speedup = baseline.annotation_seconds / row.annotation_seconds
        suffix = "baseline" if row.workers == 1 else f"{speedup:.2f}x faster"
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(minutes) * 0.02,
            (
                f"{seconds_to_label(row.annotation_seconds)}\n"
                f"{suffix}\n{record_label} variants"
            ),
            ha="center",
            va="bottom",
            fontsize=9,
        )

    ax.set_title(args.title, fontsize=16, pad=16)
    ax.set_xlabel("workers")
    ax.set_ylabel("annotation time [minutes]")
    ax.grid(axis="y", alpha=0.25)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(
        0.06,
        0.035,
        (
            f"Summary: {args.summary} | records: {args.records} | "
            "baseline: workers=1"
        ),
        ha="left",
        fontsize=9,
        color="#444444",
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-type", choices=("merged", "refseq"), required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--records", type=int, default=DEFAULT_RECORDS)
    args = parser.parse_args()

    rows = read_rows(args.summary, args.cache_type)
    validate_rows(rows, args.records)
    plot_rows(rows, args)


if __name__ == "__main__":
    main()
