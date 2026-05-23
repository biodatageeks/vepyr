#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt


COMMAND_TEXT = """Kod uruchomienia VEP WGS benchmark:
# WGS benchmark; FORK=none means no --fork argument is passed
for FORK in 16 8 4 2 none; do
  FORK_ARGS=(); if [ "$FORK" != "none" ]; then FORK_ARGS=(--fork "$FORK"); fi
  /usr/bin/time -v -o "$LOG" docker run --rm \\
    -v /home/tgambin/workspace/vep_data2:/cache:ro \\
    -v /home/tgambin/workspace/vep_data2/vep_benchmark_wgs:/out \\
    ensemblorg/ensembl-vep:release_115.1 vep \\
    --input_file /cache/HG002_GRCh38_1_22_v4.2.1_benchmark.normalized.vcf.gz \\
    --output_file /out/vep_merged_buffer20000_wgs_fork_${FORK}.vcf \\
    --offline --cache --merged --dir_cache /cache --species homo_sapiens --assembly GRCh38 --cache_version 115 \\
    --fasta /cache/Homo_sapiens.GRCh38.dna.primary_assembly.fa --format vcf --vcf --everything --buffer_size 20000 \\
    "${FORK_ARGS[@]}" --force_overwrite --no_stats
done"""


def elapsed_to_seconds(value: str) -> int:
    parts = value.split(":")
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


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
      rows = list(csv.DictReader(handle, delimiter="\t"))
    rows = [row for row in rows if row["status"] == "OK"]
    return sorted(rows, key=lambda row: fork_sort_key(row["fork"]))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("summary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--title", default="VEP merged cache WGS benchmark, buffer_size=20000")
    args = parser.parse_args()

    rows = load_rows(args.summary)
    if not rows:
      raise SystemExit(f"No successful rows found in {args.summary}")

    labels = [row["fork"] for row in rows]
    seconds = [elapsed_to_seconds(row["elapsed"]) for row in rows]
    minutes = [value / 60 for value in seconds]
    baseline = seconds[labels.index("none")] if "none" in labels else seconds[0]

    fig = plt.figure(figsize=(18, 10), dpi=160)
    command_text = COMMAND_TEXT.replace("$", r"\$")

    fig.text(
      0.06,
      0.965,
      command_text,
      ha="left",
      va="top",
      family="monospace",
      fontsize=7,
      bbox={"boxstyle": "round,pad=0.35", "facecolor": "#f8f8f8", "edgecolor": "#bbbbbb"},
    )

    ax = fig.add_axes([0.06, 0.10, 0.90, 0.48])
    colors = ["#999999", "#7a68a6", "#4c78a8", "#59a14f", "#f28e2b", "#e15759"]
    bars = ax.bar(labels, minutes, color=colors[: len(labels)], width=0.62)

    for bar, row, sec in zip(bars, rows, seconds):
      speedup = baseline / sec
      record_label = f"{int(row['output_records']):,}".replace(",", " ")
      suffix = "baseline" if row["fork"] == "none" else f"{speedup:.2f}x faster"
      ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + max(minutes) * 0.02,
        f"{seconds_to_label(sec)}\n{suffix}\n{record_label} variants",
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
      f"Summary: {args.summary} | records: {rows[0]['input_records']} | input_records == output_records for all plotted forks",
      ha="left",
      fontsize=9,
      color="#444444",
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, bbox_inches="tight")


if __name__ == "__main__":
    main()
