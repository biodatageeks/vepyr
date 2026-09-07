#!/usr/bin/env python3
"""Build the publication-style performance inset for Figure 1.

The script intentionally uses only the Python standard library.  This keeps the
figure reproducible in a clean checkout while still making Panel C a Python-
generated plot, as opposed to hand-positioned chart elements in draw.io.
"""

from __future__ import annotations

import csv
import html
import math
import statistics
from collections import defaultdict
from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]

LINUX_VEP = (
    REPO
    / "performance-tests/vep/outputs/116/merged_fork_scaling/summary.tsv"
)
LINUX_VEPYR = (
    REPO
    / "performance-tests/vepyr/outputs/116/merged_worker_scaling/summary.tsv"
)
MACOS = HERE / "data" / "macos_merged.tsv"
FILTER = HERE / "data" / "filter_benchmark.tsv"

WIDTH = 1010
HEIGHT = 665
INK = "#111111"
MUTED = "#555555"
GRID = "#dddddd"
PURPLE = INK
PURPLE_DARK = INK
PURPLE_LIGHT = "#eeeeee"
VEP_GREY = "#666666"
SOFT = "#fafafa"
AMBER = "#f4f4f4"


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def read_tsv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [row for row in csv.DictReader(handle, delimiter="\t")]


def elapsed_seconds(value: str) -> float:
    total = 0.0
    for part in value.split(":"):
        total = total * 60 + float(part)
    return total


def linux_rows() -> dict[str, dict[int, float]]:
    vep: dict[int, float] = {}
    for row in read_tsv(LINUX_VEP):
        fork = row.get("fork", "")
        if fork.isdigit() and row.get("status") == "OK":
            vep[int(fork)] = float(row["elapsed_seconds"])

    vepyr: dict[int, float] = {}
    for row in read_tsv(LINUX_VEPYR):
        if row.get("status") == "ok" and row.get("records_match_vep") == "True":
            vepyr[int(row["workers"])] = elapsed_seconds(row["process_elapsed_wall"])
    return {"Ensembl VEP": vep, "vepyr": vepyr}


def optional_platform_rows(path: Path) -> dict[str, dict[int, float]]:
    grouped: defaultdict[tuple[str, int], list[float]] = defaultdict(list)
    for row in read_tsv(path):
        if not row.get("tool") or not row.get("seconds"):
            continue
        grouped[(row["tool"], int(row["parallelism"]))].append(
            float(row["seconds"])
        )
    out: defaultdict[str, dict[int, float]] = defaultdict(dict)
    for (tool, parallelism), values in grouped.items():
        out[tool][parallelism] = statistics.median(values)
    return dict(out)


def optional_filter_rows(path: Path) -> dict[str, float]:
    grouped: defaultdict[str, list[float]] = defaultdict(list)
    for row in read_tsv(path):
        if row.get("pipeline") and row.get("seconds"):
            grouped[row["pipeline"]].append(float(row["seconds"]))
    return {key: statistics.median(values) for key, values in grouped.items()}


def svg_text(
    parts: list[str],
    x: float,
    y: float,
    text: str,
    *,
    size: float = 16,
    weight: int = 400,
    anchor: str = "start",
    fill: str = INK,
    rotate: float | None = None,
) -> None:
    transform = f' transform="rotate({rotate} {x} {y})"' if rotate else ""
    parts.append(
        f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" '
        f'font-weight="{weight}" text-anchor="{anchor}" fill="{fill}"'
        f'{transform}>{esc(text)}</text>'
    )


def rect(
    parts: list[str],
    x: float,
    y: float,
    width: float,
    height: float,
    *,
    fill: str = "white",
    stroke: str = GRID,
    radius: float = 0,
    dash: str | None = None,
) -> None:
    dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
    parts.append(
        f'<rect x="{x}" y="{y}" width="{width}" height="{height}" '
        f'rx="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"'
        f'{dash_attr}/>'
    )


def chart(
    parts: list[str],
    x: float,
    y: float,
    width: float,
    height: float,
    title: str,
    rows: dict[str, dict[int, float]],
    *,
    placeholder: str | None = None,
) -> None:
    svg_text(parts, x, y - 14, title, size=17, weight=700)
    if placeholder or not rows:
        rect(parts, x, y, width, height, fill=SOFT, stroke="#999999", dash="8 6")
        svg_text(
            parts,
            x + width / 2,
            y + height / 2 - 12,
            "DATA REQUIRED",
            size=22,
            weight=700,
            anchor="middle",
            fill=VEP_GREY,
        )
        svg_text(
            parts,
            x + width / 2,
            y + height / 2 + 20,
            placeholder or "Add benchmark rows",
            size=14,
            anchor="middle",
            fill=MUTED,
        )
        return

    left, right, top, bottom = 62, 16, 18, 48
    plot_x = x + left
    plot_y = y + top
    plot_w = width - left - right
    plot_h = height - top - bottom

    all_values = [seconds for tool in rows.values() for seconds in tool.values()]
    y_min = 100.0
    y_max = max(30_000.0, max(all_values) * 1.12)
    log_min = math.log10(y_min)
    log_max = math.log10(y_max)

    def py(seconds: float) -> float:
        fraction = (math.log10(seconds) - log_min) / (log_max - log_min)
        return plot_y + plot_h * (1 - fraction)

    workers = [1, 2, 4, 8, 16]

    def px(worker: int) -> float:
        return plot_x + workers.index(worker) * plot_w / (len(workers) - 1)

    ticks = [120, 300, 600, 1800, 3600, 10800, 21600]
    labels = ["2 min", "5 min", "10 min", "30 min", "1 h", "3 h", "6 h"]
    for tick, label in zip(ticks, labels, strict=True):
        if tick > y_max:
            continue
        ty = py(tick)
        parts.append(
            f'<line x1="{plot_x}" y1="{ty:.1f}" x2="{plot_x + plot_w}" '
            f'y2="{ty:.1f}" stroke="{GRID}" stroke-width="1"/>'
        )
        svg_text(parts, plot_x - 10, ty + 5, label, size=12, anchor="end", fill=MUTED)

    parts.append(
        f'<line x1="{plot_x}" y1="{plot_y}" x2="{plot_x}" '
        f'y2="{plot_y + plot_h}" stroke="{INK}" stroke-width="1.4"/>'
    )
    parts.append(
        f'<line x1="{plot_x}" y1="{plot_y + plot_h}" x2="{plot_x + plot_w}" '
        f'y2="{plot_y + plot_h}" stroke="{INK}" stroke-width="1.4"/>'
    )
    for worker in workers:
        xx = px(worker)
        svg_text(parts, xx, plot_y + plot_h + 24, str(worker), size=13, anchor="middle")
    svg_text(parts, plot_x + plot_w / 2, y + height - 5, "forks / workers", size=13, anchor="middle")
    svg_text(
        parts,
        x + 14,
        plot_y + plot_h / 2,
        "process wall time (log scale)",
        size=12,
        anchor="middle",
        fill=MUTED,
        rotate=-90,
    )

    specs = {
        "Ensembl VEP": (VEP_GREY, "6 4", "square"),
        "vepyr": (PURPLE, None, "circle"),
    }
    for tool, values in rows.items():
        color, dash, marker = specs.get(tool, (PURPLE_DARK, None, "circle"))
        points = [(px(worker), py(values[worker])) for worker in workers if worker in values]
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        parts.append(
            f'<polyline points="{" ".join(f"{xx:.1f},{yy:.1f}" for xx, yy in points)}" '
            f'fill="none" stroke="{color}" stroke-width="3"{dash_attr}/>'
        )
        for xx, yy in points:
            if marker == "square":
                parts.append(
                    f'<rect x="{xx - 4:.1f}" y="{yy - 4:.1f}" width="8" height="8" '
                    f'fill="white" stroke="{color}" stroke-width="2"/>'
                )
            else:
                parts.append(
                    f'<circle cx="{xx:.1f}" cy="{yy:.1f}" r="4.5" fill="{color}"/>'
                )

    lx = plot_x + plot_w - 158
    ly = plot_y + 20
    for index, (tool, (color, dash, _marker)) in enumerate(specs.items()):
        yy = ly + index * 24
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        parts.append(
            f'<line x1="{lx}" y1="{yy}" x2="{lx + 28}" y2="{yy}" '
            f'stroke="{color}" stroke-width="3"{dash_attr}/>'
        )
        svg_text(parts, lx + 36, yy + 5, tool, size=13)

    if "Ensembl VEP" in rows and "vepyr" in rows:
        if 8 in rows["Ensembl VEP"] and 8 in rows["vepyr"]:
            speedup = rows["Ensembl VEP"][8] / rows["vepyr"][8]
            xx = px(8)
            yy = py(rows["vepyr"][8])
            rect(parts, xx - 72, yy - 60, 144, 34, fill=PURPLE_LIGHT, stroke=PURPLE)
            svg_text(
                parts,
                xx,
                yy - 38,
                f"{speedup:.1f}x at 8",
                size=15,
                weight=700,
                anchor="middle",
                fill=PURPLE_DARK,
            )


def filter_comparison(parts: list[str], rows: dict[str, float]) -> None:
    x, y, width, height = 35, 445, 940, 182
    svg_text(parts, x, y - 12, "Post-annotation filtering (same rare-variant predicate)", size=17, weight=700)
    rect(parts, x, y, width, height, fill="white", stroke=GRID)

    if rows:
        maximum = max(rows.values())
        labels = ["VEP + filter_vep", "vepyr + SQL"]
        colors = [VEP_GREY, PURPLE]
        for index, (label, color) in enumerate(zip(labels, colors, strict=True)):
            seconds = rows.get(label)
            yy = y + 45 + index * 55
            svg_text(parts, x + 18, yy + 6, label, size=14, weight=700)
            if seconds is None:
                svg_text(parts, x + 230, yy + 6, "missing", size=13, fill=MUTED)
                continue
            bar_w = 440 * seconds / maximum
            parts.append(
                f'<rect x="{x + 230}" y="{yy - 11}" width="{bar_w:.1f}" height="22" '
                f'fill="{color}"/>'
            )
            svg_text(parts, x + 240 + bar_w, yy + 6, f"{seconds:.2f} s", size=13)
        return

    svg_text(parts, x + 18, y + 38, "VEP", size=14, weight=700)
    svg_text(parts, x + 78, y + 38, "annotate  →  VCF  →  filter_vep", size=17)
    svg_text(parts, x + 18, y + 79, "vepyr", size=14, weight=700, fill=PURPLE_DARK)
    svg_text(parts, x + 78, y + 79, "annotate  →  LazyFrame  →  predicate", size=17)
    rect(parts, x + 650, y + 24, 266, 78, fill=AMBER, stroke="#777777", dash="7 5")
    svg_text(parts, x + 783, y + 56, "BENCHMARK PENDING", size=16, weight=700, anchor="middle", fill=INK)
    svg_text(parts, x + 783, y + 80, "runtime + peak RSS", size=14, anchor="middle", fill=MUTED)
    svg_text(
        parts,
        x + 18,
        y + 137,
        'Predicate: gnomADg_AF < 0.01 OR gnomADg_AF IS NULL',
        size=14,
        weight=700,
    )
    svg_text(
        parts,
        x + 18,
        y + 162,
        "Compare identical input, cache, output semantics, warm cache state and storage.",
        size=12,
        fill=MUTED,
    )


def build_svg() -> str:
    linux = linux_rows()
    macos = optional_platform_rows(MACOS)
    filter_rows = optional_filter_rows(FILTER)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<g font-family="Arial, Helvetica, sans-serif">',
    ]
    chart(parts, 35, 55, 555, 355, "Linux / x86-64, merged WGS", linux)
    chart(
        parts,
        620,
        55,
        355,
        355,
        "macOS / arm64, merged WGS",
        macos,
        placeholder=(
            None
            if macos
            else "paired VEP + vepyr runs"
        ),
    )
    filter_comparison(parts, filter_rows)
    svg_text(
        parts,
        35,
        654,
        "HG002 chr1–22 · 4,096,123 variants · 116 merged · --everything + HGVS · process wall time",
        size=15,
        fill=MUTED,
    )
    parts.extend(["</g>", "</svg>"])
    return "\n".join(parts) + "\n"


if __name__ == "__main__":
    print(build_svg(), end="")
