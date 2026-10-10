#!/usr/bin/env python3
"""Draw Figure 1C from the frozen, audited whole-process timing snapshot.

Standard-library SVG generation; refresh_performance_data.py imports and
checks the original summaries, metrics and GNU time logs from a Git revision.
"""

from __future__ import annotations

import html
import json
import math
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / "data/performance-wgs.json"
WIDTH, HEIGHT = 1010, 780
INK, MUTED, GRID = "#111111", "#555555", "#dddddd"
LEVELS = (1, 2, 4, 8)


def text(
    parts, x, y, value, size=24, weight=400, anchor="start", fill=INK, rotate=None
):
    transform = f' transform="rotate({rotate} {x} {y})"' if rotate else ""
    parts.append(
        f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" '
        f'font-weight="{weight}" text-anchor="{anchor}" fill="{fill}"'
        f"{transform}>{html.escape(str(value))}</text>"
    )


def line(parts, x1, y1, x2, y2, color=GRID, width=1.5, dash=""):
    dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
    parts.append(
        f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" '
        f'y2="{y2:.1f}" stroke="{color}" stroke-width="{width}"{dash_attr}/>'
    )


def rows(panel):
    result = {"Ensembl VEP": {}, "vepyr": {}}
    for run in panel["runs"]:
        tool, level, wall = run["tool"], run["parallelism"], run["wall_seconds"]
        if tool not in result or level not in LEVELS or level in result[tool]:
            raise ValueError(f"Unexpected or duplicated measurement: {run}")
        if not math.isfinite(wall) or wall <= 0:
            raise ValueError(f"Invalid wall time: {run}")
        if tool == "Ensembl VEP" and run["fork"] != (
            "none" if level == 1 else str(level - 1)
        ):
            raise ValueError(f"Invalid fork/worker pairing: {run}")
        if tool == "vepyr" and run["version"] != panel["vepyr_version"]:
            raise ValueError(f"Inconsistent version: {run}")
        result[tool][level] = wall
    if any(set(series) != set(LEVELS) for series in result.values()):
        raise ValueError(f"Incomplete performance series: {panel['platform']}")
    return result


def format_time(seconds):
    """Compact endpoint labels, using the units in the reference chart."""
    if seconds >= 3600:
        hours, rest = divmod(round(seconds), 3600)
        return f"{hours}h {rest // 60:02d}m"
    if seconds >= 60:
        minutes, rest = divmod(round(seconds), 60)
        return f"{minutes}m {rest:02d}s"
    return f"{seconds:.1f}s"


def chart(parts, x, title, subtitle, panel):
    values = rows(panel)
    text(parts, x, 76, title, size=28, weight=700)
    text(parts, x, 106, subtitle, size=22, fill=MUTED)
    # Shared log limits and inset categorical slots leave room for endpoint labels.
    plot_x, plot_y, plot_w, plot_h = x + 70, 134, 344, 540

    def px(level):
        return plot_x + 38 + LEVELS.index(level) * (plot_w - 76) / 3

    def py(seconds):
        if not 30 <= seconds <= 80000:
            raise ValueError(f"Measurement outside shared axis limits: {seconds}")
        return plot_y + plot_h * (1 - math.log(seconds / 30) / math.log(80000 / 30))

    for tick, label in ((60, "1 min"), (600, "10 min"), (3600, "1 h"), (36000, "10 h")):
        yy = py(tick)
        line(parts, plot_x, yy, plot_x + plot_w, yy)
        text(parts, plot_x - 10, yy + 7, label, size=21, anchor="end", fill=MUTED)
    line(parts, plot_x, plot_y + plot_h, plot_x + plot_w, plot_y + plot_h, color=INK)
    for level in LEVELS:
        text(parts, px(level), plot_y + plot_h + 31, level, size=24, anchor="middle")

    for tool, color, dash in (("Ensembl VEP", "#666666", "9 6"), ("vepyr", INK, "")):
        points = [(px(level), py(values[tool][level])) for level in LEVELS]
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        coords = " ".join(f"{xx:.1f},{yy:.1f}" for xx, yy in points)
        parts.append(
            f'<polyline points="{coords}" fill="none" stroke="{color}" '
            f'stroke-width="3.5"{dash_attr}/>'
        )
        for xx, yy in points:
            if tool == "Ensembl VEP":
                parts.append(
                    f'<rect x="{xx - 5:.1f}" y="{yy - 5:.1f}" width="10" height="10" '
                    f'fill="white" stroke="{color}" stroke-width="2.5"/>'
                )
            else:
                parts.append(
                    f'<circle cx="{xx:.1f}" cy="{yy:.1f}" r="5.5" fill="{color}"/>'
                )

    for level in LEVELS:
        vep, vepyr = values["Ensembl VEP"][level], values["vepyr"][level]
        xx = px(level)
        yy = py(math.sqrt(vep * vepyr))
        parts.append(
            f'<rect x="{xx - 43:.1f}" y="{yy - 20:.1f}" width="86" height="34" fill="white"/>'
        )
        text(
            parts,
            xx,
            yy + 5,
            f"{vep / vepyr:.1f}×",
            size=24,
            weight=700,
            anchor="middle",
        )

    for tool, offset in (("Ensembl VEP", -17), ("vepyr", 29)):
        for level in (1, 8):
            text(
                parts,
                px(level),
                py(values[tool][level]) + offset,
                format_time(values[tool][level]),
                size=22,
                anchor="middle",
                fill=MUTED,
            )


def build_svg():
    data = json.loads(DATA.read_text())
    if (
        data["records"] != 4_096_123
        or data["cache_type"] != "merged"
        or data["cache_release"] != 116
    ):
        raise ValueError("Figure labels require HG002 WGS / merged cache 116")
    panels = {panel["platform"]: panel for panel in data["panels"]}
    if set(panels) != {"linux", "macos"}:
        raise ValueError("Both platform series are required")
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" role="img">',
        "<title>HG002 whole-genome annotation: VEP and vepyr on Linux and macOS</title>",
        "<desc>Whole-process wall time on a shared logarithmic scale. "
        "VEP no fork, fork 1, 3 and 7 are paired with vepyr workers 1, 2, 4 and 8. "
        "Ratios are VEP time divided by vepyr time at each setting. "
        "Endpoint labels show elapsed times. One run per setting.</desc>",
        '<rect width="100%" height="100%" fill="white"/>',
        '<g font-family="Arial, Helvetica, sans-serif">',
    ]
    line(parts, 74, 23, 114, 23, color="#666666", width=3.5, dash="9 6")
    text(parts, 125, 31, "VEP 116.0 (Docker)", size=24)
    line(parts, 455, 23, 495, 23, color=INK, width=3.5)
    text(parts, 508, 31, "vepyr", size=24)
    text(parts, 983, 31, "wall time · log scale", size=22, anchor="end", fill=MUTED)
    chart(
        parts,
        47,
        "Linux · Ryzen 9 5950X",
        "x86-64",
        panels["linux"],
    )
    chart(
        parts,
        545,
        "macOS · Apple M3 Max",
        "arm64",
        panels["macos"],
    )
    text(
        parts, WIDTH / 2, 753, "VEP processes / vepyr workers", size=25, anchor="middle"
    )
    parts.extend(["</g>", "</svg>"])
    return "\n".join(parts) + "\n"


if __name__ == "__main__":
    print(build_svg(), end="")
