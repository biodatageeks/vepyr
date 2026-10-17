#!/usr/bin/env python3
"""Figure 1 proposal, version 2, laid out for one A4 page with its caption.

Panels:

* A  Ensembl VEP and its reimplementations: the comparison table generated
     by ../tool-comparison/build_table.py (embedded vector image, full width);
* B  Interactive analysis with vepyr and polars-bio (build_tiered_annotation),
     the HG002 workflow with code fragments and its sources; the biological
     question and answer are in the caption;
* C1-C4  architecture, cache build, cache anatomy (previously A1-A3, with
     their magnification leaders) and cache lookup, a column beside C3;
* D  Query pushdown (build_pushdown), beside
* E  Performance (previously C), in the last row.

The canvas is 2960 px wide and about 1.1 times as high, which leaves room
for a caption below the figure on an A4 page at full text width.

The figure is written as TikZ by build_tikz.py, which imports this module.

    python ../tool-comparison/build_table.py --pdf   # refresh the table first
    python build_tikz.py
"""

from __future__ import annotations

import argparse
import base64
import copy
import re
from pathlib import Path

import build_cache_simplified_proposal as simplified
import build_cache_workflow_proposal as workflow
import build_figure as fig
import build_performance
import build_pushdown
import build_tiered_annotation as tiered


HERE = Path(__file__).resolve().parent
TABLE_SVG = HERE.parent / "tool-comparison" / "panel-tools-figure.svg"
WIDTH, CONTENT = 2960, 2880
LIGHT, GREY = "#bdbdbd", "#555555"
BLOCK_HEIGHT = 1385  # C1-C3 with leaders, in the coordinates of the old figure
RELABEL = {"A1": "C1", "A2": "C2", "A3": "C3", "A4": "C4"}
# Phrases joined with middle dots, rewritten as text.
REWRITE = {
    "Human GRCh38 · SNVs + indels · releases 115 / 116":
        "Human GRCh38, SNVs and indels, releases 115 and 116",
    "Ensembl / RefSeq / merged · --everything · HGVS":
        "Ensembl, RefSeq and merged caches, --everything with HGVS",
    "Arrow streaming · lazy queries · DataFrame API / SQL · declarative plugins":
        "Arrow streaming, lazy queries, DataFrame API and SQL, declarative plugins",
    "annotate_vep() · table function": "annotate_vep() table function",
    "Known variants / AF · optional plugins": "Known variants and AF, optional plugins",
    "Plugin build · TOML": "Plugin build (TOML)",
    "Small pages · sorted within tiers": "Small pages sorted within tiers",
    "chr22.parquet · …": "chr22.parquet …",
    "manifest.json · provenance": "manifest.json with provenance",
    "Probe positions · chr22:16,050,075 and chr22:16,050,213":
        "Probe positions chr22:16,050,075 and chr22:16,050,213",
    "Illustrative rows · page ranges → exact positions → allele matching":
        "Illustrative rows: page ranges → exact positions → allele matching",
}
PUSHDOWN_NOTE = (
    "PUSH-DOWN: selected columns · genomic regions · LIMIT",
    "Needed HGVS / known-variant / CSQ / plugin work only",
)
DESCRIPTION = (
    "Figure 1 proposal. (A) Ensembl VEP and three reimplementations compared. "
    "(B) Interactive analysis of the GIAB HG002 genome with vepyr and polars-bio, "
    "with code fragments and the sources read from their URLs. (C1-C4) Annotation "
    "architecture, cache build, cache anatomy and cache lookup. (D) Query pushdown "
    "in vepyr 0.10.0. (E) Performance."
)


def undot(text: str) -> str:
    return REWRITE.get(text, text.replace(" · ", ", "))


def clean(scene: fig.Scene) -> None:
    for e in scene.elements:
        if not isinstance(e, (fig.Label, fig.Box)):
            continue
        if tuple(e.lines) == PUSHDOWN_NOTE:
            e.lines = ["Pushdown of columns, regions and row limits (D)"]
            continue
        lines = [undot(line) for line in e.lines]
        if isinstance(e, fig.Label) and len(lines) == 1 and lines[0] in RELABEL:
            lines = [RELABEL[lines[0]]]
        e.lines = [re.sub(r"\bA([1-4])\b", r"C\1", line) for line in lines]


def stretched(scene: fig.Scene, k: float) -> fig.Scene:
    """Widen a panel horizontally; text keeps its size."""
    out = copy.deepcopy(scene)
    for e in out.elements:
        if isinstance(e, fig.Edge):
            e.x1 *= k
            e.x2 *= k
            e.via = tuple((x * k, y) for x, y in e.via)
        else:
            e.x *= k
            e.width *= k
    return out


def separator(s: fig.Scene, y: float) -> None:
    s.edge(40, y, 40 + CONTENT, y, arrow=False, color=LIGHT, width=1)


def table_panel() -> tuple[fig.Scene, float]:
    svg = TABLE_SVG.read_text()
    w, h = map(float, re.search(r'viewBox="0 0 ([\d.]+) ([\d.]+)"', svg).groups())
    s = fig.Scene()
    s._counter = 80000
    s.label(0, 14, 64, 44, "A", font_size=31, weight=700)
    s.label(74, 15, 1500, 42, "Ensembl VEP and its reimplementations", font_size=29,
            weight=700)
    s.label(CONTENT - 900, 18, 900, 36,
            "release and git commit under each name, details in Table Sx",
            font_size=20, color=GREY, align="right")
    height = CONTENT * h / w
    uri = "data:image/svg+xml;base64," + base64.b64encode(svg.encode("utf-8")).decode("ascii")
    s.image(0, 60, CONTENT, height, uri)
    return s, 60 + height


def architecture_block(performance_uri: str) -> fig.Scene:
    """C1-C3 of the previous figure, leaders included."""
    s = workflow.full_figure(performance_uri)

    def bottom(e) -> float:
        if isinstance(e, fig.Edge):
            return max(y for _, y in ((e.x1, e.y1), *e.via, (e.x2, e.y2)))
        return e.y + e.height

    s.elements = [e for e in s.elements if bottom(e) <= BLOCK_HEIGHT]
    return s


def performance_panel(performance_svg: str, width: float) -> tuple[fig.Scene, float]:
    svg = performance_svg.replace(" · ", ", ")
    s = fig.Scene()
    s._counter = 90000
    s.label(0, 14, 64, 44, "E", font_size=31, weight=700)
    s.label(74, 15, width - 74, 42, "Performance", font_size=29, weight=700)
    height = width * build_performance.HEIGHT / build_performance.WIDTH
    uri = "data:image/svg+xml;base64," + base64.b64encode(svg.encode("utf-8")).decode("ascii")
    s.image(0, 70, width, height, uri)
    return s, 70 + height


def full_figure(performance_svg: str) -> tuple[fig.Scene, int]:
    perf_uri = "data:image/svg+xml;base64," + base64.b64encode(
        performance_svg.encode("utf-8")).decode("ascii")
    s = fig.Scene()

    table, table_h = table_panel()
    s.elements.extend(simplified.translated(table, 40, 14).elements)
    y = 14 + table_h + 14
    separator(s, y)

    y += 10
    s.elements.extend(simplified.translated(tiered.panel_b(), 40, y).elements)
    y += tiered.B_HEIGHT + 8
    separator(s, y)

    # C1-C3 keep their geometry (x 40-2360); C4 is a column to their right.
    y -= 6
    s.elements.extend(simplified.translated(architecture_block(perf_uri), 0, y).elements)
    s.elements.extend(simplified.translated(workflow.lookup_column("C4"), 40 + CONTENT - 520,
                                            y + 14).elements)
    y += BLOCK_HEIGHT + 14

    # D and E share the last row, whose height E sets.
    perf_w = round((build_pushdown.HEIGHT - 70) * build_performance.WIDTH
                   / build_performance.HEIGHT)
    pushdown = build_pushdown.panel("D", CONTENT - perf_w - 80)
    s.elements.extend(simplified.translated(pushdown, 40, y).elements)
    perf, perf_h = performance_panel(performance_svg, perf_w)
    s.elements.extend(simplified.translated(perf, 40 + CONTENT - perf_w, y).elements)
    lookup_h = build_pushdown.HEIGHT
    clean(s)
    return s, int(y + max(perf_h, lookup_h) + 10)


def main() -> None:
    """Report the canvas; the figure itself is written by build_tikz.py."""
    full, height = full_figure(build_performance.build_svg())
    print(f"canvas {WIDTH} x {height} (height/width {height / WIDTH:.2f}); "
          f"at 488.5 pt width: {488.5 * height / WIDTH:.0f} pt high")


if __name__ == "__main__":
    main()
