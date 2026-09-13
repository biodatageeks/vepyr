#!/usr/bin/env python3
"""Build an alternative A3 panel without replacing the accepted Figure 1.

Visual reference: Apache DataFusion's 2025-03-20 Parquet pruning diagram.
The stages below describe vepyr's point-lookup reader, not a generic SQL scan.
Run from any directory: python3 -B paper/figure1/build_a3_proposal.py
"""

from __future__ import annotations

import copy
import xml.etree.ElementTree as ET
from pathlib import Path

import build_figure as fig


HERE = Path(__file__).resolve().parent
INK = fig.INK
PALE = "#b8b8b8"
MUTED = fig.MUTED


def proposal_panel() -> fig.Scene:
    s = fig.Scene()
    # Separate IDs permit insertion into the existing editable scene.
    s._counter = 10000
    s.label(1124, 764, 64, 44, "A3", font_size=31, weight=700)
    s.label(1194, 765, 1080, 42, "Selective reads · variation/chr1.parquet",
            font_size=27, weight=700)
    s.box(2286, 768, 48, 34, "ii", fill=INK, font_color="white", font_size=21)

    # Demand enters the appropriate stage; these are not extra data streams.
    s.label(1126, 825, 218, 48, "Row groups → columns\n→ pages (schematic)", font_size=18)
    s.box(1380, 827, 336, 40, "Requested chr1 positions", font_size=19,
          fill=fig.SOFT, bold_first=False)
    s.box(1858, 827, 455, 40, "Required fields + matching dependencies", font_size=18,
          fill=fig.SOFT, bold_first=False)
    s.edge(1481, 867, 1481, 895, dashed=True, width=1.5)
    s.edge(1981, 867, 1981, 895, dashed=True, width=1.5)

    xs = [1126, 1376, 1626, 1876, 2126]
    titles = ["1  Load metadata", "2  Resolve pages", "3  Locate rows",
              "4  Take payload", "5  Return rows"]
    for x, title in zip(xs, titles):
        s.box(x, 900, 210, 47, title, font_size=19, fill=INK,
              font_color="white", stroke_width=1)
    for x in xs[:-1]:
        s.edge(x + 210, 923, x + 248, 923, width=1.8)

    # Same shard at each step. The diagrams show access states, not new files.
    # K = start; P = representative required payload leaf; X = unused leaf.
    # Leaf pages deliberately have different boundaries.
    partitions = [[(0, 18), (22, 18), (44, 18)],
                  [(0, 29), (33, 29)],
                  [(0, 14), (18, 23), (45, 17)]]
    for stage, x in enumerate(xs[:4]):
        s.box(x, 972, 210, 225, "", stroke_width=1.5)
        for column, name in enumerate(["K", "P", "X"]):
            s.label(x + 35 + column * 54, 976, 46, 25, name,
                    font_size=17, mono=True, align="center", weight=700)
        for group in range(2):
            gy = 1010 + group * 79
            s.box(x + 8, gy - 5, 194, 73, "", stroke="#999999", stroke_width=1)
            s.label(x + 10, gy + 17, 22, 22, str(group), font_size=14,
                    color=MUTED, align="center")
            for column, pages in enumerate(partitions):
                px = x + 38 + column * 54
                s.box(px - 3, gy - 1, 48, 64, "", stroke=PALE, stroke_width=1)
                for page, (dy, height) in enumerate(pages):
                    key_selected = column == 0 and page == (1 if group == 0 else 0)
                    payload_selected = column == 1 and page == 0
                    planned = stage == 1 and key_selected
                    read = (stage == 2 and key_selected) or (
                        stage == 3 and (key_selected or payload_selected))
                    s.box(px, gy + dy, 42, height, "",
                          fill=INK if read else "white",
                          stroke=INK if planned or read else PALE,
                          stroke_width=2 if planned else 1,
                          dashed=not (planned or read))
                    if read:
                        # A row inside a decoded page, not row-sized disk I/O.
                        s.edge(px + 5, gy + dy + height / 2,
                               px + 37, gy + dy + height / 2,
                               arrow=False, color="white", width=1.6)
        s.box(x + 8, 1172, 194, 20,
              "Footer + page indexes" if stage == 0 else "…",
              fill=INK if stage == 0 else "white",
              font_color="white" if stage == 0 else MUTED,
              stroke=INK if stage == 0 else PALE,
              font_size=13, bold_first=False, stroke_width=1)

    # This Arrow batch contains cache rows; allele matching is downstream.
    x = xs[4]
    s.label(x, 974, 210, 30, "Arrow cache rows", font_size=20,
            weight=700, align="center")
    s.box(x + 6, 1013, 198, 99, "", stroke_width=1.5)
    s.box(x + 6, 1013, 198, 30, "", fill=fig.SOFT, stroke_width=1)
    s.label(x + 8, 1013, 73, 30, "start", font_size=15,
            mono=True, align="center")
    s.label(x + 85, 1013, 117, 30, "payload", font_size=15,
            mono=True, align="center")
    s.edge(x + 83, 1013, x + 83, 1112, arrow=False, width=1)
    for yy in [1061, 1095]:
        s.edge(x + 18, yy, x + 68, yy, arrow=False, width=3)
        s.edge(x + 96, yy, x + 188, yy, arrow=False, width=3)
    s.edge(x + 105, 1113, x + 105, 1145, width=1.8)
    s.box(x + 6, 1146, 198, 51, "Allele match\n+ annotation",
          font_size=17, bold_first=False)

    descriptions = ["Schema + page indexes\nBuild PageDir",
                    "PageDir: start min–max\nCandidate row ranges",
                    "Read start pages only\nExact position offsets",
                    "Read required pages\nSelect located rows",
                    "Cache lookup result\nfeeds the A1 engine"]
    for x, description in zip(xs, descriptions):
        s.label(x - 4, 1204, 220, 56, description, font_size=16,
                color=MUTED, align="center")

    s.edge(1126, 1270, 2336, 1270, arrow=False, color=PALE, width=1)
    s.label(1126, 1279, 540, 30, "K: start  |  P: required payload  |  X: other fields",
            font_size=17)
    s.box(1692, 1287, 23, 13, "", stroke=PALE, dashed=True, stroke_width=1)
    s.label(1721, 1279, 130, 30, "not accessed", font_size=16)
    s.box(1858, 1287, 23, 13, "", stroke=INK, stroke_width=2)
    s.label(1887, 1279, 132, 30, "candidate", font_size=16)
    s.box(2018, 1287, 23, 13, "", fill=INK, stroke_width=1)
    s.label(2049, 1279, 286, 30, "read; white line = selected row", font_size=16)
    s.label(1126, 1317, 1209, 48,
            "Both warm/cold sorted runs are searched; tiers are not row groups.\n"
            "Schematic access states, not measured I/O; page boundaries differ across columns.",
            font_size=17, color=MUTED)
    return s


def save_svg(scene: fig.Scene, name: str, crop: bool = False) -> None:
    root = ET.fromstring(fig.svg_preview(scene))
    if crop:
        root.set("width", "1260")
        root.set("height", "630")
        root.set("viewBox", "1100 750 1260 630")
        # The white canvas must cover the crop's offset coordinates.
        background = list(root)[1]
        background.set("x", "1100")
        background.set("y", "750")
        background.set("width", "1260")
        background.set("height", "630")
    ET.register_namespace("", "http://www.w3.org/2000/svg")
    (HERE / name).write_text(ET.tostring(root, encoding="unicode") + "\n")


def main() -> None:
    # Retain the existing benchmark panel exactly, rather than rereading results.
    current = ET.parse(HERE / "figure1.svg").getroot()
    performance = current.find("{http://www.w3.org/2000/svg}image")
    if performance is None:
        raise RuntimeError("Existing Figure 1 has no performance image")
    scene = fig.build_scene(performance.attrib["href"])
    start = next(i for i, e in enumerate(scene.elements) if e.id == "shard-zoom-frame") + 1
    end = next(i for i, e in enumerate(scene.elements)
               if isinstance(e, fig.Label) and e.lines == ["B"] and e.y == 1425)
    panel = proposal_panel()
    scene.elements[start:end] = panel.elements
    fig.validate_scene(scene)
    save_svg(scene, "figure1-a3-proposal.svg")
    (HERE / "figure1-a3-proposal.drawio").write_text(fig.drawio_xml(scene))
    isolated = fig.Scene()
    isolated.elements = [copy.copy(scene.elements[start - 1]), *panel.elements]
    fig.validate_scene(isolated)
    save_svg(isolated, "a3-proposal.svg", crop=True)
    print("Generated A3 proposal SVG, full-figure SVG and editable draw.io; originals unchanged.")


if __name__ == "__main__":
    main()
