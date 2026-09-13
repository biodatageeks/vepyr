#!/usr/bin/env python3
"""Generate a monochrome cache-lookup proposal and its Figure 1 context.

The cache diagrams are schematic; measured counts are transcribed from Marek's
image-panelC.png. No benchmark is run and no speedup in runtime is inferred.
Uses the existing Figure 1 drawing primitives; outputs are separate drafts.
"""

from __future__ import annotations

import argparse
import copy
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path


HERE = Path(__file__).resolve().parent
DEFAULT_FIGURE_DIR = (
    HERE if (HERE / "build_figure.py").exists()
    else Path("/Users/tgambin/workspace/vepyr/paper/figure1")
)
sys.path.insert(0, str(DEFAULT_FIGURE_DIR))
import build_figure as fig


WIDTH, HEIGHT = 1260, 630
INK = "#111111"
GREY = "#777777"
LIGHT = "#dedede"
PAGE_COUNTS = (920, 220)
BASELINE_COMMON = (1, 18, 35, 52, 69, 86)
BASELINE_RARE = (44, 92)
GROUPED_COMMON = (1, 2, 3, 5, 7, 8)
GROUPED_RARE = (43, 89)


def panel(code: str = "A3") -> fig.Scene:
    s = fig.Scene()
    s._counter = 20000
    s.label(30, 14, 66, 44, code, font_size=33, weight=700)
    s.label(104, 15, 1115, 42, "Frequency-aware cache lookup", font_size=31, weight=700)

    # One row summarizes lookup and downstream allele matching. The miniature
    # input is illustrative, and does not encode measured common/rare ratios.
    s.box(34, 90, 214, 88, "", stroke_width=1.8)
    s.label(38, 97, 206, 34, "Input variants", font_size=23, weight=700, align="center")
    for i in range(8):
        s.box(62 + i * 21, 146, 12, 12, "", fill=INK if i < 6 else "white", stroke_width=1.5)
    s.edge(250, 134, 345, 134, width=2)
    s.box(347, 90, 513, 88, "Retrieve records and match alleles",
          font_size=26, stroke_width=1.8)
    s.edge(862, 134, 959, 134, width=2)
    s.box(961, 90, 265, 88, "Known-variant\nannotations", font_size=25, stroke_width=1.8)

    s.edge(34, 207, 1226, 207, arrow=False, color=LIGHT, width=1)
    s.label(30, 220, 778, 36, "Same cache records, different layouts", font_size=26, weight=700)
    s.label(875, 220, 350, 36, "Pages read", font_size=26, weight=700)
    s.label(875, 256, 350, 29, "Position column only", font_size=21, color=GREY)
    s.edge(839, 228, 839, 561, arrow=False, color=LIGHT, width=1)

    def strip(y: int, common: tuple[int, ...], rare: tuple[int, ...]) -> None:
        # 12 pages × 8 illustrative rows. Thick outlines encode pages accessed,
        # black/open markers encode common/rare hits independently of tier.
        x, page_width, slots = 34, 64, 8
        hit_pages = {i // slots for i in (*common, *rare)}
        for p in range(12):
            px = x + p * page_width
            selected = p in hit_pages
            s.box(px, y, page_width, 44, "", fill="#f0f0f0" if selected else "white",
                  stroke=GREY, stroke_width=0.8)
            for k in range(1, slots):
                s.edge(px + 8 * k, y + 3, px + 8 * k, y + 41,
                       arrow=False, color="#d4d4d4", width=0.6)
        for i in common:
            s.box(x + i * 8 + 1, y + 10, 6, 24, "", fill=INK, stroke_width=0.7)
        for i in rare:
            s.box(x + i * 8 + 1, y + 10, 6, 24, "", fill="white", stroke_width=1.4)
        # Outline last so the read-state boundary stays unambiguous.
        for p in sorted(hit_pages):
            s.box(x + p * page_width, y, page_width, 44, "", fill="none", stroke_width=2.6)

    s.label(30, 262, 782, 35, "Position-sorted", font_size=25, weight=700)
    strip(305, BASELINE_COMMON, BASELINE_RARE)
    s.label(30, 371, 782, 35, "Frequency-grouped", font_size=25, weight=700)
    strip(414, GROUPED_COMMON, GROUPED_RARE)

    # Tier seam deliberately falls inside the second page. The warm width is
    # illustrative (10/96 rows), not the measured 5.4% share in Marek's example.
    seam = 34 + 10 * 8
    for x0, x1 in ((34, seam), (seam, 802)):
        s.edge(x0, 465, x1, 465, arrow=False, width=1.2)
        s.edge(x0, 461, x0, 469, arrow=False, width=1.2)
        s.edge(x1, 461, x1, 469, arrow=False, width=1.2)
    s.label(22, 470, 110, 30, "warm", font_size=23, weight=700, align="center")
    s.label(350, 470, 210, 30, "cold", font_size=23, align="center")
    s.label(550, 470, 253, 30, "Both tiers queried", font_size=21, color=GREY, align="right")

    s.box(36, 530, 12, 16, "", fill=INK, stroke_width=1.2)
    s.label(54, 521, 178, 33, "common hit", font_size=22)
    s.box(251, 530, 12, 16, "", fill="white", stroke_width=1.6)
    s.label(269, 521, 141, 33, "rare hit", font_size=22)
    s.box(421, 529, 28, 18, "", fill="none", stroke_width=2.6)
    s.label(455, 521, 172, 33, "page read", font_size=22)
    s.label(655, 521, 149, 33, "schematic", font_size=20, color=GREY, align="right")

    # Linear zero-based chart. Counts refer only to the locate pass over start,
    # not payload pages, bytes, I/O calls, or end-to-end annotation wall time.
    chart_x, chart_width = 880, 282
    for y, count, fill in ((312, PAGE_COUNTS[0], "#b6b6b6"), (421, PAGE_COUNTS[1], INK)):
        width = chart_width * count / 1000
        s.box(chart_x, y, width, 30, "", fill=fill, stroke_width=0)
        s.label(chart_x + width + 7, y - 5, 68, 39, f"{count:,}", font_size=25, weight=700)
    s.edge(chart_x, 472, chart_x + chart_width, 472, arrow=False, color=GREY, width=1)
    for tick in (0, 500, 1000):
        tx = chart_x + chart_width * tick / 1000
        s.edge(tx, 472, tx, 478, arrow=False, color=GREY, width=1)
        s.label(tx - 35, 481, 70, 28, f"{tick:,}", font_size=19, color=GREY, align="center")
    reduction = 100 * (1 - PAGE_COUNTS[1] / PAGE_COUNTS[0])
    s.label(875, 515, 350, 45, f"{reduction:.0f}% fewer pages", font_size=29, weight=700)

    s.edge(34, 580, 1226, 580, arrow=False, color=LIGHT, width=1)
    s.label(30, 587, 1196, 30,
            "HG002 chr22 · one 5,000-record buffer · VEP 116 merged cache",
            font_size=22, color=GREY)
    return s


def translated(scene: fig.Scene, dx: int, dy: int) -> fig.Scene:
    result = copy.deepcopy(scene)
    for e in result.elements:
        if isinstance(e, fig.Edge):
            e.x1 += dx
            e.x2 += dx
            e.y1 += dy
            e.y2 += dy
            e.via = tuple((x + dx, y + dy) for x, y in e.via)
        else:
            e.x += dx
            e.y += dy
    return result


def save(scene: fig.Scene, output: Path, width: int, height: int, title: str) -> None:
    fig.validate_scene(scene)
    root = ET.fromstring(fig.svg_preview(scene, height=height))
    root.set("width", str(width))
    root.set("height", str(height))
    root.set("viewBox", f"0 0 {width} {height}")
    root.set("role", "img")
    ns = "{http://www.w3.org/2000/svg}"
    ET.SubElement(root, ns + "title").text = title
    ET.SubElement(root, ns + "desc").text = (
        "A separate Figure 1 proposal. Cache diagrams are schematic; the chart "
        "reports 920 versus 220 position pages for one buffer, transcribed from "
        "Marek's image-panelC.png. Both warm and cold tiers remain queryable."
    )
    ET.register_namespace("", "http://www.w3.org/2000/svg")
    output.with_suffix(".svg").write_text(ET.tostring(root, encoding="unicode") + "\n")
    drawio = ET.fromstring(fig.drawio_xml(scene))
    model = drawio.find(".//mxGraphModel")
    model.set("pageWidth", str(width))
    model.set("pageHeight", str(height))
    drawio.find("diagram").set("name", title)
    output.with_suffix(".drawio").write_text(ET.tostring(drawio, encoding="unicode") + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=HERE)
    parser.add_argument("--render", action="store_true", help="Render PNG previews with Chrome")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    standalone = panel()
    save(standalone, args.output_dir / "a3-cache-simplified-proposal", WIDTH, HEIGHT,
         "A3 — Frequency-aware cache lookup")

    # Reuse the frozen performance SVG; never recalculate unrelated panels.
    original = ET.parse(DEFAULT_FIGURE_DIR / "figure1.svg").getroot()
    performance = original.find("{http://www.w3.org/2000/svg}image")
    if performance is None:
        raise ValueError("Figure 1 is missing the embedded performance panel")
    full = fig.build_scene(performance.attrib["href"])
    start = next(i for i, e in enumerate(full.elements) if e.id == "shard-zoom-frame") + 1
    end = next(i for i, e in enumerate(full.elements)
               if isinstance(e, fig.Label) and e.lines == ["B"] and e.y == 1425)
    full.elements[start:end] = translated(panel("A3"), 1100, 750).elements
    save(full, args.output_dir / "figure1-cache-simplified-proposal", fig.WIDTH, fig.HEIGHT,
         "Figure 1 — simplified frequency-aware cache proposal")
    print(f"Generated standalone and full-figure SVG / Drawio in {args.output_dir}")
    if args.render:
        chrome = shutil.which("google-chrome") or shutil.which("chromium") or \
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        for name, width, height in (
            ("a3-cache-simplified-proposal", WIDTH, HEIGHT),
            ("figure1-cache-simplified-proposal", fig.WIDTH, fig.HEIGHT),
        ):
            svg = (args.output_dir / name).with_suffix(".svg").resolve()
            png = svg.with_suffix(".png")
            before = png.stat().st_mtime_ns if png.exists() else 0
            with tempfile.TemporaryDirectory(prefix="vepyr-a3-preview-") as profile:
                command = [chrome, "--headless=new", "--disable-gpu", "--no-sandbox",
                           "--allow-file-access-from-files", "--hide-scrollbars",
                           "--disable-background-networking", "--disable-component-update",
                           f"--user-data-dir={profile}", f"--window-size={width},{height}",
                           f"--screenshot={png}", svg.as_uri()]
                proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    _, err = proc.communicate(timeout=12)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    try:
                        _, err = proc.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        _, err = proc.communicate(timeout=5)
                if not png.exists() or png.stat().st_mtime_ns <= before:
                    raise RuntimeError(f"Render failed for {name}: {err.decode(errors='replace')[-1000:]}")
            print(f"Rendered {png.name}", flush=True)


if __name__ == "__main__":
    main()
