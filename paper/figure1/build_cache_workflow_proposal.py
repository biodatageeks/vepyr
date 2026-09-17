#!/usr/bin/env python3
"""Build the A1–A4 cache workflow proposal with the existing Performance panel.

Native SVG / Drawio diagrams; PNG is an optional Chrome render. Lookup rows
are illustrative. The page-count comparison is inherited from Marek's figure.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

import build_figure as fig
import build_cache_simplified_proposal as previous


HERE = Path(__file__).resolve().parent
LOOKUP_WIDTH, LOOKUP_HEIGHT = 1450, 740
INK, GREY, LIGHT = "#111111", "#555555", "#e6e6e6"


def indexed_build_flow() -> fig.Scene:
    """A2 builders and their output; the index inset is scoped to lookup."""
    s = fig.Scene()
    s._counter = 40000
    raw = s.box(66, 832, 224, 74, "Ensembl VEP cache\nStorable / Sereal",
                font_size=19)
    build = s.box(316, 832, 246, 74, "build_cache()\nEnsembl conversion",
                  font_size=19)
    plugin_input = s.box(66, 968, 224, 98,
                         "Plugin source files\nTSV / CSV / Parquet\nVCF / BED (4 columns)",
                         font_size=17)
    plugin_build = s.box(316, 940, 246, 126, "")
    s.box(316, 940, 246, 34, "Plugin build · TOML", font_size=18,
          fill=INK, font_color="white")
    s.label(324, 982, 230, 32, "build_plugin_cache()", font_size=18,
            weight=700, align="center")
    s.label(324, 1020, 230, 34, "SQL + field mapping", font_size=18, align="center")

    built = s.box(610, 818, 352, 278, "", stroke_width=2)
    s.label(623, 825, 326, 31, "Parquet shards", font_size=23,
            weight=700, align="center")
    s.label(623, 862, 326, 28, "Per chromosome · separate schemas",
            font_size=17, align="center", color=GREY)
    s.edge(623, 899, 949, 899, arrow=False, color=LIGHT, width=1)
    s.label(623, 905, 326, 29, "Lookup: variation / plugins",
            font_size=19, weight=700, align="center")
    # Four schematic data pages. The index rows summarize per-column metadata,
    # not a separate sidecar or a shared physical page boundary across columns.
    for i in range(4):
        s.box(653 + i * 65, 937, 50, 30, f"P{i}", font_size=15,
              fill="#f3f3f3", bold_first=False, stroke_width=1)
    s.label(623, 967, 326, 28, "Small pages · sorted within tiers",
            font_size=17, align="center", color=GREY)
    s.label(623, 995, 127, 44, "ColumnIndex", font_size=18, weight=700)
    s.label(751, 995, 198, 44, "position min–max\nper data page", font_size=18)
    s.label(623, 1041, 127, 49, "OffsetIndex", font_size=18, weight=700)
    s.label(751, 1041, 204, 49, "row → page location", font_size=18)
    s.edge(290, 869, 316, 869, source=raw, target=build)
    s.edge(290, 1017, 316, 1017, source=plugin_input, target=plugin_build)
    s.edge(562, 869, 610, 869, source=build, target=built)
    s.edge(562, 1017, 610, 1017, source=plugin_build, target=built)
    return s


def cache_build_panel(full: fig.Scene) -> fig.Scene:
    """Export A2 with a small margin and without incoming zoom leaders."""
    start = next(i for i, e in enumerate(full.elements) if e.id == "cache-zoom-frame")
    end = next(i for i, e in enumerate(full.elements)
               if isinstance(e, fig.Edge) and e.target is not None
               and e.target.id == "shard-zoom-frame")
    panel = fig.Scene()
    panel.elements = full.elements[start:end]
    return previous.translated(panel, -28, -738)


def anatomy() -> fig.Scene:
    s = previous.panel()
    # Keep the layout comparison and measured chart; move retrieval to A4.
    s.elements = [e for e in s.elements
                  if (e.y1 if isinstance(e, fig.Edge) else e.y) < 65
                  or (e.y1 if isinstance(e, fig.Edge) else e.y) >= 207]
    for e in s.elements:
        if isinstance(e, fig.Label) and e.lines == ["Frequency-aware cache lookup"]:
            e.lines = ["Cache anatomy"]
    s.label(30, 65, 1120, 34, "variation/chr22.parquet", font_size=23, mono=True)
    s.box(1180, 19, 48, 34, "ii", fill=INK, font_color="white", font_size=21)
    for x, w, text in (
        (34, 330, "Keys\nposition · alleles"),
        (384, 430, "Payload\nIDs · frequencies · clinical"),
        (834, 392, "Page index\nposition ranges · row offsets"),
    ):
        s.box(x, 114, w, 76, text, font_size=23, stroke_width=1.5)
    return s


def lookup() -> fig.Scene:
    s = fig.Scene()
    s._counter = 30000
    s.label(0, 14, 68, 44, "A4", font_size=31, weight=700)
    s.label(74, 15, 1365, 42, "Cache lookup", font_size=29, weight=700)
    s.label(0, 75, 1440, 44,
            "Probe positions · chr22:16,050,075 and chr22:16,050,213",
            font_size=24)
    cols = (0, 500, 1000)
    for x, text in zip(cols, (
        "1  Page candidates\nPage-index metadata",
        "2  Locate row offsets\nRead position column",
        "3  Take\nOffsetIndex + selected rows",
    )):
        s.box(x, 154, 425, 78, text, font_size=23, stroke_width=1.8)
    s.edge(431, 193, 489, 193)
    s.edge(931, 193, 989, 193)

    # Toy page ranges are within independently sorted warm/cold runs. A page
    # number is a schematic identifier, not a measurement from Marek's cache.
    s.label(4, 253, 63, 35, "Page", font_size=21, weight=700)
    s.label(83, 253, 340, 35, "Min–max position", font_size=21, weight=700)
    s.edge(0, 290, 425, 290, arrow=False, width=1.5)
    for i, (page, limits, selected) in enumerate((
        ("P0", "16,050,001–16,050,088", True),
        ("P1", "16,051,000–16,051,999", False),
        ("P2", "16,050,201–16,050,240", True),
        ("P3", "16,053,000–16,053,999", False),
    )):
        y = 300 + i * 56
        s.box(0, y, 425, 48, "", fill="#f3f3f3" if selected else "white",
              stroke=INK if selected else LIGHT, stroke_width=2 if selected else 0.8)
        s.label(7, y + 6, 63, 34, page, font_size=21, weight=700 if selected else 400)
        s.label(83, y + 6, 337, 34, limits, font_size=21)
    s.label(0, 535, 425, 36, "Warm + cold runs queried", font_size=22, color=GREY)
    s.box(0, 597, 425, 66, "Selected pages\nP0, P2", font_size=23)

    s.label(504, 253, 145, 35, "File row", font_size=21, weight=700)
    s.label(663, 253, 258, 35, "Position", font_size=21, weight=700)
    s.edge(500, 290, 925, 290, arrow=False, width=1.5)
    for i, (row, pos, selected) in enumerate((
        (1000, "16,050,001", False),
        (1001, "16,050,075", True),
        (1002, "16,050,075", True),
        (8500, "16,050,201", False),
        (8501, "16,050,213", True),
        (8502, "16,050,240", False),
    )):
        y = 300 + i * 43
        if selected:
            s.box(500, y, 425, 40, "", fill=LIGHT, stroke_width=0)
        s.label(507, y + 3, 145, 34, str(row), font_size=22, weight=700 if selected else 400)
        s.label(663, y + 3, 258, 34, pos, font_size=22, weight=700 if selected else 400)
    s.box(500, 597, 425, 66, "File-row offsets\n1001, 1002, 8501", font_size=23)

    # These are example payload values, not factual rsIDs or measured AFs.
    for dx, w, label in ((4, 94, "Row"), (108, 111, "Alleles"),
                         (231, 80, "ID"), (320, 99, "AF")):
        s.label(1000 + dx, 253, w, 35, label, font_size=21, weight=700)
    s.edge(1000, 290, 1425, 290, arrow=False, width=1.5)
    for i, values in enumerate((("1001", "A/G", "v1", "0.12"),
                                ("1002", "A/T", "v2", "0.002"),
                                ("8501", "C/T", "v3", "0.004"))):
        y = 300 + i * 43
        s.box(1000, y, 425, 40, "", fill=LIGHT, stroke_width=0)
        for dx, w, text in zip((7, 108, 231, 320), (94, 111, 80, 99), values):
            s.label(1000 + dx, y + 3, w, 34, text, font_size=22)
    s.label(1000, 428, 425, 28, "Selected payload columns", font_size=20, color=GREY)
    s.label(1000, 454, 425, 70,
            "One position can yield\nseveral cached alleles.", font_size=23, color=GREY)
    s.edge(1212, 535, 1212, 586)
    s.box(1000, 597, 425, 66, "Allele matching in A1\nAttach to input variants", font_size=23)
    s.edge(0, 691, 1425, 691, arrow=False, color=LIGHT, width=1)
    s.label(0, 697, 1440, 34,
            "Illustrative rows · page ranges → exact positions → allele matching",
            font_size=22, color=GREY)
    return s


def full_figure(performance_uri: str) -> fig.Scene:
    s = fig.build_scene(performance_uri)
    cut = next(i for i, e in enumerate(s.elements) if e.id == "shard-zoom-frame") + 1
    # Retain A1, A2 and the zoom frames; replace A3 and all lower panels.
    s.elements = s.elements[:cut]
    build_start = next(i for i, e in enumerate(s.elements)
                       if isinstance(e, fig.Box) and e.lines[0] == "Ensembl VEP cache")
    build_end = next(i for i, e in enumerate(s.elements)
                     if isinstance(e, fig.Label) and e.lines == ["cache_dir/  [release + source]"])
    s.elements[build_start:build_end] = indexed_build_flow().elements
    replacements = {
        "Cache construction": "Cache build",
        "Input VCF\nBGZF + index": "Input VCF\nNormalized",
        "Polars LazyFrame\nlazy IO source\nexecutes on demand":
            "Polars LazyFrame\nTyped variant rows\nAligned lists",
        "AF / consequence predicates:\nfilter annotated batches":
            "AF / consequence filters\non annotated batches",
        "Annotated VCF\nCSQ + INFO / FORMAT":
            "Annotated VCF\nOriginal INFO/FORMAT\n+ CSQ",
        "Reference workflow: VEP → VCF → filter_vep\nSolid arrows: data. Dashed return: query demand.":
            "sink_* streams · collect() materializes\nPer-consequence filters: aligned list handling",
        "chr1.parquet": "chr22.parquet",
        "chr1.parquet · chr2.parquet · …\nmanifest.json · provenance":
            "chr22.parquet · …\nmanifest.json · provenance",
    }
    for e in s.elements:
        if isinstance(e, (fig.Label, fig.Box)):
            text = "\n".join(e.lines)
            if text in replacements:
                e.lines = replacements[text].split("\n")
            if text == "Annotated VCF\nCSQ + INFO / FORMAT":
                e.font_size = 18
    s.label(40, 424, 250, 76,
            "BGZF + TBI/CSI\nMatch assembly,\nFASTA and cache release",
            font_size=17, color=GREY)
    s.elements.extend(previous.translated(anatomy(), 1100, 750).elements)
    s.elements.extend(previous.translated(lookup(), 40, 1425).elements)

    # Keep the frozen vector chart intact, including pending-data placeholders.
    # Panel C retains its letter for comparison with the preceding proposal.
    s.label(1540, 1439, 64, 44, "C", font_size=31, weight=700)
    s.label(1610, 1440, 750, 42, "Performance", font_size=29, weight=700)
    s.image(1540, 1510, 820, 820 * 665 / 1010, performance_uri)
    s.label(1540, 2070, 820, 70,
            "Preliminary single runs; paired repeats\nand filter timings remain pending.",
            font_size=20, color=GREY)
    return s


def save(s: fig.Scene, path: Path, width: int, height: int, title: str) -> None:
    fig.validate_scene(s)
    for e in s.elements:
        if not isinstance(e, fig.Edge):
            if e.x < 0 or e.y < 0 or e.x + e.width > width or e.y + e.height > height:
                raise ValueError(f"Element exceeds export canvas: {e.id}")
    root = ET.fromstring(fig.svg_preview(s, height=height))
    root.set("width", str(width))
    root.set("viewBox", f"0 0 {width} {height}")
    root.set("role", "img")
    ns = "{http://www.w3.org/2000/svg}"
    ET.SubElement(root, ns + "title").text = title
    ET.SubElement(root, ns + "desc").text = (
        "Cache build, cache anatomy and cache lookup proposal, using chromosome 22. "
        "Lookup shards include ColumnIndex value ranges and OffsetIndex page locations. "
        "Lookup example rows and payload values are illustrative. The anatomy chart "
        "reports 920 versus 220 position pages for one buffer from Marek's source. "
        "Warm and cold are on-disk frequency tiers. Performance data are unchanged."
    )
    ET.register_namespace("", "http://www.w3.org/2000/svg")
    path.with_suffix(".svg").write_text(ET.tostring(root, encoding="unicode") + "\n")
    drawio = ET.fromstring(fig.drawio_xml(s, height=height))
    drawio.find(".//mxGraphModel").set("pageWidth", str(width))
    drawio.find("diagram").set("name", title)
    path.with_suffix(".drawio").write_text(ET.tostring(drawio, encoding="unicode") + "\n")


def render(path: Path, width: int, height: int) -> None:
    chrome = shutil.which("google-chrome") or shutil.which("chromium") or \
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    png = path.with_suffix(".png").resolve()
    before = png.stat().st_mtime_ns if png.exists() else 0
    with tempfile.TemporaryDirectory(prefix="vepyr-workflow-preview-") as profile:
        command = [chrome, "--headless=new", "--disable-gpu", "--no-sandbox",
                   "--allow-file-access-from-files", "--hide-scrollbars",
                   "--disable-background-networking", "--disable-component-update",
                   f"--user-data-dir={profile}", f"--window-size={width},{height}",
                   f"--screenshot={png}", path.with_suffix(".svg").resolve().as_uri()]
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
            raise RuntimeError(f"Render failed: {err.decode(errors='replace')[-1000:]}")
    print(f"Rendered {png.name}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=HERE)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    original = ET.parse(HERE / "figure1.svg").getroot()
    performance = original.find("{http://www.w3.org/2000/svg}image")
    if performance is None:
        raise ValueError("Missing original Performance SVG")
    full = full_figure(performance.attrib["href"])
    exports = (
        ("a2-cache-build-proposal", cache_build_panel(full), 974, 654,
         "A2 — Cache build with Parquet page indexes"),
        ("a3-cache-anatomy-proposal", anatomy(), 1260, 630, "A3 — Cache anatomy"),
        ("a4-cache-lookup-proposal", lookup(), LOOKUP_WIDTH, LOOKUP_HEIGHT, "A4 — Cache lookup"),
        ("figure1-cache-workflow-proposal", full,
         2400, 2200, "Figure 1 — Cache workflow proposal"),
    )
    for name, scene, width, height, title in exports:
        path = args.output_dir / name
        save(scene, path, width, height, title)
        print(f"Generated {name}.svg / .drawio", flush=True)
        if args.render:
            render(path, width, height)


if __name__ == "__main__":
    main()
