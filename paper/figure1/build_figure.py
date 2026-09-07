#!/usr/bin/env python3
"""Generate the editable draw.io source and an SVG preview for Figure 1.

All diagram elements are native mxGraph cells.  Panel C is embedded as the SVG
created by build_performance.py, keeping measured charts generated in Python.
"""

from __future__ import annotations

import argparse
import base64
import html
import shutil
import subprocess
import tempfile
import textwrap
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

import build_performance


HERE = Path(__file__).resolve().parent
DRAWIO = HERE / "figure1.drawio"
PREVIEW = HERE / "figure1.svg"

WIDTH = 2400
HEIGHT = 2200

INK = "#111111"
MUTED = "#555555"
LINE = "#888888"
SOFT = "#f3f3f3"


@dataclass
class Box:
    id: str
    x: float
    y: float
    width: float
    height: float
    lines: list[str]
    fill: str = "white"
    stroke: str = INK
    font_size: float = 20
    bold_first: bool = True
    align: str = "center"
    radius: float = 0
    dashed: bool = False
    mono: bool = False
    stroke_width: float = 1.5
    font_color: str = INK


@dataclass
class Label:
    id: str
    x: float
    y: float
    width: float
    height: float
    lines: list[str]
    font_size: float = 16
    weight: int = 400
    align: str = "left"
    color: str = INK
    mono: bool = False


@dataclass
class Edge:
    id: str
    x1: float
    y1: float
    x2: float
    y2: float
    color: str = INK
    width: float = 2
    arrow: bool = True
    dashed: bool = False
    via: tuple[tuple[float, float], ...] = ()
    source: Box | None = None
    target: Box | None = None


@dataclass
class EmbeddedImage:
    id: str
    x: float
    y: float
    width: float
    height: float
    data_uri: str


class Scene:
    def __init__(self) -> None:
        self.elements: list[Box | Label | Edge | EmbeddedImage] = []
        self._counter = 1

    def ident(self, prefix: str) -> str:
        value = f"{prefix}-{self._counter}"
        self._counter += 1
        return value

    def box(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        text: str,
        **kwargs: object,
    ) -> Box:
        box = Box(self.ident("box"), x, y, width, height, text.split("\n"), **kwargs)
        self.elements.append(box)
        return box

    def label(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        text: str,
        **kwargs: object,
    ) -> Label:
        label = Label(self.ident("text"), x, y, width, height, text.split("\n"), **kwargs)
        self.elements.append(label)
        return label

    def edge(self, x1: float, y1: float, x2: float, y2: float, **kwargs: object) -> Edge:
        edge = Edge(self.ident("edge"), x1, y1, x2, y2, **kwargs)
        self.elements.append(edge)
        return edge

    def image(self, x: float, y: float, width: float, height: float, data_uri: str) -> None:
        self.elements.append(
            EmbeddedImage(self.ident("image"), x, y, width, height, data_uri)
        )

    def panel(self, x: float, y: float, width: float, height: float, code: str, title: str) -> None:
        self.box(
            x,
            y,
            width,
            height,
            "",
            fill="none",
            stroke=LINE,
            radius=12,
            bold_first=False,
            stroke_width=1.6,
        )
        self.label(x + 16, y + 13, 62, 38, code, font_size=27, weight=700)
        self.label(x + 76, y + 14, width - 92, 36, title, font_size=22, weight=700)
        self.edge(x + 16, y + 58, x + width - 16, y + 58, color="#d5d8dc", width=1, arrow=False)


def build_scene(performance_uri: str) -> Scene:
    s = Scene()

    def heading(x, y, width, code, title):
        s.label(x, y, 64, 44, code, font_size=31, weight=700)
        s.label(x + 70, y + 1, width - 70, 42, title, font_size=27, weight=700)

    def tag(x, y, width, text, size=19):
        return s.box(x, y, width, 34, text, fill=INK, stroke=INK,
                     font_color="white", font_size=size, radius=2)

    def note(x, y, width, text, size=18, height=52):
        return s.label(x, y, width, height, text, font_size=size, color=MUTED)

    # A1: one horizontal annotation path; support data enter from below.
    heading(40, 28, 2320, "A1", "Annotation architecture")
    tag(40, 94, 250, "VEP-compatible scope")
    note(305, 86, 720, "Human GRCh38 · SNVs + indels · releases 115 / 116\nEnsembl / RefSeq / merged · --everything · HGVS", 20, 66)
    tag(1085, 94, 220, "Added interfaces")
    note(1325, 86, 1030, "Arrow streaming · lazy queries · SQL / expressions · plugin manifests\nSV parity not qualified; multi-sample VCF I/O supported, cohort parity not qualified", 19, 66)

    boundary = s.box(300, 270, 1330, 207, "", dashed=True,
                     stroke_width=1.6, radius=14)
    tag(323, 253, 315, "Native Rust / DataFusion")
    source = s.box(60, 321, 175, 88, "Input VCF\nBGZF + index", font_size=21)
    provider = s.box(330, 321, 220, 88, "VCF\nTableProvider", font_size=22)
    engine = s.box(675, 296, 495, 148, "", stroke_width=2)
    tag(695, 312, 455, "annotate_vep() · table function", 21)
    s.label(700, 355, 445, 74, "Allele match → SO consequences + HGVS\nKnown variants / AF · optional plugins",
            font_size=20, align="center")
    arrow = s.box(1290, 321, 275, 88, "Arrow RecordBatch\nstream", font_size=22)
    lf = s.box(1740, 310, 275, 110, "Polars LazyFrame\nlazy IO source\nexecutes on demand", font_size=20)
    consumer = s.box(2110, 321, 245, 88, "SQL / expressions\nfilter · join · sink", font_size=19)
    note(2090, 265, 270, "Polars / polars-bio", 19, 30)
    for a, b in ((source, provider), (provider, engine), (engine, arrow), (arrow, lf), (lf, consumer)):
        s.edge(a.x + a.width, 365, b.x, 365, source=a, target=b, width=2.2)

    # Demand goes back into annotation, rather than a second dataflow.
    s.edge(1877, 310, 923, 296, via=((1877, 210), (923, 210)),
           source=lf, target=engine, dashed=True, width=2)
    s.label(1035, 155, 755, 52,
            "PUSH-DOWN: selected columns · genomic regions · LIMIT\nNeeded HGVS / known-variant / CSQ / plugin work only",
            font_size=20, align="center")
    note(1725, 434, 360, "AF / consequence predicates:\nfilter annotated batches", 19, 65)

    cache = s.box(60, 531, 500, 88, "Partitioned Parquet cache\nCore entities + optional plugin caches", font_size=22, stroke_width=2)
    cache_focus = s.box(51, 522, 518, 106, "", fill="none", stroke=LINE, dashed=True)
    tag(61, 504, 44, "i", 21)
    access = s.box(715, 514, 455, 115,
                   "Cache access\nVariation / plugins: page lookup\nTranscript context: scan → COITree", font_size=20)
    s.edge(560, 575, 715, 575, source=cache, target=access)
    s.edge(923, 514, 923, 444, source=access, target=engine)
    fasta = s.box(1290, 622, 275, 65, "Reference FASTA\nHGVS / sequence context", font_size=19)
    s.edge(1290, 655, 1170, 420, via=((1220, 655), (1220, 420)),
           source=fasta, target=engine)
    vcf = s.box(2110, 519, 245, 80, "Annotated VCF\nCSQ + INFO / FORMAT", font_size=20)
    s.edge(1427, 409, 2110, 559, via=((1427, 559),),
           source=arrow, target=vcf)
    note(1660, 614, 700, "Reference workflow: VEP → VCF → filter_vep\nSolid arrows: data. Dashed return: query demand.", 19, 70)

    # True magnification leaders: two source corners to two target corners.
    cache_frame = Box("cache-zoom-frame", 40, 750, 950, 630, [""], stroke=LINE)
    s.edge(51, 628, 40, 750, arrow=False, color=LINE, width=1.5,
           source=cache_focus, target=cache_frame)
    s.edge(569, 628, 990, 750, arrow=False, color=LINE, width=1.5,
           source=cache_focus, target=cache_frame)
    s.elements.append(cache_frame)
    heading(62, 764, 900, "A2", "Cache directory")
    tag(914, 768, 48, "i", 21)

    raw = s.box(66, 832, 235, 70, "Ensembl VEP cache\nStorable / Sereal", font_size=19)
    build = s.box(366, 832, 255, 70, "vepyr.build_cache()\nentity + chromosome", font_size=18)
    built = s.box(686, 832, 275, 70, "Parquet shards\n+ chromosome manifests", font_size=19)
    s.edge(301, 867, 366, 867, source=raw, target=build)
    s.edge(621, 867, 686, 867, source=build, target=built)
    s.label(75, 918, 730, 32, "cache_dir/   [release + source identity]", font_size=22, mono=True, weight=700)

    entity_rows = ["variation/", "transcript/", "exon/", "translation_core/",
                   "translation_sift/", "regulatory/", "motif/  [116]"]
    s.edge(94, 966, 94, 1215, arrow=False, color=LINE, width=1.2)
    for index, entity in enumerate(entity_rows):
        yy = 962 + index * 39
        s.edge(94, yy + 19, 124, yy + 19, arrow=False, color=LINE, width=1.2)
        s.label(134, yy, 330, 37, entity, font_size=21, mono=True, weight=700 if index == 0 else 400)
    chosen = s.box(641, 961, 319, 38, "chr1.parquet", font_size=21, mono=True, stroke_width=2)
    s.edge(400, 980, 641, 980, arrow=False, width=1.2)
    tag(587, 964, 42, "ii", 19)
    note(544, 1006, 418, "Each entity: chr*.parquet + manifest", 17, 27)

    # Plugin provenance is cache-level JSON, not the core row provenance block.
    s.label(75, 1245, 451, 32, "plugin_cache_root/plugin/cadd/", font_size=19, mono=True, weight=700)
    plugin_manifest = s.box(134, 1290, 299, 40, "manifest.json", font_size=20, mono=True, stroke_width=2)
    tag(79, 1293, 42, "iii", 18)
    provenance_frame = Box("plugin-provenance-frame", 544, 1048, 418, 282, [""], stroke=LINE)
    s.edge(433, 1290, 544, 1048, arrow=False, color=LINE, width=1.3,
           source=plugin_manifest, target=provenance_frame)
    s.edge(433, 1330, 544, 1330, arrow=False, color=LINE, width=1.3,
           source=plugin_manifest, target=provenance_frame)
    s.elements.append(provenance_frame)
    tag(544, 1048, 418, "iii   CADD · provenance manifest", 20)
    s.box(558, 1094, 390, 66, "cache_source_version\n<requested ref>@<commit SHA>",
          font_size=18, mono=True, align="left", fill=SOFT)
    s.box(558, 1170, 390, 90, "sources[] · one entry per input\npart · upstream url\nmd5 / verified_md5*",
          font_size=18, mono=True, align="left")
    note(558, 1268, 390, "*Actual digest when verified.\nAlso: file / index fingerprints.", 17, 49)
    note(75, 1339, 451, "chr*.parquet · tiers inherited from variation", 17, 30)
    note(544, 1339, 418, "JSON sidecar, not repeated in each row", 17, 30)

    shard_frame = Box("shard-zoom-frame", 1100, 750, 1260, 630, [""], stroke=LINE)
    s.edge(960, 961, 1100, 750, arrow=False, color=LINE, width=1.5,
           source=chosen, target=shard_frame)
    s.edge(960, 999, 1100, 1380, arrow=False, color=LINE, width=1.5,
           source=chosen, target=shard_frame)
    s.elements.append(shard_frame)
    heading(1124, 764, 1190, "A3", "variation/chr1.parquet")
    tag(2286, 768, 48, "ii", 21)
    tag(1126, 826, 378, "Schema + identity")
    tag(1542, 826, 793, "Tiered, page-indexed Parquet")

    s.box(1126, 878, 378, 88,
          "Lookup keys\nchrom, start, end\nallele_string, tier",
          font_size=20, align="left")
    s.box(1126, 978, 378, 114,
          "Encoded payload\nknown IDs + clinical fields\nAF allele / frequency arrays\nBoolean presence flags",
          font_size=19, align="left")
    s.box(1126, 1104, 378, 120,
          "File identity / provenance\ncache release + source type\nin Arrow schema metadata\nNo transcript-style row block",
          font_size=18, align="left", fill=SOFT)

    # The tiers are sorted runs, not separate files or necessarily row groups.
    s.box(1542, 880, 355, 49, "warm · tier 0 · start ↑", fill="#dddddd", font_size=20)
    s.box(1897, 880, 438, 49, "cold · tier 1 · start ↑", fill="white", font_size=20)
    note(1542, 938, 793, "Warm: positions with max global AF ≥ 1%, plus ±1 bp.\nCold: remaining positions. Both tiers are queried. Not to scale.", 18, 48)

    s.box(1542, 1002, 793, 152, "", stroke_width=1.7)
    s.label(1554, 1008, 770, 30, "Row group · cap 1M rows (current writer)", font_size=20, weight=700)
    # One leaf-column chunk per column; page boundaries need not align.
    for xx, ww, label, sizes in (
        (1558, 192, "tier", (68, 86)),
        (1765, 238, "start", (62, 66, 68)),
        (2018, 300, "variation_name", (84, 105, 62)),
    ):
        s.box(xx, 1047, ww, 93, "", fill="white")
        s.label(xx + 5, 1050, ww - 10, 29, label, font_size=18, mono=True, align="center", weight=700)
        px = xx + 9
        for n, pw in enumerate(sizes):
            s.box(px, 1086, pw, 36, f"p{n}", font_size=16, bold_first=False,
                  fill="#dddddd" if label == "start" and n == 1 else "white")
            px += pw + 6
    note(1542, 1161, 793, "Leaf-column chunks contain pages; tier boundaries may cross groups.", 17, 28)
    s.box(1542, 1196, 793, 38,
          "Footer: schema + identity + ColumnIndex / OffsetIndex locations",
          fill=SOFT, font_size=17, bold_first=False)

    # The read path touches candidate pages, not all row-group payload.
    s.box(1126, 1250, 378, 81, "CURRENT CODE DEFAULTS\nZSTD 3 · dictionary OFF\n4 KiB / 512-row page targets", font_size=18, align="left")
    s.box(1542, 1250, 217, 81, "Resolve\nPageDir / min–max\ncandidate pages", font_size=18)
    s.box(1817, 1250, 217, 81, "Locate\nstart-only read\nexact row offsets", font_size=18)
    s.box(2092, 1250, 243, 81, "Take\nprojected payload\nmatched rows only", font_size=18)
    s.edge(1759, 1290, 1817, 1290)
    s.edge(2034, 1290, 2092, 1290)
    note(1126, 1336, 1209, "Large groups amortize metadata; small indexed pages limit decode/I/O. Realized sizes vary.\nCurrent context profile: dictionary ON / 50k rows. Historical caches differ. Parameter sweep: pending.",
         17, 43)

    # B: three test layers, without a stack of colored cards.
    heading(40, 1425, 635, "B", "Quality validation")
    tag(55, 1492, 620, "1  Component-level unit tests", 21)
    for xx, title, detail in (
        (55, "Formats", "VCF / cache I/O\nschemas + metadata"),
        (267, "Rust engine", "alleles + intervals\nHGVS + plugins"),
        (479, "Python API", "arguments + outputs\nlazy execution"),
    ):
        s.box(xx, 1538, 196, 118, title + "\n" + detail, font_size=19, align="left")
    tag(55, 1690, 620, "2  Ported VEP behavior", 21)
    categories = ["allele matching", "SO + coding / splice",
                  "HGVS + 3′ shift", "pick / ranking",
                  "known variants + AF", "regulatory / motifs"]
    for index, category in enumerate(categories):
        xx = 60 + (index % 2) * 310
        yy = 1740 + (index // 2) * 39
        s.label(xx, yy, 300, 32, category, font_size=20)
    tag(55, 1891, 620, "3  End-to-end VEP comparison", 21)
    s.label(60, 1937, 610, 76, "Six release/source profiles · HG002 chr1–22\n4,096,123 variants per profile\nZero CSQ mismatches in archived field gates", font_size=20)
    s.box(60, 2029, 610, 55, "115 / 116 × Ensembl (80), RefSeq (85), merged (86 fields)", font_size=18, fill=SOFT, bold_first=False)
    note(60, 2100, 610, "Separate harness: strict / canonical VCF-body MD5.\nField gates are not presented as MD5 validation.", 18, 65)

    # C stays a Python-generated, embedded vector chart.
    heading(735, 1425, 920, "C", "Performance")
    s.image(730, 1493, 945, 623, performance_uri)
    note(746, 2135, 920, "Preliminary single runs; paired repeats and filter timings remain pending.", 18, 30)

    # D: concise contracts plus one genuinely variant-level SQL equivalent.
    heading(1735, 1425, 625, "D", "Contracts + filtering")
    tag(1750, 1492, 595, "Input", 21)
    s.label(1755, 1534, 585, 98,
            "Normalize: bcftools norm -f ref.fa -m -both\nMatching assembly, FASTA and cache release\nBGZF + TBI/CSI for workers > 1 and region seeks",
            font_size=20)
    tag(1750, 1653, 595, "Output", 21)
    s.label(1755, 1695, 585, 114,
            "VCF: original INFO / FORMAT + CSQ\nLazyFrame: typed variant rows + aligned lists\nsink_* streams; collect() materializes\nNo direct reimplementation of filter_vep",
            font_size=20)
    s.label(1750, 1830, 595, 36, "Same predicate: rare or unknown allele frequency", font_size=20, weight=700)
    s.box(1750, 1880, 595, 91,
          'VEP + filter_vep\nfilter_vep --filter\n"gnomADg_AF < 0.01 or not gnomADg_AF"',
          font_size=18, mono=True, align="left", fill=SOFT)
    s.box(1750, 1989, 595, 112,
          "Polars SQL (variants = lf)\nSELECT * FROM variants\nWHERE gnomADg_AF < 0.01\n   OR gnomADg_AF IS NULL",
          font_size=18, mono=True, align="left")
    note(1755, 2116, 585, "Per-CSQ predicates need aligned explode / any.\nOntology expansion must be explicit.", 18, 60)

    return s


def html_value(lines: list[str], bold_first: bool) -> str:
    rendered: list[str] = []
    for index, line in enumerate(lines):
        value = html.escape(line)
        if index == 0 and bold_first and line:
            value = f"<b>{value}</b>"
        rendered.append(value)
    return "<br>".join(rendered)


def drawio_xml(scene: Scene, height: int = HEIGHT) -> str:
    mxfile = ET.Element(
        "mxfile",
        {
            "host": "app.diagrams.net",
            "modified": "2026-09-07T00:00:00.000Z",
            "agent": "vepyr figure builder",
            "version": "26.2.2",
            "type": "device",
        },
    )
    diagram = ET.SubElement(mxfile, "diagram", {"id": "vepyr-figure1", "name": "Figure 1"})
    model = ET.SubElement(
        diagram,
        "mxGraphModel",
        {
            "dx": "2400",
            "dy": str(height),
            "grid": "1",
            "gridSize": "10",
            "guides": "1",
            "tooltips": "1",
            "connect": "1",
            "arrows": "1",
            "fold": "1",
            "page": "1",
            "pageScale": "1",
            "pageWidth": str(WIDTH),
            "pageHeight": str(height),
            "math": "0",
            "shadow": "0",
            "background": "#ffffff",
        },
    )
    root = ET.SubElement(model, "root")
    ET.SubElement(root, "mxCell", {"id": "0"})
    ET.SubElement(root, "mxCell", {"id": "1", "parent": "0"})

    # Preserve construction order. Background containers are added before their
    # contents, while connectors are added after their endpoints; this keeps
    # arrows visible without letting filled group boxes obscure them.
    ordered = scene.elements
    for element in ordered:
        if isinstance(element, Edge):
            style = (
                "edgeStyle=none;rounded=0;"
                f"html=1;strokeColor={element.color};strokeWidth={element.width};"
                f"endArrow={'block' if element.arrow else 'none'};endFill=1;"
                f"dashed={1 if element.dashed else 0};"
            )
            attrs = {"id": element.id, "style": style, "edge": "1", "parent": "1"}
            for role, box, xx, yy, prefix in (
                ("source", element.source, element.x1, element.y1, "exit"),
                ("target", element.target, element.x2, element.y2, "entry"),
            ):
                if box is not None:
                    attrs[role] = box.id
                    attrs["style"] += (
                        f"{prefix}X={(xx - box.x) / box.width};"
                        f"{prefix}Y={(yy - box.y) / box.height};{prefix}Perimeter=0;"
                    )
            cell = ET.SubElement(root, "mxCell", attrs)
            geom = ET.SubElement(cell, "mxGeometry", {"relative": "1", "as": "geometry"})
            ET.SubElement(geom, "mxPoint", {"x": str(element.x1), "y": str(element.y1), "as": "sourcePoint"})
            ET.SubElement(geom, "mxPoint", {"x": str(element.x2), "y": str(element.y2), "as": "targetPoint"})
            if element.via:
                points = ET.SubElement(geom, "Array", {"as": "points"})
                for xx, yy in element.via:
                    ET.SubElement(points, "mxPoint", {"x": str(xx), "y": str(yy)})
        elif isinstance(element, Box):
            style = (
                f"rounded={int(element.radius > 0)};arcSize={max(2, int(element.radius))};whiteSpace=wrap;html=1;"
                f"fillColor={element.fill};strokeColor={element.stroke};strokeWidth={element.stroke_width};"
                f"fontFamily={'Courier New' if element.mono else 'Arial'};fontSize={element.font_size};"
                f"align={element.align};verticalAlign=middle;spacing=8;fontColor={element.font_color};"
                f"dashed={1 if element.dashed else 0};dashPattern=8 6;"
            )
            cell = ET.SubElement(
                root,
                "mxCell",
                {
                    "id": element.id,
                    "value": html_value(element.lines, element.bold_first),
                    "style": style,
                    "vertex": "1",
                    "parent": "1",
                },
            )
            ET.SubElement(
                cell,
                "mxGeometry",
                {
                    "x": str(element.x),
                    "y": str(element.y),
                    "width": str(element.width),
                    "height": str(element.height),
                    "as": "geometry",
                },
            )
        elif isinstance(element, Label):
            style = (
                "text;html=1;strokeColor=none;fillColor=none;whiteSpace=wrap;"
                f"fontFamily={'Courier New' if element.mono else 'Arial'};fontSize={element.font_size};"
                f"fontStyle={1 if element.weight >= 700 else 0};fontColor={element.color};"
                f"align={element.align};verticalAlign=middle;spacing=0;"
            )
            cell = ET.SubElement(
                root,
                "mxCell",
                {
                    "id": element.id,
                    "value": "<br>".join(html.escape(line) for line in element.lines),
                    "style": style,
                    "vertex": "1",
                    "parent": "1",
                },
            )
            ET.SubElement(
                cell,
                "mxGeometry",
                {
                    "x": str(element.x),
                    "y": str(element.y),
                    "width": str(element.width),
                    "height": str(element.height),
                    "as": "geometry",
                },
            )
        else:
            style = (
                "shape=image;verticalLabelPosition=bottom;verticalAlign=top;imageAspect=0;"
                f"aspect=fixed;image={element.data_uri.replace(';base64', '')};"
            )
            cell = ET.SubElement(
                root,
                "mxCell",
                {"id": element.id, "style": style, "vertex": "1", "parent": "1"},
            )
            ET.SubElement(
                cell,
                "mxGeometry",
                {
                    "x": str(element.x),
                    "y": str(element.y),
                    "width": str(element.width),
                    "height": str(element.height),
                    "as": "geometry",
                },
            )

    ET.indent(mxfile, space="  ")
    return ET.tostring(mxfile, encoding="unicode", xml_declaration=False) + "\n"


def wrap_lines(lines: list[str], width: float, font_size: float, mono: bool) -> list[str]:
    average = font_size * (0.62 if mono else 0.54)
    chars = max(6, int((width - 18) / average))
    out: list[str] = []
    for line in lines:
        if not line:
            out.append("")
            continue
        out.extend(textwrap.wrap(line, width=chars, break_long_words=False, break_on_hyphens=False) or [""])
    return out


def svg_text_block(
    x: float,
    y: float,
    width: float,
    height: float,
    lines: list[str],
    *,
    font_size: float,
    weight: int,
    align: str,
    color: str,
    mono: bool,
    bold_first: bool = False,
) -> str:
    wrapped = wrap_lines(lines, width, font_size, mono)
    line_height = font_size * 1.27
    block_height = max(line_height, len(wrapped) * line_height)
    baseline = y + max(font_size, (height - block_height) / 2 + font_size)
    if align == "center":
        tx = x + width / 2
        anchor = "middle"
    elif align == "right":
        tx = x + width - 4
        anchor = "end"
    else:
        tx = x + 4
        anchor = "start"
    family = "Courier New, monospace" if mono else "Arial, Helvetica, sans-serif"
    chunks = [
        f'<text x="{tx:.1f}" y="{baseline:.1f}" text-anchor="{anchor}" '
        f'font-family="{family}" font-size="{font_size}" font-weight="{weight}" fill="{color}">'
    ]
    for index, line in enumerate(wrapped):
        line_weight = 700 if bold_first and index == 0 else weight
        dy = 0 if index == 0 else line_height
        chunks.append(
            f'<tspan x="{tx:.1f}" dy="{dy:.1f}" font-weight="{line_weight}">{html.escape(line)}</tspan>'
        )
    chunks.append("</text>")
    return "".join(chunks)


def svg_preview(scene: Scene, height: int = HEIGHT) -> str:
    chunks = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{height}" viewBox="0 0 {WIDTH} {height}">',
        "<defs>",
    ]
    for name, color in (("ink", INK), ("muted", MUTED)):
        chunks.append(
            f'<marker id="arrow-{name}" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0 L8,4 L0,8 z" fill="{color}"/></marker>'
        )
    chunks.extend(["</defs>", '<rect width="100%" height="100%" fill="white"/>'])
    marker_for = {INK: "ink", MUTED: "muted"}

    ordered = scene.elements
    for element in ordered:
        if isinstance(element, Edge):
            marker = marker_for.get(element.color, "ink")
            marker_attr = f' marker-end="url(#arrow-{marker})"' if element.arrow else ""
            dash = ' stroke-dasharray="8 6"' if element.dashed else ""
            points = ((element.x1, element.y1), *element.via, (element.x2, element.y2))
            coordinates = " ".join(f"{xx},{yy}" for xx, yy in points)
            chunks.append(
                f'<polyline points="{coordinates}" fill="none" '
                f'stroke="{element.color}" stroke-width="{element.width}"{dash}{marker_attr}/>'
            )
        elif isinstance(element, Box):
            dash = ' stroke-dasharray="8 6"' if element.dashed else ""
            chunks.append(
                f'<rect x="{element.x}" y="{element.y}" width="{element.width}" height="{element.height}" '
                f'rx="{element.radius}" fill="{element.fill}" stroke="{element.stroke}" '
                f'stroke-width="{element.stroke_width}"{dash}/>'
            )
            if any(element.lines):
                chunks.append(
                    svg_text_block(
                        element.x + 6,
                        element.y + 5,
                        element.width - 12,
                        element.height - 10,
                        element.lines,
                        font_size=element.font_size,
                        weight=400,
                        align=element.align,
                        color=element.font_color,
                        mono=element.mono,
                        bold_first=element.bold_first,
                    )
                )
        elif isinstance(element, Label):
            chunks.append(
                svg_text_block(
                    element.x,
                    element.y,
                    element.width,
                    element.height,
                    element.lines,
                    font_size=element.font_size,
                    weight=element.weight,
                    align=element.align,
                    color=element.color,
                    mono=element.mono,
                )
            )
        else:
            chunks.append(
                f'<image x="{element.x}" y="{element.y}" width="{element.width}" height="{element.height}" href="{element.data_uri}" preserveAspectRatio="xMidYMid meet"/>'
            )
    chunks.append("</svg>")
    return "\n".join(chunks) + "\n"


def validate_scene(scene: Scene) -> None:
    ids = {element.id for element in scene.elements}
    if len(ids) != len(scene.elements):
        raise ValueError("Duplicate diagram cell ID")
    for element in scene.elements:
        if isinstance(element, Edge):
            for endpoint in (element.source, element.target):
                if endpoint is not None and endpoint.id not in ids:
                    raise ValueError(f"Dangling connector: {element.id}")
            if element.arrow:
                points = ((element.x1, element.y1), *element.via, (element.x2, element.y2))
                if any(a[0] != b[0] and a[1] != b[1] for a, b in zip(points, points[1:])):
                    raise ValueError(f"Dataflow arrow is not orthogonal: {element.id}")
        elif element.x < 0 or element.y < 0 or element.x + element.width > WIDTH or element.y + element.height > HEIGHT:
            raise ValueError(f"Element outside the canvas: {element.id}")


def render_previews() -> None:
    chrome = shutil.which("google-chrome") or shutil.which("chromium")
    if chrome is None:
        candidate = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
        if not candidate.exists():
            raise SystemExit("Chrome/Chromium not found; use draw.io Desktop export instead.")
        chrome = str(candidate)
    jobs = [
        ("figure1.svg", "figure1.png", HEIGHT, "--screenshot"),
        ("render-preview.html", "figure1.pdf", HEIGHT, "--print-to-pdf"),
    ]
    for source, output, height, flag in jobs:
        target = HERE / output
        before = target.stat().st_mtime_ns if target.exists() else 0
        with tempfile.TemporaryDirectory(prefix="vepyr-figure-chrome-") as profile:
            command = [chrome, "--headless=new", "--disable-gpu", "--no-sandbox",
                       "--allow-file-access-from-files", "--hide-scrollbars",
                       "--disable-background-networking", "--disable-component-update",
                       "--no-pdf-header-footer", f"--user-data-dir={profile}",
                       f"--window-size={WIDTH},{height}", f"{flag}={target}",
                       (HERE / source).as_uri()]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                _, stderr = process.communicate(timeout=12)
            except subprocess.TimeoutExpired:
                # Some macOS Chrome builds keep running after writing the file.
                process.terminate()
                try:
                    _, stderr = process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    _, stderr = process.communicate(timeout=5)
            if not target.exists() or target.stat().st_mtime_ns <= before:
                raise RuntimeError(f"Chrome did not render {output}: {stderr.decode(errors='replace')[-1000:]}")
            print(f"Rendered {output}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--render", action="store_true", help="Also export PNG/PDF using headless Chrome")
    args = parser.parse_args()
    svg_bytes = build_performance.build_svg().encode("utf-8")
    data_uri = "data:image/svg+xml;base64," + base64.b64encode(svg_bytes).decode("ascii")
    scene = build_scene(data_uri)
    validate_scene(scene)
    DRAWIO.write_text(drawio_xml(scene), encoding="utf-8")
    PREVIEW.write_text(svg_preview(scene), encoding="utf-8")
    if args.render:
        render_previews()


if __name__ == "__main__":
    main()
