#!/usr/bin/env python3
"""Figure 1 as TikZ, generated from the scene of build_figure1_v2.py.

The scene's pixel coordinates become TikZ coordinates with x = PT and
y = -PT, where PT makes the 2960-px canvas exactly \\textwidth of the
Bioinformatics template (488.5 pt). Every line of text is a node on the
baseline the SVG preview uses, so the layout is the same; font sizes are
the pixel sizes times PT, in points. The comparison table (A) is the LaTeX
table of panel_a.py, and the performance chart (E) is translated from its
SVG, drawn from data/wgs_scaling.csv.

Outputs, in figures/ of the paper repository:

* figure1-tikz.tex   the tikzpicture;
* figure1-table.tex  the panel A table as a box (\\figonetable).

The project logos of panel C1 are \\includegraphics nodes that read
\\figonelogos/<name>, by default figures/logos-bw/<name>.

Preamble needed: tikz with the arrows.meta library, xcolor and colortbl,
array, booktabs and graphicx (the OUP class loads all but arrows.meta,
colortbl and booktabs).

    python3 tools/make_numbers.py   # runs render() below and writes both files
"""

from __future__ import annotations

import base64
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import build_figure as fig
import build_figure1_v2 as v2
import build_performance
import panel_a


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]  # the paper repository
PT = 488.5 / v2.WIDTH  # pt per scene pixel
SVG_NS = "{http://www.w3.org/2000/svg}"

UNICODE = {
    "\u00a0": "~",
    "–": r"\textendash{}",
    "→": r"\ensuremath{\rightarrow}",
    "←": r"\ensuremath{\leftarrow}",
    "↓": r"\ensuremath{\downarrow}",
    "…": r"\ldots{}",
    "×": r"\ensuremath{\times}",
    "≥": r"\ensuremath{\geq}",
    "≤": r"\ensuremath{\leq}",
    "│": r"\ensuremath{|}",
    "·": ",",
}
SUPERSCRIPT = dict(zip("⁻⁰¹²³⁴⁵⁶⁷⁸⁹", "-0123456789"))


def num(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


class Colors:
    def __init__(self) -> None:
        self.names: dict[str, str] = {}

    def __call__(self, value: str) -> str:
        if not value.startswith("#"):
            return value  # a named xcolor colour, e.g. white
        value = value.lstrip("#").upper()
        if len(value) == 3:
            value = "".join(c * 2 for c in value)
        if value == "FFFFFF":
            return "white"
        return self.names.setdefault(value, f"f1c{value}")

    def definitions(self) -> str:
        return "\n".join(
            rf"\definecolor{{{n}}}{{HTML}}{{{v}}}" for v, n in self.names.items()
        )


def escape(text: str, mono: bool) -> str:
    out = []
    for c in text:
        if c == "\\":
            out.append(r"\textbackslash{}")
        elif c in "{}$&#_%":
            out.append("\\" + c)
        elif c == "^":
            out.append(r"\^{}")
        elif c == "~":
            out.append(r"\textasciitilde{}")
        elif c == " " and mono:
            out.append("~")
        elif c == "'" and mono:
            out.append(r"\textquotesingle{}")
        elif c == "-":
            out.append("-{}")  # no -- or --- ligatures
        elif c in "<>|" and not mono:
            out.append(rf"\ensuremath{{{c}}}")  # OT1 text fonts lack these glyphs
        elif c in UNICODE:
            out.append(UNICODE[c])
        elif c in SUPERSCRIPT:
            out.append(rf"\textsuperscript{{{SUPERSCRIPT[c]}}}")
        else:
            out.append(c)
    return "".join(out)


def font(size_px: float, bold: bool, mono: bool, scale: float = 1.0) -> str:
    size = size_px * scale * PT
    family = r"\ttfamily" if mono else r"\sffamily"
    weight = r"\bfseries" if bold else ""
    return rf"\fontsize{{{num(size)}}}{{{num(1.2 * size)}}}\selectfont{family}{weight}"


def text_block(
    out,
    colors,
    x,
    y,
    w,
    h,
    lines,
    *,
    size,
    weight,
    align,
    color,
    mono,
    bold_first=False,
) -> None:
    """The lines of fig.svg_text_block, one node per baseline."""
    wrapped = fig.wrap_lines(lines, w, size, mono)
    lh = size * 1.27
    block = max(lh, len(wrapped) * lh)
    base = y + max(size, (h - block) / 2 + size)
    if align == "center":
        tx, anchor = x + w / 2, "base"
    elif align == "right":
        tx, anchor = x + w - 4, "base east"
    else:
        tx, anchor = x + 4, "base west"
    for i, line in enumerate(wrapped):
        if not line.strip("\u00a0 "):
            continue
        bold = weight >= 700 or (bold_first and i == 0)
        out.append(
            rf"\node[anchor={anchor},text={colors(color)},font={font(size, bold, mono)}]"
            rf" at ({num(tx)},{num(base + i * lh)}) {{{escape(line, mono)}}};"
        )


def dash(dashed: bool, scale: float = 1.0) -> str:
    return (
        f",dash pattern=on {num(8 * scale * PT)}pt off {num(6 * scale * PT)}pt"
        if dashed
        else ""
    )


def box(out, colors, e: fig.Box) -> None:
    opts = []
    if e.fill and e.fill != "none":
        opts.append(f"fill={colors(e.fill)}")
    if e.stroke_width > 0 and e.stroke and e.stroke != "none":
        opts.append(f"draw={colors(e.stroke)},line width={num(e.stroke_width * PT)}pt")
    if e.radius:
        opts.append(f"rounded corners={num(e.radius * PT)}pt")
    if opts:
        out.append(
            rf"\path[{','.join(opts)}{dash(e.dashed)}] ({num(e.x)},{num(e.y)}) rectangle "
            rf"({num(e.x + e.width)},{num(e.y + e.height)});"
        )
    if any(e.lines):
        text_block(
            out,
            colors,
            e.x + 6,
            e.y + 5,
            e.width - 12,
            e.height - 10,
            e.lines,
            size=e.font_size,
            weight=400,
            align=e.align,
            color=e.font_color,
            mono=e.mono,
            bold_first=e.bold_first,
        )


def edge(out, colors, e: fig.Edge) -> None:
    tip = (
        f",-{{Triangle[length={num(8 * PT)}pt,width={num(8 * PT)}pt]}}"
        if e.arrow
        else ""
    )
    points = " -- ".join(
        f"({num(x)},{num(y)})" for x, y in ((e.x1, e.y1), *e.via, (e.x2, e.y2))
    )
    out.append(
        rf"\draw[{colors(e.color)},line width={num(e.width * PT)}pt{tip}{dash(e.dashed)}] {points};"
    )


def svg_chart(out, colors, svg: str, x0: float, y0: float, k: float) -> None:
    """Translate the performance chart's SVG (text, line, polyline, rect,
    circle) into TikZ, scaled by k and placed at (x0, y0)."""
    root = ET.fromstring(svg)

    def p(x: float, y: float) -> str:
        return f"({num(x0 + k * x)},{num(y0 + k * y)})"

    for el in root.iter():
        tag = el.tag.replace(SVG_NS, "")
        a = el.attrib
        width = float(a.get("stroke-width", "1"))
        stroke = a.get("stroke")
        dashed = a.get("stroke-dasharray")
        dash_opt = ""
        if dashed:
            on, off = (float(v) for v in dashed.replace(",", " ").split()[:2])
            dash_opt = (
                f",dash pattern=on {num(on * k * PT)}pt off {num(off * k * PT)}pt"
            )
        if tag == "text":
            anchor = {"middle": "base", "end": "base east"}.get(
                a.get("text-anchor"), "base west"
            )
            size = float(a.get("font-size", "16"))
            bold = a.get("font-weight", "400") in ("700", "bold")
            rotate = ""
            m = re.match(r"rotate\(([-\d.]+)", a.get("transform", ""))
            if m:
                rotate = f",rotate={num(-float(m.group(1)))}"
            out.append(
                rf"\node[anchor={anchor},text={colors(a.get('fill', '#111111'))}"
                rf",font={font(size, bold, False, k)}{rotate}]"
                rf" at {p(float(a['x']), float(a['y']))} {{{escape(el.text or '', False)}}};"
            )
        elif tag == "line":
            out.append(
                rf"\draw[{colors(stroke or '#111111')},line width={num(width * k * PT)}pt{dash_opt}] "
                rf"{p(float(a['x1']), float(a['y1']))} -- {p(float(a['x2']), float(a['y2']))};"
            )
        elif tag == "polyline":
            pts = [tuple(map(float, xy.split(","))) for xy in a["points"].split()]
            out.append(
                rf"\draw[{colors(stroke or '#111111')},line width={num(width * k * PT)}pt{dash_opt}] "
                + " -- ".join(p(x, y) for x, y in pts)
                + ";"
            )
        elif tag == "rect" and not a.get("width", "").endswith("%"):
            x, y = float(a["x"]), float(a["y"])
            w, h = float(a["width"]), float(a["height"])
            opts = (
                [f"fill={colors(a['fill'])}"] if a.get("fill", "none") != "none" else []
            )
            if stroke and stroke != "none":
                opts.append(f"draw={colors(stroke)},line width={num(width * k * PT)}pt")
            if opts:
                out.append(
                    rf"\path[{','.join(opts)}] {p(x, y)} rectangle {p(x + w, y + h)};"
                )
        elif tag == "circle":
            out.append(
                rf"\path[fill={colors(a.get('fill', '#111111'))}] "
                rf"{p(float(a['cx']), float(a['cy']))} circle[radius={num(float(a['r']) * k * PT)}pt];"
            )


def render() -> dict[str, str]:
    """The generated files, by path relative to the paper repository."""
    perf = build_performance.build_svg()
    scene, height = v2.full_figure(perf)
    colors = Colors()
    body: list[str] = []
    for e in scene.elements:
        if isinstance(e, fig.Box):
            box(body, colors, e)
        elif isinstance(e, fig.Label) and e.rotate:
            body.append(
                rf"\node[anchor=mid west,rotate=90,inner sep=0pt,text={colors(e.color)},"
                rf"font={font(e.font_size, e.weight >= 700, e.mono)}]"
                rf" at ({num(e.x + e.width / 2)},{num(e.y + e.height)})"
                rf" {{{escape(' '.join(e.lines), e.mono)}}};"
            )
        elif isinstance(e, fig.Label):
            text_block(
                body,
                colors,
                e.x,
                e.y,
                e.width,
                e.height,
                e.lines,
                size=e.font_size,
                weight=e.weight,
                align=e.align,
                color=e.color,
                mono=e.mono,
            )
        elif isinstance(e, fig.Edge):
            edge(body, colors, e)
        elif isinstance(e, fig.Logo):
            body.append(
                rf"\node[anchor=center,fill=white,inner sep={num(3 * PT)}pt] at "
                rf"({num(e.x + e.width / 2)},{num(e.y + e.height / 2)}) "
                rf"{{\includegraphics[width={num(e.width * PT)}pt,height={num(e.height * PT)}pt,"
                rf"keepaspectratio]{{\figonelogos/{e.name}}}}};"
            )
        else:
            if e.data_uri == v2.TABLE_URI:  # panel A, the LaTeX table \figonetable
                body.append(
                    rf"\node[anchor=north west] at ({num(e.x)},{num(e.y)}) "
                    rf"{{\resizebox{{{num(e.width * PT)}pt}}{{!}}{{\figonetable}}}};"
                )
            else:
                data = base64.b64decode(e.data_uri.split(",", 1)[1]).decode("utf-8")
                svg = data.replace(" · ", ", ")
                svg_chart(
                    body, colors, svg, e.x, e.y, e.width / build_performance.WIDTH
                )
    tikz = (
        "% Figure 1, generated by tools/figure1/build_tikz.py (run tools/make_numbers.py); do not\n"
        "% edit by hand, change the builders or data/ and regenerate. Width: \\textwidth of the\n"
        "% Bioinformatics template (488.5 pt). Needs \\input{figure1-table.tex} first.\n"
        "\\providecommand{\\figonelogos}{figures/logos-bw}%\n"
        f"\\begin{{tikzpicture}}[x={PT:.6f}pt,y=-{PT:.6f}pt,"
        "every node/.style={inner sep=0pt,outer sep=0pt}]\n"
        + colors.definitions()
        + "\n"
        + f"\\useasboundingbox (0,0) rectangle ({v2.WIDTH},{height});\n"
        + "\n".join(body)
        + "\n\\end{tikzpicture}%\n"
    )
    return {
        "figures/figure1-tikz.tex": tikz,
        "figures/figure1-table.tex": panel_a.table_file(),
        panel_a.SIZE_FILE: panel_a.size()[2],
    }


def main() -> None:
    for rel, text in render().items():
        (ROOT / rel).write_text(text)
        print(f"wrote {rel}")


if __name__ == "__main__":
    main()
