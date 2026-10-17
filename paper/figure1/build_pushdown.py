#!/usr/bin/env python3
"""Figure 1D proposal: what a Polars query lets vepyr skip.

Three grids side by side, in the style of the classic Parquet
predicate/projection pushdown diagram, following vepyr 0.10.0 (``annotate()``'s
IO source, Supplementary Data, section Pushdowns):

1. input VCF: chrom/start/end conjuncts become regions read by index seek
   (rows), and only the INFO/FORMAT fields a query reads are parsed (columns);
2. variation cache: the page index selects the pages whose position range can
   hold an input position;
3. annotated frame: the selected and filtered columns decide which annotation
   groups run (HGVS, known variants, CSQ string, plugins); other predicates
   filter the annotated batches.

Grids are schematic.

    python build_pushdown.py [--render]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import build_cache_workflow_proposal as workflow
import build_figure as fig


HERE = Path(__file__).resolve().parent
WIDTH, HEIGHT = 1990, 690
INK, GREY, LIGHT, SOFT = "#111111", "#555555", "#bdbdbd", "#f3f3f3"
READ, SKIP, DROP = "#4a4a4a", "#ececec", "#bdbdbd"
NBSP = " "

QUERY = (
    'lf.filter(',
    '    pl.col("chrom") == "chr22",',
    '    pl.col("start").is_between(a, b),',
    '    pl.col("gnomADg_AF") < 0.01,',
    ').select("SYMBOL", "gnomADg_AF")',
)
DESCRIPTION = (
    "Query pushdown in vepyr. The genomic predicate of a Polars query restricts "
    "the input VCF to a region read through its index and, through the input "
    "positions, the variation cache pages read through the page index; the "
    "selected and filtered columns decide which annotation is computed. Other "
    "predicates filter the annotated rows. Grids are schematic."
)


def code(lines: tuple[str, ...]) -> str:
    """Keep the spacing of code, which SVG and draw.io would collapse."""
    return "\n".join(line.replace(" ", NBSP) for line in lines)


def grid(s, x, y, cols, rows, state, cell_w, label_w, pick_rows=(), pick_cols=(),
         cell_h=50, head_size=17, label_size=18):
    """Cells coloured by ``state(row, col)``, with arrows picking columns from
    above (projection) and rows from the right (predicate)."""
    for j in pick_cols:
        cx = x + label_w + (j + 0.5) * cell_w
        s.edge(cx, y, cx, y + 28, color=INK, width=2.2)
    head = y + 32
    for j, name in enumerate(cols):
        s.label(x + label_w + j * cell_w, head, cell_w, 28, name, font_size=head_size,
                color=GREY, align="center")
    top = head + 30
    right = x + label_w + len(cols) * cell_w
    for i, row in enumerate(rows):
        s.label(x, top + i * cell_h, label_w - 8, cell_h, row, font_size=label_size,
                color=GREY, align="right")
        for j in range(len(cols)):
            s.box(x + label_w + j * cell_w, top + i * cell_h, cell_w, cell_h, "",
                  fill=state(i, j), stroke="white", stroke_width=3)
        if i in pick_rows:
            cy = top + (i + 0.5) * cell_h
            s.edge(right + 50, cy, right + 6, cy, color=INK, width=2.2)
    return top + len(rows) * cell_h


def panel(code_label: str = "D", width: float = WIDTH) -> fig.Scene:
    s = fig.Scene()
    s._counter = 70000
    s.label(0, 14, 64, 44, code_label, font_size=31, weight=700)
    s.label(74, 15, 900, 42, "Query pushdown", font_size=29, weight=700)
    s.box(0, 70, 660, 164, code(QUERY), font_size=19, mono=True, align="left",
          bold_first=False, fill=SOFT, stroke=LIGHT, stroke_width=1.2)

    # Legend beside the query.
    lx = 740
    s.edge(lx + 11, 84, lx + 11, 110, color=INK, width=2.2)
    s.label(lx + 32, 80, 520, 34, "columns chosen by the projection", font_size=20, color=GREY)
    s.edge(lx + 600, 97, lx + 562, 97, color=INK, width=2.2)
    s.label(lx + 612, 80, 520, 34, "rows chosen by the predicate", font_size=20, color=GREY)
    for k, (fill, text) in enumerate(((READ, "read or computed"), (SKIP, "skipped"),
                                      (DROP, "removed by the AF filter"))):
        x = lx + (0, 360, 650)[k]
        s.box(x, 150, 24, 24, "", fill=fill, stroke=LIGHT if fill == SKIP else fill,
              stroke_width=1)
        s.label(x + 34, 144, 320, 36, text, font_size=20, color=GREY)

    # Three grids side by side: input VCF, variation cache, annotated frame.
    gw = (width - 2 * 60) / 3
    xs = (0, gw + 60, 2 * (gw + 60))
    ty, gy = 262, 298

    def title(x: float, text: str) -> None:
        s.label(x, ty, gw, 32, text, font_size=22, weight=700)

    title(xs[0], "1  Input VCF: region by index")
    grid(s, xs[0], gy, ["CHROM", "POS", "REF", "ALT", "INFO", "FORMAT"],
         ["chr21", "chr22 <a", "chr22 a–b", "chr22 a–b", "chr22 >b", "chrX"],
         lambda i, j: READ if i in (2, 3) and j < 4 else SKIP,
         cell_w=68, label_w=128, pick_rows=(2, 3), pick_cols=(0, 1, 2, 3))

    title(xs[1], "2  Cache: pages by position range")
    grid(s, xs[1], gy, ["position", "alleles", "IDs", "AF", "ClinVar"],
         ["< a", "< a", "a–b", "a–b", "a–b", "> b"],
         lambda i, j: READ if 2 <= i <= 4 else SKIP,
         cell_w=84, label_w=86, pick_rows=(2, 3, 4))

    title(xs[2], "3  Frame: only the columns needed")
    kept = (0, 2, 3, 5)
    grid(s, xs[2], gy, ["SYMBOL", "AF", "HGVSc", "CSQ", "plugins"],
         [f"v{i + 1}" for i in range(6)],
         lambda i, j: SKIP if j >= 2 else (READ if i in kept else DROP),
         cell_w=84, label_w=86, pick_rows=kept, pick_cols=(0, 1))
    return s


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--render", action="store_true")
    args = parser.parse_args()
    path = HERE / "d-pushdown-proposal"
    workflow.save(panel(), path, WIDTH, HEIGHT, "D — Query pushdown")
    svg = path.with_suffix(".svg")
    head, _, tail = svg.read_text().partition("<desc>")
    svg.write_text(head + "<desc>" + DESCRIPTION + tail[tail.index("</desc>"):])
    print(f"Generated {path.name}.svg / .drawio", flush=True)
    if args.render:
        workflow.render(path, WIDTH, HEIGHT)


if __name__ == "__main__":
    main()
