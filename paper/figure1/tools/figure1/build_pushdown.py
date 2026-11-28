#!/usr/bin/env python3
"""Figure 1D1: query pushdown, as a table of query parts (rows) and processing steps (columns).

Follows vepyr 0.11.0 (``annotate()``'s IO source, ``_batch_source`` in src/vepyr/__init__.py)
and the engine it ships with (datafusion-bio-functions v0.27.1):

1. region: chrom/start conjuncts become engine regions. The input VCF is read through its
   .tbi/.csi index; the variation and plugin caches are Parquet files whose page index selects
   the pages that can hold an input position (parquet_cache/variation_lookup.rs,
   plugin_cache/lookup.rs); transcripts are loaded for the selected contigs only.
2. columns: the selected columns and those read by filters. Only the INFO/FORMAT fields read
   are parsed; the co-located lookup reads only the AF groups needed (lookup_exec.rs,
   variation_projection_columns); HGVS, SIFT/PolyPhen, domains are computed only when needed;
   only the plugins owning a read column are looked up (whole plugins, issue vepyr#294).
3. row limit: passed into the engine scan, which stops annotating early. Polars passes the
   limit (n_rows) to the IO source only when the query has no filter.
4. other filters: applied by the IO source to each Arrow batch, after the full predicate
   (including the region conjuncts) is evaluated there, so the result equals filtering the
   fully annotated frame.

The query column is one script (script()), run against HG002 with vepyr 0.11.0: it gives the
engine the region, the selected columns and the cadd and alphamissense plugins, and the same
result as filtering the annotated region.

The variation grid shows the Parquet columns of the cache (chr22.parquet); co-located columns
read with variation_name (clin_sig, somatic, pubmed, ...) are left out for space. Grids are
schematic.

    python build_pushdown.py [--render]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import build_cache_workflow_proposal as workflow
import build_figure as fig


HERE = Path(__file__).resolve().parent
WIDTH = 1500
INK, GREY, LIGHT, CELL, DARK = "#111111", "#666666", "#c8c8c8", "#ededed", "#3a3a3a"
READ, SKIP, PART = "#4a4a4a", "#e6e6e6", "#9a9a9a"
NBSP = " "

GAP = 18
# Fixed columns: query part and Arrow batch. The four grids share the rest so that their cells
# have one width; column headers are vertical.
QUERY_W, BATCH_W = 340, 112
ARROW_W = 34
# Row labels: input VCF (chr22 a–b), known variants (page), consequences (contig), plugins (page)
GRID_LABEL_W = (74, 44, 54, 44)
GRID_COLS = 6
HEAD_FONT, CODE_FONT, CELL_FONT = 15, 14, 17
LINE_H = CODE_FONT * 1.27  # build_figure.wrap_lines

STAGES = (
    "Read input VCF\nindexed scan (.tbi / .csi)",
    "Look up known variants\npage-index scan (Parquet)",
    "Compute consequences\nper contig (Parquet)",
    "Look up plugins\npage-index scan (Parquet)",
    "Arrow batch",
)
# The query column, read from top to bottom, is one valid script: SETUP, then the code of the
# rows in order -- one chained query, each row one part of it. Polars merges the two filters
# into one predicate for the source, so the engine receives the region, the selected columns
# plus those the filters read, and the cadd and alphamissense plugins. A row limit is not
# shown: Polars passes a limit to the source only when the query has no filter (with a
# filter, `n_rows` is None and vepyr annotates everything the filter lets through).
# am_pathogenicity is the one float plugin column (List(Float32), one value per consequence);
# 0.564 is the AlphaMissense likely-pathogenic threshold.
SETUP = (
    "import polars as pl",
    "import vepyr",
    "lf = vepyr.annotate(",
    '    "HG002.vcf.gz",',
    '    "vepyr_cache/116_GRCh38_merged",',
    '    reference_fasta="GRCh38.fa",',
    '    plugin_cache_root="plugin_cache",',
    '    plugins=["cadd", "clinvar", "spliceai",',
    '             "alphamissense", "dbnsfp",',
    '             "phenotypeorthologous"],',
    ")",
    "a, b = 20_000_000, 25_000_000",
    "df = (",
)
# (number, kind, code lines, cells per step, row height). None: grid or empty cell.
ROWS = (
    (
        "1",
        "Region",
        (
            "    lf.filter(",
            '        (pl.col("chrom") == "chr22")',
            '        & pl.col("start").is_between(a, b)',
            "    )",
        ),
        (None, None, None, None, (".filter", "region re-checked")),
        128,
    ),
    (
        "2",
        "Columns",
        (
            "    .select(",
            '        "chrom", "start", "ref", "alt",',
            '        "SYMBOL", "gnomADg_AF",',
            '        "CADD_PHRED", "am_pathogenicity",',
            "    )",
        ),
        (None, None, None, None, (".select", "plugin columns taken from CSQ")),
        128,
    ),
    (
        "3",
        "Not pushed",
        (
            "    .filter(",
            '        pl.col("am_pathogenicity").list.max()',
            "        > 0.564",
            "    )",
            "    .collect()",
            ")",
        ),
        (None, None, None, None, (".filter", "applied after annotation")),
        145,
    ),
)
RGAP, HEAD_Y, HEAD_H = 12, 110, 76
SETUP_Y = 62
TOP = round(SETUP_Y + len(SETUP) * LINE_H + 8)  # rows start below the setup code
HEIGHT = int(TOP + sum(r[4] for r in ROWS) + RGAP * (len(ROWS) - 1) + 6)

DESCRIPTION = (
    "Query pushdown in vepyr. The left column is a Polars script, read top to bottom; rows "
    "are the parts of its query, columns the processing steps. Chromosome and position filters become regions: the input VCF is read through its "
    "index, and the known-variant and plugin caches through their Parquet page indexes. The "
    "selected columns and those read by filters decide which input fields, frequency groups, "
    "annotations and plugins are read or computed; transcripts are loaded per contig. Other "
    "filters are applied to each Arrow batch, after annotation. A row limit stops annotation "
    "early only in a query without filters. Grids are schematic."
)


def script() -> str:
    """The query column as one Python script (for checking that it runs)."""
    return "\n".join((*SETUP, *(line for row in ROWS for line in row[2]))) + "\n"


def nowrap(lines) -> str:
    """Code lines with no-break spaces, so the label keeps them as written."""
    return "\n".join(line.replace(" ", NBSP) for line in lines)


def panel(code_label: str = "D1", width: float = WIDTH) -> fig.Scene:
    s = fig.Scene()
    s._counter = 70000
    grids_w = width - QUERY_W - BATCH_W - 5 * GAP
    n = len(GRID_LABEL_W)
    cw = (grids_w - sum(GRID_LABEL_W) - n * ARROW_W) / (n * GRID_COLS)
    gw = [lw + ARROW_W + GRID_COLS * cw for lw in GRID_LABEL_W]
    widths = [QUERY_W, *gw, BATCH_W]
    xs = [0.0]
    for w in widths[:-1]:
        xs.append(xs[-1] + w + GAP)
    qx, qw = xs[0], widths[0]
    sx, sw = xs[1:], widths[1:]

    def badge(x, y, num, d=34):
        s.box(
            x,
            y,
            d,
            d,
            num,
            font_size=d * 0.55,
            bold_first=True,
            fill=INK,
            stroke=INK,
            font_color="white",
            radius=d / 2,
        )

    s.label(0, 14, 64, 44, code_label, font_size=31, weight=700)
    s.label(84, 15, 600, 42, "Query pushdown", font_size=29, weight=700)

    # Legend, top right.
    lx = width - 990
    for k, (kind, text) in enumerate(
        (
            ("read", "read or computed"),
            ("part", "read at matching rows"),
            ("skip", "skipped"),
        )
    ):
        x = lx + k * 330
        s.box(
            x,
            26,
            24,
            24,
            "",
            fill=READ if kind == "read" else SKIP,
            stroke=READ if kind == "read" else LIGHT,
            stroke_width=1,
        )
        if kind == "part":
            s.box(x, 34, 24, 8, "", fill=PART, stroke=PART, stroke_width=0)
        s.label(x + 34, 20, 290, 36, text, font_size=19, color=GREY)

    # Step headers: engine steps under one bracket, the per-batch step apart.
    for k, name in enumerate(STAGES):
        engine = k < 4
        s.box(
            sx[k],
            HEAD_Y,
            sw[k],
            HEAD_H,
            name,
            font_size=15,
            bold_first=False,
            fill=DARK if engine else "white",
            stroke=DARK,
            font_color="white" if engine else INK,
            radius=6,
            stroke_width=1.5,
        )
        if k:
            s.edge(
                sx[k - 1] + sw[k - 1],
                HEAD_Y + HEAD_H / 2,
                sx[k],
                HEAD_Y + HEAD_H / 2,
                color=INK,
                width=2,
            )
    by = 88
    right = sx[3] + sw[3]
    s.edge(sx[0], by + 14, sx[0], by, color=GREY, width=1.4, arrow=False)
    s.edge(sx[0], by, right, by, color=GREY, width=1.4, arrow=False)
    s.edge(right, by, right, by + 14, color=GREY, width=1.4, arrow=False)
    s.label(
        sx[0] + 10,
        by - 30,
        600,
        28,
        "Rust engine (DataFusion)",
        font_size=17,
        color=GREY,
    )
    s.label(
        sx[4] - 90,
        by - 30,
        sw[4] + 90,
        28,
        "vepyr IO source",
        font_size=17,
        color=GREY,
        align="right",
    )
    s.label(
        qx,
        SETUP_Y,
        qw,
        len(SETUP) * LINE_H,
        nowrap(SETUP),
        font_size=CODE_FONT,
        mono=True,
        color=GREY,
    )

    # Rows.
    y = TOP
    tops = []
    for i, (num, kind, code, cells, rh) in enumerate(ROWS):
        tops.append((y, rh))
        badge(qx, y, num)
        s.label(qx + 46, y + 1, qw - 46, 34, kind, font_size=21, weight=700)
        s.label(
            qx,
            y + 36,
            qw,
            len(code) * LINE_H,
            nowrap(code),
            font_size=CODE_FONT,
            mono=True,
        )
        k = 0
        while k < 5:
            text = cells[k]
            if i < 2 and k < 4:  # grids, drawn below
                k += 1
                continue
            if isinstance(text, str) and text.startswith("span:"):
                s.box(
                    sx[0],
                    y,
                    right - sx[0],
                    rh,
                    text[5:],
                    font_size=CELL_FONT,
                    bold_first=False,
                    fill=CELL,
                    stroke=CELL,
                    radius=6,
                )
                k = 4
                continue
            if isinstance(
                text, tuple
            ):  # Arrow batch: the script name it applies, then what
                name, note = text
                s.box(sx[k], y, sw[k], rh, "", fill="white", stroke=LIGHT, radius=6)
                top = y + (12 if note else (rh - 26) / 2)
                s.label(
                    sx[k],
                    top,
                    sw[k],
                    26,
                    name,
                    font_size=15,
                    weight=700,
                    mono=True,
                    align="center",
                )
                if note:
                    s.label(
                        sx[k] + 4,
                        top + 30,
                        sw[k] - 8,
                        rh - 42 - (top - y),
                        note,
                        font_size=15,
                        color=GREY,
                        align="center",
                    )
            elif text:
                s.box(
                    sx[k],
                    y,
                    sw[k],
                    rh,
                    text,
                    font_size=CELL_FONT,
                    bold_first=False,
                    fill=CELL if k < 4 else "white",
                    stroke=CELL if k < 4 else LIGHT,
                    radius=6,
                )
            else:
                s.box(
                    sx[k],
                    y,
                    sw[k],
                    rh,
                    "–",
                    font_size=CELL_FONT,
                    bold_first=False,
                    fill="white",
                    stroke="white",
                    font_color=LIGHT,
                )
            k += 1
        if i < len(ROWS) - 1:
            yy = y + rh + RGAP / 2
            if i == 0:  # between rows 1 and 2 the grids span both
                s.edge(qx, yy, qx + qw, yy, color="#e3e3e3", width=1, arrow=False)
                s.edge(sx[4], yy, width, yy, color="#e3e3e3", width=1, arrow=False)
            else:
                s.edge(qx, yy, width, yy, color="#e3e3e3", width=1, arrow=False)
        y += rh + RGAP

    # The grids span rows 1 and 2 and start just below the step headers.
    gtop, gbottom = HEAD_Y + HEAD_H + 24, tops[1][0] + tops[1][1]

    def grid(
        x,
        label_w,
        cols,
        rows,
        state,
        pick_rows,
        pick_cols,
        head="",
        pick_name=None,
        name_right=False,
    ):
        """Row labels | row arrows (1, into the rows) | cells; column arrows (2) above the
        vertical column headers."""
        c0 = x + label_w + ARROW_W
        head_h = 104
        head_y = gtop + 34
        body = head_y + head_h + 6
        ch = (gbottom - body) / len(rows)
        for j in pick_cols:
            cx = c0 + (j + 0.5) * cw
            s.edge(cx, gtop + 4, cx, gtop + 30, color=INK, width=2.2)
        j = min(pick_cols)
        if pick_name and name_right:  # badge left of the arrow, name right of it
            s.label(
                c0 + (j + 0.5) * cw + 8,
                gtop - 2,
                140,
                30,
                pick_name,
                font_size=15,
                mono=True,
            )
            badge(c0 + j * cw - 32, gtop, "2", d=28)
        elif pick_name:
            s.label(
                c0 + j * cw - 160,
                gtop - 2,
                160 + cw / 2 - 12,
                30,
                pick_name,
                font_size=15,
                mono=True,
                align="right",
            )
            badge(c0 + j * cw - 196, gtop, "2", d=28)
        else:
            badge(c0 + j * cw - 32, gtop, "2", d=28)
        for j, name in enumerate(cols):
            s.label(
                c0 + j * cw,
                head_y,
                cw,
                head_h,
                name,
                font_size=HEAD_FONT,
                color=GREY,
                rotate=True,
            )
        if head:
            s.label(
                x,
                body - 30,
                label_w - 4,
                28,
                head,
                font_size=15,
                color=GREY,
                align="right",
            )
        for i, row in enumerate(rows):
            ry = body + i * ch
            s.label(
                x,
                ry,
                label_w - 4,
                ch,
                row.replace(" ", NBSP),
                font_size=15,
                color=GREY,
                align="right",
            )
            for j in range(len(cols)):
                st = state(i, j)
                s.box(
                    c0 + j * cw,
                    ry,
                    cw,
                    ch,
                    "",
                    fill=READ if st == "read" else SKIP,
                    stroke="white",
                    stroke_width=3,
                )
                if st == "part":
                    s.box(
                        c0 + j * cw + 5,
                        ry + ch / 2 - 4,
                        cw - 10,
                        8,
                        "",
                        fill=PART,
                        stroke=PART,
                        stroke_width=0,
                    )
            if i in pick_rows:
                cy = ry + ch / 2
                s.edge(x + label_w + 2, cy, c0 - 3, cy, color=INK, width=2.2)
        badge(
            x + label_w + ARROW_W / 2 - 14, body + min(pick_rows) * ch - 30, "1", d=28
        )

    pages = ("<a", "a–b", "a–b", "a–b", ">b")
    grid(
        sx[0],
        GRID_LABEL_W[0],
        ("CHROM", "POS", "REF", "ALT", "INFO", "FORMAT"),
        ("chr21", "chr22 <a", "chr22 a–b", "chr22 a–b", "chr22 >b", "chrX"),
        lambda i, j: "read" if i in (2, 3) and j < 4 else "skip",
        pick_rows=(2, 3),
        pick_cols=(0, 1, 2, 3),
    )
    grid(
        sx[1],
        GRID_LABEL_W[1],
        (
            "start",
            "allele_string",
            "variation_name",
            "af_global",
            "af_gnomade",
            "af_gnomadg",
        ),
        pages,
        lambda i, j: (
            ("read" if j == 0 else "part" if j in (1, 2, 5) else "skip")
            if 1 <= i <= 3
            else "skip"
        ),
        pick_rows=(1, 2, 3),
        pick_cols=(5,),
        head="page",
        pick_name="gnomADg_AF",
    )
    # Consequences: the transcript cache is read per contig, each file in full with no row
    # filter (read_context_parquet), so a chr22 region reads all of chr22. No transcript-level
    # column read: nothing is loaded. protein_features (a translation_core column) is read
    # only for DOMAINS, translation_sift only for SIFT/PolyPhen. SYMBOL comes from
    # transcript.gene_symbol. motif (read with regulatory) is left out for space.
    grid(
        sx[2],
        GRID_LABEL_W[2],
        (
            "transcript",
            "exon",
            "translation",
            "protein_features",
            "translation_sift",
            "regulatory",
        ),
        ("chr20", "chr21", "chr22", "chrX", "chrY"),
        lambda i, j: "read" if i == 2 and j in (0, 1, 2, 5) else "skip",
        pick_rows=(2,),
        pick_cols=(0,),
        pick_name="SYMBOL",
        name_right=True,
    )
    grid(
        sx[3],
        GRID_LABEL_W[3],
        ("CADD", "ClinVar", "SpliceAI", "AlphaMissense", "dbNSFP", "PhenotypeOrth."),
        pages,
        lambda i, j: "read" if 1 <= i <= 3 and j in (0, 3) else "skip",
        pick_rows=(1, 2, 3),
        pick_cols=(0, 3),
        head="page",
    )
    return s


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--render", action="store_true")
    args = parser.parse_args()
    path = HERE / "d-pushdown-proposal"
    workflow.save(panel(), path, WIDTH, HEIGHT, "D — Query pushdown")
    svg = path.with_suffix(".svg")
    head, _, tail = svg.read_text().partition("<desc>")
    svg.write_text(head + "<desc>" + DESCRIPTION + tail[tail.index("</desc>") :])
    print(f"Generated {path.name}.svg / .drawio", flush=True)
    if args.render:
        workflow.render(path, WIDTH, HEIGHT)


if __name__ == "__main__":
    main()
