#!/usr/bin/env python3
"""Figure 1D2: annotation and filtering in one vepyr query against VEP followed by filter_vep.

One row per example query (HG002 chr22, merged cache, 8 processes or workers), on a shared
logarithmic time axis:

* VEP + filter_vep: VEP 116 --fork 7 on the whole of chr22, then filter_vep (hollow square);
  for the region query R1 the baseline extracts the region with bcftools first (Supplementary
  Table 22), and its filter_vep time is not reported separately;
* filter_vep alone (hollow circle);
* vepyr: annotation and the Polars filter in one lazy query, the slowest of its three outputs
  (collect, sink_vcf, sink_parquet), as in Supplementary Tables 21-26 (filled circle).

The values are read from data/raw/polars_chr22_v0110_e2e{,_sliced}.csv, the files that also
give Supplementary Tables 21-26 (tools/make_numbers.py, polars_values). Speed-ups are computed
from the unrounded times and rounded half up, as everywhere in the paper.

    python build_e2e.py [--render]
"""

from __future__ import annotations

import argparse
import csv
import math
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import build_cache_workflow_proposal as workflow
import build_figure as fig

HERE = Path(__file__).resolve().parent
RAW = HERE.parents[1] / "data/raw"
E2E, SLICED = (
    RAW / "polars_chr22_v0110_e2e.csv",
    RAW / "polars_chr22_v0110_e2e_sliced.csv",
)
PROCESSES = "8"
# (query, label, description); R1 is compared with VEP on the region alone (bcftools view -t).
EXAMPLES = (
    ("R1", "R1", "5 Mb region (VEP on region only)"),
    ("Q5", "Q5", "high impact"),
    ("Q10", "Q10", "rare, protein-altering, canonical"),
    ("P4", "P4", "plugin: ClinVar pathogenic"),
)
WIDTH = 432
INK, GREY, LIGHT, VEP = "#111111", "#666666", "#dddddd", "#666666"
LO, HI = 0.5, 500.0  # axis limits, s


def half_up(x: float, places: int = 0) -> Decimal:
    return Decimal(repr(x)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def secs(x: float) -> str:
    """Seconds as in Supplementary Tables 22-26 (make_numbers.secs): 2 decimals below 10 s,
    1 below 1,000 s."""
    return f"{half_up(x, 2 if x < 10 else 1 if x < 1000 else 0):,} s"


def load() -> list[dict]:
    e2e = {
        r["query"]: r
        for r in csv.DictReader(E2E.open(newline=""))
        if r["processes"] == PROCESSES
    }
    sliced = {
        r["query"]: r
        for r in csv.DictReader(SLICED.open(newline=""))
        if r["processes"] == PROCESSES
    }
    rows = []
    for query, label, desc in EXAMPLES:
        r = e2e[query]
        vepyr = float(r["vepyr_slowest_s"])
        if query in sliced:
            vep, filt = float(sliced[query]["vep_sliced_total_s"]), None
            vepyr = float(sliced[query]["vepyr_slowest_s"])
        else:
            vep, filt = float(r["vep_total_s"]), float(r["vep_filter_s"])
        rows.append(
            {"label": label, "desc": desc, "vep": vep, "filter": filt, "vepyr": vepyr}
        )
    return rows


def panel(
    code_label: str = "D2", width: float = WIDTH, height: float = 694
) -> fig.Scene:
    s = fig.Scene()
    s._counter = 75000
    rows = load()
    s.label(0, 14, 64, 44, code_label, font_size=31, weight=700)
    s.label(84, 15, width - 84, 42, "Annotate + filter", font_size=29, weight=700)
    s.label(0, 58, width, 30, "chr22, 8 processes / workers", font_size=17, color=GREY)

    def dot(x, y, r, fill, stroke, square=False):
        s.box(
            x - r,
            y - r,
            2 * r,
            2 * r,
            "",
            fill=fill,
            stroke=stroke,
            stroke_width=2.2,
            radius=0 if square else r,
        )

    # Legend.
    ly = 112
    for k, (text, fill, stroke, square) in enumerate(
        (
            ("VEP, then filter_vep", "white", VEP, True),
            ("filter_vep alone", "white", VEP, False),
            ("vepyr, one query", INK, INK, False),
        )
    ):
        y = ly + k * 30
        dot(10, y, 7, fill, stroke, square)
        s.label(26, y - 15, width - 26, 30, text, font_size=17, color=GREY)

    plot_x, plot_w = 8, width - 78  # speed-up labels to the right
    top = 222
    row_h = (height - top - 66) / len(rows)  # axis labels below the last row

    def px(t: float) -> float:
        if not LO <= t <= HI:
            raise ValueError(f"time outside the axis: {t}")
        return plot_x + plot_w * math.log(t / LO) / math.log(HI / LO)

    bottom = top + len(rows) * row_h
    for tick, text in ((1, "1"), (10, "10"), (100, "100")):
        x = px(tick)
        s.edge(x, top - 6, x, bottom, color=LIGHT, width=1.2, arrow=False)
        s.label(
            x - 40, bottom + 6, 80, 26, text, font_size=16, color=GREY, align="center"
        )
    s.edge(plot_x, bottom, plot_x + plot_w, bottom, color=INK, width=1.4, arrow=False)
    s.label(
        0,
        bottom + 34,
        width,
        28,
        "wall time (s), log scale",
        font_size=16,
        color=GREY,
        align="center",
    )

    for i, r in enumerate(rows):
        y0 = top + i * row_h
        s.label(
            0, y0, width, 26, f"{r['label']}  {r['desc']}", font_size=16, weight=700
        )
        y = y0 + 48
        s.edge(px(r["vepyr"]), y, px(r["vep"]), y, color=LIGHT, width=4, arrow=False)
        if r["filter"] is not None:
            dot(px(r["filter"]), y, 7, "white", VEP)
        dot(px(r["vep"]), y, 8, "white", VEP, square=True)
        dot(px(r["vepyr"]), y, 8, INK, INK)
        s.label(
            px(r["vepyr"]) - 50,
            y + 10,
            100,
            24,
            secs(r["vepyr"]),
            font_size=14,
            color=GREY,
            align="center",
        )
        s.label(
            px(r["vep"]) - 50,
            y + 10,
            100,
            24,
            secs(r["vep"]),
            font_size=14,
            color=GREY,
            align="center",
        )
        s.label(
            plot_x + plot_w + 4,
            y - 15,
            width - plot_x - plot_w - 4,
            30,
            f"{half_up(r['vep'] / r['vepyr'])}×",
            font_size=19,
            weight=700,
            align="right",
        )
    return s


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--render", action="store_true")
    args = parser.parse_args()
    path = HERE / "d2-e2e-proposal"
    workflow.save(panel(), path, WIDTH, 694, "D2 — Annotate + filter")
    print(f"Generated {path.name}.svg / .drawio", flush=True)
    if args.render:
        workflow.render(path, WIDTH, 694)


if __name__ == "__main__":
    main()
