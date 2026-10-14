#!/usr/bin/env python3
"""Native Figure 1D: annotation, remote BED overlap and gene aggregation.

Counts are from Supplementary Data, ``Beyond filter_vep: interval operations'',
in tgambin/vepyr at 5a6da1d62b865383e6b572cebfe336ce3de09cda. They refer to
HG002 chr22, GRCh38, Ensembl cache 116 and GIAB genome stratifications v3.6.
The frequency filter retains missing gnomADg_AF values as well as values <0.01.
Consequence fields are expanded together; variant/gene pairs are deduplicated
before overlap and distinct variants are counted per gene afterward.

The shortened URL on the panel is for legibility; SEGDUP_URL is the exact input
for polars_bio.scan_bed(). Remote input and Arrow batch exchange do not imply
that overlap indexes or the final group-by require no buffering.
"""

from __future__ import annotations

import build_figure as fig


WIDTH, HEIGHT = 2320, 460
INK, GREY, LIGHT = "#111111", "#555555", "#e6e6e6"
SEGDUP_URL = (
    "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/"
    "genome-stratifications/v3.6/GRCh38@all/SegmentalDuplications/"
    "GRCh38_segdups.bed.gz"
)
GENE_COUNTS = (("RIMBP3C", 6), ("PRODH", 4))


def panel() -> fig.Scene:
    s = fig.Scene()
    s._counter = 50000
    s.label(0, 14, 64, 44, "D", font_size=31, weight=700)
    s.label(74, 15, 1500, 42, "Beyond filter_vep", font_size=29, weight=700)
    s.label(
        1810,
        18,
        510,
        36,
        "HG002 chr22 · Ensembl 116",
        font_size=22,
        color=GREY,
        align="right",
    )

    annotate = s.box(
        0,
        120,
        340,
        140,
        "Annotate\nvepyr.annotate()\nPolars LazyFrame",
        font_size=26,
        stroke_width=1.8,
    )
    filtered = s.box(
        460,
        120,
        440,
        140,
        "Filter\nHIGH / MODERATE impact\ngnomADg AF < 1% or missing",
        font_size=26,
        stroke_width=1.8,
    )
    overlap = s.box(
        1020,
        120,
        340,
        140,
        "Overlap\npolars-bio\noverlap()",
        font_size=26,
        stroke_width=1.8,
    )
    aggregate = s.box(
        1480,
        120,
        360,
        140,
        "Aggregate\nGroup by gene\nCount distinct variants",
        font_size=26,
        stroke_width=1.8,
    )
    for left, right in (
        (annotate, filtered),
        (filtered, overlap),
        (overlap, aggregate),
    ):
        s.edge(left.x + left.width, 190, right.x, 190, source=left, target=right)
    s.edge(1840, 190, 1930, 190, source=aggregate)
    s.label(
        342, 132, 116, 50, "Arrow\nbatches", font_size=20, color=GREY, align="center"
    )
    for x, w, text in (
        (0, 340, "50,861 variants"),
        (460, 440, "23 variants"),
        (1020, 340, "10 variants"),
    ):
        s.label(x, 272, w, 36, text, font_size=26, weight=700, align="center")

    # This is an independent data input, not another vepyr plugin/cache.
    remote = s.box(460, 340, 900, 104, "", stroke_width=1.8, fill="#f3f3f3")
    s.label(
        480,
        349,
        860,
        34,
        "GIAB segmental duplications · BED over HTTPS",
        font_size=25,
        weight=700,
        align="center",
    )
    s.label(
        480,
        393,
        860,
        34,
        'pb.scan_bed("https://…/GRCh38_segdups.bed.gz")',
        font_size=23,
        mono=True,
        align="center",
    )
    # Route around the count label, then enter the overlap box from below.
    s.edge(
        1360,
        392,
        1325,
        260,
        via=((1405, 392), (1405, 320), (1325, 320)),
        source=remote,
        target=overlap,
    )

    s.label(1950, 78, 370, 34, "Variants per gene", font_size=26, weight=700)
    for i, (gene, count) in enumerate(GENE_COUNTS):
        y = 133 + i * 86
        s.label(1950, y, 200, 30, gene, font_size=25, weight=700)
        s.box(1950, y + 38, count * 50, 24, "", fill=INK, stroke_width=0)
        s.label(1963 + count * 50, y + 31, 45, 36, str(count), font_size=28, weight=700)
    return s
