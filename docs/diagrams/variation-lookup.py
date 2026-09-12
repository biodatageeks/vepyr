#!/usr/bin/env python3
"""Generate the variation-lookup figure (docs/diagrams/variation-lookup.svg).

Every quantity in MEASURED below comes from `variation-lookup-measure.py`, run against
the chr22 shard of the GRCh38 merged cache for Ensembl VEP 116 and the HG002 benchmark
VCF. Re-run that script and update MEASURED whenever the cache is rebuilt; nothing here
is estimated. The cell strips in panel B are schematic and are labelled as such.

Usage:  python docs/diagrams/variation-lookup.py
Then:   rsvg-convert -f pdf -o variation-lookup.pdf docs/diagrams/variation-lookup.svg
"""

import pathlib

# ---- measured on the GRCh38 merged cache for Ensembl VEP 116, chr22 shard,
# ---- probed with the first 5,000 records of the HG002 chr22 benchmark VCF.
MEASURED = dict(
    shard_rows=15_148_238,
    pages=15_381,
    tier0_rows=821_809,
    tier0_pct=5.4,
    probes=5_000,
    matched=6_178,
    tiered_pages=220,
    untiered_pages=920,
    tiered_per_page=28.1,
    untiered_per_page=6.7,
    t0_pages=83,
    t0_rows=5_946,
    t0_per_page=71.6,
    t1_pages=137,
    t1_rows=232,
    t1_per_page=1.7,
    common_share=96,
    tiered_decoded=207_732,
    untiered_decoded=928_028,
    decoded_ratio=4.5,
)

INK, MUTED, RULE = "#111111", "#555555", "#B0B0B0"
BLUE, BLUEFILL = "#0072B2", "#D6E6F5"
ORANGE, ORANGEFILL = "#D55E00", "#F7D9C4"
GREY = "#BFBFBF"

W = 1000
o = []


def t(x, y, s, fill=INK, size=None, bold=False, anchor=None):
    a = f' font-size="{size}"' if size else ""
    b = ' font-weight="bold"' if bold else ""
    an = f' text-anchor="{anchor}"' if anchor else ""
    o.append(f'  <text x="{x}" y="{y}"{a}{b}{an} fill="{fill}">{s}</text>')


def rect(x, y, w, h, fill="#FFFFFF", stroke=INK, sw=1.2, extra=""):
    o.append(
        f'  <rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{extra}/>'
    )


def line(x1, y1, x2, y2, stroke=RULE, sw=1, dash=None):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    o.append(
        f'  <line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{stroke}" stroke-width="{sw}"{d}/>'
    )


def arrow(d, stroke=INK, sw=1.2, marker="arr"):
    o.append(
        f'  <path d="{d}" stroke="{stroke}" stroke-width="{sw}" fill="none" marker-end="url(#{marker})"/>'
    )


# ---------------------------------------------------------------- Panel A
t(20, 34, "A", size=13, bold=True)
t(36, 34, "Input buffer", size=12, bold=True)
t(20, 50, "≤ 5,000 chr22 records, position-ordered", MUTED)
rect(20, 58, 210, 118)
t(30, 76, "position", MUTED)
t(140, 76, "ref &gt; alt", MUTED)
line(30, 82, 220, 82)
for i, (p, a) in enumerate(
    [
        ("16,050,075", "A &gt; G"),
        ("16,050,115", "G &gt; A"),
        ("16,050,213", "C &gt; T"),
        ("16,050,300", "CT &gt; C"),
    ]
):
    t(30, 98 + 16 * i, p)
    t(140, 98 + 16 * i, a)
t(30, 166, "⋮")
arrow("M125 176 V200")
t(134, 186, "one or more probe", MUTED)
t(134, 200, "positions per record", MUTED)
rect(20, 202, 210, 46, BLUEFILL, BLUE, 1.4)
t(30, 221, "probe positions", bold=True)
o.append(
    '  <text x="30" y="238">p<tspan font-size="8" baseline-shift="sub">1</tspan> &lt; '
    'p<tspan font-size="8" baseline-shift="sub">2</tspan> &lt; … &lt; '
    'p<tspan font-size="8" baseline-shift="sub">k</tspan>, unique</text>'
)

# ---------------------------------------------------------------- Panel B
t(270, 34, "B", size=13, bold=True)
t(286, 34, "Why the shard is clustered by tier", size=12, bold=True)
t(
    270,
    50,
    "the chr22 shard, one Parquet file: each cell below is one cached variant, each block of 12 cells one data page",
    MUTED,
)

X0, CW, NCELL, PAGE = 270, 6.75, 96, 12


def strip(y, common_idx, rare_probe_idx):
    """Draw one layout. common_idx: cells holding a probed common variant."""
    hit_pages = sorted({i // PAGE for i in list(common_idx) + list(rare_probe_idx)})
    for pg in hit_pages:  # read-page tint behind the cells
        rect(X0 + pg * PAGE * CW, y - 4, PAGE * CW, 22, BLUEFILL, BLUE, 1.4)
    for i in range(NCELL):
        x = X0 + i * CW
        if i in common_idx:
            fill, stroke = ORANGE, ORANGE
        elif i in rare_probe_idx:
            fill, stroke = "#FFFFFF", ORANGE
        else:
            fill, stroke = GREY, GREY
        o.append(
            f'  <rect x="{x:.2f}" y="{y}" width="{CW - 0.6:.2f}" height="14" fill="{fill}" stroke="{stroke}" stroke-width="0.5"/>'
        )
    for pg in range(1, NCELL // PAGE):  # page boundaries
        line(X0 + pg * PAGE * CW, y - 4, X0 + pg * PAGE * CW, y + 18, INK, 1)
    return hit_pages


# counterfactual: common variants sprinkled through the file
scattered = [7, 23, 41, 58, 77, 90]
rare_scattered = [66]
t(270, 72, "sorted by position only", bold=True)
t(410, 72, "(counterfactual)", MUTED)
hp1 = strip(78, scattered, rare_scattered)
t(918, 72, "schematic", MUTED, anchor="end")

# as shipped: the same common variants packed into tier 0
packed = [0, 1, 2, 3, 4, 5]
rare_packed = [66]
t(270, 118, "as shipped: tier 0 first, then position", bold=True)
hp2 = strip(124, packed, rare_packed)
line(X0 + 6 * CW, 116, X0 + 6 * CW, 148, INK, 1.4, "3 3")
t(
    X0 + 6 * CW + 4,
    158,
    "tier seam: tier 0 is 5.4% of the rows, so it ends inside the first page",
    MUTED,
)
t(918, 118, "schematic", MUTED, anchor="end")

# key
ky = 176
o.append(
    f'  <rect x="270" y="{ky - 8}" width="10" height="10" fill="{ORANGE}" stroke="{ORANGE}" stroke-width="0.5"/>'
)
t(286, ky, "probed, common (allele frequency ≥ 1%)")
o.append(
    f'  <rect x="500" y="{ky - 8}" width="10" height="10" fill="#FFFFFF" stroke="{ORANGE}" stroke-width="1"/>'
)
t(516, ky, "probed, rare")
o.append(
    f'  <rect x="590" y="{ky - 8}" width="10" height="10" fill="{GREY}" stroke="{GREY}" stroke-width="0.5"/>'
)
t(606, ky, "not probed")
o.append(
    f'  <rect x="676" y="{ky - 8}" width="10" height="10" fill="{BLUEFILL}" stroke="{BLUE}" stroke-width="1"/>'
)
t(692, ky, "page that must be read")

M = MEASURED
rect(270, 182, 648, 76, "#FFFFFF", INK, 1.2)
t(280, 199, "measured", bold=True)
t(
    340,
    199,
    f"one real 5,000-record chr22 buffer, {M['matched']:,} matches: the locate read of C, over the position column",
    MUTED,
)
t(280, 217, "row layout", MUTED)
t(545, 217, f"pages read of {M['pages']:,}", MUTED)
t(690, 217, "position rows scanned", MUTED)
t(908, 217, "matches per page", MUTED, anchor="end")
t(280, 233, "sorted by position only")
t(545, 233, f"{M['untiered_pages']:,}")
t(690, 233, f"{M['untiered_decoded']:,}")
t(908, 233, f"{M['untiered_per_page']}", anchor="end")
t(280, 249, "as shipped, tier 0 first")
t(545, 249, f"{M['tiered_pages']:,}", BLUE)
t(690, 249, f"{M['tiered_decoded']:,}", BLUE)
t(908, 249, f"{M['tiered_per_page']}", BLUE, anchor="end")

rect(270, 262, 648, 44, ORANGEFILL, ORANGE, 1.4)
t(280, 280, f"{M['common_share']}% of the matches are common", bold=True)
t(
    520,
    280,
    f"{M['t0_pages']} tier-0 pages carry {M['t0_rows']:,} of them, {M['t0_per_page']} rows per page",
)
t(280, 296, f"the rare {100 - M['common_share']}% still scatter")
t(
    520,
    296,
    f"{M['t1_pages']} tier-1 pages carry {M['t1_rows']} rows, {M['t1_per_page']} rows per page",
)

# page directory + candidate pages
arrow("M230 225 H250 V344 H268", BLUE, 1.4, "arr-b")
rect(270, 318, 300, 54)
t(280, 336, "page directory", bold=True)
t(280, 352, "from the file footer, built once per chromosome", MUTED)
t(280, 366, "and shared by every worker on that contig", MUTED)
arrow("M570 345 H608")
rect(610, 318, 308, 54, BLUEFILL, BLUE, 1.4)
t(620, 336, "candidate pages", bold=True)
t(620, 352, "binary search per probe position within each tier run;", MUTED)
t(620, 366, "adjacent pages merge into row ranges (select / skip)", MUTED)

# writer properties
rect(270, 384, 648, 192)
t(280, 402, "how the shard is written", bold=True)
t(520, 402, "→ what it buys at lookup", MUTED)
line(280, 408, 908, 408)
rows = [
    (
        "rows clustered by tier,",
        "then sorted by position",
        "page min / max ascend within a run, so a probe is found by",
        "binary search; the directory keeps one run per tier",
    ),
    (
        "tier 0 = any position where",
        "max(MAF, AF, gnomADg, gnomADe) ≥ 1%,",
        "the measured block above: for the same probes, 4.2× fewer pages",
        "and 4.5× fewer position rows decoded to locate the 6,178 matches",
    ),
    (
        "plus its ± 1 bp neighbours; 5.4% of rows",
        None,
        "frequency is the only criterion, and it is decided per position:",
        "every allele at a common position joins tier 0, however rare",
    ),
    (
        "pages of 1,024 rows (the default),",
        "~2.4 KiB compressed; page statistics on",
        "footer column + offset indexes give each page's position",
        "range and first row, so skipping is decided from metadata",
    ),
    (
        "no dictionary encoding; zstd(3)",
        None,
        "a selected page decodes alone; with a dictionary, reading one row",
        "first decodes its column chunk's dictionary, built over 10⁶ rows",
    ),
]
y = 426
for l1, l2, r1, r2 in rows:
    t(280, y, l1)
    if l2:
        t(280, y + 14, l2)
    t(520, y, r1, BLUE)
    t(520, y + 14, r2, BLUE)
    y += 36

line(20, 598, 980, 598, "#DDDDDD")

# ---------------------------------------------------------------- Panel C
t(20, 624, "C", size=13, bold=True)
t(36, 624, "Two reads of the shard: locate, then take", size=12, bold=True)
rect(20, 634, 450, 194, "#FFFFFF", BLUE, 1.4)
t(30, 653, "Locate · one column, many rows", bold=True)
t(30, 668, "this is the scan counted in panel B", BLUE)
t(30, 683, "the position column alone, every row in the candidate page ranges", MUTED)
t(30, 697, "nearby page byte ranges are coalesced into single reads", MUTED)
t(30, 715, "file row", MUTED)
t(110, 715, "position", MUTED)
line(30, 721, 250, 721)
tbl = [
    ("612 352", "16,050,001", False),
    ("612 353", "16,050,075", True),
    ("612 354", "16,050,075", True),
    ("612 355", "16,050,082", False),
    ("612 356", "16,050,213", True),
    ("612 357", "16,050,213", True),
]
for i2, (fr, pos, hit) in enumerate(tbl):
    yy = 735 + 14 * i2
    t(30, yy, fr)
    t(110, yy, pos)
    if hit:
        o.append(f'  <circle cx="200" cy="{yy - 4}" r="3.5" fill="{BLUE}"/>')
t(30, 818, "⋮")
arrow("M270 727 V812", BLUE, 1.6, "arr-b")
t(282, 743, "monotonic cursor", BLUE, bold=True)
t(282, 759, "advances one file row per")
t(282, 773, "streamed value, never backwards")
t(282, 795, "● position is in the probe set")
t(282, 809, "→ its file row is recorded")
arrow("M470 721 H508")
rect(510, 684, 90, 112, BLUEFILL, BLUE, 1.4)
t(520, 703, "row offsets", bold=True)
for i2, v in enumerate(["612 353", "612 354", "612 356", "612 357"]):
    t(520, 724 + 15 * i2, v)
t(520, 787, "⋮ ascending", MUTED)
arrow("M600 721 H638")
rect(640, 634, 310, 194, "#FFFFFF", BLUE, 1.4)
t(650, 653, "Take · many columns, few rows", bold=True)
t(650, 668, "the subset of columns the run needs, only the matched rows", MUTED)
t(650, 692, "position", MUTED)
t(740, 692, "alleles", MUTED)
t(800, 692, "identifier", MUTED)
t(890, 692, "…", MUTED)
line(650, 698, 940, 698)
for i2, (pp, aa, rr) in enumerate(
    [
        ("16,050,075", "A/G", "rs587697622"),
        ("16,050,075", "A/T", "rs…"),
        ("16,050,213", "C/A", "rs…"),
        ("16,050,213", "C/T", "rs…"),
    ]
):
    yy = 716 + 16 * i2
    t(650, yy, pp)
    t(740, yy, aa)
    t(800, yy, rr)
t(650, 782, "⋮")
t(650, 804, "the only rows that leave the shard", BLUE, bold=True)

line(20, 848, 980, 848, "#DDDDDD")

# ---------------------------------------------------------------- Panel D
t(20, 864, "D", size=13, bold=True)
t(36, 864, "Join back to the buffer", size=12, bold=True)
rect(20, 874, 210, 104)
t(30, 893, "input buffer", bold=True)
for i2, (pp, aa) in enumerate(
    [
        ("16,050,075", "A &gt; G"),
        ("16,050,115", "G &gt; A"),
        ("16,050,213", "C &gt; T"),
        ("16,050,300", "CT &gt; C"),
    ]
):
    t(30, 913 + 16 * i2, pp)
    t(140, 913 + 16 * i2, aa)
arrow("M230 926 H268")
rect(270, 874, 300, 104)
t(280, 893, "allele matching per record", bold=True)
t(280, 913, "rows at its probe positions are accepted when")
t(280, 929, "• the record is not flagged as failed")
t(280, 945, "• the record interval overlaps the variant")
t(280, 961, "• the alleles match exactly (first match wins)")
arrow("M570 926 H608")
rect(610, 874, 340, 104, BLUEFILL, BLUE, 1.4)
t(620, 893, "buffer + cache columns · one row in, one row out", bold=True)
for i2, (aa, rr) in enumerate(
    [
        ("A &gt; G", "rs587697622 and its cache columns"),
        ("G &gt; A", "no record at this position → null"),
        ("C &gt; T", "C/A rejected, C/T matched"),
        ("CT &gt; C", "matched at its shifted probe position"),
    ]
):
    t(620, 913 + 16 * i2, aa)
    t(700, 913 + 16 * i2, rr)
arrow("M420 978 V1000", "#E69F00", 1.6, "arr-o")
rect(270, 1002, 680, 44, "#FBEEDC", "#E69F00", 1.4)
o.append(
    '  <text x="280" y="1021"><tspan font-weight="bold">co-located variants</tspan> at the same positions are collected '
    "separately for the transcript annotation stage:</text>"
)
t(280, 1037, "existing identifiers, allele frequencies, clinical significance")

H = 1066

ARIA = (
    "Four panels. A: one lookup slice of up to 5,000 position-ordered VCF records is reduced to a sorted unique set of probe positions. "
    "B: why the shard is clustered by tier. Two schematic strips of cells show the same probe set against two layouts: sorted by position alone "
    "the probed common variants are sprinkled through the file, while clustering tier 0 first packs them together. A measured block gives the "
    "real figures for one 5,000-record chromosome 22 buffer against this shard: 920 pages read and 928,028 rows decoded without tiering, against "
    "220 pages and 207,732 rows with it, for the same 6,178 matches. A table lists how the shard is written and what each choice buys at lookup. "
    "C: two reads that differ in both axes, one column across many rows to locate the offsets, then many columns across few rows to take the "
    "payload. D: those rows are joined back to the buffer by allele matching, giving one output row per input record, with co-located variants "
    "collected separately for the transcript annotation stage."
)

HDR = 26
H += HDR
head = (
    f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
    f'font-family="Helvetica, Arial, sans-serif" font-size="11" fill="{INK}" role="img" aria-label="{ARIA}">\n'
    "  <defs>\n"
    f'    <marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#333333"/></marker>\n'
    f'    <marker id="arr-b" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="{BLUE}"/></marker>\n'
    '    <marker id="arr-o" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#E69F00"/></marker>\n'
    "  </defs>\n"
    f'  <rect x="0" y="0" width="{W}" height="{H}" fill="#FFFFFF"/>\n'
)
header = (
    f'  <text x="20" y="18" font-size="11" fill="{MUTED}">'
    'Worked example throughout: <tspan font-weight="bold" fill="#111111">chromosome 22</tspan>'
    " of the GRCh38 merged cache for Ensembl VEP 116, probed with the HG002 benchmark VCF.</text>\n"
)
body = f'  <g transform="translate(0,{HDR})">\n' + "\n".join(o) + "\n  </g>\n"
pathlib.Path(__file__).with_name("variation-lookup.svg").write_text(
    head + header + body + "</svg>\n"
)
print("wrote", pathlib.Path(__file__).with_name("variation-lookup.svg"))
