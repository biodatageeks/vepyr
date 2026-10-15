#!/usr/bin/env python3
"""Figure 1D proposal: three tiers of annotation applied to the HG002 genome.

Replaces the chromosome 22 ``Beyond filter_vep'' panel with an overview of the
whole-genome analysis in ../hg002-tiered-annotation (Supplementary Data). All
numbers are read from that analysis' results/ directory, so the panel cannot
drift from the supplement:

    python build_tiered_annotation.py [--results DIR] [--render]

Exports d-tiered-annotation-proposal.{svg,drawio,png} (the panel alone) and
figure1-tiered-annotation-proposal.{svg,drawio,png} (the cache-workflow
figure with this panel in place of the previous panel D).
"""

from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path

import build_analytical_workflow
import build_cache_simplified_proposal as simplified
import build_cache_workflow_proposal as workflow
import build_figure as fig
import build_performance


HERE = Path(__file__).resolve().parent
RESULTS = HERE.parent / "hg002-tiered-annotation" / "results"
WIDTH, HEIGHT = 2320, 700
INK, GREY, LIGHT, SOFT = "#111111", "#555555", "#bdbdbd", "#f3f3f3"
DESCRIPTION = (
    "Three tiers of annotation applied to the GIAB HG002 v4.2.1 genome: the vepyr "
    "core cache and plugin caches feed vepyr.annotate(); ad hoc sources are read "
    "directly from their URLs by polars-bio and Polars. The funnel counts variants "
    "with a predicted loss-of-function consequence in a MANE Select transcript that "
    "remain after the filters of each tier; the bars give the share of each variant "
    "class in the two most LoF-intolerant gnomAD v4.1 LOEUF deciles. Numbers are "
    "read from the Supplementary Data analysis results."
)


def read_tsv(path: Path) -> list[dict[str, str]]:
    lines = path.read_text().splitlines()
    head = lines[0].split("\t")
    return [dict(zip(head, line.split("\t"))) for line in lines[1:]]


def panel(results: Path = RESULTS) -> fig.Scene:
    summary = json.loads((results / "summary.json").read_text())
    annotate = json.loads((results / "annotate.json").read_text())
    funnel = read_tsv(results / "funnel.tsv")
    loeuf = {r["category"]: r for r in read_tsv(results / "loeuf_summary.tsv")}
    lof = summary["lof"]

    s = fig.Scene()
    s._counter = 60000
    s.label(0, 14, 64, 44, "D", font_size=31, weight=700)
    s.label(74, 15, 1500, 42, "Three tiers of annotation", font_size=29, weight=700)
    s.label(1610, 18, 710, 36, f"HG002 genome · {summary['variants']:,} variants · Ensembl 116",
            font_size=22, color=GREY, align="right")

    # Tier boxes: refresh cadence and build step differ, the query does not.
    tiers = (
        ("1  Core cache", "Ensembl release · consequences, HGVS, AF\n~2 releases/year · build_cache()", "white"),
        ("2  Plugin caches", "ClinVar · SpliceAI · AlphaMissense · CADD\nweekly to yearly · build_plugin_cache()", "white"),
        ("3  Ad hoc sources by URL", "gnomAD constraint · ClinGen · Ensembl GFF3\nGIAB BED (HTTPS) · parental VCFs (S3)", SOFT),
    )
    boxes = []
    for i, (title, body, fill) in enumerate(tiers):
        y = 84 + i * 196
        b = s.box(0, y, 560, 168, f"{title}\n{body}", font_size=22, fill=fill,
                  align="left", stroke_width=1.8, dashed=(i == 2))
        boxes.append(b)

    engine = s.box(680, 152, 400, 140, "vepyr.annotate()\ncore + plugin fields\nPolars LazyFrame",
                   font_size=24, stroke_width=1.8)
    query = s.box(680, 440, 400, 168, "polars-bio · Polars\noverlap() · coverage()\njoins · group_by",
                  font_size=24, stroke_width=1.8)
    s.edge(560, 168, 680, 168, source=boxes[0], target=engine)
    s.edge(560, 364, 620, 270, via=((620, 364),), arrow=False, source=boxes[1])
    s.edge(620, 270, 680, 270, target=engine)
    s.edge(560, 560, 680, 560, source=boxes[2], target=query)
    s.edge(880, 292, 880, 440, source=engine, target=query)
    s.label(892, 340, 180, 60, "Arrow\nbatches", font_size=19, color=GREY)

    # Funnel: variants that keep at least one unflagged MANE pLoF record.
    s.label(1160, 70, 560, 34, "Loss of function in MANE Select", font_size=24, weight=700)
    steps = (
        ("pLoF consequence", int(funnel[2]["variants"]), "tier 1"),
        ("− last exon, AF ≥ 95%", int(funnel[3]["variants"]), "tier 1"),
        ("− SpliceAI, ClinVar benign", int(funnel[4]["variants"]), "tier 2"),
        ("− segdup, low-map, 50 bp", int(funnel[5]["variants"]), "tier 3"),
    )
    top = steps[0][1]
    for i, (name, n, tier) in enumerate(steps):
        y = 124 + i * 70
        s.label(1160, y, 320, 34, name, font_size=19)
        s.label(1160, y + 30, 120, 26, tier, font_size=16, color=GREY)
        width = max(4, 200 * n / top)
        s.box(1490, y + 6, width, 40, "", fill=INK if i == len(steps) - 1 else LIGHT, stroke_width=0)
        s.label(1500 + width, y + 2, 100, 44, f"{n:,}", font_size=22, weight=700)
    s.label(1160, 410, 600, 92,
            f"{lof['hc_variants']:,} high-confidence pLoF in {lof['hc_genes']:,} genes\n"
            f"{lof['hc_rare']:,} rare (AF < 1%) · {lof['hc_hom']:,} homozygous",
            font_size=20)
    plp = read_tsv(results / "clinvar_plp.tsv")
    if len(plp) <= 2:
        lines = []
        for r in plp:
            zyg = "hom" if r["GT"] == "1/1" else "het"
            asj = r.get("gnomADg_ASJ_AF") or ""
            fold = f" ({float(asj) / float(r['af']):.0f}× global)" if asj and float(r["af"]) else ""
            asj = f"\ngnomAD ASJ AF {float(asj):.1%}{fold}" if asj else ""
            lines.append(f"{r['SYMBOL']} {r['HGVSp'].split(':')[-1]} · {zyg} · {r['inheritance']}{asj}")
        clinvar = "ClinVar P/LP (≥1 star):\n" + "\n".join(lines)
    else:
        clinvar = f"ClinVar P/LP (≥1 star): {summary['clinvar_plp_variants']:,} variants"
    s.label(1160, 490, 640, 190,
            f"{clinvar}\n"
            f"{summary['clingen_genes_lt90']:,} of {summary['clingen_genes']:,} ClinGen genes "
            f"< 90% callable",
            font_size=20)

    # Selection in one genome: share in the two most constrained LOEUF deciles.
    s.label(1810, 70, 510, 34, "Genes most intolerant to LoF", font_size=24, weight=700)
    s.label(1810, 104, 510, 30, "% in LOEUF deciles 1–2 (gnomAD v4.1)", font_size=18, color=GREY)
    ref = 4.2 * 100 * float(loeuf["Rare synonymous (reference)"]["frac_constrained"])
    rows = (
        ("Rare synonymous", "Rare synonymous (reference)", LIGHT),
        ("Rare missense, AM pathogenic", "Rare missense, AlphaMissense likely pathogenic", GREY),
        ("All pLoF", "All pLoF, MANE Select", INK),
        ("Rare HC pLoF", "Rare HC pLoF (AF < 1%)", INK),
    )
    for i, (name, key, color) in enumerate(rows):
        r = loeuf.get(key)
        if r is None or r["n"] == "0":
            continue
        pct = 100 * float(r["frac_constrained"])
        y = 150 + i * 78
        s.label(1810, y, 420, 30, f"{name} (n={int(r['n']):,})", font_size=18)
        s.box(1810, y + 36, max(3, 4.2 * pct), 22, "", fill=color, stroke_width=0)
        s.label(1822 + max(4.2 * pct, ref + 4), y + 28, 90, 36, f"{pct:.0f}%", font_size=20, weight=700)
        if i:
            s.edge(1810 + ref, y + 32, 1810 + ref, y + 62, arrow=False, color=GREY, width=1.5)
    s.label(1810, 470, 510, 26, "│ = rare synonymous reference", font_size=15, color=GREY)

    mins = annotate["seconds"] / 60
    sources = json.loads((results / "sources.json").read_text())
    s.label(1810, 590, 510, 90,
            f"annotation with 4 plugins: {mins:.1f} min ({annotate['workers']} workers)\n"
            f"{len(sources)} ad hoc sources read by URL: {sum(x['seconds'] for x in sources):.0f} s",
            font_size=17, color=GREY)
    return s


def full_figure(performance_uri: str, results: Path) -> tuple[fig.Scene, int]:
    full = workflow.full_figure(performance_uri)
    old = {e.id for e in build_analytical_workflow.panel().elements}
    full.elements = [e for e in full.elements if e.id not in old]
    full.elements.extend(simplified.translated(panel(results), 40, 2230).elements)
    return full, 2230 + HEIGHT + 30


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=RESULTS)
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=HERE)
    args = parser.parse_args()
    perf = build_performance.build_svg()
    uri = "data:image/svg+xml;base64," + base64.b64encode(perf.encode("utf-8")).decode("ascii")
    full, full_height = full_figure(uri, args.results)
    exports = (
        ("d-tiered-annotation-proposal", panel(args.results), WIDTH, HEIGHT,
         "D — Three tiers of annotation (HG002)"),
        ("figure1-tiered-annotation-proposal", full, 2400, full_height,
         "Figure 1 — with three-tier annotation panel"),
    )
    for name, scene, width, height, title in exports:
        path = args.output_dir / name
        workflow.save(scene, path, width, height, title)
        # workflow.save() writes the description of the previous panel D.
        svg = path.with_suffix(".svg")
        head, _, tail = svg.read_text().partition("<desc>")
        svg.write_text(head + "<desc>" + DESCRIPTION + tail[tail.index("</desc>"):])
        print(f"Generated {name}.svg / .drawio", flush=True)
        if args.render:
            workflow.render(path, width, height)


if __name__ == "__main__":
    main()
