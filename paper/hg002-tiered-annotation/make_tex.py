#!/usr/bin/env python3
"""Turn results/ into LaTeX macros and tables used by hg002-tiered-annotation.tex.

Every number quoted in the document comes from here, so rerunning the analysis
and this script keeps text, tables and figures consistent.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl


HERE = Path(__file__).resolve().parent
RESULTS = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "results"
OUT = RESULTS / "tex"


def esc(text: object) -> str:
    s = "" if text is None else str(text)
    for a, b in (("\\", r"\textbackslash{}"), ("&", r"\&"), ("%", r"\%"), ("_", r"\_"),
                 ("#", r"\#"), ("$", r"\$"), ("{", r"\{"), ("}", r"\}"), ("~", r"\textasciitilde{}"),
                 (">", r"\textgreater{}"), ("<", r"\textless{}")):
        s = s.replace(a, b)
    return s


def num(n: float | int) -> str:
    return f"{n:,}".replace(",", "{,}") if isinstance(n, int) else f"{n}"


def read(name: str) -> pl.DataFrame:
    return pl.read_csv(RESULTS / name, separator="\t", infer_schema_length=100000)


def macros() -> None:
    s = json.loads((RESULTS / "summary.json").read_text())
    a = json.loads((RESULTS / "annotate.json").read_text())
    loeuf = read("loeuf_summary.tsv")
    flags = read("lof_flags.tsv")
    m = {
        "NVariants": num(s["variants"]),
        "NManeRecords": num(s["mane_records"]),
        "NPlof": num(s["lof"]["plof_variants"]),
        "NHc": num(s["lof"]["hc_variants"]),
        "NHcRare": num(s["lof"]["hc_rare"]),
        "NHcCommon": num(s["lof"]["hc_common"]),
        "NHcHom": num(s["lof"]["hc_hom"]),
        "NHcRareHom": num(s["lof"]["hc_rare_hom"]),
        "NHcGenes": num(s["lof"]["hc_genes"]),
        "NFlagged": num(s["lof"]["flagged_variants"]),
        "NPlp": num(s["clinvar_plp_variants"]),
        "NPlpHet": num(s["clinvar_plp_het"]),
        "NPlpHom": num(s["clinvar_plp_hom"]),
        "NPlpArHet": num(s["clinvar_plp_ar_het"]),
        "NBiallelic": num(s["biallelic_genes"]),
        "NManeGenes": num(s["mane_genes"]),
        "NManeGenesLtNinety": num(s["mane_genes_lt90"]),
        "NClinGenGenes": num(s["clingen_genes"]),
        "NClinGenLtNinety": num(s["clingen_genes_lt90"]),
        "NClinGenLtFifty": num(s["clingen_genes_lt50"]),
        "NClinGenLtNinetySegdup": num(s["clingen_lt90_segdup_gt50"]),
        "NClinGenLtNinetyDifficult": num(s["clingen_lt90_difficult_gt50"]),
        "ClinGenFalseDup": ", ".join(f"\\textit{{{g}}}" for g in sorted(s["clingen_falsedup"])),
        "AnnotateMin": f"{a['seconds'] / 60:.1f}",
        "AnnotateSec": f"{a['seconds']:.0f}",
        "Workers": str(a["workers"]),
        "TierThreeMin": f"{s['seconds']['tier3_reads'] / 60:.1f}",
        "AnalyzeMin": f"{s['seconds']['total'] / 60:.1f}",
        "VepyrVersion": s["versions"]["vepyr"],
        "PolarsVersion": s["versions"]["polars"],
        "PolarsBioVersion": s["versions"]["polars_bio"],
    }
    for row in flags.iter_rows(named=True):
        key = "".join(p.capitalize() for p in row["flag"].replace("50bp", "fiftybp").split("_"))
        m[f"Flag{key}"] = num(row["variants"])
    tags = {
        "Rare synonymous (reference)": "Syn",
        "Rare missense, AlphaMissense likely benign": "AmLb",
        "Rare missense, AlphaMissense likely pathogenic": "AmLp",
        "All pLoF, MANE Select": "AllPlof",
        "Common HC pLoF (AF >= 1%)": "HcCommon",
        "Rare HC pLoF (AF < 1%)": "HcRare",
        "Flagged pLoF": "Flagged",
    }
    for row in loeuf.iter_rows(named=True):
        t = tags[row["category"]]
        m[f"Loeuf{t}N"] = num(row["n"])
        m[f"Loeuf{t}Pct"] = f"{100 * row['frac_constrained']:.1f}" if row["n"] else "--"
        m[f"Loeuf{t}Or"] = f"{row['odds_ratio_vs_synonymous']:.2f}" if row["n"] else "--"
        p = row["fisher_p"]
        m[f"Loeuf{t}P"] = (f"{p:.1e}".replace("e-0", r"\times10^{-").replace("e-", r"\times10^{-") + "}"
                           if row["n"] and p < 0.001 else (f"{p:.3f}" if row["n"] else "--"))
        m[f"Loeuf{t}Median"] = f"{row['median_LOEUF']:.2f}" if row["n"] else "--"
    funnel = read("funnel.tsv")["variants"].to_list()
    m.update(NCoding=num(funnel[1]), NAfterTierOne=num(funnel[3]), NAfterTierTwo=num(funnel[4]),
             NAfterTierThree=num(funnel[5]))
    src = json.loads((RESULTS / "sources.json").read_text())
    m["NRemoteSources"] = str(len(src))
    m["RemoteSec"] = f"{sum(r['seconds'] for r in src):.0f}"
    m["RemoteRows"] = f"{sum(r['rows'] for r in src) / 1e6:.1f}"
    rare = read("hc_rare_plof.tsv").with_columns(
        asj=pl.max_horizontal(pl.col("gnomADe_ASJ_AF").cast(pl.Float64), pl.col("gnomADg_ASJ_AF").cast(pl.Float64)))
    m["NRareAsjCommon"] = str(rare.filter(pl.col("asj") >= 0.01).height)
    m["NRareMaternal"] = str(rare.filter(pl.col("inheritance") == "maternal").height)
    m["NRarePaternal"] = str(rare.filter(pl.col("inheritance") == "paternal").height)
    plp = read("clinvar_plp.tsv")
    if plp.height:
        r = plp.row(0, named=True)
        m["PlpGene"] = r["SYMBOL"]
        m["PlpHgvsp"] = esc(r["HGVSp"].split(":")[-1])
        m["PlpAf"] = f"{100 * r['af']:.3f}"
        m["PlpAsj"] = f"{100 * max(v for v in (r['gnomADe_ASJ_AF'], r['gnomADg_ASJ_AF']) if v is not None):.2f}"
        m["PlpFold"] = f"{max(v for v in (r['gnomADe_ASJ_AF'], r['gnomADg_ASJ_AF']) if v is not None) / r['af']:.0f}"
        m["PlpParent"] = {"paternal": "father", "maternal": "mother"}.get(r["inheritance"], r["inheritance"])
    lines = [f"\\newcommand{{\\{k}}}{{{v}}}" for k, v in m.items()]
    (OUT / "macros.tex").write_text("\n".join(lines) + "\n")


def table_sources() -> None:
    src = json.loads((RESULTS / "sources.json").read_text())
    label = {
        "segdups": "GIAB v3.6 segmental duplications (BED)",
        "lowmap": "GIAB v3.6 low mappability, all (BED)",
        "difficult": "GIAB v3.6 all difficult regions (BED)",
        "falsedup": "GIAB v3.6 GRCh38 false duplications (BED)",
        "hg002_bed": "HG002 v4.2.1 benchmark regions (BED)",
        "hg003_bed": "HG003 v4.2.1 benchmark regions (BED)",
        "hg004_bed": "HG004 v4.2.1 benchmark regions (BED)",
        "constraint": "gnomAD v4.1 gene constraint (TSV)",
        "clingen": "ClinGen gene--disease validity (CSV)",
        "gff3": "Ensembl 116 GFF3 (gene models)",
        "hg003_vcf": "HG003 (father) v4.2.1 benchmark VCF",
        "hg004_vcf": "HG004 (mother) v4.2.1 benchmark VCF",
    }
    rows = [f"{esc(label[r['source']])} & {esc(r['reader'])} & {num(r['rows'])} & {r['seconds']:.0f} \\\\"
            for r in src]
    (OUT / "tab_sources.tex").write_text("\n".join(rows) + "\n")


def short_hgvs(p: str | None, c: str | None) -> str:
    if p:
        return esc(p.split(":", 1)[-1])
    return esc((c or "").split(":", 1)[-1])


def table_variants(name: str, out: str, limit: int | None = None) -> None:
    df = read(name)
    rows = []
    for r in df.iter_rows(named=True):
        csq = (r["Consequence"] or "").split("&")[0].replace("_variant", "").replace("_", " ")
        stars = ("4" if "practice_guideline" in (r["ClinVar_CLNREVSTAT"] or "") else
                 "3" if "expert_panel" in (r["ClinVar_CLNREVSTAT"] or "") else
                 "2" if "multiple_submitters" in (r["ClinVar_CLNREVSTAT"] or "") else
                 "1" if "single_submitter" in (r["ClinVar_CLNREVSTAT"] or "") else "")
        clnsig = (r["ClinVar_CLNSIG"] or "").split("&")[0] \
            .replace("Conflicting_classifications_of_pathogenicity", "conflicting") \
            .replace("Pathogenic/Likely_pathogenic", "P/LP") \
            .replace("Likely_pathogenic", "LP").replace("Pathogenic", "P").replace("Likely_benign", "LB") \
            .replace("Benign", "B").replace("_", " ")
        if clnsig and stars:
            clnsig += f" ({stars}\\textbigstar)"
        af = r["af"]
        asj = max((v for v in (r.get("gnomADe_ASJ_AF"), r.get("gnomADg_ASJ_AF")) if v is not None), default=None)
        fmt = lambda v: "--" if v is None else "0" if v == 0 else f"{v:.1e}" if v < 0.001 else f"{v:.3f}"  # noqa: E731
        rows.append(" & ".join([
            f"\\textit{{{esc(r['SYMBOL'])}}}",
            f"{esc(r['chrom'].removeprefix('chr'))}:{r['start']:,}".replace(",", "{,}"),
            short_hgvs(r["HGVSp"], r["HGVSc"]),
            esc(csq),
            "hom" if r["GT"] == "1/1" else "het",
            fmt(af),
            fmt(asj),
            clnsig,
            esc(r["MOI"] or ""),
            esc(r["inheritance"] or ""),
        ]) + " \\\\")
    if limit:
        rows = rows[:limit]
    (OUT / out).write_text("\n".join(rows) + "\n")


def short_disease(text: str | None, width: int = 44) -> str:
    first = (text or "").split(";")[0]
    if len(first) <= width:
        return first
    return first[:width].rsplit(" ", 1)[0] + "\u2026"


def table_callability() -> None:
    cal = read("callability.tsv").filter(pl.col("ClinGen").is_not_null(), pl.col("callable_frac") < 0.9)
    rows = []
    for r in cal.sort("callable_frac").head(25).iter_rows(named=True):
        rows.append(" & ".join([
            f"\\textit{{{esc(r['SYMBOL'])}}}", num(r["cds_bp"]), f"{100 * r['callable_frac']:.0f}",
            f"{100 * r['segdup_frac']:.0f}", f"{100 * r['difficult_frac']:.0f}", esc(r["MOI"]),
            esc(short_disease(r["ClinGen_disease"])),
        ]) + " \\\\")
    (OUT / "tab_callability.tex").write_text("\n".join(rows) + "\n")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    macros()
    table_sources()
    table_variants("clinvar_plp.tsv", "tab_plp.tex")
    table_variants("hc_rare_plof.tsv", "tab_hc_rare.tex")
    table_callability()
    print(f"LaTeX fragments written to {OUT}")


if __name__ == "__main__":
    main()
