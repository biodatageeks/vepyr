#!/usr/bin/env python3
"""Three tiers of annotation applied to the GIAB HG002 genome.

Tier 1  vepyr core cache (Ensembl release 116, ``--everything`` fields)
Tier 2  vepyr plugin caches (ClinVar, AlphaMissense, SpliceAI, CADD)
Tier 3  ad hoc sources, read directly from their URLs on every run with
        polars-bio (BED, VCF) or Polars (TSV/CSV/GFF3 text); nothing from
        tier 3 is downloaded to disk or converted into a cache.

    python analysis.py annotate   # tiers 1+2: annotated frame -> Parquet
    python analysis.py analyze    # tier 3 + downstream analysis -> results/

Paths to local inputs are taken from HG002_DATA (default: ../data_vepyr
layout used on the authoring machine).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import time
from pathlib import Path

import polars as pl
import polars_bio as pb
import vepyr
from scipy.stats import fisher_exact


HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
DATA = Path(os.environ.get("HG002_DATA", "/Users/tgambin/workspace/data_vepyr"))
VCF = DATA / "hg002" / "HG002_v4.2.1.split.vcf.gz"
CACHE = DATA / "vepyr_cache" / "116_GRCh38_ensembl"
PLUGIN_ROOT = DATA / "vepyr_plugin_cache"
FASTA = DATA / "Homo_sapiens.GRCh38.dna.primary_assembly.fa"
ANNOTATED = DATA / "hg002" / "HG002_v4.2.1.vepyr116.plugins.parquet"
PLUGINS = ["clinvar", "alphamissense", "spliceai", "cadd"]
WORKERS = 8

GIAB = "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release"
TRIO = f"{GIAB}/AshkenazimTrio"
STRAT = f"{GIAB}/genome-stratifications/v3.6/GRCh38@all"
URL = {
    "segdups": f"{STRAT}/SegmentalDuplications/GRCh38_segdups.bed.gz",
    "lowmap": f"{STRAT}/Mappability/GRCh38_lowmappabilityall.bed.gz",
    "difficult": f"{STRAT}/Union/GRCh38_alldifficultregions.bed.gz",
    "falsedup": f"{STRAT}/OtherDifficult/GRCh38_false_duplications_correct_copy.bed.gz",
    "hg002_bed": f"{TRIO}/HG002_NA24385_son/NISTv4.2.1/GRCh38/"
    "HG002_GRCh38_1_22_v4.2.1_benchmark_noinconsistent.bed",
    "hg003_bed": f"{TRIO}/HG003_NA24149_father/NISTv4.2.1/GRCh38/"
    "HG003_GRCh38_1_22_v4.2.1_benchmark_noinconsistent.bed",
    "hg004_bed": f"{TRIO}/HG004_NA24143_mother/NISTv4.2.1/GRCh38/"
    "HG004_GRCh38_1_22_v4.2.1_benchmark_noinconsistent.bed",
    # polars-bio 0.36 streams VCF from object stores (S3/GCS/Azure) but not
    # from plain HTTP(S); the GIAB release is mirrored in the public s3://giab.
    "hg003_vcf": "s3://giab/release/AshkenazimTrio/HG003_NA24149_father/"
    "NISTv4.2.1/GRCh38/HG003_GRCh38_1_22_v4.2.1_benchmark.vcf.gz",
    "hg004_vcf": "s3://giab/release/AshkenazimTrio/HG004_NA24143_mother/"
    "NISTv4.2.1/GRCh38/HG004_GRCh38_1_22_v4.2.1_benchmark.vcf.gz",
    "constraint": "https://storage.googleapis.com/gcp-public-data--gnomad/"
    "release/4.1/constraint/gnomad.v4.1.constraint_metrics.tsv",
    "clingen": "https://search.clinicalgenome.org/kb/gene-validity/download",
    "gff3": "https://ftp.ensembl.org/pub/release-116/gff3/homo_sapiens/"
    "Homo_sapiens.GRCh38.116.gff3.gz",
}

VARIANT_COLS = [
    "chrom", "start", "end", "ref", "alt", "GT", "most_severe_consequence",
    "gnomADe_AF", "gnomADg_AF", "MAX_AF", "gnomADe_ASJ_AF", "gnomADg_ASJ_AF",
    "ClinVar", "ClinVar_CLNSIG",
    "ClinVar_CLNREVSTAT", "ClinVar_CLNDN", "CADD_PHRED",
]
CSQ_COLS = [
    "Consequence", "IMPACT", "SYMBOL", "Gene", "Feature", "BIOTYPE",
    "MANE_SELECT", "EXON", "HGVSc", "HGVSp", "am_class", "am_pathogenicity",
    "SpliceAI_pred_DS_AG", "SpliceAI_pred_DS_AL", "SpliceAI_pred_DS_DG",
    "SpliceAI_pred_DS_DL",
]
SPLICEAI_DS = [c for c in CSQ_COLS if c.startswith("SpliceAI_pred_DS")]
# The four LOFTEE consequence classes.
LOF_RE = "stop_gained|frameshift_variant|splice_acceptor_variant|splice_donor_variant"
PTC_RE = "stop_gained|frameshift_variant"
SPLICE_RE = "splice_acceptor_variant|splice_donor_variant"
RARE_AF = 0.01
REF_MINOR_AF = 0.95  # alternate allele near fixation: the reference carries the rare allele
SPLICEAI_MIN = 0.2  # SpliceAI "high-recall" cut-off
CLINGEN_LEVELS = ["Definitive", "Strong", "Moderate"]
# vepyr writes multiple ClinVar values separated by "&" (VEP field escaping).
PLP_RE = r"^(Pathogenic|Likely_pathogenic|Pathogenic/Likely_pathogenic)(&|\||$)"
# At least one ClinVar review star; "&" stands for "," in VEP-style fields.
STARRED_RE = r"criteria_provided&_(single_submitter|multiple_submitters)|reviewed_by_expert_panel|practice_guideline"

SOURCES: list[dict] = []


def timed(name: str, how: str, fn):
    """Run one remote read, record its wall time and size for provenance."""
    t0 = time.perf_counter()
    df = fn()
    seconds = time.perf_counter() - t0
    SOURCES.append({"source": name, "url": URL[name], "reader": how,
                    "rows": df.height, "seconds": round(seconds, 1)})
    print(f"[tier 3] {name:10s} {df.height:>10,d} rows {seconds:7.1f} s  ({how})", flush=True)
    return df


def one_based(df: pl.DataFrame) -> pl.DataFrame:
    """Declare 1-based closed coordinates for polars-bio interval operations."""
    df.config_meta.set(coordinate_system_zero_based=False)
    return df


# --------------------------------------------------------------------------
# Tiers 1 + 2
# --------------------------------------------------------------------------

def annotate() -> None:
    lf = vepyr.annotate(
        str(VCF), str(CACHE), reference_fasta=str(FASTA), workers=WORKERS,
        show_progress=False, plugin_cache_root=str(PLUGIN_ROOT), plugins=PLUGINS,
    )
    t0 = time.perf_counter()
    lf.select(*VARIANT_COLS, *CSQ_COLS).sink_parquet(ANNOTATED, row_group_size=50_000)
    seconds = time.perf_counter() - t0
    n = pl.scan_parquet(ANNOTATED).select(pl.len()).collect().item()
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "annotate.json").write_text(json.dumps({
        "variants": n, "seconds": round(seconds, 1), "workers": WORKERS,
        "plugins": PLUGINS, "vepyr": vepyr.__version__, "polars": pl.__version__,
        "machine": platform.platform(), "cpu": platform.processor(),
    }, indent=2))
    print(f"annotated {n:,d} variants in {seconds:.1f} s -> {ANNOTATED}")


# --------------------------------------------------------------------------
# Tier 3: ad hoc sources, straight from their URLs
# --------------------------------------------------------------------------

def read_bed(name: str) -> pl.DataFrame:
    return timed(name, "polars-bio scan_bed (HTTPS)",
                 lambda: pb.scan_bed(URL[name]).select("chrom", "start", "end").collect())


def read_parent(name: str) -> pl.DataFrame:
    return timed(name, "polars-bio scan_vcf (S3)", lambda: (
        pb.scan_vcf(URL[name], info_fields=[], format_fields=["GT"])
        .select("chrom", "start", "ref", "alt", "GT")
        .collect()
    ))


def read_constraint() -> pl.DataFrame:
    def fn() -> pl.DataFrame:
        return (
            pl.scan_csv(URL["constraint"], separator="\t", infer_schema=False)
            .filter(pl.col("mane_select") == "true", pl.col("transcript").str.starts_with("ENST"))
            .select(
                pl.col("transcript").alias("Feature"),
                pl.col("gene").alias("constraint_gene"),
                pl.col("lof.oe_ci.upper").cast(pl.Float64, strict=False).alias("LOEUF"),
                pl.col("lof.oe_ci.upper_bin_decile").cast(pl.Int32, strict=False).alias("LOEUF_decile"),
            )
            .collect()
        )
    return timed("constraint", "Polars scan_csv (HTTPS)", fn)


def read_clingen() -> pl.DataFrame:
    def fn() -> pl.DataFrame:
        return (
            pl.read_csv(URL["clingen"], skip_rows=4, infer_schema=False)
            .filter(~pl.col("GENE SYMBOL").str.starts_with("+"))
        )
    raw = timed("clingen", "Polars read_csv (HTTPS)", fn)
    return (
        raw.filter(pl.col("CLASSIFICATION").is_in(CLINGEN_LEVELS))
        .group_by(pl.col("GENE SYMBOL").alias("SYMBOL"))
        .agg(
            pl.col("MOI").unique().sort().str.join("/").alias("MOI"),
            pl.col("CLASSIFICATION").unique().sort().str.join("/").alias("ClinGen"),
            pl.col("DISEASE LABEL").unique().sort().str.join("; ").alias("ClinGen_disease"),
        )
    )


def read_gff3() -> dict[str, pl.DataFrame]:
    """MANE Select transcripts, CDS and exons from the Ensembl 116 GFF3."""
    def fn() -> pl.DataFrame:
        return pl.read_csv(
            URL["gff3"], separator="\t", comment_prefix="#", has_header=False,
            quote_char=None, columns=[0, 2, 3, 4, 6, 8], infer_schema=False,
            new_columns=["seqid", "type", "start", "end", "strand", "attributes"],
        ).with_columns(pl.col("start", "end").cast(pl.Int64))
    gff = timed("gff3", "Polars read_csv (HTTPS)", fn)
    attr = lambda key: pl.col("attributes").str.extract(f"(?:^|;){key}=([^;]+)")  # noqa: E731
    gff = gff.filter(pl.col("seqid").is_in([str(c) for c in range(1, 23)])).with_columns(
        chrom=pl.lit("chr") + pl.col("seqid"),
        ID=attr("ID"), Parent=attr("Parent"), Name=attr("Name"), tag=attr("tag"),
        rank=attr("rank").cast(pl.Int32, strict=False),
    )
    genes = gff.filter(pl.col("type") == "gene").select(
        gene=pl.col("ID").str.strip_prefix("gene:"), SYMBOL=pl.col("Name"))
    mane = (
        gff.filter(pl.col("type") == "mRNA", pl.col("tag").str.contains("MANE_Select"))
        .select(Feature=pl.col("ID").str.strip_prefix("transcript:"),
                gene=pl.col("Parent").str.strip_prefix("gene:"), strand="strand")
        .join(genes, on="gene", how="left")
    )
    child = lambda kind: (  # noqa: E731
        gff.filter(pl.col("type") == kind)
        .with_columns(Feature=pl.col("Parent").str.strip_prefix("transcript:"))
        .join(mane, on="Feature", how="semi")
    )
    cds = child("CDS").select("chrom", "start", "end", "Feature")
    exons = child("exon").select("Feature", "rank", exon_start="start", exon_end="end")
    return {"mane": mane, "cds": cds, "exons": exons}


# --------------------------------------------------------------------------
# Downstream analysis
# --------------------------------------------------------------------------

def mane_records(ann: pl.LazyFrame) -> pl.LazyFrame:
    """One row per variant and MANE Select transcript."""
    schema = ann.collect_schema()
    lists = [c for c in CSQ_COLS if isinstance(schema[c], pl.List)]
    ds = [pl.col(c).cast(pl.Float64) for c in SPLICEAI_DS]
    return (
        ann.with_row_index("vid")
        .explode(lists, empty_as_null=True)
        .filter(pl.col("MANE_SELECT").is_not_null())
        .with_columns(
            af=pl.max_horizontal("gnomADe_AF", "gnomADg_AF").fill_null(0.0),
            n_alt=pl.col("GT").str.count_matches("1"),
            exon_k=pl.col("EXON").str.extract(r"^(\d+)/").cast(pl.Int32),
            exon_n=pl.col("EXON").str.extract(r"/(\d+)$").cast(pl.Int32),
            spliceai_max=pl.max_horizontal(*ds),
        )
    )


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (math.nan, math.nan)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (c - h, c + h)


def analyze() -> None:
    RESULTS.mkdir(exist_ok=True)
    t_start = time.perf_counter()
    ann = pl.scan_parquet(ANNOTATED)
    n_variants = ann.select(pl.len()).collect().item()

    # ---- tier 3 reads (direct from URLs, every run) ----------------------
    segdups = one_based(read_bed("segdups"))
    lowmap = one_based(read_bed("lowmap"))
    difficult = one_based(read_bed("difficult"))
    falsedup = one_based(read_bed("falsedup"))
    bench = {s: one_based(read_bed(f"{s}_bed")) for s in ("hg002", "hg003", "hg004")}
    constraint = read_constraint()
    clingen = read_clingen()
    gff = read_gff3()
    t_tier3 = time.perf_counter()

    # ---- MANE records and pLoF -------------------------------------------
    t0 = time.perf_counter()
    rec = mane_records(ann).collect()
    t_explode = time.perf_counter() - t0
    plof = rec.filter(pl.col("Consequence").str.contains(LOF_RE))

    # Tier 1 flags: premature stop in the last exon (or a single-exon gene)
    # escapes nonsense-mediated decay; an alternate allele at >= 95% in gnomAD
    # means the reference genome carries the rare (often truncating) allele.
    plof = plof.with_columns(
        f_last_exon=pl.col("Consequence").str.contains(PTC_RE)
        & (pl.col("exon_k") == pl.col("exon_n")),
        f_ref_minor=pl.col("af") >= REF_MINOR_AF,
    )
    # Tier 3 flag: 50-bp rule, i.e. a premature stop within the last 50 bp of
    # the penultimate exon, from Ensembl GFF3 exon coordinates.
    penult = (
        plof.filter(pl.col("Consequence").str.contains(PTC_RE), pl.col("exon_k") == pl.col("exon_n") - 1)
        .select("vid", "Feature", "start", "exon_k")
        .join(gff["exons"], left_on=["Feature", "exon_k"], right_on=["Feature", "rank"])
        .join(gff["mane"].select("Feature", "strand"), on="Feature")
        .with_columns(dist=pl.when(pl.col("strand") == "+")
                      .then(pl.col("exon_end") - pl.col("start"))
                      .otherwise(pl.col("start") - pl.col("exon_start")))
        .filter(pl.col("dist").is_between(0, 49))
        .select("vid", "Feature", f_50bp=pl.lit(True))
    )
    plof = plof.join(penult, on=["vid", "Feature"], how="left").with_columns(pl.col("f_50bp").fill_null(False))
    # Tier 2 flags: canonical splice-site variant without SpliceAI support,
    # or a benign ClinVar classification.
    plof = plof.with_columns(
        f_spliceai=pl.col("Consequence").str.contains(SPLICE_RE)
        & ~pl.col("Consequence").str.contains(PTC_RE)
        & (pl.col("spliceai_max") < SPLICEAI_MIN).fill_null(False),
        f_clinvar_benign=pl.col("ClinVar_CLNSIG").str.contains("(?i)benign")
        .fill_null(False) & ~pl.col("ClinVar_CLNSIG").str.contains("(?i)pathogenic").fill_null(False),
    )
    # Tier 3 flags: technically difficult regions (GIAB v3.6 stratifications),
    # via polars-bio interval overlap.
    keys = one_based(plof.select("chrom", "start", "end", "vid").unique())
    for flag, regions in (("f_segdup", segdups), ("f_lowmap", lowmap)):
        hit = pb.overlap(keys, regions, overlap_output="left", distinct_output=True).collect()
        plof = plof.with_columns(pl.col("vid").is_in(hit["vid"].implode()).alias(flag))

    flag_tier = {"f_last_exon": 1, "f_ref_minor": 1, "f_spliceai": 2, "f_clinvar_benign": 2,
                 "f_50bp": 3, "f_segdup": 3, "f_lowmap": 3}
    flags = list(flag_tier)
    plof = plof.with_columns(
        flagged=pl.any_horizontal(*flags),
        rare=pl.col("af") < RARE_AF,
        hom=pl.col("n_alt") == 2,
    ).join(constraint, on="Feature", how="left")

    # Funnel over variants: a variant stays while any of its MANE pLoF
    # records is unflagged at the given tier.
    def n_var(df: pl.DataFrame) -> int:
        return df["vid"].n_unique()

    coding = rec.filter(pl.col("Consequence").str.contains(
        "missense|synonymous|stop_|start_|frameshift|inframe|splice_(acceptor|donor)_variant|protein_altering|coding_sequence"))
    t1 = plof.filter(~pl.col("f_last_exon") & ~pl.col("f_ref_minor"))
    t2 = t1.filter(~pl.col("f_spliceai") & ~pl.col("f_clinvar_benign"))
    t3 = t2.filter(~pl.col("f_50bp") & ~pl.col("f_segdup") & ~pl.col("f_lowmap"))
    hc = t3
    funnel = [
        ("All variants (GIAB v4.2.1, chr1-22)", "input", n_variants),
        ("Coding or splice-site, MANE Select", "tier 1", n_var(coding)),
        ("pLoF, MANE Select", "tier 1", n_var(plof)),
        ("not in last exon, reference allele not minor", "tier 1", n_var(t1)),
        ("SpliceAI-supported, not ClinVar benign", "tier 2", n_var(t2)),
        ("outside segdups / low mappability, 50-bp rule", "tier 3", n_var(t3)),
    ]
    pl.DataFrame(funnel, schema=["step", "tier", "variants"], orient="row").write_csv(
        RESULTS / "funnel.tsv", separator="\t")
    flag_counts = pl.DataFrame(
        [(f.removeprefix("f_"), flag_tier[f], plof.filter(pl.col(f))["vid"].n_unique()) for f in flags],
        schema=["flag", "tier", "variants"], orient="row",
    )
    flag_counts.write_csv(RESULTS / "lof_flags.tsv", separator="\t")

    hc_var = hc.group_by("vid").agg(pl.col("rare").first(), pl.col("hom").first())
    lof_summary = {
        "plof_variants": n_var(plof),
        "hc_variants": hc_var.height,
        "hc_rare": hc_var.filter(pl.col("rare")).height,
        "hc_common": hc_var.filter(~pl.col("rare")).height,
        "hc_hom": hc_var.filter(pl.col("hom")).height,
        "hc_rare_hom": hc_var.filter(pl.col("rare") & pl.col("hom")).height,
        "hc_genes": hc["SYMBOL"].n_unique(),
        "flagged_variants": n_var(plof.filter(pl.col("flagged"))),
    }
    plof.drop("vid").sort("chrom", "start").write_csv(RESULTS / "plof_records.tsv", separator="\t")

    # ---- LOEUF: is selection visible in one genome? ----------------------
    rec_c = rec.join(constraint, on="Feature", how="left").with_columns(rare=pl.col("af") < RARE_AF)
    categories = {
        "Rare synonymous (reference)": rec_c.filter(
            pl.col("Consequence").str.contains("^synonymous_variant"), pl.col("rare")),
        "Rare missense, AlphaMissense likely benign": rec_c.filter(
            pl.col("Consequence").str.contains("missense_variant"), pl.col("rare"),
            pl.col("am_class") == "likely_benign"),
        "Rare missense, AlphaMissense likely pathogenic": rec_c.filter(
            pl.col("Consequence").str.contains("missense_variant"), pl.col("rare"),
            pl.col("am_class") == "likely_pathogenic"),
        "All pLoF, MANE Select": plof,
        "Common HC pLoF (AF >= 1%)": hc.filter(~pl.col("rare")),
        "Rare HC pLoF (AF < 1%)": hc.filter(pl.col("rare")),
        "Flagged pLoF": plof.filter(pl.col("flagged")),
    }
    rows, stats = [], []
    ref = categories["Rare synonymous (reference)"].filter(pl.col("LOEUF_decile").is_not_null())
    ref_k = ref.filter(pl.col("LOEUF_decile") <= 1).height
    for name, df in categories.items():
        df = df.filter(pl.col("LOEUF_decile").is_not_null()).unique(["vid", "Feature"])
        n = df.height
        counts = df.group_by("LOEUF_decile").len()
        for d in range(10):
            k = counts.filter(pl.col("LOEUF_decile") == d)["len"].sum()
            rows.append((name, d, k, n, k / n if n else math.nan))
        k = df.filter(pl.col("LOEUF_decile") <= 1).height
        lo, hi = wilson(k, n)
        odds, p = fisher_exact([[k, n - k], [ref_k, ref.height - ref_k]]) if n else (math.nan, math.nan)
        stats.append((name, n, k, k / n if n else math.nan, lo, hi, odds, p,
                      df["LOEUF"].median() if n else math.nan))
    pl.DataFrame(rows, schema=["category", "decile", "n", "total", "fraction"], orient="row").write_csv(
        RESULTS / "loeuf_deciles.tsv", separator="\t")
    pl.DataFrame(stats, schema=["category", "n", "n_constrained", "frac_constrained", "ci_low", "ci_high",
                                "odds_ratio_vs_synonymous", "fisher_p", "median_LOEUF"], orient="row").write_csv(
        RESULTS / "loeuf_summary.tsv", separator="\t")

    # ---- ClinVar P/LP and carrier status ---------------------------------
    plp = (
        rec.filter(pl.col("ClinVar_CLNSIG").str.contains(PLP_RE),
                   pl.col("ClinVar_CLNREVSTAT").str.contains(STARRED_RE))
        .join(clingen, on="SYMBOL", how="left")
        # prefer the gene the variant lies in over a neighbour within 5 kb
        .sort("vid", pl.col("Consequence").str.contains("upstream_gene|downstream_gene"),
              pl.col("MOI").is_null())
        .unique("vid", keep="first", maintain_order=True)
    )
    hc_rare_rec = hc.filter(pl.col("rare")).unique("vid", maintain_order=True).join(clingen, on="SYMBOL", how="left")

    # ---- Trio: parent of origin, polars-bio streams parental VCFs from S3 -
    cand = pl.concat([plp.select("vid", "chrom", "start", "end", "ref", "alt", "GT"),
                      hc_rare_rec.select("vid", "chrom", "start", "end", "ref", "alt", "GT")]).unique("vid")
    cand = one_based(cand)
    origin = cand.select("vid")
    for parent, who in (("hg003", "father"), ("hg004", "mother")):
        vcf = (read_parent(f"{parent}_vcf").with_columns(pl.col("alt").str.split(","))
               .explode("alt", empty_as_null=True))
        gt = cand.join(vcf, on=["chrom", "start", "ref", "alt"], how="left").select("vid", pl.col("GT_right").alias(who))
        inbed = pb.overlap(cand.select("chrom", "start", "end", "vid"), bench[parent],
                           overlap_output="left", distinct_output=True).collect()
        origin = origin.join(gt, on="vid", how="left").with_columns(
            pl.when(pl.col(who).is_not_null()).then(pl.col(who))
            .when(pl.col("vid").is_in(inbed["vid"].implode())).then(pl.lit("0/0"))
            .otherwise(pl.lit("no call")).alias(who))
    origin = origin.with_columns(
        inheritance=pl.when(pl.col("father").str.contains("1") & pl.col("mother").str.contains("1")).then(pl.lit("both"))
        .when(pl.col("father").str.contains("1") & (pl.col("mother") == "0/0")).then(pl.lit("paternal"))
        .when(pl.col("mother").str.contains("1") & (pl.col("father") == "0/0")).then(pl.lit("maternal"))
        .when((pl.col("father") == "0/0") & (pl.col("mother") == "0/0")).then(pl.lit("de novo?"))
        .otherwise(pl.lit("unresolved"))
    )
    table_cols = ["chrom", "start", "ref", "alt", "GT", "SYMBOL", "Consequence", "HGVSp", "HGVSc", "af",
                  "gnomADe_ASJ_AF", "gnomADg_ASJ_AF",
                  "ClinVar_CLNSIG", "ClinVar_CLNREVSTAT", "ClinVar_CLNDN", "am_class", "spliceai_max",
                  "CADD_PHRED", "LOEUF", "MOI", "ClinGen", "father", "mother", "inheritance"]
    plp_out = plp.join(constraint.select("Feature", "LOEUF"), on="Feature", how="left").join(origin, on="vid", how="left")
    plp_out.select(*table_cols).sort("chrom", "start").write_csv(RESULTS / "clinvar_plp.tsv", separator="\t")
    hc_rare_out = hc_rare_rec.join(origin, on="vid", how="left")
    hc_rare_out.select(*table_cols).sort("chrom", "start").write_csv(RESULTS / "hc_rare_plof.tsv", separator="\t")

    # Putative biallelic events: two qualifying heterozygous variants in one
    # gene inherited from different parents, or one homozygous variant.
    qual = pl.concat([plp_out.select("vid", "SYMBOL", "GT", "inheritance", "MOI"),
                      hc_rare_out.select("vid", "SYMBOL", "GT", "inheritance", "MOI")]).unique("vid")
    biallelic = (
        qual.group_by("SYMBOL")
        .agg(pl.len().alias("n"), pl.col("inheritance").unique().sort().str.join(","),
             pl.col("GT").unique().sort().str.join(","), pl.col("MOI").first())
        .filter((pl.col("GT").str.contains("1/1"))
                | ((pl.col("n") >= 2) & pl.col("inheritance").str.contains("paternal")
                   & pl.col("inheritance").str.contains("maternal")))
    )
    biallelic.write_csv(RESULTS / "biallelic.tsv", separator="\t")

    # ---- Callability of MANE coding sequence (polars-bio coverage) --------
    cds = one_based(gff["cds"])
    cov = pb.coverage(cds, bench["hg002"]).collect().rename({"coverage": "callable_bp"})
    for name, regions in (("segdup", segdups), ("difficult", difficult), ("falsedup", falsedup)):
        part = pb.coverage(cds, regions).collect().select(
            "chrom", "start", "end", pl.col("coverage").alias(f"{name}_bp"))
        cov = cov.join(part, on=["chrom", "start", "end"], how="left")
    callab = (
        cov.with_columns(cds_bp=pl.col("end") - pl.col("start") + 1)
        .group_by("Feature")
        .agg(pl.col("cds_bp", "callable_bp", "segdup_bp", "difficult_bp", "falsedup_bp").sum())
        .join(gff["mane"].select("Feature", "SYMBOL"), on="Feature")
        .join(clingen, on="SYMBOL", how="left")
        .with_columns(callable_frac=pl.col("callable_bp") / pl.col("cds_bp"),
                      segdup_frac=pl.col("segdup_bp") / pl.col("cds_bp"),
                      difficult_frac=pl.col("difficult_bp") / pl.col("cds_bp"),
                      falsedup_frac=pl.col("falsedup_bp") / pl.col("cds_bp"))
        .sort("callable_frac")
    )
    callab.write_csv(RESULTS / "callability.tsv", separator="\t")
    clin = callab.filter(pl.col("ClinGen").is_not_null())

    t_total = time.perf_counter() - t_start
    summary = {
        "variants": n_variants,
        "mane_records": rec.height,
        "lof": lof_summary,
        "clinvar_plp_variants": plp.height,
        "clinvar_plp_het": plp.filter(pl.col("n_alt") == 1).height,
        "clinvar_plp_hom": plp.filter(pl.col("n_alt") == 2).height,
        "clinvar_plp_ar_het": plp.filter(pl.col("n_alt") == 1, pl.col("MOI").str.contains("AR")).height,
        "biallelic_genes": biallelic.height,
        "mane_genes": callab.height,
        "mane_genes_lt90": callab.filter(pl.col("callable_frac") < 0.9).height,
        "clingen_genes": clin.height,
        "clingen_genes_lt90": clin.filter(pl.col("callable_frac") < 0.9).height,
        "clingen_genes_lt50": clin.filter(pl.col("callable_frac") < 0.5).height,
        "clingen_lt90_segdup_gt50": clin.filter(pl.col("callable_frac") < 0.9, pl.col("segdup_frac") > 0.5).height,
        "clingen_lt90_difficult_gt50": clin.filter(pl.col("callable_frac") < 0.9,
                                                   pl.col("difficult_frac") > 0.5).height,
        "clingen_falsedup": clin.filter(pl.col("falsedup_frac") > 0)["SYMBOL"].to_list(),
        "inheritance_counts": dict(origin.group_by("inheritance").len().iter_rows()),
        "seconds": {"tier3_reads": round(t_tier3 - t_start, 1), "explode_mane": round(t_explode, 1),
                    "total": round(t_total, 1)},
        "versions": {"vepyr": vepyr.__version__, "polars": pl.__version__, "polars_bio": pb.__version__},
    }
    (RESULTS / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    (RESULTS / "sources.json").write_text(json.dumps(SOURCES, indent=2))
    print(json.dumps(summary, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("stage", choices=["annotate", "analyze"])
    args = parser.parse_args()
    {"annotate": annotate, "analyze": analyze}[args.stage]()


if __name__ == "__main__":
    main()
