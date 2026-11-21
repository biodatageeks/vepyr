import polars as pl
import polars_bio as pb
import vepyr

lf = vepyr.annotate(
    "HG002_chr22.vcf.gz",
    "vepyr_cache/116_GRCh38_ensembl",
    reference_fasta="chr22.fa",
    workers=4,
    show_progress=False,
)
coding = (
    lf.select("chrom", "start", "end", "ref", "alt", "SYMBOL", "IMPACT", "gnomADg_AF")
    .explode(["SYMBOL", "IMPACT"], empty_as_null=True)
    .filter(
        pl.col("IMPACT").is_in(["HIGH", "MODERATE"])
        & (pl.col("gnomADg_AF").is_null() | (pl.col("gnomADg_AF") < 0.01))
    )
    .unique(["chrom", "start", "ref", "alt", "SYMBOL"])
)
segdups = pb.scan_bed("GRCh38_segdups.bed.gz")
hits = pb.overlap(coding, segdups, overlap_output="left", distinct_output=True)
per_gene = (
    hits.group_by("SYMBOL")
    .agg(variants=pl.struct("start", "ref", "alt").n_unique())
    .sort("variants", "SYMBOL", descending=[True, False])
)
print(per_gene.collect())
