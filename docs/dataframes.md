# Polars DataFrames

`vepyr.annotate()` without `output_vcf` returns a `polars.LazyFrame`. Nothing
runs until you `collect()` or `sink_*()` it, and every collect starts a fresh
annotation stream, so the same LazyFrame can be evaluated more than once.

```python
import polars as pl
import vepyr

lf = vepyr.annotate(
    "input.vcf.gz",
    "/data/vepyr_cache/116_GRCh38_merged",
    reference_fasta="GRCh38.fa",
)
df = lf.collect()
```

!!! tip "No columns selected means `everything`"
    A frame created without annotation flags and collected without a
    `select()` is the full VEP `--everything` result, provided a
    `reference_fasta` is given. Without a FASTA the engine cannot compute
    the HGVS and `everything`-only columns: they stay null and a warning
    names them. Add a `select()` and only the flags its columns need are run, see
    [below](#what-is-pushed-into-the-engine). Flags you pass explicitly are
    honoured as given.

## Schema

`collect_schema()` is free: it comes from a probe that opens the cache but
annotates nothing. This is the schema for chr22 of HG002 on the release-116
Ensembl cache:

```python
>>> lf.collect_schema()
Schema({
    'chrom': String,
    'start': UInt32,                     # VCF POS, 1-based
    'end': UInt32,                       # POS + len(REF) - 1
    'id': String,                        # '' when the VCF has '.'
    'ref': String,
    'alt': String,
    'qual': Float64,
    'filter': String,
    'most_severe_consequence': String,
    'Allele': String,
    'Consequence': List(String),         # one element per CSQ entry, in CSQ order ...
    'IMPACT': List(String),
    'SYMBOL': List(String),
    'Gene': List(String),
    'Feature_type': List(String),
    'Feature': List(String),
    'BIOTYPE': List(String),
    'EXON': List(String),
    'INTRON': List(String),
    'HGVSc': List(String),
    'HGVSp': List(String),
    'cDNA_position': List(String),
    'CDS_position': List(String),
    'Protein_position': List(String),
    'Amino_acids': List(String),
    'Codons': List(String),
    'Existing_variation': List(String),  # distinct values per variant, not per entry
    'DISTANCE': List(Int64),
    'STRAND': List(Int8),
    'FLAGS': List(String),
    'VARIANT_CLASS': String,
    'SYMBOL_SOURCE': List(String),
    'HGNC_ID': List(String),
    'CANONICAL': List(String),
    'MANE': List(String),
    'MANE_SELECT': List(String),
    'MANE_PLUS_CLINICAL': List(String),
    'TSL': List(Int8),
    'APPRIS': List(String),
    'CCDS': List(String),
    'ENSP': List(String),
    'SWISSPROT': List(String),
    'TREMBL': List(String),
    'UNIPARC': List(String),
    'UNIPROT_ISOFORM': List(String),
    'GENE_PHENO': List(String),
    'SIFT': List(String),
    'PolyPhen': List(String),
    'DOMAINS': List(String),
    'miRNA': List(String),
    'HGVS_OFFSET': List(Int64),
    'AF': Float32,                       # frequencies are per variant ...
    'AFR_AF': Float32,
    'AMR_AF': Float32,
    'EAS_AF': Float32,
    'EUR_AF': Float32,
    'SAS_AF': Float32,
    'gnomADe_AF': Float32,
    'gnomADe_AFR_AF': Float32,
    'gnomADe_AMR_AF': Float32,
    'gnomADe_ASJ_AF': Float32,
    'gnomADe_EAS_AF': Float32,
    'gnomADe_FIN_AF': Float32,
    'gnomADe_MID_AF': Float32,
    'gnomADe_NFE_AF': Float32,
    'gnomADe_REMAINING_AF': Float32,
    'gnomADe_SAS_AF': Float32,
    'gnomADg_AF': Float32,
    'gnomADg_AFR_AF': Float32,
    'gnomADg_AMI_AF': Float32,
    'gnomADg_AMR_AF': Float32,
    'gnomADg_ASJ_AF': Float32,
    'gnomADg_EAS_AF': Float32,
    'gnomADg_FIN_AF': Float32,
    'gnomADg_MID_AF': Float32,
    'gnomADg_NFE_AF': Float32,
    'gnomADg_REMAINING_AF': Float32,
    'gnomADg_SAS_AF': Float32,
    'MAX_AF': Float32,
    'MAX_AF_POPS': String,
    'CLIN_SIG': List(String),            # distinct values per variant
    'SOMATIC': String,
    'PHENO': String,
    'PUBMED': List(String),              # distinct values per variant
    'MOTIF_NAME': List(String),          # per entry; null outside MotifFeature entries
    'MOTIF_POS': List(Int64),
    'HIGH_INF_POS': List(String),
    'MOTIF_SCORE_CHANGE': List(Float32),
    'TRANSCRIPTION_FACTORS': List(String),
    'clin_sig_allele': List(String),     # cache-only columns, no CSQ counterpart
    'clinical_impact': String,
    'minor_allele': String,
    'minor_allele_freq': Float32,
    'clinvar_ids': List(String),
    'cosmic_ids': List(String),
    'dbsnp_ids': List(String),
})
```

The 80 columns from `Allele` to `TRANSCRIPTION_FACTORS` are the CSQ fields in
VCF header order. A `List` column that is aligned with `Consequence` has one
element per CSQ entry; `Existing_variation`, `CLIN_SIG` and `PUBMED` hold the
distinct values for the variant instead. Values that VEP repeats on every
entry, such as the frequencies, are stored once as scalars. The input's other
`INFO` fields and its sample columns are not in the frame; use `output_vcf`
for those. Add `skip_csq=False` to get the raw `CSQ` string as a column.

To narrow the frame, `select()` the columns you need, see
[below](#what-is-pushed-into-the-engine); the engine then only computes what
those columns require.

### Plugin columns

With a plugin cache configured, every plugin CSQ field is a named column
appended after the block above; see [Plugins](plugins.md). Its shape and
type come from the plugin's manifest, not from the CSQ text it travels in:

- A plugin keyed on the variant alone, such as CADD or ClinVar, gives one
  value per row: `CADD_PHRED: String`, `ClinVar_CLNSIG: String`.
- A plugin keyed on a transcript feature, such as SpliceAI (by gene symbol)
  or AlphaMissense (by protein change), gives one value per consequence
  entry, aligned with `Consequence`: `SpliceAI_pred_DP_AG: List(Int32)`,
  `am_pathogenicity: List(Float32)`.
- The element type is the manifest's `type`: `Utf8`, `Int32` or `Float32`.
  CADD's scores are declared `Utf8` so the VCF reproduces the source digits
  exactly, which is why `CADD_PHRED` is a string; cast it when you need a
  number.

The engine only runs the plugin lookup, and only builds the CSQ string the
values are carried in, when a query reads one of those columns:

```python
lf = vepyr.annotate(
    "input.vcf.gz", cache, reference_fasta="GRCh38.fa",
    plugin_cache_root="/data/plugin_cache", plugins=["cadd"],
)
lf.select("chrom", "start", "ref", "alt", "CADD_PHRED")   # plugin lookup, no HGVS
lf.select("chrom", "start", "SYMBOL")                     # neither
```

Selecting a plugin column on a frame without a plugin cache fails when the
plan is built, with Polars' `ColumnNotFoundError` listing the columns the
frame has.

## What is pushed into the engine

The frame is backed by a Polars IO plugin that pulls Arrow batches from the
native annotator. Two things reach the engine:

- **`head(n)` and `limit(n)`** become a SQL `LIMIT`, so previewing is fast.
- **A narrowing `select()`**, together with the columns a pushed-down
  `filter()` reads, decides the annotation flags. Only three groups of columns
  depend on flags at all: `HGVSc` and `HGVSp` on `hgvs`; the co-located
  columns (`Existing_variation`, `CLIN_SIG`, `SOMATIC`, `PHENO`, `PUBMED`, the
  `AF` family and the cache-only columns) on `check_existing` and the `af`
  flags; and the `everything`-only extras (`MANE`, `APPRIS`, `SIFT`,
  `PolyPhen`, `DOMAINS`, `miRNA`, `HGVS_OFFSET` and the gnomAD
  sub-populations) on `everything`. A group nobody selected is switched off
  for the run. A group a column needs is switched on when you gave no flags;
  flags you did set are kept exactly as configured. HGVS and the `everything`
  extras need `reference_fasta`, and selecting them without one raises rather
  than returning nulls. The selected columns are value-identical to a run
  with the flags spelled out.

`filter()` itself is applied to each batch after annotation. It bounds memory,
because a batch is dropped as soon as it has been reduced, but it does not
reduce the engine's work; filtering by region is cheaper done on the input VCF
with `bcftools view -r`. Adding `skip_csq=False` and selecting `CSQ` disables
pruning, since the string needs every flag. Plugin lookups run only when the
query reads a plugin column or `CSQ`.

```python
preview = lf.head(20).collect()              # LIMIT 20 in the engine

high = (
    lf.filter(pl.col("IMPACT").list.contains("HIGH"))
      .select("chrom", "start", "ref", "alt", "SYMBOL", "Consequence")
      .collect()
)                                            # runs without HGVS or the co-located lookup

lf.select("chrom", "start", "SYMBOL", "Consequence").collect()   # no flags at all
lf.select("chrom", "start", "HGVSc").collect()                    # hgvs only
lf.select("chrom", "start", "AF", "CLIN_SIG").collect()           # the co-located lookup only
lf.select("chrom", "start", "SIFT").collect()                     # everything
lf.collect()                                                      # no projection: everything
```

Without a projection there is nothing to infer from, so the flags you passed
are what runs, and a frame created without flags runs `everything` when it
has a FASTA and the co-located lookup when it does not.

On the release-116 Ensembl cache with a FASTA and `workers=1`:

| Input | Query | Wall time |
|---|---|---|
| chr22, 50,861 variants | `collect()` | 2.3 s |
| | `select(chrom, start, ref, alt, SYMBOL, Consequence, IMPACT)` | 1.2 s |
| | `select(chrom, start, HGVSc, HGVSp)` | 1.4 s |
| | `select(chrom, start, Existing_variation, AF, MAX_AF, CLIN_SIG, PUBMED)` | 1.9 s |
| chr1, 323,430 variants | `collect()` | 15.2 s |
| | `select(chrom, start, ref, alt, SYMBOL, Consequence, IMPACT)` | 5.8 s |
| | `select(chrom, start, HGVSc, HGVSp)` | 9.0 s |
| | `select(chrom, start, Existing_variation, AF, MAX_AF, CLIN_SIG, PUBMED)` | 14.2 s |

## One row per consequence

Most downstream work, and every `filter_vep` expression, is about one CSQ
entry: a variant paired with one transcript, regulatory or motif feature.
Explode the aligned list columns together to get that long format. The helper
pads a null list to the row's entry count first, so a column that is null on
some rows cannot break the alignment.

```python
UNALIGNED = {"Existing_variation", "CLIN_SIG", "PUBMED",
             "clin_sig_allele", "clinvar_ids", "dbsnp_ids"}


def consequence_rows(df: pl.DataFrame) -> pl.DataFrame:
    """One row per CSQ entry. Keeps every scalar and per-variant column as is."""
    cols = [
        c for c, t in df.schema.items()
        if isinstance(t, pl.List) and c not in UNALIGNED
    ]
    padded = [
        pl.when(pl.col(c).is_null())
        .then(pl.col("Consequence").list.eval(pl.lit(None)).cast(df.schema[c]))
        .otherwise(pl.col(c))
        .alias(c)
        for c in cols
    ]
    return (
        df.filter(pl.col("Consequence").is_not_null())
          .with_columns(padded)
          .explode(cols)
    )


long = consequence_rows(df)
```

Rows whose `ALT` is `*` have no consequences and are dropped by the filter.
On chr22 of HG002 the 50,861 variants become 943,901 consequence rows.

## `filter_vep` expressions in Polars

Ensembl's [`filter_vep`](https://jun2026.archive.ensembl.org/info/docs/tools/vep/script/vep_filter.html)
evaluates an expression per CSQ entry and keeps the variant when any entry
matches; with `--only_matched` it also drops the entries that did not. Both
map onto the frame:

- On the **long frame** (`long` above), `filter()` keeps matching entries only.
  This is `filter_vep --only_matched`.
- On the **wide frame** (`df`), wrap the per-entry test in
  `list.eval(...).list.any()` to keep the whole variant when any entry
  matches. This is plain `filter_vep`.

The table uses the long frame. Fields that `filter_vep` treats as numbers
after stripping text, such as `SIFT`'s score in parentheses or the exon
number in `2/10`, need the same extraction here.

| `filter_vep` expression | Polars expression on `long` |
|---|---|
| `Feature is ENST00000307301` | `pl.col("Feature") == "ENST00000307301"` |
| `Feature_type is Transcript` | `pl.col("Feature_type") == "Transcript"` |
| `Consequence is missense_variant` | `pl.col("Consequence").str.split("&").list.contains("missense_variant")` |
| `Consequence matches stream` | `pl.col("Consequence").str.contains("stream")` |
| `Consequence match stop` | `pl.col("Consequence").str.contains("stop")` |
| `SIFT != tolerated` | `pl.col("SIFT").str.replace(r"\(.*\)$", "") != "tolerated"` |
| `SIFT match tolerated` | `pl.col("SIFT").str.contains("tolerated")` |
| `SIFT < 0.1` | `pl.col("SIFT").str.extract(r"\(([\d.]+)\)").cast(pl.Float64) < 0.1` |
| `Protein_position < 10` | `pl.col("Protein_position").str.extract(r"^(\d+)").cast(pl.Int64) < 10` |
| `Exon > 1` | `pl.col("EXON").str.extract(r"^(\d+)").cast(pl.Int64) > 1` |
| `MANE` (field exists) | `pl.col("MANE").is_not_null()` |
| `SYMBOL` | `pl.col("SYMBOL").is_not_null()` |
| `not Existing_variation` | `pl.col("Existing_variation").is_null()` |
| `AF < 0.01 or not AF` | `pl.any_horizontal(pl.col("AF") < 0.01, pl.col("AF").is_null())` |
| `AFR_AF > 0.1 or EUR_AF > 0.1` | `pl.any_horizontal(pl.col("AFR_AF") > 0.1, pl.col("EUR_AF") > 0.1)` |
| `AFR_AF > #EUR_AF` | `pl.col("AFR_AF") > pl.col("EUR_AF")` |
| `(AFR_AF > 0.1 or EUR_AF > 0.1) and (EAS_AF < 0.1 and SAS_AF < 0.1)` | `pl.any_horizontal(pl.col("AFR_AF") > 0.1, pl.col("EUR_AF") > 0.1) & pl.all_horizontal(pl.col("EAS_AF") < 0.1, pl.col("SAS_AF") < 0.1)` |
| `Consequence is missense_variant and CCDS and DOMAINS` | `pl.col("Consequence").str.split("&").list.contains("missense_variant") & pl.col("CCDS").is_not_null() & pl.col("DOMAINS").is_not_null()` |
| `SYMBOL in BRCA1,BRCA2` | `pl.col("SYMBOL").is_in(["BRCA1", "BRCA2"])` |
| `Feature in /data/files/motifs_list.txt` | `pl.col("Feature").is_in(Path("/data/files/motifs_list.txt").read_text().split())` |
| `Consequence is coding_sequence_variant` with `--ontology` | No ontology expansion; list the child terms: `pl.col("Consequence").str.split("&").list.eval(pl.element().is_in(CHILD_TERMS)).list.any()` |

Operator by operator:

| `filter_vep` | Polars |
|---|---|
| `is`, `=`, `eq` | `==`; on `&`-joined fields `str.split("&").list.contains(...)` |
| `!=`, `ne` | `!=` |
| `match`, `matches`, `re`, `regex` | `str.contains(pattern)` |
| `<`, `>`, `<=`, `>=` | the same, after `cast()` from the string form where needed |
| `exists`, `ex`, `defined`, bare field | `is_not_null()` |
| `not` | `~` or `is_null()` |
| `and`, `or`, parentheses | `&` and the pipe operator, each side in its own parentheses; or `pl.all_horizontal()` / `pl.any_horizontal()` |
| `in a,b,c` / `in file` | `is_in([...])` |

Where a value in the frame is null, the `filter_vep` field is empty, so
`is_null()` and `is_not_null()` give the `not FIELD` and `FIELD` semantics
exactly.

The same expressions on the wide frame keep whole variants:

```python
any_stream = df.filter(
    pl.col("Consequence").list.eval(pl.element().str.contains("stream")).list.any()
)
any_high = df.filter(pl.col("IMPACT").list.contains("HIGH"))
```

## Writing results to disk

`collect()` holds the whole result in memory. On chromosome 1 of a
whole-genome sample (323,430 variants, `everything=True`) the collected frame
is about 2.3 GB, but the process peaks at 12.6 GB because Polars buffers the
Arrow batches on top of the frame. Two things bring that down.

**Leave the `CSQ` string off.** It is off by default (`skip_csq=True`) and
roughly halves peak memory. Turn it on only when you need the exact VEP string.

**Stream with `sink_parquet` and a small row group.** Polars holds one row
group in memory before writing it, and its default group is far larger than an
annotation batch. Setting the group to the engine's buffer size keeps the
stream flat:

```python
lf.sink_parquet("annotated.parquet", row_group_size=5000)
```

Measured on the release-116 Ensembl cache with `workers=1`:

| Input | Path | Wall time | Peak RSS |
|---|---|---|---|
| chr22, 50,861 variants | `output_vcf` (bgzf) | 3.9 s | 1.0 GB |
| | `collect()` | 2.4 s | 1.6 GB |
| | `sink_parquet()` default | 2.7 s | 2.1 GB |
| | `sink_parquet(row_group_size=5000)` | 2.5 s | 1.3 GB |
| chr1, 323,430 variants | `output_vcf` (bgzf) | 21.6 s | 2.9 GB |
| | `collect()` | 15.3 s | 6.9 GB |
| | `sink_parquet()` default | 15.8 s | 6.0 GB |
| | `sink_parquet(row_group_size=5000)` | 16.5 s | 3.5 GB |

With `skip_csq=False` add roughly 1 GB on chr1 for the small-row-group sink and
5 GB for `collect()`. The engine itself needs about 3 GB on chr1 whichever
output you choose, so the small-row-group sink is within half a gigabyte of the
VCF writer. Wall time is unaffected and the Parquet file grows by about 6 %.

List columns cannot be written to CSV directly. Join them with `&` first, on
the wide frame or on the long frame, where the per-variant value sets are the
only lists left:

```python
def join_lists(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.col(pl.List).cast(pl.List(pl.String)).list.join("&"))


join_lists(df).write_csv("annotated.tsv", separator="\t")
join_lists(consequence_rows(df)).write_csv("annotated_long.tsv", separator="\t")
```

## Agreement with the VCF output

The DataFrame and VCF paths run the same engine. On chr22 and chr1 of HG002
against the release-116 Ensembl cache with `everything=True`, the `CSQ`
column (`skip_csq=False`) matched the VCF's `INFO/CSQ` byte for byte on every
record, the variant columns matched `CHROM`, `POS`, `ID`, `REF`, `ALT`, `QUAL`
and `FILTER`, and every typed column matched its CSQ field element for element,
motif entries included. The only representational difference is the three
per-variant value-set columns:

!!! note "Per-variant value sets"
    `Existing_variation`, `CLIN_SIG` and `PUBMED` are deduplicated per
    variant rather than repeated per consequence. No values are lost, but
    their list length does not match the per-consequence columns. Elements
    are split on the cache's `,` separator only, so a combined assertion such
    as `benign&likely_benign` stays one element; match with `str.contains`.

When a downstream step needs the string itself, decode it directly:

```python
CSQ_FIELDS = [...]  # the Format: list from the VCF header, or the schema names from "Allele" on

entries = pl.col("CSQ").str.split(",")
decoded = df.with_columns(
    entries.list.eval(pl.element().str.split("|").list.get(i, null_on_oob=True))
           .alias(name)
    for i, name in enumerate(CSQ_FIELDS)
)
```
