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
    'MOTIF_NAME': List(String),          # per entry, MotifFeature entries only; everything only
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

See [String scores and numeric casts in the vepyr-plugins README](https://github.com/biodatageeks/vepyr-plugins/blob/master/README.md#string-scores-and-numeric-casts)
for per-field numeric types and Polars examples covering scalar columns,
consequence-aligned lists and multiple scores inside a dbNSFP element.

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
native annotator. Three things reach the engine:

- **`head(n)` and `limit(n)`** become a SQL `LIMIT`, so previewing is fast.
- **A `filter()` on `chrom`, `start` or `end`** restricts the input before
  annotation; see [Region filters](#region-filters) below.
- **A narrowing `select()`**, together with the columns a pushed-down
  `filter()` reads, decides the annotation flags. Only three groups of columns
  depend on flags at all: `HGVSc` and `HGVSp` on `hgvs`; the co-located
  columns (`Existing_variation`, `CLIN_SIG`, `SOMATIC`, `PHENO`, `PUBMED`, the
  `AF` family and the cache-only columns) on `check_existing` and the `af`
  flags; and the `everything`-only extras (`MANE`, `APPRIS`, `SIFT`,
  `PolyPhen`, `DOMAINS`, `miRNA`, `HGVS_OFFSET`, the five motif columns and
  the gnomAD sub-populations) on `everything`. A group nobody selected is switched off
  for the run. A group a column needs is switched on when you gave no flags;
  flags you did set are kept exactly as configured. HGVS and the `everything`
  extras need `reference_fasta`, and selecting them without one raises rather
  than returning nulls. The selected columns are value-identical to a run
  with the flags spelled out.

Every other `filter()` is applied to each batch after annotation. It bounds
memory, because a batch is dropped as soon as it has been reduced, but it does
not reduce the engine's work. The raw `CSQ` string (`skip_csq=False`) needs every
flag, so a query that reads it runs like a plain `collect()`: with the flags
you gave, or the flagless default. Plugin lookups run only when the query
reads a plugin column or `CSQ`.

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

On the release-116 Ensembl cache with a FASTA and `workers=1`, measured on an
Apple Silicon M3 Max (16 cores, 64 GiB):

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

### Region filters

A `filter()` on `chrom`, `start` or `end` is pushed into the engine before
annotation: contigs outside the filter are never prepared, and an indexed
input (bgzip + `.tbi`/`.csi`) is read by seek.

```python
df = lf.filter(
    (pl.col("chrom") == "chr22") & pl.col("start").is_between(20_000_000, 25_000_000)
).collect()
```

The result is always identical to filtering after `collect()`; only the work
changes. Coordinates are the frame's own `start`/`end` columns (1-based,
closed). Recognised shapes:

- `chrom` conjuncts that name specific contigs: `==`, `is_in`,
  `str.starts_with` and boolean combinations of them. A conjunct that would
  accept any name (`!=`, `is_not_null()`) narrows nothing on its own and is
  only pushed together with a range.
- `start`/`end` conjuncts: comparisons with an integer literal and
  `is_between`. `end <= b` bounds the range; `end >= a` does not.
- Several regions: an `|` of `(chrom & range)` groups, one region per group.

Anything else (a float literal, a range compared to another column, a cast,
an `|` *inside* a range conjunct) is not pushed down and is applied by Polars
after annotation, which is still correct, just not faster.

Without a tabix/CSI index next to the input a `RuntimeWarning` is emitted:
the whole file is parsed once to find which contigs it carries and once more
to filter it before annotation, and only the selected rows are annotated. On Merged and RefSeq caches a range costs one
extra positional pass over each selected contig, which keeps the result
byte-identical to a whole-file run.

On the release-116 caches with a FASTA, `everything=True` and `workers=1`
(HG002 slices, indexed input), measured on an Apple Silicon M3 Max (16 cores, 64 GiB):

| Input | Query | Ensembl | Merged | RefSeq |
|---|---|---|---|---|
| chr22, 50,861 variants | `collect()` | 2.6 s | 3.2 s | 2.0 s |
| | `filter(chr22:20,000,000-25,000,000)`, 5,406 rows | 0.6 s | 1.3 s | 0.8 s |
| | `filter(chr22:30,000,000-30,100,000)`, 59 rows | 0.1 s | 0.7 s | 0.5 s |
| chr1, 323,430 variants | `collect()` | 17.0 s | 22.5 s | 14.9 s |
| | `filter(chr1:20,000,000-25,000,000)`, 7,871 rows | 1.2 s | 2.9 s | 1.7 s |
| | `filter(chr1:30,000,000-30,100,000)`, 275 rows | 0.6 s | 1.8 s | 1.1 s |

## One row per consequence

Most downstream work, and every `filter_vep` expression, is about one CSQ
entry: a variant paired with one transcript, regulatory or motif feature.
Explode the aligned list columns together to get that long format. The helper
pads a null list to the row's entry count first, so a column that is null on
some rows cannot break the alignment.

```python
UNALIGNED = {"Existing_variation", "CLIN_SIG", "PUBMED",
             "clin_sig_allele", "clinvar_ids", "cosmic_ids", "dbsnp_ids"}


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

## Plugins

Point the frame at a plugin cache and the plugin fields are columns like any
other; see [Plugin columns](#plugin-columns) for how their shape and type are
decided. All examples below use the four published caches:

```python
lf = vepyr.annotate(
    "input.vcf.gz", cache, reference_fasta="GRCh38.fa",
    plugin_cache_root="/data/plugin_cache",
    plugins=["cadd", "clinvar", "spliceai", "alphamissense"],
)
```

**CADD** is per variant, and its scores are strings so the VCF keeps the
source digits. Cast, then filter:

```python
high_cadd = (
    lf.select("chrom", "start", "ref", "alt", "SYMBOL", "CADD_PHRED")
      .with_columns(pl.col("CADD_PHRED").cast(pl.Float32))
      .filter(pl.col("CADD_PHRED") >= 20)
      .collect()
)
```

**ClinVar** is per variant too, so its fields are plain string columns.
`ClinVar` holds the variation id, and the assertion fields keep VEP's `&`
joins, so match with a pattern:

```python
clinvar = (
    lf.select("chrom", "start", "ref", "alt", "ClinVar", "ClinVar_CLNSIG", "ClinVar_CLNREVSTAT")
      .filter(pl.col("ClinVar_CLNSIG").str.contains("(?i)pathogenic"))
      .collect()
)
```

**SpliceAI** is matched by gene symbol, so every field is a list aligned with
`Consequence`. The delta scores are strings in the cache; cast the lists and
take the largest score across the variant's entries:

```python
DS = ["SpliceAI_pred_DS_AG", "SpliceAI_pred_DS_AL", "SpliceAI_pred_DS_DG", "SpliceAI_pred_DS_DL"]

splice = (
    lf.select("chrom", "start", "ref", "alt", "SpliceAI_pred_SYMBOL", *DS)
      .with_columns(pl.col(DS).cast(pl.List(pl.Float32)))
      .with_columns(pl.max_horizontal(pl.col(DS).list.max()).alias("spliceai_max_ds"))
      .filter(pl.col("spliceai_max_ds") >= 0.5)
      .collect()
)
```

**AlphaMissense** is matched by protein change, so `am_class` and
`am_pathogenicity` (already `List(Float32)`) belong to the consequence entry
that produced the change. Explode them together with the transcript columns
to see which transcript each score refers to:

```python
am = (
    lf.select("chrom", "start", "ref", "alt", "Feature", "Protein_position",
              "Amino_acids", "am_class", "am_pathogenicity")
      .filter(pl.col("am_class").list.contains("likely_pathogenic"))
      .explode(["Feature", "Protein_position", "Amino_acids", "am_class", "am_pathogenicity"])
      .filter(pl.col("am_class") == "likely_pathogenic")
      .collect()
)
```

Plugin columns combine freely with the base annotation, and the projection
still decides what runs. This query runs the co-located lookup for
`gnomADg_AF` and the CADD lookup, and nothing else:

```python
candidates = (
    lf.select("chrom", "start", "ref", "alt", "SYMBOL", "Consequence", "gnomADg_AF", "CADD_PHRED")
      .with_columns(pl.col("CADD_PHRED").cast(pl.Float32))
      .filter(
          pl.any_horizontal(pl.col("gnomADg_AF").is_null(), pl.col("gnomADg_AF") < 0.001)
          & (pl.col("CADD_PHRED") >= 25)
      )
      .collect()
)
```

The [`consequence_rows`](#one-row-per-consequence) helper works unchanged on
a frame with plugins: per-feature plugin lists explode alongside the
transcript columns, per-variant scalars such as `CADD_PHRED` repeat on each
row, like the frequencies do.

A query that reads no plugin column skips the plugin lookup entirely. On
chr22 with the four caches, `select(chrom, start, SYMBOL)` takes 1.2 s and
`select(chrom, start, CADD_PHRED)` 8.4 s, so keep plugin columns out of
queries that do not need them.

## Workers and the query engine

### `workers`

`workers=N` splits each contig into grid-aligned runs that are annotated
concurrently and released in order. It applies to the LazyFrame path as well as
`output_vcf`, and the frame is identical to `workers=1`, row order included.

```python
lf = vepyr.annotate(
    "input.vcf.gz",
    "/data/vepyr_cache/116_GRCh38_ensembl",
    reference_fasta="GRCh38.fa",
    workers=8,
)
df = lf.collect()
```

!!! warning "`workers > 1` needs an indexed input"
    The input VCF must be bgzip-compressed with a `.tbi` or `.csi` index. An
    unindexed input raises rather than silently falling back.

See [Performance](performance.md#workers-on-the-lazyframe-path) for a sweep
across worker counts and cache profiles.

### `engine="streaming"`

Polars' streaming engine works on the LazyFrame path and returns exactly the
same frame, but it does **not** reduce peak memory. vepyr feeds the frame
through a Polars IO plugin, and `collect()` materializes every batch whichever
engine plans the query — the streaming engine streams Polars' own operators, not
the annotation source.

Measured on the release-116 Ensembl cache with `everything=True`, a FASTA and
the default `skip_csq=True`, on an Apple Silicon M3 Max (16 cores, 64 GiB) while
other work ran on the host:

| Input | workers | Engine | Wall time | Peak RSS |
|---|--:|---|--:|--:|
| chr22, 50,284 variants | 1 | default | 2.5 s | 1.76 GB |
| | 1 | `streaming` | 2.5 s | 1.82 GB |
| | 4 | default | 1.6 s | 2.00 GB |
| | 4 | `streaming` | 1.6 s | 2.04 GB |
| chr1, 319,349 variants | 1 | default | 17.5 s | 7.85 GB |
| | 1 | `streaming` | 16.5 s | 7.79 GB |
| | 4 | default | 7.1 s | 7.69 GB |
| | 4 | `streaming` | 7.0 s | 7.88 GB |

All eight frames were row-for-row identical. Wall-time differences are within
run-to-run variance on a shared host; peak RSS is flat.

### What does reduce peak memory

A `filter()` on ordinary (non-coordinate) columns *is* pushed into the IO
source, on both engines: surviving rows are kept per batch, so a selective
query never accumulates the rows it discards. On chr1 with `everything=True`:

| Filter | Rows kept | Peak RSS |
|---|--:|--:|
| none | 319,349 | 7.85 GB |
| `MAX_AF > 0.5` | 208,015 | 7.74 GB |
| `MAX_AF > 0.99` | 41,376 | 6.52 GB |

The effect is real but bounded: dropping 87 % of the rows saved 17 % of peak
memory, because the annotation engine's working set and the Arrow batches in
flight dominate the total, and no predicate shrinks those.

To actually bound memory, stream to disk with `sink_parquet` instead — see
[below](#writing-results-to-disk).

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
