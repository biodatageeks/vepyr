# vepyr
vepyr (/ˈvaɪpər/) — VEP Yielding Performant Results — a blazing-fast Rust reimplementation of Ensembl's Variant Effect Predictor.

![logo.png](docs/logo.png)

## Setup with uv

1. Install `uv`.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Clone the repository and enter it.

```bash
git clone git@github.com:biodatageeks/vepyr.git
cd vepyr
```

3. Sync dependencies and build the package in place.

```bash
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
```

4. Run Python commands inside the managed environment.

```bash
uv run python -c "import vepyr; print(vepyr.__all__)"
```

5. Run the test suite.

```bash
uv run pytest
```

## Quick start

The repository ships with small test fixtures so you can verify the full
pipeline — build, annotate with indexed Parquet, and write VCF output —
without downloading any external data.

### 1. Build a cache from a local Ensembl VEP cache directory

`tests/data/ensembl_cache` contains a tiny slice of the Ensembl VEP 115
offline cache (chr22). Convert it to the default indexed Parquet cache:

```python
import vepyr

results = vepyr.build_cache(
    release=115,
    cache_dir="/tmp/vepyr_cache",
    cache_type="ensembl",
    local_cache="tests/data/ensembl_cache",  # skip download
)
for path, rows in results:
    print(f"{path}: {rows:,} rows")
```

This vepyr release supports exactly cache 115 with VEP 115.2 semantics and
cache 116 with VEP 116.0 semantics. `build_cache()` embeds
`bio.vep.cache_version` in every generated Parquet shard. Annotation requires
that metadata and validates it lazily per contig across every participating
entity; metadata-less, mixed, malformed, or unsupported caches are rejected.
Directory names and sidecar files are never used as annotation-cache identity.
The optional `expected_cache_version="115"` (or `"116"`) argument is a strict
assertion, not an override.

### 2a. Annotate variants

A small 5-variant VCF for chr22 ships with the cache fixture:

```python
import vepyr

cache_dir = "/tmp/vepyr_cache/115_GRCh38_ensembl"

lf = vepyr.annotate(
    vcf="tests/data/ensembl_cache/sample.vcf",
    cache_dir=cache_dir,
    check_existing=True,
    af=True,
    af_gnomadg=True,
    max_af=True,
)

df = lf.collect()
print(df.select("chrom", "start", "ref", "alt", "most_severe_consequence").head())
```

`workers` controls how many within-contig annotation pipelines run
concurrently. `workers=1` is the serial path; `workers > 1` requires a
tabix-indexed (bgzip + `.tbi`) input VCF.

```python
df = vepyr.annotate(
    "input.vcf.gz",
    cache_dir,
    workers=4,
).collect()
```

`build_cache()` writes variation as `chrN_warm.parquet` and
`chrN_cold.parquet` files, plus cold-position and variant-bloom indexes.
Re-running
`build_cache()` is idempotent by default; pass `overwrite=True` to rebuild
existing cache outputs.

```python
out = vepyr.annotate(
    "input.vcf.gz",
    cache_dir,
    workers=8,
    output_vcf="annotated.vcf",
)
```

### 2b. Write annotated VCF output

Instead of a LazyFrame, write results directly to a VCF file with CSQ in the
INFO column — use `.vcf.gz` for bgzf compression or `.vcf` for plain text:

```python
out_path = vepyr.annotate(
    vcf="tests/data/ensembl_cache/sample.vcf",
    cache_dir=cache_dir,
    check_existing=True,
    af=True,
    af_gnomadg=True,
    max_af=True,
    output_vcf="/tmp/annotated.vcf",  # or .vcf.gz for bgzf
)
print(f"Wrote annotated VCF to {out_path}")
```

### 3. Full `--everything` annotation (golden test data)

`tests/data/golden` has a pre-built chr1 cache, a 100-variant VCF, and a
matching reference FASTA. Run a full `--everything` annotation:

```python
import vepyr

lf = vepyr.annotate(
    vcf="tests/data/golden/input.vcf.gz",
    cache_dir="tests/data/golden/cache",
    everything=True,
    reference_fasta="tests/data/golden/reference.fa",
)

df = lf.collect()
print(f"{df.height} variants × {df.width} columns")
print(df.select("chrom", "start", "ref", "alt",
                "most_severe_consequence", "SYMBOL", "IMPACT").head(5))
```

## Documentation

Build and serve the docs locally:

```bash
uv sync --extra docs
uv run mkdocs serve
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000). Docs are auto-deployed to GitHub Pages on each tag push.

### One-liner smoke test

Exercises cache build, indexed Parquet annotation, and VCF output:

```bash
uv run python -c "
import vepyr, tempfile, os
with tempfile.TemporaryDirectory() as d:
    r = vepyr.build_cache(115, d, cache_type='ensembl', local_cache='tests/data/ensembl_cache', show_progress=False)
    cache = os.path.join(d, '115_GRCh38_ensembl')
    print(f'build_cache : {len(r)} parquet files, {sum(n for _,n in r):,} rows')
    vcf = 'tests/data/ensembl_cache/sample.vcf'
    df1 = vepyr.annotate(vcf, cache, check_existing=True, af=True, max_af=True).collect()
    print(f'indexed     : {df1.height} variants × {df1.width} columns')
    out = os.path.join(d, 'annotated.vcf')
    vepyr.annotate(vcf, cache, check_existing=True, af=True, max_af=True, output_vcf=out, show_progress=False)
    print(f'vcf output  : {os.path.getsize(out):,} bytes')
    assert os.path.getsize(out) > 0, 'empty VCF'
lf = vepyr.annotate('tests/data/golden/input.vcf.gz', 'tests/data/golden/cache', everything=True, reference_fasta='tests/data/golden/reference.fa')
df = lf.collect()
print(f'everything  : {df.height} variants × {df.width} columns')
assert df.height > 0 and df.width > 80, 'smoke test failed'
print('smoke test passed')
"
```


| Source                                                                   | Added fields                                                     | Count |
  |--------------------------------------------------------------------------|------------------------------------------------------------------|------:|
| VCF CSQ fixed base fields                                                | Allele, Consequence, IMPACT, SYMBOL, Gene, etc.                  |    18 |
| --everything --hgvs flag-derived fields, de-duplicated against VCF base  | includes frequency, MANE, UniProt, HGVS offset, regulatory, etc. |    59 |
| VEP option-set implication: frequency/pubmed flags enable check_existing | CLIN_SIG, SOMATIC, PHENO                                         |     3 |
| --merged                                                                 | REFSEQ_MATCH, SOURCE, REFSEQ_OFFSET                              |     3 |
| --flag_pick_allele_gene                                                  | PICK                                                             |     1 |
| BAM-edited cache auto-enables --use_transcript_ref + bam_edited          | GIVEN_REF, USED_REF, BAM_EDIT                                    |     3 |
| Total                                                                    |                                                                  |    87 |


|  # | Field                 | Breakdown bucket                        |
|---:|-----------------------|-----------------------------------------|
|  1 | Allele                | VCF CSQ fixed base                      |
|  2 | Consequence           | VCF CSQ fixed base                      |
|  3 | IMPACT                | VCF CSQ fixed base                      |
|  4 | SYMBOL                | VCF CSQ fixed base                      |
|  5 | Gene                  | VCF CSQ fixed base                      |
|  6 | Feature_type          | VCF CSQ fixed base                      |
|  7 | Feature               | VCF CSQ fixed base                      |
|  8 | BIOTYPE               | VCF CSQ fixed base                      |
|  9 | EXON                  | VCF CSQ fixed base                      |
| 10 | INTRON                | VCF CSQ fixed base                      |
| 11 | HGVSc                 | VCF CSQ fixed base                      |
| 12 | HGVSp                 | VCF CSQ fixed base                      |
| 13 | cDNA_position         | VCF CSQ fixed base                      |
| 14 | CDS_position          | VCF CSQ fixed base                      |
| 15 | Protein_position      | VCF CSQ fixed base                      |
| 16 | Amino_acids           | VCF CSQ fixed base                      |
| 17 | Codons                | VCF CSQ fixed base                      |
| 18 | Existing_variation    | VCF CSQ fixed base                      |
| 19 | DISTANCE              | Default / --everything flag-derived     |
| 20 | STRAND                | Default / --everything flag-derived     |
| 21 | FLAGS                 | Default / --everything flag-derived     |
| 22 | PICK                  | --flag_pick_allele_gene                 |
| 23 | VARIANT_CLASS         | --everything                            |
| 24 | SYMBOL_SOURCE         | --everything                            |
| 25 | HGNC_ID               | --everything                            |
| 26 | CANONICAL             | --everything                            |
| 27 | MANE                  | --everything                            |
| 28 | MANE_SELECT           | --everything                            |
| 29 | MANE_PLUS_CLINICAL    | --everything                            |
| 30 | TSL                   | --everything                            |
| 31 | APPRIS                | --everything                            |
| 32 | CCDS                  | --everything                            |
| 33 | ENSP                  | --everything                            |
| 34 | SWISSPROT             | --everything                            |
| 35 | TREMBL                | --everything                            |
| 36 | UNIPARC               | --everything                            |
| 37 | UNIPROT_ISOFORM       | --everything                            |
| 38 | REFSEQ_MATCH          | --merged                                |
| 39 | SOURCE                | --merged                                |
| 40 | REFSEQ_OFFSET         | --merged                                |
| 41 | GIVEN_REF             | BAM-edited cache / --use_transcript_ref |
| 42 | USED_REF              | BAM-edited cache / --use_transcript_ref |
| 43 | BAM_EDIT              | BAM-edited cache                        |
| 44 | GENE_PHENO            | --everything                            |
| 45 | SIFT                  | --everything                            |
| 46 | PolyPhen              | --everything                            |
| 47 | DOMAINS               | --everything                            |
| 48 | miRNA                 | --everything                            |
| 49 | HGVS_OFFSET           | --everything --hgvs                     |
| 50 | AF                    | --everything                            |
| 51 | AFR_AF                | --everything                            |
| 52 | AMR_AF                | --everything                            |
| 53 | EAS_AF                | --everything                            |
| 54 | EUR_AF                | --everything                            |
| 55 | SAS_AF                | --everything                            |
| 56 | gnomADe_AF            | --everything                            |
| 57 | gnomADe_AFR_AF        | --everything                            |
| 58 | gnomADe_AMR_AF        | --everything                            |
| 59 | gnomADe_ASJ_AF        | --everything                            |
| 60 | gnomADe_EAS_AF        | --everything                            |
| 61 | gnomADe_FIN_AF        | --everything                            |
| 62 | gnomADe_MID_AF        | --everything                            |
| 63 | gnomADe_NFE_AF        | --everything                            |
| 64 | gnomADe_REMAINING_AF  | --everything                            |
| 65 | gnomADe_SAS_AF        | --everything                            |
| 66 | gnomADg_AF            | --everything                            |
| 67 | gnomADg_AFR_AF        | --everything                            |
| 68 | gnomADg_AMI_AF        | --everything                            |
| 69 | gnomADg_AMR_AF        | --everything                            |
| 70 | gnomADg_ASJ_AF        | --everything                            |
| 71 | gnomADg_EAS_AF        | --everything                            |
| 72 | gnomADg_FIN_AF        | --everything                            |
| 73 | gnomADg_MID_AF        | --everything                            |
| 74 | gnomADg_NFE_AF        | --everything                            |
| 75 | gnomADg_REMAINING_AF  | --everything                            |
| 76 | gnomADg_SAS_AF        | --everything                            |
| 77 | MAX_AF                | --everything                            |
| 78 | MAX_AF_POPS           | --everything                            |
| 79 | CLIN_SIG              | implied check_existing                  |
| 80 | SOMATIC               | implied check_existing                  |
| 81 | PHENO                 | implied check_existing                  |
| 82 | PUBMED                | --everything                            |
| 83 | MOTIF_NAME            | --everything                            |
| 84 | MOTIF_POS             | --everything                            |
| 85 | HIGH_INF_POS          | --everything                            |
| 86 | MOTIF_SCORE_CHANGE    | --everything                            |
| 87 | TRANSCRIPTION_FACTORS | --everything                            |
