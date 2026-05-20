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

## Plugin sources

`vepyr` can now download raw source files and convert them into plugin parquet
plus `<plugin>.fjall` point-lookup stores via `build_plugin()`.

```python
import vepyr

source_path = vepyr.fetch_plugin_source("alphamissense", "/data/vep/cache")
vepyr.build_plugin("alphamissense", source_path, "/data/vep/cache")
```

You can also keep the workflow chromosome-scoped:

```python
source_path = vepyr.fetch_plugin_source(
    "clinvar",
    "/data/vep/cache",
    chromosomes=["1"],
)
vepyr.build_plugin(
    "clinvar",
    source_path,
    "/data/vep/cache",
    chromosomes=["1"],
)

vepyr.build_plugin(
    "spliceai",
    "/data/plugins/spliceai.vcf.gz",
    "/data/vep/cache/115_GRCh38_vep",
    assume_sorted_input=True,
)

vepyr.build_plugin(
    "dbnsfp",
    "/data/plugins/dbNSFP5.3.1a_grch38.gz",
    "/data/vep/cache/115_GRCh38_vep",
    chromosomes=["1"],
    preview_rows=1000,
)
```

Downloaded files are stored under:

```text
<cache_dir>/plugin_sources/<plugin>/<assembly>/<version>/<scope>/
```

Built plugin caches are stored under:

```text
<cache_dir>/<release>_<assembly>_<method>/<plugin>/chr*.parquet
<cache_dir>/<release>_<assembly>_<method>/<plugin>.fjall/
```

For local plugin files that are already sorted by `chrom,pos,ref,alt`, you can
opt in to skipping the SQL `ORDER BY` during conversion with
`assume_sorted_input=True`. This currently applies only to single-source
plugins; `cadd` still keeps the explicit sort because it merges SNV and indel
inputs.

`preview_rows=` is also available on `build_plugin()` for reduced-scope local
validation. It is most useful together with `chromosomes=[...]`, especially
for indexed plugin sources that can be sliced through `tabix`.

Supported automated sources in the current implementation:

- `alphamissense`
- `cadd` (SNV source file)
- `spliceai` (GRCh38 Ensembl plugin VCF)
- `dbnsfp` (GRCh38 merged source prepared from the vendor zip)
- `clinvar`

Current chromosome-aware source strategies:

- `clinvar`, `spliceai`: indexed VCF region slicing via `tabix` + `bgzip`
- `dbnsfp`: chromosome-aware assembly from per-chromosome files inside the
  vendor zip
- `alphamissense`, `cadd`: full source download followed by local
  chromosome filtering

When building from already-downloaded local files, CADD materializes one shared
cache. The issue-like default is to point at the SNV source file and keep the
official indel file next to it in the same directory:

- `vepyr.build_plugin("cadd", "/data/plugins/whole_genome_SNVs.tsv.gz", "/data/vep/cache/115_GRCh38_vep")` -> `/data/vep/cache/115_GRCh38_vep/cadd/` + `/data/vep/cache/115_GRCh38_vep/cadd.fjall/`

You can also build the core cache and selected plugin caches in one call via
`build_cache(..., plugins=...)`.

List mode auto-downloads supported sources:

```python
import vepyr

vepyr.build_cache(
    release=115,
    cache_dir="/tmp/vepyr_cache",
    cache_type="ensembl",
    local_cache="tests/data/ensembl_cache",  # skip download
    build_fjall=True,                         # parquet + fjall
    plugins=["clinvar", "spliceai", "cadd"],  # optional plugin sources
)
```

Mapping mode uses explicit local paths instead of downloading. Logical `cadd`
accepts the SNV source path `whole_genome_SNVs.tsv.gz` and resolves the
official sibling indel file `gnomad.genomes.r4.0.indel.tsv.gz` automatically.
Dict and tuple forms remain accepted for compatibility.

```python
import vepyr

cache_dir = "/tmp/vepyr_cache/parquet/115_GRCh38_ensembl"

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

### 2b. Annotate variants (fjall backend)

Pass `use_fjall=True` to use the embedded KV store instead of Parquet for
co-located variant lookups — same API, faster on large caches:

```python
lf = vepyr.annotate(
    vcf="tests/data/ensembl_cache/sample.vcf",
    cache_dir=cache_dir,
    check_existing=True,
    af=True,
    af_gnomadg=True,
    max_af=True,
    use_fjall=True,  # <-- only difference
)

df = lf.collect()
print(df.select("chrom", "start", "ref", "alt", "most_severe_consequence").head())
```

### 2c. Write annotated VCF output

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

Exercises cache build, both annotation backends, and VCF output:

```bash
uv run python -c "
import vepyr, tempfile, os
with tempfile.TemporaryDirectory() as d:
    r = vepyr.build_cache(115, d, cache_type='ensembl', local_cache='tests/data/ensembl_cache', build_fjall=True, show_progress=False)
    cache = os.path.join(d, 'parquet', '115_GRCh38_ensembl')
    print(f'build_cache : {len(r)} parquet files, {sum(n for _,n in r):,} rows')
    vcf = 'tests/data/ensembl_cache/sample.vcf'
    df1 = vepyr.annotate(vcf, cache, check_existing=True, af=True, max_af=True).collect()
    print(f'parquet     : {df1.height} variants × {df1.width} columns')
    df2 = vepyr.annotate(vcf, cache, check_existing=True, af=True, max_af=True, use_fjall=True).collect()
    print(f'fjall       : {df2.height} variants × {df2.width} columns')
    assert df1.height == df2.height and df1.width == df2.width, 'backend mismatch'
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

### Plugin source mapping

Beyond the auto-download list form, you can pass an explicit path map. Logical
`cadd` accepts the SNV source `whole_genome_SNVs.tsv.gz` and resolves the
sibling indel file automatically.

```python
vepyr.build_cache(
    release=115,
    cache_dir="/data/vep/cache",
    species="homo_sapiens",
    assembly="GRCh38",
    plugins={
        "clinvar": "/data/plugins/clinvar.vcf.gz",
        "alphamissense": "/data/plugins/AlphaMissense_hg38.tsv.gz",
        "cadd": "/data/plugins/whole_genome_SNVs.tsv.gz",
    },
)
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
