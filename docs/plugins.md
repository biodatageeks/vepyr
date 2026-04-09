# Plugins

vepyr supports external variant annotation databases as **plugins**. Each plugin converts a raw data source (TSV, VCF) into sorted, partitioned Parquet files that sit alongside the Ensembl cache and are used for annotation lookups.

!!! note "Status"
    Plugin support is under active development. See the [tracking issue](https://github.com/biodatageeks/datafusion-bio-formats/issues/137) for progress.

## Supported plugins

| Plugin | Description | Input format | Raw size | Parquet size |
|---|---|---|---|---|
| **CADD v1.7** | Combined Annotation Dependent Depletion scores | TSV.gz (tabix) | ~80 GB | ~15-20 GB |
| **SpliceAI** | Deep-learning splice variant predictions | VCF.gz / TSV | ~30 GB | ~5-8 GB |
| **AlphaMissense** | Protein pathogenicity predictions | TSV.gz | ~1 GB | ~200 MB |
| **ClinVar** | NCBI clinical variant classifications | VCF.gz | ~100 MB | ~30 MB |
| **dbNSFP v4.x** | Aggregated functional prediction scores (30+ predictors) | TSV.gz (tabix) | ~30 GB | ~10-15 GB |

## Planned usage

### Build cache with plugins (auto-download)

```python
import vepyr

vepyr.build_cache(
    release=115,
    cache_dir="/data/vep",
    plugins=["cadd", "spliceai", "alphamissense", "clinvar"],
)
```

### Build cache with local plugin data

```python
vepyr.build_cache(
    release=115,
    cache_dir="/data/vep",
    plugins={
        "cadd": "/path/to/whole_genome_SNVs.tsv.gz",
        "spliceai": "/path/to/spliceai_scores.vcf.gz",
    },
)
```

### Annotate with plugin scores

Once plugin data is built into the cache, annotation picks it up automatically:

```python
lf = vepyr.annotate(
    vcf="input.vcf.gz",
    cache_dir="/data/vep/parquet/115_GRCh38_vep",
    everything=True,
    reference_fasta="GRCh38.fa",
)

# Plugin columns are available alongside core VEP annotations
df = lf.select(
    "chrom", "start", "ref", "alt",
    "SYMBOL", "Consequence",
    "cadd_phred",           # CADD
    "am_pathogenicity",     # AlphaMissense
    "clnsig",               # ClinVar
    "revel_score",          # dbNSFP
    "spliceai_ds_ag",       # SpliceAI
).collect()
```

## Plugin schemas

### CADD

Raw and PHRED-scaled deleteriousness scores for SNVs and indels.

| Column | Type | Description |
|---|---|---|
| `chrom` | Utf8 | Chromosome |
| `pos` | UInt32 | Position (1-based) |
| `ref` | Utf8 | Reference allele |
| `alt` | Utf8 | Alternate allele |
| `raw_score` | Float32 | Raw CADD score |
| `phred_score` | Float32 | PHRED-scaled score |

Source: [CADD v1.7](https://cadd.gs.washington.edu/)

### SpliceAI

Delta scores and positions for splice-altering variants.

| Column | Type | Description |
|---|---|---|
| `chrom` | Utf8 | Chromosome |
| `pos` | UInt32 | Position (1-based) |
| `ref` | Utf8 | Reference allele |
| `alt` | Utf8 | Alternate allele |
| `symbol` | Utf8 | Gene symbol |
| `ds_ag` | Float32 | Delta score — acceptor gain |
| `ds_al` | Float32 | Delta score — acceptor loss |
| `ds_dg` | Float32 | Delta score — donor gain |
| `ds_dl` | Float32 | Delta score — donor loss |
| `dp_ag` | Int32 | Delta position — acceptor gain |
| `dp_al` | Int32 | Delta position — acceptor loss |
| `dp_dg` | Int32 | Delta position — donor gain |
| `dp_dl` | Int32 | Delta position — donor loss |

Source: [SpliceAI](https://github.com/Illumina/SpliceAI)

### AlphaMissense

Protein variant pathogenicity predictions from DeepMind.

| Column | Type | Description |
|---|---|---|
| `chrom` | Utf8 | Chromosome |
| `pos` | UInt32 | Position (1-based) |
| `ref` | Utf8 | Reference allele |
| `alt` | Utf8 | Alternate allele |
| `genome` | Utf8 | Genome assembly |
| `uniprot_id` | Utf8 | UniProt accession |
| `transcript_id` | Utf8 | Ensembl transcript ID |
| `protein_variant` | Utf8 | Protein change (e.g. `A123T`) |
| `am_pathogenicity` | Float32 | Pathogenicity score (0-1) |
| `am_class` | Utf8 | `likely_benign`, `ambiguous`, or `likely_pathogenic` |

Source: [AlphaMissense (Zenodo)](https://zenodo.org/records/8208688)

### ClinVar

NCBI clinical significance classifications.

| Column | Type | Description |
|---|---|---|
| `chrom` | Utf8 | Chromosome |
| `pos` | UInt32 | Position (1-based) |
| `ref` | Utf8 | Reference allele |
| `alt` | Utf8 | Alternate allele |
| `clnsig` | Utf8 | Clinical significance (e.g. `Pathogenic`) |
| `clnrevstat` | Utf8 | Review status |
| `clndn` | Utf8 | Disease name |
| `clnvc` | Utf8 | Variant class |
| `clnvi` | Utf8 | Variant identifiers |

Source: [ClinVar](https://www.ncbi.nlm.nih.gov/clinvar/)

### dbNSFP

Aggregated functional prediction and conservation scores from 30+ tools.

| Column | Type | Description |
|---|---|---|
| `chrom` | Utf8 | Chromosome |
| `pos` | UInt32 | Position (1-based) |
| `ref` | Utf8 | Reference allele |
| `alt` | Utf8 | Alternate allele |
| `sift4g_score` | Utf8 | SIFT4G scores (semicolon-separated per transcript) |
| `polyphen2_hdiv_score` | Utf8 | PolyPhen-2 HumDiv scores |
| `polyphen2_hvar_score` | Utf8 | PolyPhen-2 HumVar scores |
| `lrt_score` | Utf8 | LRT scores |
| `mutationtaster_score` | Utf8 | MutationTaster scores |
| `fathmm_score` | Utf8 | FATHMM scores |
| `provean_score` | Utf8 | PROVEAN scores |
| `vest4_score` | Utf8 | VEST4 scores |
| `metasvm_score` | Utf8 | MetaSVM scores |
| `metalr_score` | Utf8 | MetaLR scores |
| `revel_score` | Float32 | REVEL score |
| `gerp_rs` | Float32 | GERP++ rejected substitutions |
| `phylop100way` | Float32 | PhyloP 100-way score |
| `phylop30way` | Float32 | PhyloP 30-way score |
| `phastcons100way` | Float32 | PhastCons 100-way score |
| `phastcons30way` | Float32 | PhastCons 30-way score |
| `siphy_29way` | Float32 | SiPhy 29-way score |
| `cadd_raw` | Float32 | CADD raw score (from dbNSFP) |
| `cadd_phred` | Float32 | CADD PHRED score (from dbNSFP) |

Source: [dbNSFP](https://sites.google.com/site/jpaborern/dbNSFP)

!!! info "Multi-value columns"
    Columns marked as Utf8 in dbNSFP often contain semicolon-separated values (one per transcript). The annotation engine resolves the correct value based on the matched transcript.

## Output format

All plugins produce chromosome-partitioned, sorted Parquet files:

```
plugin_name/
├── chr1.parquet
├── chr2.parquet
├── ...
├── chrX.parquet
├── chrY.parquet
└── other.parquet
```

| Property | Value |
|---|---|
| Compression | ZSTD |
| Sort order | `(chrom, pos)` or `(chrom, pos, ref, alt)` |
| Row group size | ~100K rows (optimized for point lookups) |
| Bloom filter | On `chrom` and `pos` columns |

## Roadmap

- [x] Plugin architecture design ([datafusion-bio-formats#137](https://github.com/biodatageeks/datafusion-bio-formats/issues/137))
- [ ] CADD TSV → Parquet conversion (SNVs + indels)
- [ ] SpliceAI VCF → Parquet conversion
- [ ] AlphaMissense TSV → Parquet conversion
- [ ] ClinVar VCF → Parquet conversion (with multi-allelic decomposition)
- [ ] dbNSFP TSV → Parquet conversion
- [ ] `build_cache()` plugin integration (local paths + auto-download)
- [ ] Annotation engine plugin column lookups
- [ ] Round-trip tests per plugin: convert → read → verify schema and row counts
