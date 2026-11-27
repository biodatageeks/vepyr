# HG002 trio: three tiers of annotation

The analysis behind Figure 1B and Supplementary Section 5.7 of the vepyr paper:
the GIAB HG002 genome (NIST v4.2.1, chromosomes 1-22, 4,096,123 variants after
splitting) is annotated with vepyr and analysed together with data of both
parents and other sources read from their URLs, in one Python session.

| Tier | Source | Mechanism |
|---|---|---|
| 1 core cache | Ensembl 116 `ensembl` vepyr cache | `vepyr.annotate()` |
| 2 plugin caches | ClinVar (weekly release 2026-07-06), AlphaMissense, SpliceAI, CADD v1.7 | `vepyr.annotate(plugins=...)` |
| 3 ad hoc | GIAB v3.6 stratifications, v4.2.1 benchmark regions and VCFs of HG002's parents, gnomAD v4.1 constraint, ClinGen gene-disease validity, Ensembl 116 GFF3 (12 files) | read from the URL on every run (polars-bio, Polars) |

## Reproduce

```bash
python -m venv hg002 && . hg002/bin/activate
pip install "vepyr[polars-bio]==0.11.0" "polars-bio==0.38.0" scipy matplotlib
export HG002_DATA=/path/to/data      # layout below
python analysis.py all               # tiers 1-3 in one Python session -> results/
python figures.py                    # figures/figS_*.pdf
```

`python analysis.py annotate` and `python analysis.py analyze` run the two
stages separately. `HG002_CACHE`, `HG002_ANNOTATED` and `HG002_RESULTS`
override the core cache, the annotated Parquet file and the results directory.

Inputs under `$HG002_DATA`:

- `hg002/HG002_v4.2.1.split.vcf.gz` (+ `.tbi`): the GIAB v4.2.1 benchmark VCF,
  chromosomes 1-22, multiallelic records split;
- `vepyr_cache/116_GRCh38_ensembl`: the core cache, chromosomes 1-22 (30 GiB),
  from `vepyr.download_cache(116, "vepyr_cache", cache_type="ensembl",
  chroms=[str(c) for c in range(1, 23)])`. vepyr 0.11.0 needs a cache with the
  `gene_start` and `gene_end` transcript columns, so a cache built or downloaded
  with vepyr 0.10.0 or earlier does not work;
- `vepyr_plugin_cache/plugin/{clinvar,alphamissense,spliceai,cadd}`: plugin
  caches, chromosomes 1-22 (90 GiB), built with `vepyr.build_plugin_cache()`
  from the vepyr-plugins manifests v0.1.1 (commit 3e1c039);
- `Homo_sapiens.GRCh38.dna.primary_assembly.fa` (+ `.fai`): the Ensembl
  GRCh38 reference.

Tier-3 sources are never stored. `results/sources.json` records what was read,
from where, how many rows and how long each read took. The ClinGen file
changes daily, so the ClinGen counts depend on the day of the run.

## Results

`results/` holds the run reported in the paper (27 November 2026, vepyr 0.11.0,
polars-bio 0.38.0, polars 1.44.2, MacBook Air with Apple M2 and 24 GB, on mains
power): annotation 179.9 s with 8 workers, queries 87.6 s (68.5 s of it reading
the 12 files), 269.3 s for the whole session (`results/session.json`). The
annotation is identical, in all 33 columns written, to that of vepyr 0.10.0.
