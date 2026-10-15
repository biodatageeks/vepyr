# HG002: three tiers of annotation

Prototype of a Supplementary Data section and of Figure 1D for the vepyr
paper. `hg002-tiered-annotation.pdf` is the working document; each block in it
is tagged with its destination (main text, supplementary, not for the paper).

| Tier | Source | Mechanism |
|---|---|---|
| 1 core cache | Ensembl 116 `ensembl` vepyr cache | `vepyr.annotate()` |
| 2 plugin caches | ClinVar, AlphaMissense, SpliceAI, CADD | `vepyr.annotate(plugins=...)` |
| 3 ad hoc | gnomAD v4.1 constraint, ClinGen, Ensembl 116 GFF3, GIAB v3.6/v4.2.1 BED, parental VCFs | read from the URL on every run (polars-bio / Polars) |

## Reproduce

```bash
python -m venv hg002 && . hg002/bin/activate
pip install "vepyr[polars-bio]==0.10.0" "polars-bio==0.36.2" scipy matplotlib
# inputs (see the Data and software section of the PDF): split HG002 VCF,
# vepyr_cache/116_GRCh38_ensembl and vepyr_plugin_cache for chr1-22, FASTA
export HG002_DATA=/path/to/data      # layout as in analysis.py
python analysis.py annotate          # tiers 1+2, ~4 min, 8 workers -> Parquet
python analysis.py analyze           # tier 3 + analysis, ~1.5 min -> results/
python figures.py                    # figures/figS_*.pdf
python make_tex.py                   # results/tex/*.tex (macros and tables)
(cd ../figure1 && python3 build_tiered_annotation.py --render)   # Figure 1D
pdflatex hg002-tiered-annotation.tex && pdflatex hg002-tiered-annotation.tex
```

Disk: core cache 30 GiB and plugin caches 90 GiB (chr1-22). Tier-3 sources are
never stored; `results/sources.json` records what was read, from where and how
long it took.
