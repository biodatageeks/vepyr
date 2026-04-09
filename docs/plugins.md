# Plugins

vepyr will support external variant annotation databases as plugins, converting raw data sources into optimized Parquet files for fast annotation lookups.

!!! warning "Not yet available"
    Plugin support is under active development. See [datafusion-bio-formats#137](https://github.com/biodatageeks/datafusion-bio-formats/issues/137) for progress.

## Planned plugins

| Plugin | Description | Source |
|---|---|---|
| **CADD v1.7** | Combined Annotation Dependent Depletion scores (SNVs + indels) | [cadd.gs.washington.edu](https://cadd.gs.washington.edu/) |
| **SpliceAI** | Deep-learning splice variant predictions | [Illumina/SpliceAI](https://github.com/Illumina/SpliceAI) |
| **AlphaMissense** | Protein pathogenicity predictions (DeepMind) | [Zenodo](https://zenodo.org/records/8208688) |
| **ClinVar** | NCBI clinical variant classifications | [ncbi.nlm.nih.gov/clinvar](https://www.ncbi.nlm.nih.gov/clinvar/) |
| **dbNSFP v4.x** | Aggregated functional prediction scores (30+ predictors) | [dbNSFP](https://sites.google.com/site/jpaborern/dbNSFP) |
