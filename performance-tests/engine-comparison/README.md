# vepyr, vep-rs and fastVEP against Ensembl VEP

[`scripts/compare_vep_engines.py`](scripts/compare_vep_engines.py) puts the three
Rust VEP engines through the same measurement on one chromosome:

- builds [vep-rs](https://github.com/natera-open-source/vep-rs) and
  [fastVEP](https://github.com/Huang-lab/fastVEP) from source and installs a
  vepyr release from PyPI;
- prepares each engine's cache from the Ensembl VEP cache that vepyr's Parquet
  cache was converted from, for both the Ensembl and the merged cache;
- times every engine at each thread count, recording wall time, CPU time and
  peak RSS;
- compares every CSQ column of every engine's output with an Ensembl VEP
  `--everything --hgvs` reference, cell by cell;
- writes `results.json` and `report.md`: the columns each engine emits, the
  columns with no mismatches, cache sizes with and without the SIFT/PolyPhen
  matrices, timings, and whether each engine's output is the same at every
  thread count.

The script uses only the Python standard library. It needs `git`, `cargo`,
`perl` (Storable, JSON::PP and MIME::Base64 are core modules), `bcftools` and
`tabix` on `PATH`, and network access for the clones, the GFF3 downloads and
PyPI.

Results from one run: [`outputs/116/macos_mwiewior_chr22_20261017`](outputs/116/macos_mwiewior_chr22_20261017/report.md).

## Running it

```bash
D=/data/vepyr
python3 performance-tests/engine-comparison/scripts/compare_vep_engines.py all \
  --work-dir /scratch/engine-comparison --chrom 22 \
  --input-vcf $D/input/HG002_chr22.vcf.gz \
  --fasta $D/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
  --vep-cache-ensembl $D/homo_sapiens/116_GRCh38 \
  --vep-cache-merged $D/homo_sapiens_merged/116_GRCh38 \
  --vepyr-cache-ensembl $D/cache/116_GRCh38_ensembl \
  --vepyr-cache-merged $D/cache/116_GRCh38_merged \
  --reference-ensembl $D/output/116/HG002_annotated_wgs_everything_hgvs_vep.vcf.gz \
  --reference-merged $D/output/116/HG002_annotated_wgs_everything_hgvs_merged.vcf.gz
```

`all` runs the five steps in order: `build`, `caches`, `run`, `compare`,
`report`. Each step can also be run on its own, and each skips work whose
outputs already exist, so an interrupted run resumes where it stopped;
`--force` redoes it. `--vep-rs-ref` and `--fastvep-ref` pick a branch, tag or
commit (defaults: `main`, `master`), `--vepyr-version` the PyPI release
(default `0.10.0`) or `--vepyr-bin` an executable already installed.
`--threads` (default `1,2`) and `--repeats` (default 3) set the timing matrix.

Inputs:

- **Input VCF**: bgzipped, tabix-indexed and normalised with
  `bcftools norm -m -both`, like every parity comparison in this repository.
  It may hold the whole genome; only `--chrom` is annotated by the other engines'
  caches, so give it a one-chromosome slice for timings to mean anything.
- **References**: Ensembl VEP's `--everything --hgvs --vcf` output on the same
  input, with and without `--merged`, tabix-indexed. Whole-genome files are
  fine; the script slices the chromosome out.
- **Ensembl VEP caches**: the Storable caches (`homo_sapiens/116_GRCh38` and
  `homo_sapiens_merged/116_GRCh38`), read but never written to.

## What runs

| Engine | Mode | Cache it reads | Command |
|---|---|---|---|
| vepyr | `default` | vepyr's Parquet cache | `vepyr annotate --fork N` |
| vep-rs | `sift` | the Storable converted to JSON, with SIFT/PolyPhen matrices | `vep --everything --hgvs --fork N`, `RAYON_NUM_THREADS=N` |
| vep-rs | `nosift` | the same, converted by `storable_to_json.pl` as published (no matrices) | same |
| fastVEP | `tabix` | transcripts from the Ensembl (and RefSeq) GFF3; co-located variants from the Storable cache's `all_vars.gz` | `fastvep annotate --everything --hgvs --cache-dir`, `RAYON_NUM_THREADS=N` |
| fastVEP | `osa2` | the same transcripts; co-located variants from a `custom_vcf` `.osa2` built from `all_vars.gz` | `fastvep annotate --everything --hgvs --sa-dir` |

Notes on the engines' caches:

- **vep-rs.** `scripts/data/storable_to_json.pl` from the vep-rs checkout
  converts the transcripts. As published it drops the SIFT/PolyPhen matrices:
  the Storable keeps each one gzip-compressed in `matrix`, and the converter
  copies only `predictions_data`, which a Storable never has. The `sift` mode
  uses a copy patched to base64-encode `matrix` into `predictions_data`, which
  is what vep-rs reads. The co-located variants are converted from
  `all_vars.gz` to vep-rs's `variations/` JSON. vep-rs 0.4.0 reads only plain
  `.json` variation shards and skips anything else without a warning; the
  script checks the checkout's loader and writes gzipped shards only when it
  reads them. vep-rs has no `--merged`: on the merged cache it annotates every
  transcript of the merged conversion and writes no `SOURCE`.
- **vep-rs with the fixes.** The
  [`converted-cache-parity`](https://github.com/mwiewior/vep-rs/tree/converted-cache-parity)
  branch of mwiewior/vep-rs puts into vep-rs itself the three things this
  script otherwise does around it: the loader reads gzipped variation shards,
  `storable_to_json.pl` writes the matrices, and
  `scripts/data/all_vars_to_json.py` converts the co-located variants. Run it
  with
  `--vep-rs-repo https://github.com/mwiewior/vep-rs.git --vep-rs-ref converted-cache-parity`.
  The script detects both fixes in the checkout: it writes gzipped variation
  shards and uses the converter as it is, and the `nosift` cache then has the
  matrices stripped from its shards, so the two modes mean the same thing on
  either branch.
- **fastVEP.** Transcripts always come from GFF3: the release's per-chromosome
  Ensembl GFF3, plus for the merged cache the RefSeq GFF the merged cache's
  `info.txt` names (`source_refseq`). `--cache-dir` needs `all_vars.gz` with a
  `.tbi`, and the Ensembl cache ships a `.csi`, so the script symlinks the file
  into its own directory and indexes it there. The `.osa2` is built from the
  same `all_vars.gz`, written as a VCF with anchored alleles and normalised
  with `bcftools norm`: fastVEP looks an `.osa2` up by the input's exact
  (POS, REF, ALT), and many cache indels are stored across a whole repeat. Its
  values go to an `FV_VEPCACHE` INFO field rather than CSQ, so the `osa2` mode
  is timed and its co-located coverage reported, but its CSQ is not scored.

## How it measures

**Timing.** Each configuration gets one discarded warm-up run, then
`--repeats` timed runs, and the report gives the median. fastVEP's `tabix`
mode re-reads the tabix index for every variant and takes minutes per run on
a chromosome, so it runs `--fastvep-tabix-repeats` times (default 1) with no
warm-up. Wall time, user and system CPU time and peak RSS come from `wait4`
on the engine process. vep-rs needs `RAYON_NUM_THREADS` as well as `--fork`:
`--fork` sizes only its annotation pool, while loading the variation cache runs
on rayon's global pool, which otherwise takes every core.

**Comparison.** A row is one CSQ entry, keyed by (Allele, Feature_type,
Feature), and every column is compared as an exact string match. This is
stricter than the parity gate in `e2e-testing/`, which pairs entries and
applies vepyr-specific equivalence rules; here all three engines go through
the same comparator. Before comparing fastVEP, its GFF3 identifiers
(`rna-NM_…` in Feature and HGVSc) and the `-` placeholders on its intergenic
rows are rewritten to Ensembl's form; nothing else is changed.

A column's status:

| Status | Meaning |
|---|---|
| exact | every shared row has the same value |
| empty in both | Ensembl has no value in this column anywhere on the chromosome |
| never filled | Ensembl's values are all on rows the engine does not write, such as the motif columns for an engine with no regulatory annotation; not counted as a match |
| a percentage | share of shared rows with the same value |
| not emitted | the column is not in the engine's CSQ |

"Columns with no mismatches" counts exact and empty-in-both columns.

**Cache sizes** cover the chromosome only. For the Storable cache the matrices
are measured inside its transcript files, counting each shared matrix object
once; for vepyr they are the `translation_sift` table; for vep-rs they are the
difference between the `sift` and `nosift` conversions. fastVEP has nowhere to
keep them: its CSQ `SIFT` and `PolyPhen` are always empty, so it has no "with"
figure.

## Limits

- vep-rs loads every co-located variant of the chromosome into memory before
  annotating, about 24 GB for chr22. The largest chromosomes are about six
  times bigger, so they may not fit on the machine running the comparison.
- The script compares one chromosome per run. The comparator streams both files
  a record at a time, so the limit is the caches: every chromosome would need
  its own conversion, and vep-rs's memory grows with the chromosome as above.
