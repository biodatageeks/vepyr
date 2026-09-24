# Polars integration supplement: filtering and pushdown benchmarks

Date: 2026-09-24. Status: approved in brainstorming, awaiting spec review.

## Goal

Add a supplementary document to the vepyr paper
(`~/research/git/papers/vepyr`, git-synced with Overleaf) with a
**Polars integration** section and two subsections, **Filtering** and
**Pushdowns**, backed by reproducible benchmarks. They compare vepyr + Polars
with Ensembl VEP 116 + `filter_vep` on variant-prioritisation queries.

The first round is **HG002 chr22 only**. After reviewing its results we decide
whether to repeat everything on chr1. WGS is out of scope for this round.

## Scope rules

- **Comparable queries only.** A query is in if `filter_vep` can express it.
  `filter_vep` sees the VCF fixed columns (CHROM, POS, ID, REF, ALT, QUAL,
  FILTER), INFO fields and CSQ sub-fields. It does **not** see FORMAT/sample
  fields (verified in `filter_vep` lines 245-270, release/115.2), so genotype
  filters (GT, DP, GQ) are excluded.
- **Process-count pairing.** VEP `--fork N` runs N+1 processes. Pairs:
  VEP no fork ↔ vepyr `workers=1`, `--fork 1` ↔ `workers=2`,
  `--fork 3` ↔ `workers=4`, `--fork 7` ↔ `workers=8`. Ratios are reported only
  for matched pairs. Tables and figures use a "processes" axis.
- **Release 116 only**: image `ensemblorg/ensembl-vep:release_116.0`, vepyr
  caches `~/workspace/data_vepyr/cache/116_GRCh38_{ensembl,merged}`, plugin
  caches `~/workspace/data_vepyr/plugin_cache_116` (confirm the directory
  before the first run).

## Query catalogue

`performance-tests/polars/queries.py` holds one explicit record per query:
`id`, `tier`, `filter_vep` expression, Polars expression (a function of the
frame shape), `cache` (`ensembl` or `merged`), `plugins` (bool), `note`.
The pairs are hand-written, with no `filter_vep`→Polars translator. The same
module generates the supplement's equivalence table.

| Id | Tier | `filter_vep` |
|---|---|---|
| R1 | region | `CHROM is chr22 and POS >= 20000000 and POS <= 25000000` |
| R2 | region | `CHROM is chr22 and POS >= 30000000 and POS <= 30100000` |
| R3 | region | ACMG SF genes on chr22 as an `or` of (CHROM, POS range) groups |
| Q1 | frequency | `MAX_AF < 0.01 or not MAX_AF` |
| Q2 | frequency | `gnomADg_AF < 0.001 or not gnomADg_AF` |
| Q3 | known | `not Existing_variation` |
| Q4 | known | `CLIN_SIG match pathogenic` |
| Q5 | consequence | `IMPACT is HIGH` |
| Q6 | consequence | `IMPACT in HIGH,MODERATE` |
| Q7 | consequence | `Consequence in stop_gained,frameshift_variant,splice_acceptor_variant,splice_donor_variant,start_lost,stop_lost and (CANONICAL is YES or MANE_SELECT)` |
| Q8 | consequence | `Consequence is missense_variant and SIFT match deleterious and PolyPhen match damaging` |
| Q9 | consequence | `SYMBOL in panels/acmg_sf.txt` |
| Q10 | composite | `IMPACT in HIGH,MODERATE and (MAX_AF < 0.01 or not MAX_AF) and (CANONICAL is YES or MANE_SELECT)` |
| P1 | plugin | `CADD_PHRED > 20` |
| P2 | plugin | `am_class is likely_pathogenic` |
| P3 | plugin | SpliceAI: `SpliceAI_pred_DS_AG >= 0.5 or SpliceAI_pred_DS_AL >= 0.5 or SpliceAI_pred_DS_DG >= 0.5 or SpliceAI_pred_DS_DL >= 0.5` |
| P4 | plugin | `ClinVar_CLNSIG match Pathogenic` |
| P5 | plugin | Q10 `and (CADD_PHRED > 20 or am_class is likely_pathogenic)` |

Tiers R and Q run on the Ensembl cache. Tier P runs on the merged cache with
the five plugins (ClinVar, SpliceAI, CADD, AlphaMissense, dbNSFP), matching
the existing VEP plugin reference outputs.

**Semantics.** `filter_vep` evaluates the whole expression against one CSQ
entry at a time (with the variant's INFO and fixed columns merged in) and
keeps the record if any entry passes. The Polars expressions reproduce this
with per-entry evaluation: `list.eval` over aligned lists, or the long frame
followed by a unique over the variant key. They never test each list
independently. Multi-field queries (Q7, Q8, Q10, P5) are where the two would
differ. Before writing R1-R3, confirm the exact field names `filter_vep`
gives the fixed columns (`CHROM` vs `#CHROM`).

**Panel.** `panels/acmg_sf.txt` is the ACMG SF v3.2 gene symbol list, with
its version recorded in the file header. R3's loci come from the 116 Ensembl
transcript cache (gene start/end per symbol, chr22 only) through a small
committed script. The resulting BED is committed so the region set does not
drift.

## Correctness gate

`scripts/verify.py` runs before any timing is accepted.

- **Record-set parity**, per query and input: the Polars result's
  (CHROM, POS, REF, ALT) set equals `filter_vep`'s. Any mismatch fails that
  query, and its timings are not reported.
- **Body parity (Experiment B only)**: vepyr's `pb.sink_vcf` output, after the
  same query, must equal VEP + `filter_vep`'s output line for line on the
  body, including CSQ. `filter_vep` runs **without** `--only_matched` so CSQ
  stays whole on both sides.
- An expected, explained difference is written into the supplement, never
  silently tolerated.

## Experiments (chr22)

**Inputs.** The HG002 chr22 slice (50,861 records) from
`HG002_normalized.vcf.gz`, tabix-indexed. VEP 116 annotations of it come
from a fresh VEP run in Experiment B. Experiment A reuses the fork-0 output
of that run.

**A. Filter only.** One annotated VCF per cache (Ensembl for R/Q, merged
with plugins for P):
- `filter_vep -i annotated.vcf -filter "<expr>" -o out.vcf` in the release_116.0
  container.
- Polars: `pb.scan_vcf(annotated.vcf)` → split CSQ into per-entry fields →
  filter → count/sink. CSQ parsing is inside the timed region.
Single process on both sides. The results record wall time, peak RSS and
output rows.

**B. End to end, including polars-bio.**
- VEP: `vep --fork N --everything [--plugin …]` then `filter_vep`, for N in
  {0,1,3,7}. Annotation and filter are timed separately and summed.
- vepyr: `annotate(vcf, cache, everything=True, reference_fasta=…, skip_csq=False, workers=W)`
  → Polars filter → `pb.sink_vcf`, for W in {1,2,4,8}. A region filter is
  pushed down, and prioritisation filters run per batch.
- Every query runs on both sides at every process count. Body parity (above)
  is checked at W=1 against N=0, and at every W against the same-N VEP output.

**C. Pushdown ablation (vepyr only, W=1 and W=8).**
- Region: R1-R3 pushed down vs the same predicate applied after `collect()`,
  which forces the full annotation.
- Projection: Q1-Q10 and P1-P5 with a narrow `select()` of the columns the
  query reads, vs the full frame. This shows the annotation flags that get
  switched off. The `sink_vcf` path needs CSQ, which re-enables every flag, so
  this ablation uses `collect()` / `sink_parquet`, and the write-up states why.
- `head(n)` as a `LIMIT` for completeness: one row, cheap.

## Measurement

- Each configuration runs in its own subprocess, and its peak RSS is attributed
  to that process only. Docker runs are timed from the host with
  `/usr/bin/time`, as in PR #117. The Docker VM overhead is stated.
- Per configuration: one discarded warm-up run, then 3 timed repeats. The
  median is reported and the min/max kept in raw results.
- The 1-minute load average is recorded before each run. A run is refused if
  the load exceeds a threshold (default: 2.0), because the host is shared.
- Every result JSON records: host, OS, CPU, date, vepyr commit and engine
  pin, polars + polars-bio versions, Docker image digest, cache paths, and
  the query id.
- Raw results go to
  `performance-tests/polars/outputs/116/macos_<host>_chr22_<date>/`.

## Layout

vepyr (`performance-tests/polars/`):
```
README.md             reproduction block and result summary
queries.py            the catalogue
panels/               acmg_sf.txt, acmg_sf_chr22.bed, make_panel_bed.py
scripts/
  prepare_chr22.sh    slice + index the chr22 input
  run_vep_chr22.sh    VEP --fork {0,1,3,7}, with and without plugins
  run_filter_only.py  Experiment A
  run_e2e.py          Experiment B
  run_pushdown.py     Experiment C
  verify.py           correctness gate
  make_supplement.py  results → LaTeX tables + PDF figures
outputs/116/…         raw results (committed; VCF outputs are not)
```

Paper (`papers/vepyr/supplementary/`):
```
vepyr-supplementary.tex   standalone, shares ../references.bib
tables/*.tex              generated by make_supplement.py
figures/*.pdf             generated by make_supplement.py
```
Supplement structure: **Polars integration** →
**Filtering**: the equivalence table, the per-entry semantics, Experiment A,
and Experiment B as the annotate → filter → `sink_vcf` workflow.
**Pushdowns**: region, projection and `LIMIT`, and Experiment C.
Generated files are never edited by hand.

## Order of work

1. Catalogue + panel + chr22 inputs + VEP fork runs (these produce the
   `filter_vep` references).
2. `verify.py`: all 18 queries pass record-set parity on chr22. Stop and
   report any that do not.
3. Experiment A, then C, then B.
4. `make_supplement.py` + the LaTeX supplement.
5. Review the chr22 results with the user. Decide on chr1.

## Out of scope

WGS and chr1 (until decided), genotype/FORMAT filters, a general
`filter_vep` parser, `--ontology` expansion, and RefSeq-cache runs.
