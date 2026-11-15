# ClinVar performance snapshots

Measurements taken while optimising the ClinVar workload before 0.11.0, oldest
first. Each page links its raw measurements in this directory.

1. [`clinvar-performance-2026-11-06.md`](clinvar-performance-2026-11-06.md) —
   the engine #326 snapshot, before the one-worker reference default changed
2. [`clinvar-serial-performance-2026-11-06.md`](clinvar-serial-performance-2026-11-06.md) —
   one-worker reference reuse
3. [`clinvar-followup-performance-2026-11-06.md`](clinvar-followup-performance-2026-11-06.md) —
   HGVS reuse and worker balancing
4. [`clinvar-final-chromosome-performance-2026-11-07.md`](clinvar-final-chromosome-performance-2026-11-07.md) —
   fresh chr2 and chr17 comparison against the previous engine and vep-rs
