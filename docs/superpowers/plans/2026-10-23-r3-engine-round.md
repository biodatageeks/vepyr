# R3 ClinVar engine round

This branch carries the draft functions PR stack #289–#294 through
`r3/integration` at `bc3cfc3a13f97956fe1058132893319bb2754b98`. The engine
changes cover vepyr issues #211, #212, #213, #220, #228, #232, #234, #237,
#245 and #247. The formats pin remains
`6af1fa1480b2fc527e3b218585eae3a087f2aca9`; the formats release is postponed.

This is an engine-pin round only. The integration branch was built and tested
before this pin; the vepyr branch runs its Rust and Python suites, then builds
the head and `master` `b7538e9` with identical native release settings.

Acceptance gates are strict HG002 md5 across all 22 contigs, ClinVar full-contig
comparison against the R2 25-contig baseline, the targeted panel against R0,
and interleaved 1/8-worker performance against vepyr master. Any record that
was identical in the R2 full-contig baseline but differs here is a regression
and blocks the PR/review stage. Reviews and CI run on the draft vepyr PR;
`/parity-wgs` remains pending for the coordinator and runs last after upstream
merges and re-pinning.
