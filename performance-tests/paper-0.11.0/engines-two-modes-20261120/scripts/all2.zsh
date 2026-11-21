#!/bin/zsh
# Panel order requested by the user on 2026-11-20: finish merged:HG002 (including the repeat of the one-shot vepyr
# and fastVEP blocks, see remeasure.txt), then merged:ClinVar, then ensembl:HG002 and ensembl:ClinVar. Each panel
# in both modes, per chromosome first, then one shot. The driver is resumable: finished runs are skipped.
S=/Users/mwiewior/research/benchmarks/engines-two-modes-20261120/scripts
RUN_PANELS="merged:HG002" zsh $S/run.zsh || exit $?
RUN_PANELS="merged:ClinVar" zsh $S/run.zsh || exit $?
RUN_PANELS="ensembl:HG002" zsh $S/run.zsh || exit $?
RUN_PANELS="ensembl:ClinVar" zsh $S/run.zsh
