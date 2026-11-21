#!/bin/zsh
# Order of the stages: HG002 in both modes, ClinVar per chromosome, ClinVar as whole VCFs (needs the most disk).
S=/Users/mwiewior/research/benchmarks/engines-two-modes-20261120/scripts
RUN_PANELS="merged:HG002 ensembl:HG002" zsh $S/run.zsh || exit $?
RUN_PANELS="merged:ClinVar ensembl:ClinVar" RUN_MODES=split zsh $S/run.zsh || exit $?
RUN_PANELS="merged:ClinVar ensembl:ClinVar" RUN_MODES=whole zsh $S/run.zsh
