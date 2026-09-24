# Gene panel

`acmg_sf_v3.2.txt`: the 78 gene symbols in the "Gene via GTR" column of
https://www.ncbi.nlm.nih.gov/clinvar/docs/acmg/ (ACMG SF v3.2), fetched
2026-09-24. One symbol per line and no comments, because `filter_vep --filter
"SYMBOL in <file>"` reads every line as a value.

`acmg_sf_v3.2_chr22.bed`: the loci of those genes on chr22, from
`make_panel_bed.py` over the release-116 Ensembl transcript cache: the
min(start)-max(end) of each gene's transcripts, 0-based half-open, `chr`-prefixed.
Only NF2 is on chr22.
