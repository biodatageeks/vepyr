# MiRNA transcript and genomic reference selection

This genuine release-116 merged GRCh38 cache slice covers the original
[ported miRNA test](https://github.com/biodatageeks/vepyr/issues/150),
`21:25573985 C>T`. The supplied REF is retained verbatim. `control.vcf` is a
separate `A>T` positive control.

Both inputs were normalized with bcftools 1.23 `norm -m -both --no-version`.
Both golden outputs come from actual Docker VEP 116.2 runs, using `--everything
--merged`, the full native merged cache and the full GRCh38 FASTA. Commands,
input SHA-256 and exact body MD5 are in `provenance.json`.

`prepare.py` filters the downloaded merged cache without relabelling its source
or changing biological fields. The root reference policy records the native
`info.txt` BAM declaration; older converted shards lack this optional metadata.
The FASTA retains original chromosome coordinates, with real sequence at
25567000–25613000 and N padding elsewhere. The test window retains every
transcript within 5 kb, its exons/translations, co-located variations and
overlapping regulatory features.

The miRNA ENST00000385060 requires `n.6A>T` and USED_REF=A. The negative intronic
ENST00001110240 requires `c.970-626T>A` while retaining USED_REF=C. The downstream
ENST00000779379 retains USED_REF=C and has no HGVSc. These distinctions prevent
conflating genomic HGVS reference with transcript reference or rewriting input.
