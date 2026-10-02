# Mismatched REF / transcript CDS oracle

`input.vcf` reproduces [porting-tests #221](https://github.com/biodatageeks/vepyr-porting-tests/issues/221).
`golden.vcf` is the output from the pinned Docker Ensembl VEP 116 image in
`provenance.json`, using `--everything` without `--check_ref` or `--lookup_ref`.
Both deletion/insertion records have INFO-bearing and INFO-free twins.
`control.vcf` changes only REF to the genome-matching bases, with its own Docker
oracle. Inputs were normalized using `bcftools norm -m -both --no-version`.

The cache contains all 34 transcripts selected by this locus, their exons,
translation/protein features and prediction rows, nearby known variations, and
empty regulatory/motif tables with the original schema. The BGZF FASTA retains
chr21 coordinates and the sequence around every selected transcript; bases
outside the retained interval are `N`. This slice reproduces the full-cache
Docker body, including CSQ order and DOMAINS.

Regenerate the cache, FASTA and indexed input copies with:

```sh
DATA_VEPYR_DIR=/path/to/data_vepyr uv run python tests/data/ref_genome_mismatch/prepare.py
```

That directory must contain the release-116 Ensembl parquet cache under
`cache/116_GRCh38_ensembl` and the primary-assembly FASTA under `input`.
The script requires `samtools`, `bgzip`, and `tabix`. It does not overwrite the
Docker oracles. `provenance.json` records the VEP invocation and source revisions.
