# chrMT codon table (vepyr#200)

Twelve ClinVar 20261011 records in MT-ND1 (MT:3307–4029), one per distinct
amino-acid change on which the standard and vertebrate mitochondrial codes
disagree: ATA is Met, AGA/AGG are stops, TGA is Trp. They are the `codon_table`
fixture of the ClinVar chrMT parity run
(`e2e-testing/golden/116/clinvar/mismatches/chrMT/codon_table/` on branch
`analysis/clinvar-chrmt-mismatches`).

- `vep116.golden.vcf`: Ensembl VEP 116.0 `--everything --merged --hgvs`
  output for `input.vcf`. VEP 116.2 (`release_116.2`, ensembl-variation
  `db7394a`) writes byte-identical bodies for these records.
- `raw/116_GRCh38_merged/`: the raw Ensembl VEP 116 merged cache for MT
  (`info.txt` plus `MT/`), under the canonical directory name `build_cache()`
  derives the release from. Every MT transcript carries `codon_table = "2"` on
  `_variation_effect_feature_cache`; the test rebuilds it with `build_cache()`
  so it covers the bio-formats reader.
- `published/`: the chrMT shards of the published converted cache
  (`biodatageeks/vepyr_116_GRCh38_merged@5b83dd8`), where `codon_table` is null
  on every row. Motif and regulatory have no chrMT shard, so their manifests are
  empty. It covers the engine fallback that resolves a null on a mitochondrial
  contig to table 2.
- `input.vcf.gz` (+ `.tbi`): the same records, indexed, for the `workers=2` run.
- `reference.fa.gz`: MT only (16,569 bp), bgzip with `.fai` and `.gzi`.

`prepare.py` regenerates everything from `$DATA_VEPYR_DIR`.
