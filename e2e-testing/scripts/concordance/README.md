# VEP-vs-vepyr Concordance Harness

One entrypoint runs the shared preparation step and then either the MD5 check,
the DataFrame check, or both.

```bash
./run_concordance.sh md5
./run_concordance.sh dataframe
./run_concordance.sh both
```

The default profile is the publication seed profile: merged cache, Fjall
backend, `--everything`, and `--hgvs`.

Override defaults with `PROFILE=vep|merged|refseq`, `BACKEND=fjall|parquet`,
`INPUT_VCF`, `VEP_VCF`, `CACHE_DIR`, `FASTA`, or `OUT_DIR`.

Existing vepyr output is reused. Set `FORCE=1` to re-run vepyr annotation.

## Layout

| Directory | Purpose |
|-----------|---------|
| `prep/` | shared input normalization and vepyr execution |
| `md5/` | VCF container patch and canonical MD5 comparator |
| `data_frame/` | semantic CSQ DataFrame comparator |

## Outputs

By default, outputs are written to `e2e-testing/results/concordance/`:

- `input.normalized.vcf.gz`
- `vepyr.{profile}.everything.hgvs.{backend}.vcf`
- `vepyr.{profile}.everything.hgvs.{backend}.container-patched.vcf`
- `canonical-md5.{profile}.everything.hgvs.{backend}.txt`
- `dataframe.{profile}.everything.hgvs.{backend}.txt`

## Call Graph

```mermaid
flowchart TD
    run[run_concordance.sh<br/>md5 | dataframe | both]

    input[INPUT_VCF]
    vep[VEP_VCF oracle]
    cache[CACHE_DIR]
    fasta[FASTA]

    norm[prep/normalize_vcf.sh<br/>bcftools norm + bgzip + tabix]
    normvcf[input.normalized.vcf.gz]
    annotate[prep/run_vepyr_vcf.py<br/>vepyr annotate]
    raw[vepyr output VCF]

    patch[md5/patch_vcf_container_for_md5.py]
    patched[container-patched vepyr VCF]
    md5[md5/canonical_md5_vcf.py]
    md5out[canonical-md5 report]

    df[data_frame/compare_annotation_frames.py]
    dfout[dataframe report]

    run --> input
    run --> vep
    run --> cache
    run --> fasta

    input --> norm --> normvcf
    normvcf --> annotate
    cache --> annotate
    fasta --> annotate
    annotate --> raw

    normvcf --> patch
    raw --> patch --> patched
    vep --> md5
    patched --> md5 --> md5out

    vep --> df
    raw --> df --> dfout
```
