# Canonical MD5 Comparator

This directory contains only the MD5-specific steps:

- `patch_vcf_container_for_md5.py` restores non-INFO VCF container fields from
  the normalized input while preserving vepyr `INFO/CSQ`.
- `canonical_md5_vcf.py` ignores headers, sorts INFO keys, sorts CSQ entries,
  sorts records, and compares canonical VCF body MD5 values.

Run through the parent harness:

```bash
../run_concordance.sh md5
```

Or run the MD5-specific steps directly:

```bash
python patch_vcf_container_for_md5.py input.normalized.vcf.gz vepyr.vcf vepyr.container-patched.vcf
python canonical_md5_vcf.py vep.vcf vepyr.container-patched.vcf
```

