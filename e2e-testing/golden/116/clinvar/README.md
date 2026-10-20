# ClinVar 20261011 md5 profile: VEP 116.0 vs vepyr, merged cache

The `clinvar_merged` comparison profile annotates every GRCh38 record of the ClinVar release of 2026-10-11 with vepyr and the merged cache. It compares the result per contig by body md5 against Ensembl VEP 116.0, the same check the HG002 golden suite runs.

The VCFs are too large for Git and stay in the data directory (`$DATA_VEPYR_DIR`, default `~/workspace/data_vepyr`). The golden VCF and index sizes and SHA-256 values are pinned in `clinvar_profiles` of [`../../wgs-manifest.json`](../../wgs-manifest.json), and the runner checks those bytes before comparing. The input's size and SHA-256 are provenance fields: the runner allows a regenerated BGZF header and instead checks record count, record-body MD5, and that indexed queries return the same records.

| File | Location |
|------|----------|
| ClinVar release and local index | `input/clinvar/clinvar_20261011.vcf.gz{,.tbi}` |
| Preprocessed input | `input/clinvar/clinvar_20261011.norm.vcf.gz{,.tbi}` |
| VEP reference (all contigs) | `output/116/clinvar_20261011_annotated_wgs_everything_hgvs_merged.vcf.gz{,.tbi}` |

The per-contig mismatch fixtures and the triage that attributed every differing record to a root cause are on branch `e2e/clinvar-parity`, which the parity issues link to. They are not part of this profile. Per-issue progress is measured with `e2e-testing/scripts/clinvar_progress.py`; see biodatageeks/vepyr#240.

| Symbolic (`<DEL>`, `<DUP>`, ...) or breakend alleles | 0 |
| An allele of 50 bp or more (longest 9,983 bp) | 6,798 |

## 2. Preprocessing

```bash
bcftools view -e 'ALT="."' \
    -t 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,X,Y,MT \
    clinvar_20261011.vcf.gz -Ou \
  | bcftools norm -m -both -Oz -o clinvar_20261011.norm.vcf.gz
tabix -p vcf clinvar_20261011.norm.vcf.gz
```

Run with bcftools 1.23.1 (htslib 1.24). The three steps:

1. **`-e 'ALT="."'` removes the 1,341 records with no alternate allele.** In
   `--vcf` mode VEP drops such lines from its output unless
   `--allow_non_variant` is given, and the golden runs do not give it. There is
   nothing to annotate in them, and they would only leave the two outputs with
   different record sets.
2. **`-t 1,...,22,X,Y,MT` removes the 51 records on RefSeq scaffold accessions.**
   The comparison covers the 25 chromosomes, as the HG002 suite covers
   chromosomes 1-22.
3. **`bcftools norm -m -both` splits multiallelic records**, the step applied to
   the HG002 input. ClinVar has one ALT per record, so nothing is split here
   (`total/split = 4593372/0`). The step is kept so that both inputs go through
   the same procedure. As for HG002, there is no left-alignment (`-f`) and no
   REF check.

Nothing else is removed. The file has no structural variants in the VEP sense
(symbolic or breakend alleles). The 6,795 remaining records with an allele of
50 bp or more carry their full sequence, and both VEP and vepyr annotate them as
ordinary sequence variants, so they stay in the input and in the comparison.

The result has **4,593,372 records**. bcftools writes the run date into the
header, so the file checksum changes between runs; the md5 of the records
(`bcftools view -H clinvar_20261011.norm.vcf.gz | md5sum`) is
`547285cb0e2b3f97c7222156a748c371`.

## 3. VEP reference

```bash
e2e-testing/scripts/generate_vep_clinvar_reference.sh
```

The script first checks the free disk space, then fetches whatever is missing
under `$DATA_VEPYR_DIR`: the Ensembl 116 primary-assembly FASTA (validated
against its SHA-256 in `porting-tests/PINS.toml`, then indexed with
`samtools faidx`), the raw VEP merged cache (checked against the BSD `sum`
Ensembl publishes in `CHECKSUMS`, `26554 29618744`, then extracted into a
temporary directory and published only after extraction completes), the
Docker image and the ClinVar release. A reused `.fai` is regenerated after the
FASTA passes its checksum. This script supports release 2026-10-11 only;
it replaces an existing raw VCF whose checksum is wrong and does not reuse
partial download files. `VEP_REFERENCE_JOBS` must be a positive integer and
defaults to one. It preprocesses the release as above and runs
the manifest-pinned image
`ensemblorg/ensembl-vep:release_116.0@sha256:f354dd8d09073e4d943acbbd02f5eb234a9d9e9d444371c1c349910f2123de11`
with the flags of the HG002 golden runs
([`vep-docker.md`](../../../vep-docker.md)):

```
vep --dir /opt/vep/.vep --cache --offline --assembly GRCh38 --merged \
    --vcf --force_overwrite --no_stats --everything --hgvs \
    --fasta Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

VEP runs without `--fork`. Each contig is a separate VEP process on its own
slice of the preprocessed input, and `VEP_REFERENCE_JOBS` contigs run at the
same time. A contig is reused only when its VCF, index, and provenance stamp
match the FASTA, raw-cache identity, image, normalized-input body, and VEP
arguments for the current run. A stale or unstamped result stops the run;
remove that contig's three files or set `FORCE_REFERENCE=1` to regenerate it.
The normalized input tabix index is rebuilt after its record-body check before
the generator slices any contigs.
The script removes an old VCF/index pair before publishing the replacement and
writes the stamp only after the VEP record count matches the input. Contig outputs
are joined in input order by copying their records as text: `bcftools concat`
would reprint ClinVar's floating-point INFO values and change the bytes the md5
check compares. Abandoned per-contig work directories older than 24 hours are
removed at the start of a run; recent work directories are preserved for a
possibly active VEP process.

## 4. Running the comparison

The profile uses the preprocessed input and defaults to `--no-normalize`:

Before an annotated comparison, the Parquet cache root must contain a `.revision`
JSON file with the manifest's exact `repo_id` and full `revision`. Download the
cache at that revision first, then record the source identity, for example:

```bash
hf download biodatageeks/vepyr_116_GRCh38_merged --repo-type dataset \
  --revision 5b83dd8d249106c6cc3f1c04c522b4bec716cc97 \
  --local-dir "$DATA_VEPYR_DIR/cache/116_GRCh38_merged"
python3 -c 'import json, os; from pathlib import Path; p=Path(os.environ["DATA_VEPYR_DIR"])/"cache/116_GRCh38_merged/.revision"; p.write_text(json.dumps({"repo_id":"biodatageeks/vepyr_116_GRCh38_merged","revision":"5b83dd8d249106c6cc3f1c04c522b4bec716cc97"}, sort_keys=True)+"\n", encoding="utf-8")'
```

The profile rejects a missing or mismatched identity marker. Report-only modes
that do not annotate do not require the cache.

```bash
python e2e-testing/scripts/run_comparison.py --release 116 --profile clinvar_merged \
  --chroms all --comparison-mode md5 --md5-mode both --bgzf --workers 8
```

- `--chroms` takes space-separated contig names (`22 Y` or `chr22 chrY`) or `all`. For a quick check, use `--chroms 22 Y`.
- A full run takes about an hour on a laptop. ClinVar is about 20× slower per record than HG002 (biodatageeks/vepyr#258).
- It needs about 30 GB of temporary slices.

Before annotation, the runner scans the 4,593,372 input records and checks the
indexed queries. It also hashes the 5.9 GB golden before comparison. At vepyr
master `99dd788` (2026-10-20), chrY matches byte for byte, and the other 24
contigs still differ on 4,983 of the 4,593,372 records.
