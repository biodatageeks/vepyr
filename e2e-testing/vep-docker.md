# Ensembl VEP reference runs (Docker)

The exact commands used to generate the golden-truth VEP outputs the e2e parity suite
compares against. Each `--everything --hgvs` run over the 4M-variant HG002 callset takes
**4-7 hours**, so generate each reference once and keep it.

For the surrounding workflow — downloading and normalizing the input, converting caches
to Parquet, and running the comparison — see
[`docs/testing-vep.md`](../docs/testing-vep.md).

## Setup

Commands below are parameterized against the standard data layout. Set these once:

```bash
export DATA_VEPYR_DIR=~/workspace/data_vepyr

# Release 116 (current)
export RELEASE=116
export VEP_IMAGE=ensemblorg/ensembl-vep:release_116.0
export OUT_DIR="$DATA_VEPYR_DIR/output/116"

# Release 115 (what the current reports were generated from)
# export RELEASE=115
# export VEP_IMAGE=ensemblorg/ensembl-vep:release_115.2
# export OUT_DIR="$DATA_VEPYR_DIR/output/115.2"

mkdir -p "$OUT_DIR"
```

The output directory for 115 is `115.2`, matching the VEP point release the references
were produced with; 116 is plain `116`. The comparison runner keeps that mapping
explicitly, so `--release 115` resolves to `output/115.2/` for you.

Expected layout:

```
$DATA_VEPYR_DIR/
  input/HG002_normalized.vcf.gz                    + .tbi
  input/Homo_sapiens.GRCh38.dna.primary_assembly.fa + .fai
  homo_sapiens{,_refseq,_merged}/{RELEASE}_GRCh38/  # raw VEP caches
  output/{116,115.2}/                               # what these commands write
```

## Runner

Every command below is the same invocation with a different cache, transcript-set flag,
pick mode, and output name. This wrapper captures the shared part:

```bash
vep_run() {
  local species_dir=$1   # homo_sapiens | homo_sapiens_refseq | homo_sapiens_merged
  local out_name=$2      # output filename, without directory
  shift 2                # everything else is passed through to vep

  time docker run --rm \
    -v "$DATA_VEPYR_DIR/$species_dir/${RELEASE}_GRCh38:/opt/vep/.vep/$species_dir/${RELEASE}_GRCh38:ro" \
    -v "$DATA_VEPYR_DIR/input:/input:ro" \
    -v "$OUT_DIR:/output" \
    "$VEP_IMAGE" \
    vep \
    --dir /opt/vep/.vep \
    --cache \
    --offline \
    --assembly GRCh38 \
    --input_file /input/HG002_normalized.vcf.gz \
    --output_file "/output/$out_name" \
    --vcf \
    --force_overwrite \
    --no_stats \
    --everything --hgvs \
    --fasta /input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
    "$@"
}

PICK_ORDER=biotype,rank,mane_select,tsl,canonical,appris,ccds,length
```

Two things that will silently waste a multi-hour run if you get them wrong:

- **Verify the cache directory is populated before starting.** Docker creates a missing
  `-v` source as an empty directory instead of failing, and VEP then skips every variant
  with `Chromosome chr1 not found in annotation sources or synonyms`. Check with
  `ls "$DATA_VEPYR_DIR/homo_sapiens_merged/${RELEASE}_GRCh38" | wc -l` — expect ~1,900
  entries including `chr_synonyms.txt`.
- **`--input_file` takes the block-gzipped VCF.** VEP opens it multistream via
  `gzip -dc`, costing under a second, and it saves ~2.7 GB over the plain copy.

## Baseline references

### Ensembl cache

```bash
vep_run homo_sapiens HG002_annotated_wgs_everything_hgvs_vep.vcf
```

### RefSeq cache

```bash
vep_run homo_sapiens_refseq HG002_annotated_wgs_everything_hgvs_refseq.vcf --refseq
```

### Merged cache

```bash
vep_run homo_sapiens_merged HG002_annotated_wgs_everything_hgvs_merged.vcf --merged
```

## Merged cache + pick-mode references

These generate the VEP references used by the e2e pick-mode profiles. All use the merged
cache, the same normalized input, `--everything --hgvs`, and the pick ranking order
`biotype,rank,mane_select,tsl,canonical,appris,ccds,length`.

The `--pick*` modes *filter* the CSQ list down to the selected consequence; the
`--flag_pick*` modes keep every consequence and mark the winner with `PICK=1`.

```bash
# merged_pick_filter -- e2e profile: --pick
vep_run homo_sapiens_merged HG002_annotated_wgs_everything_hgvs_merged_pick_filter.vcf \
  --merged --pick --pick_order "$PICK_ORDER"

# merged_pick_allele -- e2e profile: --pick_allele
vep_run homo_sapiens_merged HG002_annotated_wgs_everything_hgvs_merged_pick_allele.vcf \
  --merged --pick_allele --pick_order "$PICK_ORDER"

# merged_per_gene -- e2e profile: --per_gene
vep_run homo_sapiens_merged HG002_annotated_wgs_everything_hgvs_merged_per_gene.vcf \
  --merged --per_gene --pick_order "$PICK_ORDER"

# merged_pick_allele_gene -- e2e profile: --pick_allele_gene
vep_run homo_sapiens_merged HG002_annotated_wgs_everything_hgvs_merged_pick_allele_gene.vcf \
  --merged --pick_allele_gene --pick_order "$PICK_ORDER"

# merged_flag_pick -- e2e profile: --flag_pick
vep_run homo_sapiens_merged HG002_annotated_wgs_everything_hgvs_merged_flag_pick.vcf \
  --merged --flag_pick --pick_order "$PICK_ORDER"

# merged_flag_pick_allele -- e2e profile: --flag_pick_allele
vep_run homo_sapiens_merged HG002_annotated_wgs_everything_hgvs_merged_flag_pick_allele.vcf \
  --merged --flag_pick_allele --pick_order "$PICK_ORDER"

# merged_flag_pick_allele_gene -- e2e profile: --flag_pick_allele_gene
vep_run homo_sapiens_merged HG002_annotated_wgs_everything_hgvs_merged_flag_pick_allele_gene.vcf \
  --merged --flag_pick_allele_gene --pick_order "$PICK_ORDER"
```

> The `merged_flag_pick_allele_gene` e2e profile currently reads
> `HG002_annotated_wgs_everything_hgvs_merged_pick.vcf`, not the
> `..._flag_pick_allele_gene.vcf` name above. That local artifact is misnamed: chr16
> validation showed it contains unfiltered CSQs with `PICK`, which is the
> flag_pick_allele_gene output. Regenerating it under the correct name is pending.

## After each run

VEP writes plain VCF — 15-29 GB per run at `--everything --hgvs`. Compress and index as
soon as a run finishes, both to reclaim the space and because the comparison harness
slices a block-gzipped, indexed reference by tabix seek instead of scanning the whole
file once per contig:

```bash
cd "$OUT_DIR"
for f in *.vcf; do
  bgzip --threads 6 "$f" && tabix -p vcf "${f}.gz"
done
```

`bgzip` deletes the plain input only on success. Typical results on this dataset:
27 GB → 1.6 GB (merged), 16 GB → 987 MB (ensembl), 10 GB → 718 MB (refseq).

## Recorded observations

Both RefSeq and merged caches are BAM-edited, so VEP logs this at startup and annotates
against the transcript reference rather than the REF allele supplied. Pass
`--use_given_ref` to override:

```
INFO: BAM-edited cache detected, enabling --use_transcript_ref; use --use_given_ref to override this
```

### Timings (release 115.2, 16-core workstation)

| Run | Wall clock |
|---|---|
| RefSeq cache, `--everything --hgvs` | 4:02:40 |
| Merged cache, pick-mode reference | 7:17:55 |

### Transcript-assembly mismatch warnings

The merged cache emits these for 43 variants. They are expected and captured in the
`*_warnings.txt` file written next to each output:

```
WARNING: Transcript-assembly mismatch in chr1_16053748_A/G
WARNING: Transcript-assembly mismatch in chr1_22893000_T/C
WARNING: Transcript-assembly mismatch in chr1_25317062_T/C
WARNING: Transcript-assembly mismatch in chr1_155324483_A/G
WARNING: Transcript-assembly mismatch in chr2_9904885_C/T
WARNING: Transcript-assembly mismatch in chr2_73385904_T/TGGA
WARNING: Transcript-assembly mismatch in chr2_115768384_G/A
WARNING: Transcript-assembly mismatch in chr2_219248520_T/G
WARNING: Transcript-assembly mismatch in chr3_4725578_T/C
WARNING: Transcript-assembly mismatch in chr3_4775373_T/C
WARNING: Transcript-assembly mismatch in chr3_4814496_T/C
WARNING: Transcript-assembly mismatch in chr3_38698083_T/C
WARNING: Transcript-assembly mismatch in chr3_38722372_G/C
WARNING: Transcript-assembly mismatch in chr3_38726809_T/C
WARNING: Transcript-assembly mismatch in chr3_114139503_C/T
WARNING: Transcript-assembly mismatch in chr3_114171968_C/T
WARNING: Transcript-assembly mismatch in chr3_184319745_A/G
WARNING: Transcript-assembly mismatch in chr3_184321878_A/G
WARNING: Transcript-assembly mismatch in chr5_120686122_C/A
WARNING: Transcript-assembly mismatch in chr5_120686124_G/A
WARNING: Transcript-assembly mismatch in chr6_18138983_G/A
WARNING: Transcript-assembly mismatch in chr8_17628773_T/C
WARNING: Transcript-assembly mismatch in chr8_17642714_T/C
WARNING: Transcript-assembly mismatch in chr8_39223113_C/G
WARNING: Transcript-assembly mismatch in chr9_33798019_A/G
WARNING: Transcript-assembly mismatch in chr9_127868360_A/G
WARNING: Transcript-assembly mismatch in chr9_128473042_A/G
WARNING: Transcript-assembly mismatch in chr10_48174883_G/T
WARNING: Transcript-assembly mismatch in chr10_48180858_C/T
WARNING: Transcript-assembly mismatch in chr10_102399439_A/G
WARNING: Transcript-assembly mismatch in chr10_102400677_A/G
WARNING: Transcript-assembly mismatch in chr11_64243969_T/C
WARNING: Transcript-assembly mismatch in chr11_89400281_C/T
WARNING: Transcript-assembly mismatch in chr11_89490933_G/T
WARNING: Transcript-assembly mismatch in chr12_56236660_G/C
WARNING: Transcript-assembly mismatch in chr13_30462544_A/G
WARNING: Transcript-assembly mismatch in chr14_59596754_A/G
WARNING: Transcript-assembly mismatch in chr16_29697029_A/G
WARNING: Transcript-assembly mismatch in chr17_7101608_C/T
WARNING: Transcript-assembly mismatch in chr19_5844526_A/G
WARNING: Transcript-assembly mismatch in chr19_53805300_C/T
WARNING: Transcript-assembly mismatch in chr20_35496586_C/G
WARNING: Transcript-assembly mismatch in chr20_62306724_T/C
```

## Cache downloads

```bash
cd "$DATA_VEPYR_DIR"
for t in homo_sapiens homo_sapiens_refseq homo_sapiens_merged; do
  wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
    "https://ftp.ensembl.org/pub/release-${RELEASE}/variation/indexed_vep_cache/${t}_vep_${RELEASE}_GRCh38.tar.gz"
  tar xzf "${t}_vep_${RELEASE}_GRCh38.tar.gz"
done
```

Extract from `$DATA_VEPYR_DIR`, never from inside a species directory — each archive
already carries its own `homo_sapiens*/` prefix, so extracting one level down nests the
tree and leaves the path VEP reads empty. See the extraction warning in
[`docs/testing-vep.md`](../docs/testing-vep.md) for the symptom and the fix.
