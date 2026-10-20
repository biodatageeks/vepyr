# ClinVar parity check: VEP 116.0 vs vepyr, merged cache

All GRCh38 records of the ClinVar release of 2026-10-11 are annotated by
Ensembl VEP 116.0 and by vepyr with the merged cache. The two outputs are
compared per contig by body md5, the same check the HG002 golden suite runs
(`run_comparison.py --comparison-mode md5 --md5-mode both`).

This directory keeps the description of the run, the checksums of every input
(`manifest.json`) and the comparison results. The VCFs are too large for Git and
stay in the data directory (`$DATA_VEPYR_DIR`, default `~/workspace/data_vepyr`):

| File | Location |
|------|----------|
| ClinVar release and local index | `input/clinvar/clinvar_20261011.vcf.gz{,.tbi}` |
| Preprocessed input | `input/clinvar/clinvar_20261011.norm.vcf.gz{,.tbi}` |
| VEP reference (all contigs) | `output/116/clinvar_20261011_annotated_wgs_everything_hgvs_merged.vcf.gz{,.tbi}` |
| VEP reference per contig, logs, timings | `output/116/clinvar/{contigs,logs,timings.tsv}` |
| vepyr outputs and reports | `clinvar_e2e/{results,reports}/` |

## 1. Input

Source: <https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/>, files
`clinvar_20261011.vcf.gz` (md5 `0f3a1a5e84252386033d5fcab1bf1f6a`, as published
in `clinvar_20261011.vcf.gz.md5`) and its tabix index. The reference generator
downloads to a temporary file, checks that published MD5, and builds the local
tabix index so an interrupted partial index cannot be reused.

What the release contains:

| | Records |
|---|---:|
| All records | 4,594,764 |
| Chromosomes 1-22, X, Y, MT | 4,594,713 |
| RefSeq scaffolds (`NT_*`, `NW_*`, 7 contigs) | 51 |
| No alternate allele (`ALT="."`) | 1,341 |
| Multiallelic records | 0 |
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
under `$DATA_VEPYR_DIR`: the Ensembl 116 primary-assembly FASTA (indexed with
`samtools faidx`), the raw VEP merged cache (checked against the BSD `sum`
Ensembl publishes in `CHECKSUMS`, `26554 29618744`, then extracted into a
temporary directory and published only after extraction completes), the
Docker image and the ClinVar release. This script supports release 2026-10-11 only;
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
same time. A contig is skipped only when its VCF and index are both present.
The script removes an old pair before publishing the new VCF and index, so an
interruption between moves leaves a missing file and the contig is rerun. It
warns when a contig output holds fewer records than its input. Contig outputs
are joined in input order by copying their records as text: `bcftools concat`
would reprint ClinVar's floating-point INFO values and change the bytes the md5
check compares. Abandoned per-contig work directories older than 24 hours are
removed at the start of a run; recent work directories are preserved for a
possibly active VEP process.

Memory and runtime varied substantially across contigs in the reference run:

- **Memory.** Most VEP processes used 2.5-4.5 GiB, while chr3 and chr4 reached
  6.2 and 7.4 GiB. A chr17 process reached 22.4 GiB around BRCA1. The
  generator therefore defaults to one contig at a time; allow at least 30 GiB
  of Docker memory for that single process. Increase `VEP_REFERENCE_JOBS` only
  when the Docker memory limit can cover the observed per-contig peaks together.
- **Time.** ClinVar variants are almost all in genes, which makes them much
  slower for VEP than the HG002 calls: seven jobs together annotated about 75
  variants per second, which would take 15-17 hours for the whole release. The
  full run was therefore moved to a larger Linux machine. chrMT and chrY were
  annotated on the MacBook.

## 4. vepyr and the md5 comparison

The `clinvar_merged` profile uses the preprocessed input at
`$DATA_VEPYR_DIR/input/clinvar/clinvar_20261011.norm.vcf.gz` and the merged
cache revision used by this run (`5b83dd8`). The reference keeps its canonical
filename, `$DATA_VEPYR_DIR/output/116/clinvar_20261011_annotated_wgs_everything_hgvs_merged.vcf.gz`;
the runner resolves that path from `clinvar_profiles` in
`e2e-testing/golden/wgs-manifest.json` and checks its size and SHA-256 before
comparison. It verifies the record-body MD5
`547285cb0e2b3f97c7222156a748c371` and record count, then queries every
indexed contig and checks that the combined records have the same MD5 and
count as the input. This verifies the existing tabix index without rewriting
the input or sidecar. The manifest records the compressed SHA-256 and size
for provenance, while the body MD5 is the stable input identity because
bcftools embeds the preprocessing date in the header, changing the compressed
bytes on regeneration. The local ClinVar metadata is separate from the
`profiles` map consumed by `/parity-wgs`, so this profile does not expand that
workflow's HG002 profile matrix.

The input is already preprocessed, so the profile defaults to `--no-normalize`:

```bash
python e2e-testing/scripts/run_comparison.py --release 116 --profile clinvar_merged --chroms all --comparison-mode md5 --md5-mode both --bgzf --workers 8
```

## 5. Results

vepyr 0.10.0 from PyPI, whose engine is the one on `master` (no engine change
since the 0.10.0 tag), against the cache revision pinned by that release
(`biodatageeks/vepyr_116_GRCh38_merged@5b83dd8`).

**chrY matches VEP byte for byte. The other 24 contigs differ: 9,249 of the
4,593,372 records (0.20%) are not the same in the two outputs**, 6,922 of them
on chromosomes 1-22 and X and 2,327 on chrMT. Each contig with differences has
a directory under `mismatches/` with all its differing records and a fixture
per class (see [Mismatch classes](#mismatch-classes)).

| Contig | Records | Differing | md5 strict | md5 canonical | VEP (1 process) | vepyr (32 workers) | Analysis |
|---|---:|---:|---|---|---:|---:|---|
| chr1 | 414,715 | 383 | **differs** | **differs** | 15,028 s | 104.5 s | [`chr1/`](mismatches/chr1/README.md) |
| chr2 | 401,230 | 1,057 | **differs** | **differs** | 20,027 s | 592.6 s | [`chr2/`](mismatches/chr2/README.md) |
| chr3 | 251,389 | 198 | **differs** | **differs** | 10,486 s | 74.3 s | [`chr3/`](mismatches/chr3/README.md) |
| chr4 | 163,605 | 118 | **differs** | **differs** | 5,886 s | 88.8 s | [`chr4/`](mismatches/chr4/README.md) |
| chr5 | 222,343 | 177 | **differs** | **differs** | 8,597 s | 107.9 s | [`chr5/`](mismatches/chr5/README.md) |
| chr6 | 206,599 | 134 | **differs** | **differs** | 7,287 s | 92.4 s | [`chr6/`](mismatches/chr6/README.md) |
| chr7 | 223,219 | 233 | **differs** | **differs** | 8,068 s | 66.5 s | [`chr7/`](mismatches/chr7/README.md) |
| chr8 | 155,536 | 112 | **differs** | **differs** | 5,744 s | 74.7 s | [`chr8/`](mismatches/chr8/README.md) |
| chr9 | 203,226 | 155 | **differs** | **differs** | 8,928 s | 73.4 s | [`chr9/`](mismatches/chr9/README.md) |
| chr10 | 171,717 | 517 | **differs** | **differs** | 6,991 s | 66.1 s | [`chr10/`](mismatches/chr10/README.md) |
| chr11 | 267,817 | 293 | **differs** | **differs** | 10,753 s | 74.0 s | [`chr11/`](mismatches/chr11/README.md) |
| chr12 | 211,578 | 161 | **differs** | **differs** | 7,541 s | 64.2 s | [`chr12/`](mismatches/chr12/README.md) |
| chr13 | 90,238 | 875 | **differs** | **differs** | 2,882 s | 30.4 s | [`chr13/`](mismatches/chr13/README.md) |
| chr14 | 145,471 | 75 | **differs** | **differs** | 5,409 s | 53.8 s | [`chr14/`](mismatches/chr14/README.md) |
| chr15 | 159,327 | 199 | **differs** | **differs** | 5,271 s | 49.3 s | [`chr15/`](mismatches/chr15/README.md) |
| chr16 | 237,223 | 356 | **differs** | **differs** | 9,341 s | 93.2 s | [`chr16/`](mismatches/chr16/README.md) |
| chr17 | 277,114 | 1,167 | **differs** | **differs** | 13,349 s | 254.4 s | [`chr17/`](mismatches/chr17/README.md) |
| chr18 | 76,630 | 51 | **differs** | **differs** | 2,627 s | 28.6 s | [`chr18/`](mismatches/chr18/README.md) |
| chr19 | 261,537 | 209 | **differs** | **differs** | 8,666 s | 72.2 s | [`chr19/`](mismatches/chr19/README.md) |
| chr20 | 99,377 | 54 | **differs** | **differs** | 3,503 s | 23.9 s | [`chr20/`](mismatches/chr20/README.md) |
| chr21 | 51,667 | 50 | **differs** | **differs** | 1,567 s | 14.9 s | [`chr21/`](mismatches/chr21/README.md) |
| chr22 | 99,409 | 100 | **differs** | **differs** | 3,894 s | 30.8 s | [`chr22/`](mismatches/chr22/README.md) |
| chrX | 198,574 | 248 | **differs** | **differs** | 6,420 s | 52.3 s | [`chrX/`](mismatches/chrX/README.md) |
| chrY | 697 | 0 | match | match | 20 s | 3.0 s |  |
| chrMT | 3,134 | 2,327 | **differs** | **differs** | 38 s | 1.4 s | [`chrMT/`](mismatches/chrMT/README.md) |
| **all** | **4,593,372** | **9,249** | 1 of 25 match | 1 of 25 match | 178,323 s | 2,187.6 s | |

The VEP time of a contig is that of the run that wrote its output (see
[The run](#the-run)). The vepyr time is the annotation time the runner reports
(`annotation.time_s` in `reports/`).

### The run

All 25 contigs were run on one Linux desktop: AMD Ryzen 9 5950X (16 cores, 32
threads), 62.7 GiB RAM and 2 GiB swap, NVMe SSD, Ubuntu 22.04.3, Docker 24.0.7.
Docker on Linux runs without a VM, so the VEP containers share the host
memory. The preprocessing of section 2, repeated there with bcftools 1.13
(htslib 1.19.1), gave the same record md5, `547285cb0e2b3f97c7222156a748c371`.

**VEP.** `generate_vep_clinvar_reference.sh` started at 13:03 with
`VEP_REFERENCE_JOBS=17`: 32 logical CPUs less two, capped by 62.7 GiB at
3.5 GiB per job. The last contig, chr17, finished at 20:18. The reference took
7 h 15 min of wall time and 178,323 s (49.5 h) of VEP process time, 25.8
records per second per process. The 3.5 GiB per process seen on the MacBook did
not hold for ClinVar:

- Most VEP processes stayed at 2.5-4.5 GiB; those of chr3 and chr4 reached 6.2
  and 7.4 GiB.
- The first chr17 process grew to 13.7 GiB and was killed by the kernel OOM
  killer at 14:50. The second one reached 22.4 GiB around BRCA1, where single
  output lines carry 0.1-0.4 MB of CSQ.

To keep the machine out of swap, the number of parallel jobs was lowered in
steps from 17 to 6 by 14:25 (GNU xargs takes `SIGUSR2` as one job fewer), and
six running contigs (chr3, chr4, chr7, chr14, chr16, chr19) were stopped
between 13:24 and 14:24. Every stopped or killed contig was run again from the
start, never resumed, with the same script and the contigs as arguments
(`19 3`, `16 7` and `4 14` between 14:50 and 15:06, `17` at 16:35). chrY and chrMT were run
on Linux as well, for the comparison with the MacBook below. The run without
arguments then found all 25 outputs and joined them. Between 6 and 17 VEP
processes shared the 16 cores, so the times per contig are not those of an
idle machine.

**vepyr.** After VEP had finished, one run of the unchanged runner on the
joined reference:

```bash
python e2e-testing/scripts/run_comparison.py \
  --release 116 --profile merged --chroms all \
  --vcf "$DATA_VEPYR_DIR/input/clinvar/clinvar_20261011.norm.vcf.gz" \
  --vep "$DATA_VEPYR_DIR/output/116/clinvar_20261011_annotated_wgs_everything_hgvs_merged.vcf.gz" \
  --fasta "$DATA_VEPYR_DIR/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa" \
  --cache-dir "$DATA_VEPYR_DIR/cache/116_GRCh38_merged" \
  --output-dir "$DATA_VEPYR_DIR/clinvar_e2e" \
  --comparison-mode md5 --md5-mode both --bgzf --no-normalize --workers 32
```

The annotation of the 25 contigs took 2,187.6 s (36.5 min). The whole run,
with the extraction of the inputs and both md5 checks, took 1 h 8 min 42 s,
with a peak RSS of 17.3 GiB.

Before that, while VEP was still running, each contig had been annotated on its
own as soon as its VEP output was written (`--chroms <contig> --workers 2`,
against the contig output). Both runs give the same vepyr body md5, strict and
canonical, on all 25 contigs, and the joined reference gives the same VEP body
md5 per contig as the contig outputs. The vepyr output did not change with 32
workers instead of 2, and the analyses in `mismatches/`, made from the earlier
runs, hold for this run.

### Where the reference is kept

Like the HG002 whole-genome goldens (`../../wgs-manifest.json`), the VEP
reference is not in Git. `manifest.json` records it under `vep_reference`: its
path under `$DATA_VEPYR_DIR`, the sha256 and size of the VCF and of its index,
and the number of records. The same file is kept, unchanged, next to the HG002
references, under the name `clinvar_20261011_annotated_wgs_everything_hgvs_merged.vcf.gz`
(`published_as`) with its `.tbi`. Only the MacBook references of chrMT and chrY
are in this directory (`merged_chrMT.vcf.gz`, `merged_chrY.vcf.gz`, Git LFS).
The runner's reports of the Linux run are in `reports/`, those of the two
MacBook runs in `reports/macbook/`.

### chrMT and chrY on the MacBook and on Linux

The Linux run annotated chrMT and chrY again, with the same image, cache and
flags as the MacBook. The records are identical: both runs give the same body
md5, strict and canonical (strict: chrMT `148b2439b649d7c8cbedf4c5c22e56fc`,
chrY `a45836ecf992614f5471b866254ae2af`). The headers differ only in the
bcftools lines and the `time` of the `##VEP` line. vepyr also writes the same
records on both machines. `manifest.json` keeps the MacBook values for these
two contigs and adds the Linux ones under `linux_check`; `vepyr` holds the
Linux run and `vepyr_macbook` the MacBook one.

### Mismatch classes

Each directory under `mismatches/` lists every differing record of its contig
in `records.tsv`, with a small input, VEP and vepyr fixture per class; `vepyr
annotate` on each fixture input writes its `vepyr.vcf` byte for byte. The rules
are described in [`mismatches/chr15/`](mismatches/chr15/README.md), in the
order below, and the first rule that matches decides the class. On chromosomes
1-22 and X:

| Class | Rule | Records |
|---|---|---:|
| `info_percent_encoding` | an input INFO value differs (vepyr decodes `%3B` and `%3D`) | 1,966 |
| `allele_with_n` | REF or ALT contains `N` | 667 |
| `used_ref` | `USED_REF` differs | 8 |
| `large_deletion` | REF of 500 bp or more | 281 |
| `splice` | only splice terms differ | 269 |
| `cds_utr_boundary` | a UTR term is present and a start, stop or coding term differs | 714 |
| `start_codon` | `start_lost` or `start_retained_variant` differs | 140 |
| `stop_codon` | `stop_lost` or `stop_retained_variant` differs | 148 |
| `incomplete_terminal_codon` | `incomplete_terminal_codon_variant` is present | 92 |
| `inframe_indel` | any other `Consequence` difference | 996 |
| `hgvs` | `Consequence` is the same; `HGVSc`, `HGVSp` or positions differ | 1,641 |
| **all** | | **6,922** |

Further facts from these records:

- No record on these contigs differs in the order of its CSQ entries; that
  happens on chrMT only.
- 1,198 of the 6,922 have an allele of 50 bp or more. These contigs have 6,775
  such records, so 17.7% of them differ, against 0.12% of the other 4,582,766.
  They are sequence alleles, not symbolic structural variants, and stay in the
  comparison (section 2).
- Some classes sit in a few places. 770 of the 875 differing chr13 records and
  848 of the 1,167 on chr17 are `info_percent_encoding`. 371 of the 395 `hgvs`
  records on chr10 differ only on NM_001304718.2 (PTEN), and 690 of the 730 on
  chr2 only on NM_015120.4 (ALMS1).
- For two records vepyr writes a different `HGNC_ID` on a RefSeq `NR_`
  transcript when the record is annotated on its own than in the full run:
  20:63674046 on NR_037882.1 and 7:117544096 on NR_149084.1. In the full run
  vepyr agrees with VEP. These two records are therefore in `records.tsv` but
  not in the fixtures; see [`mismatches/chr20/`](mismatches/chr20/README.md)
  and [`mismatches/chr7/`](mismatches/chr7/README.md).
- One differing record comes from VEP itself. 22:26666332 G>A (ClinVar
  2691795, class `hgvs` in [`mismatches/chr22/`](mismatches/chr22/README.md))
  differs only in `CLIN_SIG`, which joins two values,
  `risk_factor%3Brisk_factor` and `benign`. VEP writes them in either order,
  and the choice changes from one CSQ entry to the next and from one run to
  the next. Three VEP runs of chr22 (this reference on the desktop, and the
  whole contig and the contig in 12 parts on a cloud VM) put
  `risk_factor%3Brisk_factor` first in 39, 33 and 39 of the 67 entries, not in
  the same entries, and agree on every other chr22 record. vepyr puts it first
  in all 67. No other differing record of the release has a `CLIN_SIG`
  difference.

### chrMT

2,327 of the 3,134 records differ, for three independent reasons:

1. **CSQ entry order, 2,030 records with identical content.** All of them
   differ by the same swap: VEP writes the RefSeq transcript `ND4L` before
   `ND4`, vepyr writes `ND4` first (plain alphabetical order). The rest of the
   order agrees.
2. **Mitochondrial codon table, 294 records.** vepyr translates MT transcripts
   with the standard genetic code instead of the vertebrate mitochondrial one.
   For example MT:3307 A>T in ENST00000361390 (ND1): ATA is Met in mitochondria,
   so VEP writes `M/L` and `p.Met1?`, vepyr `I/L` and `p.Ile1?`.
   `Amino_acids` (543 entries), `HGVSp` (541), `Consequence` (281, e.g. VEP
   `stop_gained&start_lost` against vepyr `start_lost`), `IMPACT` (273), `SIFT`
   (239) and `PolyPhen` (249) follow from it. Most cases are in CYTB, ATP6, ND5
   and COX1.
3. **HGVSc and HGVS_OFFSET of four indels, 4 records.** For the 9,120 bp
   deletion at MT:6467 VEP writes a deletion of each transcript that lies
   inside it, e.g. `ATP6.1:c.1_681del`; vepyr leaves `HGVSc` empty. For three
   1 bp indels in MT-TT (MT:15933, 15939) vepyr places the variant 3 bases
   further towards the 3' end than VEP (`n.50del` against `n.47del`).

chrMT is outside the HG002 validation, which covers chromosomes 1-22, so these
are new findings rather than regressions. The Linux run writes the same VEP
and vepyr records as the MacBook, so the same 2,327 records differ. All
differing records, and a small input/VEP/vepyr fixture for each class, are in
[`mismatches/chrMT/`](mismatches/chrMT/README.md).
