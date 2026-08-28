# Testing against Ensembl VEP

vepyr's correctness bar is **zero mismatches against Ensembl VEP `--everything --hgvs`**
for the supported scope (`homo_sapiens`, GRCh38, Ensembl release 116). Reaching that bar
means running both tools over the same input and diffing the resulting CSQ fields
variant by variant.

This page covers the input preparation, the reference (golden truth) VEP run, and the
comparison harness under `e2e-testing/`.

## Test dataset

The parity suite runs on the GIAB **HG002 / NA24385** benchmark callset (Ashkenazim
son, NIST v4.2.1, GRCh38, chr1-22) — roughly 4M variants, large enough to exercise
every consequence type and small enough to re-run after every dependency bump.

| Item | Value |
|---|---|
| Source | GIAB AshkenazimTrio, `NISTv4.2.1/GRCh38` |
| Download size | ~156 MB (`.vcf.gz`) |
| Input records | 4,048,342 |
| Records after normalization | 4,096,123 |

## Preprocessing

All commands below assume a working directory that will hold the test data. The
e2e scripts default to `~/workspace/data_vepyr` and honour `DATA_VEPYR_DIR`.

```bash
export DATA_VEPYR_DIR=~/workspace/data_vepyr
mkdir -p "$DATA_VEPYR_DIR"/{input,cache,output}
cd "$DATA_VEPYR_DIR"
```

You need `bcftools`, `bgzip`, `tabix`, and `samtools` on `PATH`. The reference numbers
on this page were produced with **bcftools 1.21 / htslib 1.21**.

### Data directory layout

The harness expects three subdirectories under `$DATA_VEPYR_DIR`, separating what you
feed in, what vepyr reads, and what Ensembl VEP produces:

```
$DATA_VEPYR_DIR/
  input/
    HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz     + .tbi   # downloaded benchmark
    HG002_normalized.vcf.gz                       + .tbi   # normalized, fed to both tools
    Homo_sapiens.GRCh38.dna.primary_assembly.fa   + .fai   # reference FASTA
  cache/
    {release}_GRCh38_{ensembl,refseq,merged}/              # vepyr Parquet caches
  output/
    116/                                                   # Ensembl VEP reference VCFs
    115.2/                                                 # (115 references live here)
  homo_sapiens{,_refseq,_merged}/                          # raw Ensembl VEP caches
```

The raw `homo_sapiens*` caches stay at the top level: they are consumed by the VEP
Docker containers, not by vepyr, and vepyr reads only the converted Parquet caches
under `cache/`.

Note the asymmetry in `output/`: release 115 references live in `115.2` (the VEP point
release they were generated with), release 116 in `116`. The runner keeps that mapping
explicitly rather than deriving the directory from the release number, so `--release 115`
finds `output/115.2/` without you having to remember it.

!!! tip "Migrating an older layout"
    Earlier versions kept inputs and Parquet caches at the top level. The runner still
    finds them there and prints a one-line notice per path telling you what to move, so
    you can reorganise whenever it is convenient. Move the caches only when no vepyr
    annotation is running, and the inputs only when no VEP container has
    `$DATA_VEPYR_DIR` bind-mounted — a running container reads the FASTA and the
    normalized VCF by path.

### 1. Download the benchmark VCF

```bash
cd "$DATA_VEPYR_DIR/input"

wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
  https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz

wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
  https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz.tbi
```

The `-c` flag resumes a partial download, which matters on this file size. Confirm you
got the same release used for the numbers on this page:

```bash
md5sum HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz
# dc750b3807d4af1f7ffec852e9c2f771
```

### 2. Normalize to biallelic records

VEP consequence calls are per-allele, so multiallelic sites must be decomposed into one
record per ALT allele before either tool sees them. Without this step the two tools
disagree on allele ordering and the comparison is meaningless.

```bash
cd "$DATA_VEPYR_DIR/input"

bcftools norm -m -both \
  -o HG002_normalized.vcf \
  HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz
```

Expected output:

```
Lines   total/split/joined/realigned/removed/skipped:	4048342/47781/0/0/0/0
```

47,781 multiallelic sites split, giving 4,096,123 biallelic records in a ~2.9 GB plain
VCF. Note this is `-m -both` only — no `-f`/left-alignment against the reference, so the
step is deterministic and needs no FASTA.

### 3. Compress and index

```bash
bgzip -f HG002_normalized.vcf
tabix -p vcf HG002_normalized.vcf.gz
```

`bgzip` replaces the 2.9 GB plain VCF with a ~151 MB `.vcf.gz`, and `tabix` writes the
`.tbi` beside it. Both tools consume this pair directly, so there is no reason to keep
the uncompressed copy:

- **vepyr** needs the index for within-contig parallelism (`workers` > 1) and for
  per-chromosome extraction in the e2e scripts.
- **Ensembl VEP** accepts a block-gzipped `--input_file` and decompresses it in a little
  under a second — negligible against a multi-hour run.

### 4. Reference FASTA

Needed by both tools for HGVS notation (`--hgvs` / `reference_fasta=`):

```bash
cd "$DATA_VEPYR_DIR/input"

wget -c https://ftp.ensembl.org/pub/release-116/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
gzip -d Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
samtools faidx Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

The uncompressed FASTA is ~3.1 GB. It must stay uncompressed: both tools index it with
`.fai` for random access, which plain gzip does not support.

### 5. Ensembl VEP caches

VEP ships three GRCh38 caches for release 116, differing only in which transcript set
they contain. Download whichever ones you intend to compare against — the parity suite
uses all three:

| Cache | Transcripts | Download |
|---|---|---|
| Ensembl | Ensembl/GENCODE only | [`homo_sapiens_vep_116_GRCh38.tar.gz`](https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_vep_116_GRCh38.tar.gz) |
| RefSeq | RefSeq only | [`homo_sapiens_refseq_vep_116_GRCh38.tar.gz`](https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_refseq_vep_116_GRCh38.tar.gz) |
| Merged | Ensembl + RefSeq | [`homo_sapiens_merged_vep_116_GRCh38.tar.gz`](https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_merged_vep_116_GRCh38.tar.gz) |

These are the *indexed* caches, which is what vepyr's converter and VEP's `--offline`
mode both expect. The full FTP directory is
[`release-116/variation/indexed_vep_cache/`](https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/).

```bash
cd "$DATA_VEPYR_DIR"

wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
  https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_vep_116_GRCh38.tar.gz
wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
  https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_refseq_vep_116_GRCh38.tar.gz
wget -c --tries=20 --waitretry=5 --retry-connrefused --timeout=30 \
  https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_merged_vep_116_GRCh38.tar.gz

# Extract from $DATA_VEPYR_DIR, never from inside a species directory -- see below
for t in homo_sapiens homo_sapiens_refseq homo_sapiens_merged; do
  tar xzf "${t}_vep_116_GRCh38.tar.gz"
done
```

Each archive unpacks to a `<species_dir>/116_GRCh38/` tree — the directory name encodes
the transcript set, which is why the three can coexist under one root:

| Cache | Extracted path | vepyr Parquet cache |
|---|---|---|
| Ensembl | `homo_sapiens/116_GRCh38` | `cache/116_GRCh38_ensembl` |
| RefSeq | `homo_sapiens_refseq/116_GRCh38` | `cache/116_GRCh38_refseq` |
| Merged | `homo_sapiens_merged/116_GRCh38` | `cache/116_GRCh38_merged` |

!!! warning "The archive carries its own species directory"
    Each tarball already contains the `homo_sapiens_refseq/` prefix, so extracting from
    *inside* that directory produces `homo_sapiens_refseq/homo_sapiens_refseq/116_GRCh38`
    and leaves the path VEP actually looks at empty. Docker then silently creates the
    missing bind-mount source as an empty directory, and every variant is skipped with:

    ```
    WARNING: Chromosome chr1 not found in annotation sources or synonyms
    ```

    That message is misleading — nothing is wrong with the chromosome naming. An empty
    version directory has no per-chromosome subdirectories *and* no `chr_synonyms.txt`,
    which is the only thing that maps `chr1` to Ensembl's `1`. The run completes after
    hours having annotated nothing.

    Verify before starting a multi-hour run:

    ```bash
    for t in homo_sapiens homo_sapiens_refseq homo_sapiens_merged; do
      n=$(ls "$DATA_VEPYR_DIR/$t/116_GRCh38" 2>/dev/null | wc -l)
      echo "$t/116_GRCh38: $n entries $([ -e "$DATA_VEPYR_DIR/$t/116_GRCh38/chr_synonyms.txt" ] \
        && echo '(chr_synonyms.txt present)' || echo '*** MISSING chr_synonyms.txt ***')"
    done
    ```

    Expect roughly 1,900 entries and `chr_synonyms.txt` for each. If one is nested,
    lift it back up:

    ```bash
    cd "$DATA_VEPYR_DIR/homo_sapiens_refseq"
    rmdir 116_GRCh38 && mv homo_sapiens_refseq/116_GRCh38 . && rmdir homo_sapiens_refseq
    ```

Convert each extracted cache to vepyr's Parquet format with
[`build_cache()`](caches.md), writing into `cache/`, before annotating.

!!! note "Reproducibility of the normalized VCF"
    `bcftools norm` writes a `##bcftools_normCommand=` header line containing the output
    path and the wall-clock date of the run, so a whole-file checksum differs between two
    runs of the same command on the same input. Compare header and records separately:

    ```bash
    # Header: expect a single differing ##bcftools_normCommand= line
    diff <(bcftools view -h A.vcf.gz) <(bcftools view -h B.vcf.gz)

    # Records: expect no output and exit status 0
    cmp <(bcftools view -H A.vcf.gz) <(bcftools view -H B.vcf.gz)
    ```

    Re-running steps 1-3 on the md5 above reproduces the records byte for byte
    (md5 `f47259aebaa00b4eb6840c636ea14783` over all 4,096,123 records).

## Generating the reference VEP output

The golden truth is Ensembl VEP 116 run offline in Docker over the same normalized VCF.
This is the slow half of the exercise — a full chr1-22 `--everything --hgvs` run takes
**4-7 hours per cache**, so generate each once and keep it.

The three commands below differ only in the cache mounted, the transcript-set flag
(none / `--refseq` / `--merged`), and the output filename. All of them run
`--offline --cache --everything --hgvs` against the same
`HG002_normalized.vcf.gz` from the preprocessing steps, reading from `input/` and
writing into `output/116/`.

```bash
mkdir -p "$DATA_VEPYR_DIR/output/116"
```

=== "Ensembl"

    ```bash
    time docker run --rm \
      -v "$DATA_VEPYR_DIR/homo_sapiens/116_GRCh38:/opt/vep/.vep/homo_sapiens/116_GRCh38:ro" \
      -v "$DATA_VEPYR_DIR/input:/input:ro" \
      -v "$DATA_VEPYR_DIR/output/116:/output" \
      ensemblorg/ensembl-vep:release_116.0 \
      vep \
      --dir /opt/vep/.vep \
      --cache \
      --offline \
      --assembly GRCh38 \
      --input_file /input/HG002_normalized.vcf.gz \
      --output_file /output/HG002_annotated_wgs_everything_hgvs_vep.vcf \
      --vcf \
      --force_overwrite \
      --no_stats \
      --everything --hgvs \
      --fasta /input/Homo_sapiens.GRCh38.dna.primary_assembly.fa
    ```

=== "RefSeq"

    ```bash
    time docker run --rm \
      -v "$DATA_VEPYR_DIR/homo_sapiens_refseq/116_GRCh38:/opt/vep/.vep/homo_sapiens_refseq/116_GRCh38:ro" \
      -v "$DATA_VEPYR_DIR/input:/input:ro" \
      -v "$DATA_VEPYR_DIR/output/116:/output" \
      ensemblorg/ensembl-vep:release_116.0 \
      vep \
      --dir /opt/vep/.vep \
      --cache \
      --refseq \
      --offline \
      --assembly GRCh38 \
      --input_file /input/HG002_normalized.vcf.gz \
      --output_file /output/HG002_annotated_wgs_everything_hgvs_refseq.vcf \
      --vcf \
      --force_overwrite \
      --no_stats \
      --everything --hgvs \
      --fasta /input/Homo_sapiens.GRCh38.dna.primary_assembly.fa
    ```

=== "Merged"

    ```bash
    time docker run --rm \
      -v "$DATA_VEPYR_DIR/homo_sapiens_merged/116_GRCh38:/opt/vep/.vep/homo_sapiens_merged/116_GRCh38:ro" \
      -v "$DATA_VEPYR_DIR/input:/input:ro" \
      -v "$DATA_VEPYR_DIR/output/116:/output" \
      ensemblorg/ensembl-vep:release_116.0 \
      vep \
      --dir /opt/vep/.vep \
      --cache \
      --merged \
      --offline \
      --assembly GRCh38 \
      --input_file /input/HG002_normalized.vcf.gz \
      --output_file /output/HG002_annotated_wgs_everything_hgvs_merged.vcf \
      --vcf \
      --force_overwrite \
      --no_stats \
      --everything --hgvs \
      --fasta /input/Homo_sapiens.GRCh38.dna.primary_assembly.fa
    ```

!!! warning "Docker creates missing bind-mount sources"
    If a `-v` source path does not exist, Docker creates it as an empty directory rather
    than failing. A typo in a cache path therefore produces a silent no-op run, not an
    error — which is the failure mode described under the extraction warning above.
    Run the verification loop before committing hours to a run.

!!! warning "BAM-edited cache"
    The RefSeq and merged caches are BAM-edited, so VEP logs
    `BAM-edited cache detected, enabling --use_transcript_ref` and annotates against the
    transcript reference rather than the REF allele you supplied. Pass `--use_given_ref`
    to override. Both caches also emit `WARNING: Transcript-assembly mismatch` lines for
    a few dozen variants; these are expected and are captured in the
    `*_warnings.txt` files next to the output.

The pick-mode reference commands (`--pick`, `--pick_allele`, `--per_gene`,
`--flag_pick`, ...) are recorded verbatim in
[`e2e-testing/vep-docker.md`](https://github.com/biodatageeks/vepyr/blob/master/e2e-testing/vep-docker.md),
which still documents the release 115 runs the current reports were generated from.
All pick modes use the ranking order
`biotype,rank,mane_select,tsl,canonical,appris,ccds,length`.

### Compress and index the reference output

VEP writes plain VCF. With `--everything --hgvs` over 4M variants that is **15-29 GB per
run**, and a full profile matrix will fill a disk. Block-gzip and index each output as
soon as its run finishes:

```bash
cd "$DATA_VEPYR_DIR/output/116"

for f in *.vcf; do
  bgzip --threads 6 "$f" && tabix -p vcf "${f}.gz"
done
```

`bgzip` removes the plain input only after it succeeds, so a failure leaves the original
intact. Compression is roughly 15x on this data:

| Reference | Plain | Block-gzipped |
|---|---|---|
| `..._hgvs_merged.vcf` | 27 GB | 1.6 GB |
| `..._hgvs_vep.vcf` | 16 GB | 987 MB |
| `..._hgvs_refseq.vcf` | 10 GB | 718 MB |

This is not merely housekeeping. The comparison harness reads block-gzipped references
through `tabix`, so extracting one contig becomes a seek instead of a scan of the whole
file — across 22 contigs that is the difference between 22 full reads of a multi-gigabyte
file and 22 index lookups. Plain `.vcf` references still work, and are read by streaming
scan.

Verify before deleting anything upstream:

```bash
bgzip -t HG002_annotated_wgs_everything_hgvs_merged.vcf.gz   # BGZF framing intact
tabix -l HG002_annotated_wgs_everything_hgvs_merged.vcf.gz   # expect chr1..chr22
```

## Running the comparison

With the normalized input, the converted Parquet cache, and the reference VEP output in
place, the harness under `e2e-testing/scripts/` annotates with vepyr and diffs against
the reference field by field.

```bash
cd e2e-testing/scripts

# Single chromosome
uv run python run_comparison.py --release 115 --chroms 1

# All detected contigs, with a timestamped Markdown report
uv run python run_comparison.py --release 115

# A specific pick-mode profile
uv run python run_comparison.py --release 115 --profile merged_pick_allele_gene

# Plugin fields, against a VEP reference built with the same five plugins.
# Plugin references are written one file per contig, so name the contig.
uv run python run_comparison.py --release 116 --profile merged_plugins --chroms 22
```

!!! note "Plugin profiles need a contig"
    `generate_vep_plugin_references.sh` writes one reference per contig under
    `output/{release}/plugins/`, so unlike the whole-genome profiles there is no
    single file to slice. Pass a single `--chroms` value, or `--vep` explicitly.
    Modes that read no reference at all, such as `--skip-annotate` aggregation
    of stored reports, do not need either.

!!! note "Plugin profiles"
    `merged_plugins` attaches the plugin cache
    (`cache/plugin_cache_{release}/`, or `--plugin-cache`) so ClinVar, SpliceAI,
    CADD, AlphaMissense, and dbNSFP CSQ fields are comparable against a VEP
    reference produced with the same five plugins. `merged_plugins_base` reads
    that same reference with no plugin cache attached: the comparison then
    restricts itself to the shared fields, which separates a core-field
    difference from anything the plugin machinery introduced.

    These are comparison scenarios, not release gates. `verify_parity_gate.py`
    pins the Ensembl core CSQ contract and refuses a plugin profile outright
    rather than silently gating a field set it does not describe.

!!! note "`--release` is required"
    It selects both the Parquet cache (`cache/{release}_GRCh38_{flavour}`) and the VEP
    reference (`output/{release}/`), so a release 115 cache can never be compared against
    a release 116 reference. There is no default, because a wrong default here produces a
    plausible-looking report full of mismatches that are artefacts rather than bugs.

    The Ensembl, merged, and RefSeq baselines are qualified for both release 115 and
    release 116. Optional selection profiles still depend on their corresponding
    reference VCF. Ask for an unavailable combination and the run fails in
    milliseconds—before normalizing anything—and prints the live availability matrix:

    ```
    profile                                   115          116
    ensembl                                    ok           ok
    merged                                     ok           ok
    refseq                                     ok           ok
    merged_pick_allele                         ok no reference
    merged_plugins                              -  no plugins
    ```

Contigs default to whatever the reference's tabix index contains, intersected with the
input — so the same command covers chr1-22 here and adapts automatically to a dataset
with different contigs. Detection deliberately reads the index rather than the
`##contig` headers: the headers on these references list all 195 GRCh38 primary-assembly
sequences, of which only 22 carry records.

Outputs land in `e2e-testing/reports/`: per-contig JSON
(`fast_{chrom}_{profile}_{release}_report.json`) and an aggregate summary
(`fast_{span}_{profile}_{release}_summary_YYYYMMDD_HHMM.md`) with per-contig timings,
mismatch counts classified by root cause, and per-field mismatch examples.
Intermediates live under `e2e-testing/results/{release}/`.

The harness normalizes the input itself if a normalized copy is not already present,
using exactly the `bcftools norm -m -both` → `bgzip` → `tabix` sequence above — so
running the preprocessing by hand and pointing it at the result gives identical
inputs. Both plain and block-gzipped VCFs are accepted on the vepyr and VEP sides;
when the reference is bgzipped and indexed, the per-contig slice is a tabix seek
rather than a full scan.

## Checking byte-level agreement

The field-by-field comparison above answers whether the annotation *content* matches.
`md5_concordance.py` answers the stricter question of whether the *bytes* match, which
is what makes vepyr's output a drop-in replacement for VEP's rather than an equivalent
one. It hashes each file's header and record body separately and compares the digests.

```bash
cd e2e-testing/scripts

# One pair, with a breakdown of what differs
uv run python md5_concordance.py \
    --pair ../results/116/fast_chr21/vep_chr21_merged.vcf /tmp/vepyr_chr21.vcf \
    --mode strict --explain

# Every per-contig pair under a results directory
uv run python md5_concordance.py --results-dir ../results/116 --mode strict
```

`--mode canonical` (the default) normalizes the differences that are known to be
cosmetic before hashing — QUAL rendered numerically, INFO and FORMAT keys sorted,
FORMAT keys missing in every sample dropped — so a matching canonical digest means the
two files carry identical annotation content and differ only in how they were written.
`--mode strict` hashes the record bytes as-is.

Each tool stamps the header with run provenance that can never match (wall-clock time,
absolute cache paths, tool versions), so those lines are excluded from the header digest
on both sides. A `HEADER DIFF` alongside a passing body is therefore a real difference
worth reading — most often a `##bcftools_normCommand` showing that the two sides were
annotated from differently normalized inputs.

!!! note "Whole-genome runs are disk-hungry"
    A plain-text annotated WGS output is ~29 GB, and the parallel path needs roughly
    twice that while it assembles worker shards. Compare one contig at a time and
    delete each output before starting the next.

See [`e2e-testing/README.md`](https://github.com/biodatageeks/vepyr/blob/master/e2e-testing/README.md)
for the full flag reference, the profile-to-reference-file mapping, the expected data
directory layout, and the dependency-bump workflow.
