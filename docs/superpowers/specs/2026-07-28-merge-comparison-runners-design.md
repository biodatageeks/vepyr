# Merge the e2e comparison runners into one script

**Date:** 2026-07-28
**Status:** Approved (design)
**Supersedes:** `e2e-testing/scripts/run_annotation_fast.py`, `e2e-testing/scripts/run_annotation_fast_all.py`

## Problem

Two scripts in `e2e-testing/scripts/` run the vepyr-vs-Ensembl-VEP parity comparison:

- `run_annotation_fast.py` (809 lines) — one contig: normalize, slice, annotate, slice the VEP
  reference, compare, write `reports/fast_chr{N}{suffix}_report.json`.
- `run_annotation_fast_all.py` (780 lines) — spawns the former once per contig via
  `subprocess.run`, then reads those JSONs back to aggregate, classify, and emit Markdown.

They share no imports and are coupled only through forwarded argv and a report filename. That
loose coupling makes them mergeable, but three defects block the workflows we need today.

### Defect 1 — block-gzipped VEP references crash the comparison

`extract_chrom_from_vep()` uses a bare `open(vep_vcf)` (`run_annotation_fast.py:358`) while every
other reader goes through `open_text()`. Verified empirically:

```
extract_chrom_from_vep on PLAIN vep reference  ->  OK, 2 records
extract_chrom_from_vep on BGZF  vep reference  ->  UnicodeDecodeError:
                                                   'utf-8' codec can't decode byte 0x8b
```

All VEP references are now block-gzipped under `output/115.2/` and `output/116/`, so **9 of the 10
profiles point at files that no longer exist**, and the tenth resolves only to a partially written
file from an in-flight run.

### Defect 2 — uncompressed input is only half-supported

The default path normalizes, which bgzips and indexes as a side effect. But `--no-normalize` with a
plain `.vcf` dies in `ensure_tabix_index`, verified: `CalledProcessError`, with tabix reporting
`the compression of 'x.vcf' is not BGZF`.

### Defect 3 — the profile table is duplicated and release-blind

`_CACHE_PROFILES` (`run_annotation_fast.py:42-148`) and `PROFILE_SUFFIXES`
(`run_annotation_fast_all.py:28-39`) hold the same ten keys, maintained separately;
`tests/test_run_annotation_fast.py:61` exists solely to catch drift between them. Neither models the
Ensembl release, while on disk the caches are `115_GRCh38_*` plus `116_GRCh38_merged` and the
references are split across `output/115.2/` and `output/116/`. Comparing a 115 cache against a 116
reference produces a flood of mismatches that look like engine bugs.

## Goals

1. One script covering a single contig and all contigs.
2. Plain and block-gzipped VCFs accepted symmetrically on both the vepyr and VEP sides.
3. Release modelled explicitly so a cache and a reference can never be silently mismatched.

## Non-goals

Adding profiles for the two orphan references (`..._merged_am.vcf.gz`,
`..._everything_vep.vcf.gz`), building the missing 116 caches, and regenerating the
never-created `merged_flag_pick` / `merged_pick_filter` references. The runner reports these
combinations as unavailable; producing them is separate work.

## Current availability matrix

Verified on disk 2026-07-28.

| Profile | 115 ref | 115 cache | 116 ref | 116 cache |
|---|---|---|---|---|
| `ensembl` | OK | OK | — | — |
| `merged` | OK | OK | OK | OK |
| `refseq` | OK | OK | OK | — |
| `merged_flag_pick_allele` | OK | OK | — | OK |
| `merged_flag_pick_allele_gene` | OK | OK | — | OK |
| `merged_per_gene` | OK | OK | — | OK |
| `merged_pick_allele` | OK | OK | — | OK |
| `merged_pick_allele_gene` | OK | OK | — | OK |
| `merged_flag_pick` | **—** | OK | — | OK |
| `merged_pick_filter` | **—** | OK | — | OK |

`merged_flag_pick` and `merged_pick_filter` are broken today and fail silently; under this design
they fail immediately with a listing of what is available.

## Architecture

```
e2e-testing/scripts/
  run_comparison.py        # entry point: sys.argv -> comparison.cli.main()
  comparison/
    __init__.py
    profiles.py            # profile x release matrix, path derivation, availability check
    vcfio.py               # open_text, is_bgzf, ensure_bgzf, contig detection, slicing
    compare.py             # compare_vcfs(a, b) -> dict     (no vepyr, no argparse)
    annotate.py            # run_one(chrom, cfg) -> dict     (the ONLY vepyr importer)
    report.py              # ISSUES registry, aggregate, classify, markdown
    cli.py                 # argparse, orchestration loop, --isolate
```

Each module has one responsibility and a stated dependency boundary:

- `vcfio` knows compression and indexing, nothing about CSQ.
- `compare` takes two paths and returns a dict; it touches no CLI, no Markdown, and does not
  import `vepyr`. This is the highest-value code in the tree and becomes unit-testable without a
  built native extension.
- `report` takes dicts and returns a string; it touches no filesystem.
- `annotate` is the sole importer of `vepyr`.

`run_annotation_fast.py` and `run_annotation_fast_all.py` are deleted.

## CLI surface

```
run_comparison.py --release 115                          # all detected contigs
run_comparison.py --release 115 --chroms 22              # one contig
run_comparison.py --release 116 --profile merged --chroms 1 2 22
  [--force] [--bgzf] [--workers N] [--isolate]
  [--skip-annotate] [--skip-compare] [--no-normalize]
  [--vcf ...] [--vep ...] [--cache-dir ...] [--fasta ...]
```

### Defaults

| Flag | Default | Rationale |
|---|---|---|
| `--release` | **required** | Forces a conscious choice; the strongest guard against cache/reference mismatch |
| `--chroms` | auto-detected | See "Contig detection"; `all` is a synonym for detect |
| `--profile` | `merged` | The only profile that fully resolves at both 115 and 116, so a bare run works at either release (both current scripts default to `ensembl`, which has no 116 cache or reference) |
| `--force` | off (reuse) | The expensive option is opted into, not out of |
| `--bgzf` | off (plain output) | Both current scripts agree |
| `--workers` | `1` | Matches `docs/performance.md` |
| normalize | **on** (`--no-normalize` disables) | Current single-contig default |
| `--isolate` | off (in-process) | Subprocess isolation available on demand |
| `--skip-annotate` | off | From the all-contigs script |
| `--skip-compare` | off | Both |
| `--vcf` | `$DATA/input/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz` | Moved under `input/`; see "Data directory layout" |
| `--fasta` | `$DATA/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa` | Moved under `input/`; see "Data directory layout" |
| `--vep`, `--cache-dir` | derived from profile x release | Explicit values override |
| `$DATA` | `$DATA_VEPYR_DIR` or `~/workspace/data_vepyr` | Unchanged |

Dropped: `--no-force` (polarity resolved to `--force`) and the hidden `--cache` alias.

At startup, before any work, the runner echoes the resolved `cache_dir`, `vep_vcf`, and detected
contigs. The 115/116 trap is silent precisely because nobody sees which files a run opened.

## Data directory layout

`$DATA` currently mixes inputs, VEP outputs, caches, logs, and stray artifacts at the top level.
Inputs move into `$DATA/input/`, mirroring the `$DATA/output/{release}/` convention that already
exists:

```
$DATA/
  input/
    HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz     + .tbi
    HG002_normalized.vcf.gz                       + .tbi   # the VEP-side normalized input
    Homo_sapiens.GRCh38.dna.primary_assembly.fa   + .fai
  output/{115.2,116}/                                       # VEP reference VCFs
  {115,116}_GRCh38_{ensembl,merged,refseq}/                 # Parquet caches
```

**Resolution order.** Default input paths resolve `$DATA/input/{name}` first and fall back to
`$DATA/{name}`, logging a deprecation warning when the legacy location is used. This decouples the
code change from the disk move: the runner works before, during, and after the reorganisation, and
an explicit `--vcf` / `--fasta` always wins.

**The disk move is a separate manual step, not part of this change.** At the time of writing an
Ensembl-116 VEP container is running with `$DATA` bind-mounted as both `/fasta` and `/work`,
reading the FASTA and `HG002_normalized.vcf.gz` by path. Moving those files while it runs risks
killing a multi-hour job. The move must wait until no container holds them, and the Docker `-v`
paths in `docs/testing-vep.md` and `e2e-testing/vep-docker.md` must be updated in the same pass.

## Profile and release resolution

`profiles.py` holds one dict of profile -> `{flavour, vep_basename, annotate_kwargs, suffix}` and
derives both paths. Note that `suffix` is stored **without** a leading underscore (`"merged"`, not
`"_merged"`, as the current tables use), so the filename templates below compose cleanly; the
rendered names are unchanged for the legacy fallback.

```
cache_dir = $DATA/{release}_GRCh38_{flavour}
vep_vcf   = $DATA/output/{RELEASE_DIRS[release]}/{vep_basename}{.vcf.gz | .vcf}
RELEASE_DIRS = {"115": "115.2", "116": "116"}
```

Releases are strings throughout (`--release 115`), never ints, because `115.2` is a directory name
and future releases may not be purely numeric.

`.vcf.gz` is preferred and `.vcf` accepted. `RELEASE_DIRS` is explicit because the on-disk `115.2`
directory name is not derivable from the release number. Resolution runs before any other work and,
on failure, lists what is available for every release, so `--profile refseq --release 116` reports
"reference OK, no `116_GRCh38_refseq` cache" rather than failing after an hour of annotation.

## Contig detection

Detection reads the **index, not the header**. Measured on the real files:

```
tabix -l   ->  chr1 chr2 ... chr22    (22 contigs, in coordinate order)
##contig   ->  195 entries            (whole GRCh38 primary assembly + scaffolds/alts)
```

Parsing `##contig` would launch 195 contigs, 173 of them empty. `tabix -l` also returns index
order, giving `chr1..chr22` rather than the lexicographic `chr1, chr10, chr11, ...` a naive sort
produces.

Algorithm:

1. `tabix -l` on the resolved VEP reference.
2. Intersect with `tabix -l` on the prepared input VCF, preserving reference order. Only contigs
   present in both can be compared, and the input is always indexed because per-contig slicing
   requires it.
3. If the reference is plain or unindexed, fall back to the input's contig list and warn that
   detection was degraded.
4. With `--skip-compare` there is no reference, so detect from the input alone.
5. An explicit `--chroms` wins but is validated against the detected list, failing fast with the
   available contigs instead of producing an empty slice.

This removes the hardcoded `range(1, 23)` and makes X/Y/MT work automatically on datasets that
contain them.

## Compression handling

| Path | Today | Merged runner |
|---|---|---|
| VEP ref, bgzf + `.tbi` | `UnicodeDecodeError` | `tabix ref chrN` — seek, not scan |
| VEP ref, bgzf, no index | `UnicodeDecodeError` | stream via `open_text` |
| VEP ref, plain | linear scan | unchanged |
| vepyr output, plain or bgzf | works | unchanged, keeps `is_bgzf` validation |
| Input, plain, `--no-normalize` | `CalledProcessError` | auto-bgzip into the work dir, then index |
| Input, plain, normalizing | works | unchanged |

The tabix path also eliminates 22 full scans of a 1.6 GB reference per all-contigs run.
`count_data_lines()` replaces the `gunzip -c | grep -cv '^#'` shellout at
`run_annotation_fast.py:697`.

## Artifact naming

Two latent bugs are fixed here, both made likely by reuse-by-default.

**Release must appear in artifact names.** Today `results/fast_chr1/vepyr_parquet_chr1_merged.vcf`
and `reports/fast_chr1_merged_report.json` carry no release token
(`run_annotation_fast.py:706,797`). A `--release 116` run would find the 115 artifact, judge it
present, and reuse it — publishing a "116 parity report" built from 115 annotations. New names
embed the release:

```
results/{release}/_shared/normalized.vcf.gz          # + .tbi, + normalized.source.json
results/{release}/fast_{chrom}/input_{chrom}.vcf.gz  # + .tbi
results/{release}/fast_{chrom}/vepyr_parquet_{chrom}_{suffix}.vcf[.gz]
results/{release}/fast_{chrom}/vep_{chrom}_{suffix}.vcf
reports/fast_{chrom}_{suffix}_{release}_report.json
reports/fast_{span}_{suffix}_{release}_summary_{timestamp}.md
```

Every intermediate lives under `results/{release}/`. Reports stay in the flat `reports/` directory,
where the release is carried in the filename instead, so the existing 408 historical reports remain
in place and loadable.

`{span}` derives from the detected contig set rather than the hardcoded `chr1_22`. The report
loader tries the release-qualified name first and falls back to the legacy
`fast_{chrom}_{suffix}_report.json`, so `--skip-annotate` keeps working against the 408 existing
reports while new runs cannot collide.

**Normalization must be shared per release, not per contig.** `normalize_vcf(args.vcf, work_dir)`
is called with the per-contig work directory (`run_annotation_fast.py:692`) and writes
`normalized.vcf.gz` inside it, so a 22-contig sweep runs `bcftools norm` over the full 2.9 GB input
22 times and keeps 22 copies of the result. It moves up one level, to
`results/{release}/_shared/normalized.vcf.gz`, computed once per release and reused by every
contig.

Normalization is in principle release-independent — `bcftools norm -m -both` touches only the input
VCF — so scoping it per release costs one redundant pass per release. That is deliberate. It buys a
single total invariant that is trivial to verify by inspection:

> Nothing under `results/{release}/` is ever read by a run of a different release.

A shared-across-releases cache would make that invariant conditional, and conditional invariants are
how the 115/116 mismatch got in.

**Normalized output is keyed to its source.** A per-release directory still reuses
`normalized.vcf.gz` blindly if `--vcf` changes between runs at the same release. Alongside the
normalized file the runner writes `normalized.source.json` recording the input path, size, and
mtime; on reuse it compares, and re-normalizes on any mismatch rather than silently annotating a
stale decomposition.

## Execution and error handling

Contigs run in-process in a loop, each wrapped in `try/except`. A failure is recorded and the loop
continues; the run exits non-zero at the end listing failed contigs.

`--isolate` re-runs each contig as a subprocess, so a SIGSEGV in the native extension loses only
that contig. This preserves the crash isolation the current subprocess design provides
accidentally, as an explicit documented flag rather than an implementation side effect.

Profile resolution raises before any work begins, so a bad `--release` never wastes a
normalization pass. A `--bgzf` output that fails BGZF validation remains a hard exit.

## Testing

`tests/test_run_comparison.py` replaces `tests/test_run_annotation_fast.py`. The ~20 existing tests
port over where still applicable, minus `test_fast_all_profile_suffixes_match_single_runner_profiles`,
which the merge makes meaningless.

New tests the module split makes possible, none of which need a built native extension:

- `compare_vcfs` over all four compression combinations (plain/plain, bgzf/bgzf, bgzf/plain,
  plain/bgzf) returns identical dicts. This is the direct regression test for Defect 1.
- The tabix slice path and the linear slice path produce byte-identical output.
- `profiles.resolve` raises with an availability listing for an unavailable combination, and
  omitting `--release` is an argparse error.
- Default input resolution prefers `$DATA/input/`, falls back to `$DATA/` with a warning, and an
  explicit `--vcf` / `--fasta` overrides both.
- Contig detection prefers the index over the header: given a fixture whose header lists more
  contigs than the index, only the indexed ones are returned, in index order.
- Artifact names for the same profile at two releases do not collide, and every intermediate path
  a run resolves is contained under its own `results/{release}/` — the isolation invariant asserted
  directly rather than by inspection.
- Reused normalization is invalidated when `--vcf` changes: the same release run twice with
  different inputs re-normalizes instead of reusing `normalized.vcf.gz`.
- `report.generate_markdown` on a synthetic aggregate, with no filesystem access.

A `conftest.py` puts `e2e-testing/scripts` on `sys.path` so the package imports normally.

## Migration

- Delete `run_annotation_fast.py`, `run_annotation_fast_all.py`, and
  `tests/test_run_annotation_fast.py`.
- Rewrite `e2e-testing/README.md:61-194` for the single command surface, adding `--release` to
  every example.
- Preserve the JSON report contract via the legacy-name fallback described above.
- Preserve `--skip-annotate` and `--skip-compare` behaviour.

## Risks

| Risk | Mitigation |
|---|---|
| `--release` now required breaks every documented command | README rewritten in the same change; argparse error names the valid values |
| In-process execution loses crash isolation | `--isolate` restores it on demand |
| Legacy report fallback masks a genuine naming bug | Fallback logs when it fires, so silent reliance is visible |
| Historical reports predate the release axis and cannot be attributed | Accepted; they are only read by `--skip-annotate` |
| Per-release normalization costs one redundant `bcftools norm` pass and ~2.9 GB per release | Accepted deliberately to keep the isolation invariant unconditional |
| Existing `results/fast_chr*/` directories predate the release layout | Left in place, unread by the new layout; removable by hand |
| Moving inputs into `$DATA/input/` breaks a running VEP container | The move is manual, deferred, and gated on no container holding the files; the fallback keeps the runner working either way |
| Docker `-v` paths in the docs point at the old input locations | Updated in the same pass as the disk move, not with the code change |
