# Ensembl VEP porting tests

Curated data-problem tests ported from Ensembl VEP, moved here from
`biodatageeks/vepyr-porting-tests` (snapshot of commit 6f59db4, without
history). This directory is its own uv project: run every command from
`porting-tests/`, never from the repository root.

In CI, `./run_tests --wheel <wheel>` runs against the wheel built from the
code under review (see "Continuous integration" in the root README).

References below to `AGENTS.md`, the issue/PR tooling, the campaign scripts
and `docs/porting` point to files that were not imported: read them in the
archived upstream repository at
[biodatageeks/vepyr-porting-tests@6f59db4](https://github.com/biodatageeks/vepyr-porting-tests/tree/6f59db4).

## ./run_tests

Run the 205 named data tests through the vepyr Python CLI:

```bash
export VEPYR_CACHE_ROOT=/tmp/cache
./run_tests 0.9.2
./run_tests <full-40-character-git-sha>
```

Give exactly one of the version under test or `--wheel`. Two optional flags
work with either: `--summary-md PATH` writes a Markdown table of every test that
did not pass (for `$GITHUB_STEP_SUMMARY`), and `--keep-failed DIR` copies each
mismatching run's vepyr output to `DIR/<fixture>/` for upload as a CI artifact.

```bash
./run_tests --wheel dist/vepyr-0.9.2-cp310-abi3-manylinux_2_17_x86_64.whl \
    --summary-md summary.md --keep-failed failed/
```

`--wheel PATH` installs that local wheel (dependencies still come from PyPI,
wheels only). Its install is cached under the cache root by the SHA-256 of the
file, so a rebuilt wheel is never confused with an earlier one.

The version argument is the version under test. A release version installs from
PyPI using wheels only; it never compiles vepyr. A full Git SHA checks out that
exact commit of `biodatageeks/vepyr` and builds its wheel with `uv build`, using
vepyr's own dependencies. Git builds require the Rust compiler required by that
vepyr revision. This repository contains no Rust harness or Cargo dependency pins.
Both paths install into an isolated environment under the cache root and reuse
successful installations of the same version or SHA.

The runner fetches the **merged** cache and the full reference FASTA at the
revisions in `PINS.toml`. Required contigs are derived from enabled fixtures
(currently `chr1`, `chr21`, `chr22`). Missing shards, manifests, chromosome synonyms
and reference policy files are downloaded automatically. Preparation prints stage
progress; downloads, FASTA decompression, hashing and indexing show progress too.
Git/uv output remains visible while vepyr builds, with a heartbeat during quiet steps.

The cache defaults to `~/vepyr-test-cache`. To place it elsewhere:

```bash
export VEPYR_CACHE_ROOT=/mnt/vepyr-test-cache
./run_tests 0.9.0
```

The pinned cache revision is printed and recorded in `PROVENANCE.json`. A newer
Hub commit does not silently change the corpus or block a pinned run. Existing
cache data from a different pinned revision is refused; select a different cache
root when changing the dataset pin. The FASTA remains full even with contig shards.

Each named test prints `RUN`, then `PASS`, `MISMATCH`, `ERROR`, or `SKIP`, alongside
a simple `completed/205` progress bar. Tests sharing a fixture reuse its annotation
and receive its whole-body comparison result. These are 205 named coverage entries,
not 205 independent field-specific comparisons. There are 70 fixtures and 74 run
configurations; symbolic deletion disables one fixture/run/test explicitly.

Non-default buffer sizes use `vepyr annotate --buffer-size N` (vepyr PR #169).
A release or commit without that option fails those runs visibly; the runner never
ignores a requested setting. Selecting an older version may expose other genuine
annotation differences from the stored VEP oracle.

Runner unit tests are separate and never execute as part of `./run_tests`:

```bash
uv run --frozen pytest tools/
```

Synthetic tooling fixtures live under `tools/fixtures/`. The annotated VCF data
suite lives under `tests/data/`.

Exit codes: `0` no failed data tests (explicit skips are counted separately),
`2` invalid arguments or fixture metadata, `3` cache revision mismatch,
`4` missing files or download failure, `5` cache verification failure,
`6` vepyr installation/execution error, `8` an output-body mismatch. An execution
error takes precedence over a mismatch. No oracle is generated or changed by this command.

## tools/check_ledger (assertion ledger coverage)

`tools/check_ledger` checks that the assertion ledger CSV (`ledger/assertions.csv`,
schema of #109; committed by #109: 1965 assertions of 49 files, one row each) has
exactly one row per assertion of the 49 upstream `t/*.t` files of Ensembl VEP at
the upstream tag pinned in `tools/check_ledger` (`DEFAULT_REF`, #110), and nothing
else. The ledger is its own axis: it stays at that tag (VEP 116.0) while the
data-test oracles use the VEP 116.2 pin of `tools/vep_pin.toml` (#239).

```bash
tools/check_ledger --csv ledger/assertions.csv               # schema + coverage, clones upstream
tools/check_ledger --csv ledger/assertions.csv --upstream UP  # offline, an existing checkout
tools/check_ledger --upstream UP --list                       # vep_file<TAB>n<TAB>perl_line<TAB>perl_kind
tools/check_ledger --upstream UP --sweep                      # names the enumerator did not count
tools/check_ledger --upstream UP --sweep --glob 't/*.pm'      # the 8 support modules
```

Without `--upstream` it makes a partial sparse clone of the tag into a temp dir
(`t/*.t`, `t/*.pm` and `modules/Bio/EnsEMBL/VEP/Config.pm`, about 1 MB); with
`--upstream DIR` it uses that checkout. Either way `git rev-parse HEAD` must be
`PINNED_COMMIT` of `tools/check_ledger` and `git status --porcelain` empty;
`--ref` only picks the tag to clone, the pin does not move. The files the glob
selects on disk must also equal those in `git ls-tree -r HEAD` at the pin: a clean,
pinned but sparse checkout that omits or adds a file exits 2 (`files matching ...
differ from the pinned tree`), so a missing file cannot pass as covered. `DIR` must
be the top level of the work tree (`git rev-parse --show-prefix` empty): a
subdirectory such as `ensembl-vep/t` exits 2 (`not the top level of the work tree`),
and so does a glob that selects no file of the pinned tree (`the pinned tree has no
file matching ...`), so an empty enumeration cannot pass as covered.

The default mode checks the schema (the 13 columns of #109, optionally followed by
`data_test_verdict`; field counts, enums, integer `n`/`perl_line`, conditional
columns, `issue` empty or `https://`, no newline in a field, rows sorted by
`vep_file` bytes then `n`, unique `(vep_file, n)`, bytes equal to their canonical
RFC 4180 form with LF and no BOM), then compares the CSV keys with the upstream
enumeration both ways: `missing row`, `orphan row`, `perl_line` and `perl_kind`
mismatches. The last stdout line is `rows R, files F, missing M, orphan O`.
`--sweep` prints ``file:line: uncounted `name`: text`` for every documented Test::*
function name the enumerator did not count and no fixed rule explains, and a tally
of the explained ones on stderr; `--sweep-dir DIR` does the same on a plain
directory without the pin check (for fixtures; its summary says `unpinned`).

Exit codes: `0` ok, `1` violations (one `file:n: reason` line each), `2` cannot
measure (CSV missing, unreadable or not UTF-8, a CSV field over 131072 bytes (the
Python `csv` field size limit; fails closed), upstream unavailable, another commit,
dirty tree, file set differing from the pinned tree, no `git`). `.github/workflows/ledger-check.yml` runs the unit tests, the
two sweeps and, once `ledger/assertions.csv` exists, the CSV check (Actions is
disabled, see `AGENTS.md`). Tests: `tools/test_check_ledger.py` (offline, against a
synthetic upstream repository).

## tools/check_vep_version (one VEP software pin)

Every data-test oracle is produced by **VEP software 116.2** against **VEP cache
116** (VEP point releases reuse the release-116 cache; there is no 116.2 cache).
The pin is defined once, in `tools/vep_pin.toml` (`[vep]` `image_tag`,
`image_digest`, `upstream_tag`, `upstream_commit`, `cache_version`); `./bless`,
`tools/check_campaign.py` and the Python fixture-loader tests read it, and no
other code spells the digest or the commit (#239).

```bash
tools/check_vep_version          # exit 0 consistent, 1 a violation, 2 pin/allow-list/git unusable
```

It requires every `tests/data/*/test.toml` to record the pinned `[vep] image`
digest and every `[[tests]] vep_test` at the pinned release tag, with no VEP 116.0 literal in the file, and
`git grep`s the rest of the repo for VEP 116.0 literals (the ledger axis,
`tests/INDEX.csv`, `docs/porting/**` and the checker's own two files are skipped).
All committed data fixtures use the pinned VEP image and merged cache;
the legacy image allowlist has been removed. The `test-index` workflow runs the check and its unit tests
(`tools/test_check_vep_version.py`).

## ./check_env (local prerequisites)

`./check_env` (issue #170) answers "can the repo tools run on this machine" with
one `PASS`/`FAIL`/`SKIP <name>: <detail>` line per check and a summary line:

```bash
./check_env [--vepyr-cache-root DIR] [--vep-cache-dir DIR] [--vep-fasta FILE] [--docker-timeout SECONDS]
```

Checks, each reusing the code of the tool that depends on it: `UV_PROJECT_ENVIRONMENT`
(set, absolute, outside every git checkout), `tool uv` / `tool git` (on
`PATH`), `bcftools pin` (`tools/normalize_input`'s own version check; the pin has no
second copy), `docker daemon` (`./bless`'s probe with a wall-clock limit, default 30 s),
and with their flags `vepyr cache` (`./run_tests`'s precheck: `PROVENANCE.json`, pinned
revisions, pinned FASTA name, plus a `116_GRCh38_<flavour>` dataset directory for each
flavour recorded in `PROVENANCE.json`), `vep cache` and `vep fasta` (`./bless`'s cache and FASTA
checks, plus `<fasta>.fai`); without a flag that check prints `SKIP`. Exit codes: 0
every check passed (`SKIP` does not fail), 1 a check failed, 2
`UV_PROJECT_ENVIRONMENT` unset, relative or inside a checkout (or bad usage), 3
unexpected. It never writes; it runs with `uv run --no-project`, so it creates no
environment before checking `UV_PROJECT_ENVIRONMENT`. The skill helper `dt env` calls it.

## ./bless

`./bless` makes and checks the oracle of a data-test directory `tests/data/<name>/`:
`expected_output.vcf`, the real output of native VEP 116.2 on the directory's
normalised `input.vcf` (made by `tools/normalize_input`, #85). It runs Ensembl's
official image `ensemblorg/ensembl-vep:release_116.2`, with the same fixed command plus
the flags listed in `[vep] extra_flags`:

```
vep --offline --cache --dir_cache <CACHE> --species homo_sapiens --cache_version 116 \
    --assembly GRCh38 --fasta <FASTA> --everything --vcf --input_file input.vcf \
    --output_file expected_output.vcf --force_overwrite
```

That is the one data-test mode, `--everything`; see
[One mode: --everything](#one-mode---everything). A bless and `--check --reproduce`
exit 1 with `unsupported mode` when `[vepyr]` does not match it.

Three modes:

| Command | Needs | Does |
|---------|-------|------|
| `./bless CACHE FASTA <dir>` | Docker, cache, FASTA | Runs VEP, writes `expected_output.vcf`, fills `[vep]` and `[compare] body_md5` in `test.toml` |
| `./bless --check <dir>` | only the repo | Recomputes the md5 of the body (lines not starting with `#`) of `expected_output.vcf` on disk and compares it with `[compare] body_md5`. No Docker, no cache, changes nothing |
| `./bless --check --reproduce CACHE FASTA <dir>` | Docker, cache, FASTA | First runs the drift check of `--check` (stops with exit 1 before any Docker call if the file drifted), then re-runs the image recorded in `[vep] image` with the fixed command plus `[vep] extra_flags` into a temp directory and compares that fresh body md5 with `[compare] body_md5`. Changes nothing |

`--check` is the cheap integrity check anyone can run when reviewing a PR: it
catches an oracle that was hand-edited or corrupted after it was blessed.
`--check --reproduce` is the expensive audit: it runs the same drift check first,
before any Docker call, and then proves the file is still derivable from the
pinned image, cache and input, not just unedited. It is opt-in.

**Extra VEP flags.** The extra flags of a test are data: the list `[vep] extra_flags`
in `test.toml` (absent means none). `--vep-flag=FLAG` (repeatable) fills it at the
first bless; each flag is appended after the fixed command in the order given, the
bless writes the list as `[vep] extra_flags` and generates `[vep] command` from it
(an audit record, never parsed back):

```bash
./bless --vep-cache-dir ~/vep-cache --vep-fasta ~/GRCh38.fa --vep-flag=--check_existing tests/data/NAME
```

- Use the `=` form: `--vep-flag --check_existing` is read by argparse as two options
  and exits 2.
- Only flags in `ALLOWED_VEP_FLAGS` (`tools/bless/vep.py`) are accepted, matched
  exactly on the whole token; today these are `--check_existing` and `--merged`. Aliases (`--fa`),
  abbreviations (`--input_f`), single-dash tokens, `--name=value`, unknown flags and
  a flag given twice exit 1 with `bless: --vep-flag: FLAG is not allowed; allowed: ...`.
  It is an allowlist because VEP's Getopt::Long accepts aliases and abbreviations,
  so no denylist can be complete.
- Policy: adding a flag is one line in `ALLOWED_VEP_FLAGS` plus a data-test that
  needs it. Boolean flags only; a flag with a value needs its own design.
- A re-bless without `--vep-flag=` uses the recorded `extra_flags`; a typed list
  equal to it is fine; a different one exits 1 (edit `extra_flags` in `test.toml`
  to change the flags). An empty list is not written, so a test without extra
  flags has no `extra_flags` key.
- `--check` (with or without `--reproduce`) refuses `--vep-flag` (exit 1): a check
  replays only what is recorded. `--check --reproduce` reads `[vep] extra_flags`
  (an array of strings, else exit 1), passes it through the same allowlist (a flag
  removed from `ALLOWED_VEP_FLAGS` stops replaying, exit 1) and hands it to
  `docker run` in order. It also requires `[vep] command` to be exactly the
  canonical command generated from the list (`vep.vep_command`): a command that
  differs, even only in quoting or whitespace, exits 1 naming the file and both
  strings.
- The Python loader (`tools/run_tests/fixtures.py`) accepts `extra_flags` as an optional
  array of strings and checks only its type; the allowlist is enforced by
  `./bless`.

**Cache and FASTA.** A bless and `--check --reproduce` need exactly one flag from each
pair. There is no default path and no environment variable; neither or both flags of
a pair exits 1 with a message naming the pair.

| Flag | Meaning |
|------|---------|
| `--vep-cache-dir PATH` | An existing VEP cache root (the directory that holds `homo_sapiens/116_GRCh38/`). It must be complete: `info.txt` and the directories `1`–`22`, `X`, `Y`, `MT`. An incomplete cache exits 1 naming what is missing; nothing is fetched or written into it |
| `--download-vep-cache-to-dir PATH` | Fetches `homo_sapiens_vep_116_GRCh38.tar.gz` (27.6 GB) from `https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/`, checks it against Ensembl's `CHECKSUMS` value pinned in `tools/bless/ensembl.py`, unpacks it into `PATH`, then uses it, all in the same run |
| `--vep-fasta PATH` | An existing uncompressed GRCh38 FASTA. A missing, unreadable or gzipped file exits 1. A `.fai` index is written next to it when absent |
| `--download-vep-fasta-to PATH` | Fetches `Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz` from `https://ftp.ensembl.org/pub/release-116/fasta/homo_sapiens/dna/`, checks it the same way, decompresses it to `PATH`, then uses it |

```bash
# an existing cache and FASTA (e.g. on vepyr-tests-01, or a previous download)
./bless --vep-cache-dir ~/vep-cache --vep-fasta ~/GRCh38.fa tests/data/NAME
# fetch both first, in the same run
./bless --download-vep-cache-to-dir ~/vep-cache --download-vep-fasta-to ~/GRCh38.fa tests/data/NAME
# mixed: existing cache, fetched FASTA
./bless --vep-cache-dir ~/vep-cache --download-vep-fasta-to ~/GRCh38.fa tests/data/NAME
```

Downloads run as parallel HTTP range requests (Ensembl's FTP is slow per
connection) and resume: re-running the same command after an interruption keeps the
chunks already fetched. A path filled by a `--download-...` flag is ordinary local
state afterwards; pass it to `--vep-cache-dir`/`--vep-fasta` next time and nothing is
downloaded. `bless` does not remember paths; the flags are the only state.

**`--dry-run`** prints the planned steps (`# fetch ...` for each download, the copy of
`input.vcf` into a temp directory) and the exact `docker run ... vep ...` command,
then exits 0 without running anything. For a bless it shows the tag
`ensemblorg/ensembl-vep:release_116.2`; the real run resolves it to a digest first.

**What a bless records** in `test.toml`:

```toml
[vep]
image = "ensemblorg/ensembl-vep@sha256:..."   # the digest that ran, never the tag
command = "vep --offline --cache --dir_cache /opt/vep/.vep ..."  # generated from extra_flags; paths inside the container
date = "2026-09-23"
cache_source = "https://huggingface.co/datasets/biodatageeks/vepyr_116_GRCh38_merged/tree/5b83dd8d249106c6cc3f1c04c522b4bec716cc97"
vep_cache = "https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_merged_vep_116_GRCh38.tar.gz"
vep_cache_checksum = "unverified"
fasta_source = "https://ftp.ensembl.org/pub/release-116/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"
fasta_checksum = "sha256:... sum:22450 861294"
extra_flags = ["--merged"]  # the source of truth; key order is not significant
[compare]
body_md5 = "..."
```

`cache_source` is the vepyr dataset URL at the revision in `PINS.toml`.
`vep_cache` and `fasta_source` are the native VEP cache and FASTA download URLs.
`vep_cache_checksum` and `fasta_checksum` come from the corresponding
`.bless-source.toml` download receipts. Without a valid receipt, `bless` records
the declared Ensembl download URL and keeps the checksum `unverified`. These
URLs identify the intended data; they do not establish that a local file was
downloaded from that URL. No machine-local paths are stored in source fields.

**Create a merged fixture** using Docker, an existing native merged cache
under `<VEP_CACHE>/homo_sapiens_merged/116_GRCh38`, and the GRCh38 FASTA.
After normalization, complete `test.toml` using the merged schema below:

```bash
tools/normalize_input raw.vcf.gz tests/data/my_test        # writes input.vcf + [input]
# Complete test.toml, including flavour = "merged" and extra_flags = ["--merged"].
./bless --vep-cache-dir "$VEP_CACHE" --vep-fasta "$VEP_FASTA" tests/data/my_test
./bless --check tests/data/my_test                           # exit 0
./bless --check --reproduce --vep-cache-dir "$VEP_CACHE" --vep-fasta "$VEP_FASTA" tests/data/my_test
```

The native cache downloader currently downloads Ensembl-only data; it does not
provision the merged cache required by the committed suite.

**Work directory.** Each VEP run copies `input.vcf` into its own `bless-*`
directory created under `--docker-work-dir PATH`, which is bind-mounted into the
container and removed afterwards. Without the flag it is `<repo-root>/.bless/`
(located from the script, not the current directory; listed in `.gitignore`),
mirroring `./run_tests`'s `<repo-root>/.run_tests/`. There is no environment
fallback (`TMPDIR` is not used). If the repo checkout itself is not in a
Docker-Desktop-shared location, pass `--docker-work-dir` at a path that is, just as
`--vep-cache-dir` and `--vep-fasta` must be.

**Docker Desktop (macOS).** The cache, the FASTA's directory and the work
directory are bind-mounted into the container, so they must be inside a directory
listed under Settings > Resources > File sharing. Before any download, `bless`
probes each path from inside a container and exits 1 naming the first one Docker
cannot see, so an unshared `--download-vep-cache-to-dir` fails in seconds, not
after a 27 GB fetch.

`bless` refuses a directory whose `test.toml` `[input]` table does not carry the
`tools/normalize_input` command. It does not read issue bodies and never logs into
another machine: to use `vepyr-tests-01`'s cache, log in there and run the same
command with its local paths.

Exit codes: `0` success (for `--check`, the hash matches), `1` any failure, always
with one `bless: ...` line on stderr naming the problem, `2` usage (unknown flag,
no `<test-dir>`).

## One mode: --everything

Every data-test runs in exactly one mode: VEP with `--everything` and a reference
FASTA on one side, vepyr with the matching `[vepyr]` values on the other. That is
the configuration the [vepyr CLI docs](https://biodatageeks.org/vepyr/cli/)
describe as validated against Ensembl VEP. A run without `--everything` is
unsupported and disabled: `./bless` refuses it, and the Python loader fails with
`[<name>] unsupported mode` on a `[vepyr]` (or `[[vepyr_run]]`) value that differs
from the table below, or on a `[vep] command` that lacks one of its VEP flags (an
oracle made by the old command).

The mapping has one source of truth, `tools/vep_flags.toml`, read by `./bless`
(`tools/bless/vep.py`) and by `tools/run_tests/fixtures.py`.
`uv run --frozen pytest tools/test_bless.py -k everything_mode` fails when that
file, `VEP_ARGV` and this table disagree.

| VEP flag | `[vepyr]` in `test.toml` | Why |
|---|---|---|
| `--everything` | `everything = true` | all annotation features, the full `--everything` CSQ layout |
| `--fasta` | `reference_fasta = true` | reference FASTA, required by `--everything`; vepyr reads `$VEPYR_CACHE_ROOT`'s GRCh38 FASTA |
| `--vcf` | `preserve_record_layout = true` | VCF output: VEP copies each input line and only appends CSQ to INFO |

`[vepyr]` has no `fields` key: vepyr emits its full `--everything` CSQ layout (80
fields in VEP 116.2, regulatory and motif fields included), as VEP does, and the
loader rejects `fields` as an unknown key. The other VEP flags of the fixed command
(`--offline`, `--cache`, `--dir_cache`, `--species`, `--cache_version`,
`--assembly`, input/output names) select the cache and files, not annotation, and
have no `[vepyr]` counterpart; `flavour` and `required_contigs` pick vepyr's cache.
Every committed data fixture uses `flavour = "merged"` and records
`extra_flags = ["--merged"]` in `[vep]`; the recorded VEP command and every
`[[vepyr_run]]` must use the same cache flavour. The runner requires merged fixtures, including external campaign fixtures. For merged oracles, pass `--vep-cache-dir` pointing to the parent
of `homo_sapiens_merged/116_GRCh38`; merged-cache downloads through `./bless`
are not implemented. The runner fetches only the pinned merged dataset; unused Ensembl or RefSeq pins cannot block the suite.
Before each run the runner checks that every cache entity vepyr reads in
`--everything` mode (all seven; `motif` and `regulatory` excepted on `chrMT`) has a
shard for each `required_contigs` entry.

Extra flags stay as described under [./bless](#bless): the allowlist
`ALLOWED_VEP_FLAGS` accepts `--check_existing` (#18) and `--merged`, and `[vep] extra_flags` (#108)
remains the mechanism that records them. Neither is part of the vepyr CLI docs;
they are appended to the `--everything` command, never replace it.

## Data fixtures and validation tools

`tests/INDEX.csv` lists one row per named test. The `dir` column identifies its
shared fixture; `id` and `description` identify the test inside `[[tests]]`.
Other columns are `vep_test`, `cache_source`, `vep_cache`, `fasta_source`,
`required_contigs` (`;`-joined), `vepyr_runs`, `body_md5`, and `skip_reason`
(empty for enabled fixtures).
It is generated by `tools/build_test_index` and never edited by hand:
after adding or changing a test, run `tools/build_test_index` and commit the index
with the change (the file stays tracked; `.gitattributes` marks it
`linguist-generated`, so GitHub collapses it in diffs; a merge conflict in it is
resolved by regeneration, never by hand: the recipe and the merge order are in
`AGENTS.md`, "`tests/INDEX.csv` in parallel PRs", #151; `dt verify` runs the check
as its `build_test_index` step). `tools/build_test_index --check` changes nothing
and exits 0 if the committed file is current, 1 if it is stale, 2 if a `test.toml`
is missing or lacks a key a column needs; CI (`test-index.yml`) runs it on every PR
and push to `master`.

`tools/normalize_input <raw> <test-dir>` writes a data-test's `input.vcf` with
the one fixed `bcftools norm -m -both` command (issue #85) and requires exactly
**bcftools 1.23 on htslib 1.23.1** — the toolchain the oracles were generated
with — refusing any other version before it touches the test directory. VEP and vepyr
always read that same normalised `input.vcf`; a data-test directory does not
store the raw pre-normalisation file (rationale: issue #90).

`./check_normalised_input [DATA_DIR]` (issue #89, default `tests/data`) re-runs
`tools/normalize_input` on every committed `input.vcf` in a temporary directory
and compares `input.vcf` and `test.toml` byte for byte with the committed files.
It prints `OK <dir>` or `MISMATCH <dir>` (plus a unified diff) per test and exits
0 only if every test matches and at least one was found. `DATA_DIR` may also be
one data-test directory (it holds a `test.toml`), which checks only that test
(#166). It never writes under
`DATA_DIR`, and needs the same pinned bcftools 1.23 / htslib 1.23.1. It checks
that `input.vcf` is already normalised (a fixed point of `normalize_input`) and
that `[input]` matches what the script writes; it does not verify the raw source
file (raw inputs are not committed, #90; raw provenance: #111). CI runs it in the
`input-normalised-check` workflow (`.github/workflows/input-normalised-check.yml`).

`./check_test_dir [DIR]` (issue #164) checks the structure of data-test
directories. `DIR` is a data root (default `tests/data`; every immediate
subdirectory holding a `test.toml` is checked) or one data-test directory. Five
checks run on every directory, all of them every time: `files` (exactly
`input.vcf`, `expected_output.vcf` and `test.toml`, each non-empty, nothing
else), `input-records` (`input.vcf` has at least one record), `order` (POS
ascends within each contig and each contig is one contiguous block),
`oracle-meta` (exactly one `##VEP=` line in the oracle and `[vep] image` pinned as
`ensemblorg/ensembl-vep@sha256:<64 hex>`) and `one-to-one` (#193: the oracle
body is the input's records minus those whose every ALT allele is `.`, which VEP
116 skips without `--allow_non_variant`, in input order, compared line by line
on columns 1-5 verbatim; a lost, extra, reordered or substituted line fails, and
so does an input whose every record has ALT `.`, since nothing would be compared;
there is no option or `test.toml` key to skip it). It prints
`OK <dir>`, or one `FAIL <dir> <check>: <detail>` line per failing check, and
exits 0 only if every directory is OK and at least one was found; 1 on any
failure, no test found, or `DIR` not a directory; 2 on bad usage. It never
writes, needs no VEP, cache or bcftools, and reads records with the shared
`tools/vcf_records.py`. It does not check the `test.toml` schema (the loader),
the body md5 (`./bless --check`) or REF against the FASTA. The Python loader also runs these structural checks before annotation. CI workflows remain disabled.

`tools/fixture_match --input PATH --fixture SRC --records N [--rust-const NAME]
[--by-pos] [--negative-control]` (issue #171) checks that the first `N` records
of an input equal an upstream fixture's (CHROM, POS, ID, REF, ALT). `PATH` is a
VCF (plain or gzip, detected by magic bytes) or a data-test directory (its
`input.vcf`); `SRC` is a local path or an `http(s)://` URL (there is no
`git:<repo>:<rev>:<path>` form). `--rust-const NAME` reads the fixture as Rust
source and takes `const NAME: &str = "...";` (line continuations and the
escapes `\n`, `\t`, `\\`, `\"`; raw strings are rejected). Records are compared
in order; with `--by-pos` each input record is compared with the fixture record
at the same CHROM:POS, so the fixture may hold other rows. It prints one
`PASS|FAIL fixture-match first N: <detail>` line; `--negative-control` also
appends `A` to the first input record's ALT in memory, expects a mismatch
(`PASS|FAIL fixture-match-negative: ...`) and prints a `summary` line. Exit 0
match, 1 mismatch (including fewer than `N` records on either side), 2 usage or
unreadable/invalid input, 3 network error or anything unexpected. Records are
read with the shared `tools/vcf_records.py`. `dt fixture-match` of the
data-test skill runs it with `--negative-control`.

`tools/workspace_guard` (issue #163) holds the workspace safety rules for agents
working in clones; it never writes or deletes anything and prints one
`OK <what>` or `REFUSED <what>: <reason>` line. `write-target DIR --protect PATH
[--protect PATH ...] [--expect-checkout PATH]` allows a write only if `DIR` is
absolute and a direct child of `<toplevel>/tests/data` of the git checkout
containing it (compared after `realpath`), the cwd is in that checkout, it is
the `--expect-checkout` checkout if given, and it is none of the `--protect`
paths, compared by inode (case-folded fallback, so symlink and APFS case
variants are caught); `DT_ALLOW_MAIN=1` is the only override. At least one
`--protect` is required, and a relative, empty or missing one is exit 2, never
resolved against the cwd. `outside-checkouts PATH...` refuses a path whose
nearest existing ancestor is inside any git checkout or worktree (e.g.
`"$UV_PROJECT_ENVIRONMENT"` or a scratch root). `base [--ref origin/master]`
checks that `HEAD` contains the ref; `upstream --not REF` refuses a current
branch that tracks `REF` (no upstream or a detached `HEAD` is fine). Exit 0
allowed/ok, 1 refused or check failed, 2 usage, unusable input or a git failure
(e.g. a missing ref). The data-test skill's `dt` runs it for `raw2input`,
`bless`, `verify` (scratch root) and `env` (repo, scratch root, base,
upstream), supplying the policy (`main_checkout`, `scratch_root`) from its
config; `UV_PROJECT_ENVIRONMENT` is checked by `./check_env`.

### Caveats

The shell entry point and installation locking require a POSIX environment
(Linux, macOS, or WSL). The full reference FASTA is needed for HGVS annotation.
The downloaded contig subset must cover all entities needed by the input:
`motif` and `regulatory` have no `chrMT` shards; a chrMT-only selection is refused.

## Porting method

A data-test is a **directory**, `tests/data/<name>/`, compared against the real
output of native Ensembl VEP 116. The Python runner
walks the directories and invokes the installed vepyr CLI for each configuration.

```
tests/data/<name>/
  input.vcf             # normalised input: tools/normalize_input (#85)
  expected_output.vcf   # real VEP 116.2 output on input.vcf: ./bless (#32)
  test.toml             # provenance, how vepyr runs, the body md5
```

**Making one.** Pick a candidate, write its raw VCF, then:

```bash
tools/normalize_input raw.vcf.gz tests/data/<name>   # input.vcf + [input]
./bless --vep-cache-dir ~/vep116 --vep-fasta ~/GRCh38.fa tests/data/<name>   # oracle + [vep] + [compare]
# then fill name, description, [[tests]] and [vepyr] by hand
VEPYR_CACHE_ROOT=/mnt/hf-cache ./run_tests 0.9.0
```

VEP and vepyr read the same `input.vcf`, byte for byte. Candidates come from the
assertion ledger `ledger/assertions.csv`; each named test links directly to the
corresponding upstream assertion using its release tag.

**One fixture, several tests.** Each directory stores one distinct input,
configuration and expected output. All committed fixtures use `[[tests]]`,
including fixtures with only one test. Each test has a globally unique `id`, a
description and one `vep_test` URL. One test id equals the fixture directory name.
The top-level description describes the shared fixture.

A **run** executes a fixture with one configuration. Without `[[vepyr_run]]`,
the runner uses `[vepyr]` once. With overrides, it executes once per override;
there is no additional base run. The 70 fixtures configure 74 runs because
`runner_buffer_size_invariance` supplies five buffer sizes. Named `[[tests]]`
entries describe coverage and do not create additional runs.

```toml
name = "example_fixture"
description = "Fixture covering two tests."

[input]
command = "bcftools norm -m -both -o <out.vcf> <in.vcf.gz>"
bcftools_version = "bcftools 1.23"

[vepyr]
flavour = "merged"
required_contigs = ["chr21"]
everything = true
preserve_record_layout = true
reference_fasta = true

[vep]
# Written by ./bless; source URLs and checksum meanings are documented above.
image = "ensemblorg/ensembl-vep@sha256:..."
command = "vep ..."
date = "2026-10-09"
cache_source = "https://huggingface.co/datasets/biodatageeks/vepyr_116_GRCh38_merged/tree/<revision>"
vep_cache = "https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/homo_sapiens_merged_vep_116_GRCh38.tar.gz"
vep_cache_checksum = "unverified"
fasta_source = "https://ftp.ensembl.org/pub/release-116/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"
fasta_checksum = "unverified"
extra_flags = ["--merged"]

[compare]
body_md5 = "..."

[[tests]]
id = "example_fixture"
description = "Report the expected transcript consequence."
vep_test = "https://github.com/Ensembl/ensembl-vep/blob/release/116.2/t/Runner.t#L244-L292"

[[tests]]
id = "example_second_test"
description = "Report the expected transcript identifier."
vep_test = "https://github.com/Ensembl/ensembl-vep/blob/release/116.2/t/Runner.t#L244-L292"
```

The loader rejects unknown keys and requires a tagged Ensembl VEP `.t` URL.
`vep_test_pinned`, `vep_subject`, `ledger`, and `issue` are no longer fixture
metadata. The software commit and Docker digest remain in `tools/vep_pin.toml`.
All committed source links use that pin's release tag. The loader also
accepts `[origin]` containing only `vep_test` for external single-test fixtures;
it cannot be combined with `[[tests]]`. The old `[[property]]` spelling is rejected.

Optional runtime overrides remain `[[vepyr_run]]`, with keys from `[vepyr]`.
Optional `[vep] extra_flags` is governed by the blessing allowlist. A test may
carry `focus` metadata with kinds `csq`, `csq_values`, `column`, `info` or
`record_count`. This change does not add separate field evaluation to the runner:
each fixture still executes once per run configuration and compares the entire
output body. The named tests explain the coverage of that comparison.

To disable annotation for an unsupported feature, set a top-level reason,
before any TOML table:

```toml
skip_reason = "Symbolic deletions (<DEL> with END) are not supported by vepyr yet."
```

The reason must be a non-empty string. The runner prints `SKIP <test-id>: <reason>`
for every named test in the fixture, separately from passes. All its runs are skipped;
the fixture stays in `tests/INDEX.csv`. Schema, input structure and oracle integrity
are validated before skipping. Normalization checks also include skipped fixtures.
Only `sv_deletion_end_feature_truncation` is currently skipped; literal sequence
deletions remain enabled.

```bash
tools/check_unique_dirs tests/data
tools/merge_duplicate_dirs --index tests/INDEX.csv tests/data
```

The duplicate key is input body, oracle body, `[vepyr]`, `[[vepyr_run]]`, and
`[vep] command`. The idempotent merger keeps the first directory in sort order,
moves every test into its `[[tests]]` list and preserves retained input/oracle
bytes and all test ids. Add a test to the matching fixture instead of duplicating
the directory. The merger refuses fixtures with different `skip_reason` values
(including a skipped fixture paired with an enabled one).
The committed suite has **70 fixtures and 205 named tests**, with
no duplicate comparisons. The remaining 15 Ensembl-only fixtures were re-blessed
with native VEP 116.2 and the merged cache, using byte-identical original inputs.
Four additional pairs became identical comparisons and were grouped, preserving
every input body and all named tests. Equivalent contig headers can differ between
grouped inputs. The five buffer-size configurations are retained. One fixture, one run
and one named test are explicitly skipped for unsupported symbolic deletion.
The [migration audit](docs/porting/merged-fixture-migration/audit.json) records
old/new oracle hashes, retained test ids and property checks for every re-bless.

**What the runner checks**, per directory:

1. *Self-check:* `[compare] body_md5` equals the md5 of the body of
   `expected_output.vcf` (body = every line not starting with `#`), otherwise
   `[<name>] oracle edited`.
2. A fixture with `skip_reason` is reported as skipped. Otherwise, vepyr
   annotates `input.vcf` once per run (one run from `[vepyr]`, or one per
   `[[vepyr_run]]` entry, each printed as `run <n>/<N>: <overrides>`).
3. The md5 of vepyr's body must equal `body_md5`. Otherwise
   `[<name>] body md5 mismatch`, `expected <md5>, got <md5>`, and the first
   differing record on a `VEP:` and a `vepyr:` line. CSQ group order is part of the
   body, so it is asserted too.
4. An executed comparison that mismatches always fails. An explicit skip for an
   unsupported feature disables annotation; it never turns a mismatch into a pass.

`everything`, `preserve_record_layout` and `reference_fasta` must hold the values
of [One mode: --everything](#one-mode---everything); there is no `fields` key, so
vepyr emits its full `--everything` CSQ layout (86 fields for the merged oracles).

The loader and runner's negative controls run separately with
`uv run --frozen pytest tools/test_run_tests_cli.py`. Campaign tooling selects its
new fixture through the Python `run_selection()` function; the public command runs
the complete suite.

Corpus dataset pins (`PINS.toml`) are documented in
[docs/dataset-pins.md](docs/dataset-pins.md).

### How do we know there are no more assertions in the Perl files?

**How they are enumerated.** The upstream test files are the 49 `t/*.t` of Ensembl
VEP at the tag and commit pinned in `tools/check_ledger` (`DEFAULT_REF`,
`PINNED_COMMIT`; the ledger axis, still VEP 116.0). A line is
one assertion if it matches a line-start regex over 22 function names:
`ok is isnt like unlike is_deeply cmp_ok isa_ok can_ok new_ok pass fail use_ok
require_ok` (Test::More), `throws_ok dies_ok lives_ok lives_and` (Test::Exception)
and `cmp_deeply cmp_bag cmp_set cmp_methods` (Test::Deep). All 22 are assertion
functions. The Test::Warnings functions `warning` / `warnings` are capture functions,
not assertions (they run a block and return its warnings), and are deliberately not
in the rule, so the 5 bare `warning { ... };` statements are not rows. `n` is the
ordinal of the assertion in its file, `perl_line` the line it starts on.
`tools/check_ledger --list` prints the enumeration.

**Why it is trusted.** The rule gives 1965 assertions (is 841, is_deeply 588, ok 272,
use_ok 148, throws_ok 99, like 11, cmp_deeply 3, dies_ok 2, isa_ok 1). An earlier
independent Rust lexer from the deprecated porting repository found the same
`(file, line, kind)` rows, with 0 differences in 49 files apart from its 5 `warning`
capture rows. `tools/check_ledger --sweep` searches the same files for every function
name documented by Test::More, Test::Exception, Test::Deep and Test::Warnings as a
bare word, at line start or mid-line, after removing comments, strings, regex
literals, sigiled names, every `{ word }` and every `word =>`, and reports every
occurrence the rule did not count; on the pinned files the only ones are explained
non-assertions (`use`/`no warnings`, `done_testing`, `skip`, `diag`, Test::Deep
comparators as arguments, import lists and the 6 `warning {` captures), and nothing
unexplained. The CSV is compared with the enumeration in both directions (missing
and orphan rows, `perl_line`, `perl_kind`), and the tool refuses any other commit,
so a moved tag or an edited file cannot pass silently.

**Limits.** It is not a general Perl parser and is correct for
these 49 files at this ref only: a line-start scan does not see assertions reached through helper subs or
names outside the documented lists, and a line run many times in a loop is counted
once; support files such as `t/VEPTestingConfig.pm` are not test files and are not
read for rows (none of the 8 top-level `t/*.pm` contains an assertion). The sweep
does not see three forms, none of which occurs in the pinned files: an assertion
alone in a block, `if (1) { fail }`, and `eval { pass };` (every `{ word }` is
blanked as a hash key), and a call with the `&` sigil, `&ok(1, "x");` (blanked as
a variable).
The tool ignores inherited `GIT_*` variables and compares the bytes it reads with
the pinned tree: every selected file's content is hashed in Python and must equal
its blob id in `git ls-tree -r HEAD`, so an edit that `git status` does not show
(through the checkout's own `core.worktree` or `core.fsmonitor`) is refused; a file
flagged assume-unchanged or skip-worktree in the index is refused by name. Git
runs with `--no-replace-objects`, so a `refs/replace/*` ref in the checkout cannot
swap the pinned tree that the blob ids are read from. A
`core.autocrlf=true` checkout is accepted: a file also passes when its bytes after
git's autocrlf CRLF-to-LF conversion hash to the blob id, which changes no line
number or assertion kind.

