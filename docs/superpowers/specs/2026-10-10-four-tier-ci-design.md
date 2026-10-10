# Four-tier CI/CD testing — design

Date: 2026-10-10
Status: approved in conversation, awaiting written-spec review

## Goal

Every change to vepyr is checked at four levels before it reaches `master`,
and every release is checked at all four before it reaches PyPI:

1. **Unit** — the existing lint, `cargo test`, pytest and wheel-build jobs.
2. **Porting** — the Ensembl VEP data-problem tests, moved into this repo from
   `biodatageeks/vepyr-porting-tests`.
3. **Integration** — the chr22 VEP 116 parity profiles from PR #144.
4. **nf-core** — the `vepyr/annotate` module and `vcf_annotate_vepyr`
   subworkflow tests in `nf-core-module/`.

Tiers 2–4 always test the code under review: the wheel built from the PR head
(or the release tag), never a PyPI release. For nf-core, CI builds the task
container from that wheel.

## Non-goals

- arm64 nf-core containers (x86_64 only; that is the image `main.nf` names).
- Pushing PR containers to a registry.
- Self-hosted runners.
- Importing the porting repo's history or its issue/PR process tooling.

## 1. When each tier runs

| Tier | Draft PR | Non-draft PR | Push to master | Release |
|---|---|---|---|---|
| 1. Unit | auto | auto, required | auto | auto, gates publish |
| 2. Porting | — | admin dispatch, required status | skipped | auto, gates publish |
| 3. Integration | — | admin dispatch, required status | skipped | auto, gates publish |
| 4. nf-core | — | auto, required | auto | auto, gates publish |

- `ci.yml` adds `ready_for_review` to its `pull_request` types so tier 4
  starts when a draft is marked ready. Tier 4 jobs carry
  `if: github.event_name != 'pull_request' || !github.event.pull_request.draft`.
  A skipped required job on a draft is harmless: GitHub never merges drafts.
- Tiers 2 and 3 never trigger on push to master.
- Heavy tiers consume the manylinux x86_64 wheel artifact built earlier in the
  same run; they never recompile. A release therefore tests the exact bytes it
  publishes.

### Workflow layout

```
.github/workflows/
  ci.yml                  # existing + porting-checks job (tier 1) + tier 4 jobs
  parity-tests.yml        # NEW workflow_dispatch(pr): tiers 2 + 3 for one PR
  publish_to_pypi.yml     # existing + calls tiers 2, 3, 4; publish needs them
  _tier-porting.yml       # NEW reusable (workflow_call): inputs ref, wheel-artifact
  _tier-integration.yml   # NEW reusable
  _tier-nfcore.yml        # NEW reusable
```

Each reusable workflow takes `ref` (commit SHA to check out) and
`wheel-artifact` (name of the artifact holding the manylinux x86_64 wheel),
and uploads one `result-<name>.json` per verdict-bearing job
(`{"name": ..., "conclusion": "success"|"failure"|"error", "summary": ...}`).

## 2. Porting tier

### Import

Snapshot copy (no history) of a subset of `vepyr-porting-tests` into a
top-level `porting-tests/` directory. It remains its **own uv project**
(`porting-tests/pyproject.toml`, `uv.lock`, Python >= 3.12), invoked as
`uv run --project porting-tests ...`; its dependencies never enter vepyr's
environment. Both repos are Apache-2.0.

Brought over:

- data: `tests/data/` (70 fixtures), `tests/INDEX.csv`, `PINS.toml`
- ledger: `ledger/assertions.csv`, `tools/check_ledger` and its tests
- entry points: `run_tests`, `bless`, `check_test_dir`,
  `check_normalised_input`, `check_env`
- tools: `tools/run_tests/`, `tools/bless/`, `build_test_index`,
  `check_vep_version`, `vep_pin.py`, `vep_pin.toml`, `vep_flags.toml`,
  `pins.py`, `fixture_*.py`, `vcf_records.py`, `normalize_input`,
  `check_test_dir.py`, `check_normalised_input.py`, `check_env.py`,
  `check_unique_dirs*`, `merge_duplicate_dirs*`, `workspace_guard`
- their `tools/test_*.py` and the `tools/fixtures/` subtrees those tests read
- docs needed to operate the above: `docs/dataset-pins.md`, and the README
  sections for `run_tests`, `bless` and the ledger, rewritten as
  `porting-tests/README.md`

The cut rule: keep everything the entry points above import. The import is
correct when `uv run --project porting-tests pytest porting-tests/tools`
passes and `build_test_index --check`, `check_vep_version` and
`check_ledger` pass in the new location. Anything that fails only because it
belongs to the dropped process tooling is removed with its test.

Left behind in the archived repo: `issue_check`, `issue_status`, `pr_status`,
`set_state`, `port_campaign` / `check_campaign` / `report_campaign`,
`.claude/skills`, issue templates, `AGENTS.md`, `CLAUDE.md`, `docs/porting/`.

Paths in the copied tools that assume the repo root (e.g. `tests/INDEX.csv`)
are resolved relative to `porting-tests/`.

### Runner changes

1. `./run_tests --wheel PATH` — mutually exclusive with the positional
   version/SHA. Installs that wheel into the isolated env under the cache
   root, keyed by the wheel's sha256. Exit codes unchanged.
2. `--summary-md PATH` — writes a Markdown table of every named test whose
   verdict is not `PASS`: test, fixture, verdict, first differing line.
   Mismatching output bodies stay in a documented directory for artifact
   upload.

### vepyr repo changes

- Root `pyproject.toml` gains `[tool.pytest.ini_options] testpaths = ["tests"]`
  (none exists today, so root pytest would collect `porting-tests/tools`).
- New tier 1 job `porting-checks` in `ci.yml` (no data downloads):
  `build_test_index --check`, `check_vep_version`, the normalised-input check,
  `check_ledger`, `pytest porting-tests/tools`.
- The root ruff pre-commit hook now covers `porting-tests/`; it uses the
  nearest config (the porting project's own `[tool.ruff]`). Any reformatting
  it produces lands as a separate commit.

### Heavy job (`_tier-porting.yml`)

1. Install `uv`; download the wheel artifact.
2. FASTA: restore `actions/cache` keyed on the FASTA sha256 from
   `PINS.toml`, caching only the compressed `.gz` (0.9 GB). Verify the
   download against `PINS.toml` after restore; a mismatch is treated as a
   cache miss. Decompress and index on each run.
3. Merged cache shards (chr1, chr21, chr22): downloaded fresh from Hugging
   Face at the pinned revision every run. No `actions/cache`.
4. `./run_tests --wheel <wheel> --summary-md summary.md`.
5. Append the summary to `$GITHUB_STEP_SUMMARY`; upload mismatching outputs on
   failure; upload `result-porting.json`.
6. Measured on `ubuntu-latest` (probe run 38047319640, 2026-10-10, vepyr
   0.9.2): 204 pass / 1 skipped, 17m52s wall end to end including downloads,
   peak RSS 2.1 GB, 7.7 GB of data (3.9 GB shards + 3.8 GB FASTA); the runner
   had 86 GB free, 4 cores, 15 GB RAM. No free-disk step is needed.

## 3. Integration tier

PR #144 (`download_chr22.py`, the VEP 116 chr22 goldens and manifest via Git
LFS, `run_comparison.py --output-dir`, the reviewer Docker image, and their
tests) is folded into this work: its three commits are carried onto the
`ci/four-tier-testing` branch and land in the same PR, and #144 is closed
pointing at it.

CI does not use #144's Docker image (it installs vepyr from PyPI); jobs run
natively with the wheel plus `bcftools`/`tabix` from apt.

```
profiles   # restore goldens (below); read manifest.json → matrix JSON of profiles
profile    # matrix over that list, fail-fast: false
  1. install the wheel into a venv
  2. restore goldens from actions/cache; verify md5s against manifest.json
  3. download_chr22.py --flavour <ensembl|refseq|merged>  (fresh from HF, pinned, checksummed)
  4. run_comparison.py --profile X --output-dir out/       (strict + canonical md5)
  5. step summary line (profile, expected vs actual md5s, record count);
     upload out/ on failure; upload result-integration-<profile>.json
```

- The profile list comes from the manifest, so a new golden adds a job with
  no workflow edit. Today: ensembl, refseq, merged, merged_flag_pick,
  merged_flag_pick_allele, merged_flag_pick_allele_gene, merged_per_gene,
  merged_pick_allele, merged_pick_allele_gene, merged_pick_filter.
- `download_chr22.py` gains a flavour selector if #144 lacks one, so each job
  fetches only the cache its profile uses.
- Git LFS bandwidth: goldens (178 MB) are cached in `actions/cache` keyed on
  the hash of `manifest.json`. Only on a miss does the `profiles` job run
  `git lfs pull --include 'e2e-testing/golden/116/chr22/*'` and save the
  cache. Matrix jobs check out with `lfs: false`.

## 4. nf-core tier

### PR container

`nf-core-module/dev/Dockerfile.pr`:

- base `mambaorg/micromamba` (pinned digest), installing every package in
  `modules/nf-core/vepyr/annotate/environment.yml` except `bioconda::vepyr`
  (list generated from the file at build time)
- `pip install` of the run's manylinux x86_64 wheel, with dependencies
- `procps` (Nextflow needs `ps` in task containers)

Built with `docker build -t vepyr-pr:<sha>` inside the job; never pushed. Used
through the existing `VEPYR_CONTAINER` hook in `dev/local.config` with
`VEPYR_DOCKER_PLATFORM=linux/amd64`.

### Jobs (`_tier-nfcore.yml`)

Both `needs:` the x86_64 wheel job; each builds the image, sets up pinned
Java 17, Nextflow and nf-test, and runs through `dev/nf-test-local.sh` (which
fetches the pinned upstream `untar`, `bcftools/norm`, `huggingface/download`
modules and stages fixtures).

- `nfcore-module` — `modules/nf-core/vepyr/annotate/tests/main.nf.test`
- `nfcore-subworkflow-parity` —
  `subworkflows/nf-core/vcf_annotate_vepyr/tests/main.nf.test`,
  `dev/tests/hg002_chr22.nf.test` (50,861 records, VEP 116 md5
  `f0a0a7021c498d2b4e38c9caf5959f77`),
  `dev/tests/hg002_chr22_normalize.nf.test`

### Snapshots

- Commit the module and subworkflow `main.nf.test.snap` files verbatim from
  nf-core/modules master. They come from the published test-datasets.
- The submission tests run against published test data (`VEPYR_NF_TESTDATA`
  unset); the `dev/` parity tests keep the in-repo LFS fixture
  (`tests/data/hg002_chr22`).
- Before running, CI rewrites only the vepyr version string in a working copy
  of each `.snap` to the wheel's version (read from the wheel metadata). All
  md5s stay strict; the mirrored test files are untouched.
- nf-test runs with `--ci`, so a missing snapshot fails instead of being
  created.
- `dev/nf-test-local.sh` gains whatever switch is needed to run a chosen
  subset of test files and to leave `VEPYR_NF_TESTDATA` unset.

## 5. Dispatch, statuses, protection, release

### `parity-tests.yml`

```
authorize    # no checkout · permissions: statuses: write, pull-requests: read
  - gh api repos/{repo}/collaborators/{actor}/permission → must be "admin"
  - resolve PR → head SHA; refuse closed PRs; output sha
  - post "pending" for parity/porting and parity/integration on sha (target_url = run)
build-wheel  # contents: read · checkout sha · maturin manylinux x86_64 → artifact
porting      # uses _tier-porting.yml (ref: sha) · contents: read
integration  # uses _tier-integration.yml (ref: sha) · contents: read
report       # if: always() · no checkout · statuses: write
  - download result-*.json
  - post parity/integration/<profile> (informational)
  - post parity/porting and parity/integration (required)
  - missing result file → error, never success
```

- The run tests and reports on the SHA resolved at start, not the moving PR
  ref. A push mid-run leaves the new head without a status, so it stays
  blocked; an admin re-dispatches.
- Fork PRs work: the base repo fetches `refs/pull/<n>/head`. Only `authorize`
  and `report`, which never check out PR code, hold `statuses: write`.
- Anything restored from `actions/cache` (FASTA, goldens) is re-verified
  against its pinned checksum before use, because PR code runs in the base
  repo's cache scope.
- `concurrency: parity-${{ inputs.pr }}`, `cancel-in-progress: true`.
- Every job has `timeout-minutes`.
- Invocation: Actions tab → "Parity tests" → PR number, or
  `gh workflow run parity-tests.yml -f pr=<n>`.

### Branch protection (ruleset on `master`)

Required: `lint`, `test-rust`, the five `linux-tests` entries,
`windows-tests`, `linux-arm64-tests`, `porting-checks`, `nfcore-module`,
`nfcore-subworkflow-parity`, `parity/porting`, `parity/integration` (source:
GitHub Actions). Delivered as a `gh api` script in the PR; applied by hand
only after each check has reported at least once, on the maintainer's
go-ahead.

### Release (`publish_to_pypi.yml`)

New jobs call `_tier-porting`, `_tier-integration` and `_tier-nfcore` with the
existing `linux` job's x86_64 wheel artifact at the release tag; `publish`
adds them to `needs:`. No admin step.

### Verifying the CI

- `actionlint` added to tier 1.
- Before protection is enabled: a throwaway PR that breaks one integration
  profile and one porting fixture, to confirm each fails in the right job and
  status.
- A dispatch from a non-admin account, to confirm it is refused.
- A dry run of the release path that stops before `publish`.

### Retiring `vepyr-porting-tests`

After the import merges: point its README at `vepyr/porting-tests/` and
archive it. Done by hand, on the maintainer's go-ahead.

## Open risks

- **Porting wall time (~18 min)** is the longest tier. It is acceptable for an
  admin-dispatched gate; if it becomes a bottleneck, shard the 74 run
  configurations across a matrix. Self-hosted runners are not needed.
- **Upstream snapshot drift:** the committed `.snap` files mirror
  nf-core/modules and are refreshed by hand when the upstream tests change,
  like the rest of `nf-core-module/`.
- **#144 review state:** its commits land unreviewed except by this PR's
  review; the PR description calls them out as a distinct section.
