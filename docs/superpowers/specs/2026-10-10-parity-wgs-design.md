# Whole-genome parity on the self-hosted runner (`/parity-wgs`) — design

Date: 2026-10-10
Status: approved in conversation, awaiting written-spec review
Builds on: PR #174 (four-tier CI); this branch (`ci/parity-wgs`) starts from its head `c44ea3f` and merges after it.

## Goal

An admin can run the full HG002 GRCh38 **whole-genome** md5 parity gate against
the Ensembl VEP 116.0 golden VCFs for the three cache profiles (`ensembl`,
`merged`, `refseq`), on demand, for any PR, on the biodatageeks self-hosted
runner (organization runner id 4682: 8 cores, 64 GB RAM, 1 TB disk). The result
is a commit status on the PR head, `parity/wgs`.

This is the same check maintainers run locally today with
`e2e-testing/scripts/run_comparison.py --comparison-mode md5 --bgzf`, moved to
a runner where the data is staged once.

## Non-goals

- Making `parity/wgs` a required check (left to the maintainer; one line in
  `ci/apply_branch_protection.py` later).
- Nightly or release-time WGS runs.
- Performance or timing gates.
- Release 115 and the seven pick/per-gene profiles at whole-genome scale. Today's
  goldens do not support them: VEP 116 pick references exist for chr1 and chr22
  only, and there are no 115 ensembl/refseq parquet caches. A follow-up PR
  generates the missing goldens once on the runner and extends the matrix. This
  design keeps the release and profile list data-driven (the manifest) so the
  follow-up adds data and manifest entries, not workflow logic.
- Ephemeral or VM-isolated runners (option considered and declined; see
  "Isolation" for the accepted residual risk).

## Decisions taken in conversation

| Question | Decision |
|---|---|
| Scope | Whole genome (chr1–22, 4,096,123 variants), release 116, profiles `ensembl`, `merged`, `refseq`, strict + canonical body md5 vs the VEP 116.0 golden VCFs (the only WGS goldens that exist for 116); pick profiles and 115 deferred to a follow-up PR |
| Trigger | Separate admin command `/parity-wgs` (PR comment) plus `workflow_dispatch`; `/parity` unchanged |
| Releases | 116 only in this PR (the user narrowed scope after the golden inventory) |
| Fork PRs | Allowed after the admin command; every job runs in a fresh container with data mounted read-only |
| Isolation mechanism | GitHub `container:` jobs on the existing persistent runner (not ephemeral runners) |
| Data | Staged once on the runner host; no per-run downloads |
| Harness | `run_comparison.py` md5 mode must stop writing uncompressed VEP reference slices |

## 1. Trigger and trust flow

New workflow `.github/workflows/parity-wgs.yml`, structured like `parity-tests.yml`:

```
on: issue_comment [created] (/parity-wgs)  +  workflow_dispatch(pr)
command      # GitHub-hosted · default-branch ci/ only · tested helper recognises the command
authorize    # GitHub-hosted · master-only · admin for actor AND triggering_actor · resolve PR head SHA once
             #   post pending status · reply with run link (comment route) · forks allowed
build-wheel  # GitHub-hosted · manylinux x86_64 wheel from that SHA · sccache off
profile      # matrix over the profiles listed in the WGS manifest (today: ensembl, merged, refseq) · runs-on: [self-hosted, linux, x64, vepyr-wgs]
             #   container: local image, volumes /data/vepyr:/data:ro and the per-job scratch dir
             #   permissions: contents: read · runs PR code
report       # GitHub-hosted · default-branch ci/ only · posts aggregate + per-profile statuses + summary reply
```

- **Status contexts:** `parity/wgs` (aggregate) plus `parity/wgs/<profile>` (informational).
- **Command helper:** `ci_helpers.is_parity_wgs_command(text) -> bool`. It accepts exactly `/parity-wgs`, with the same trailing-whitespace-only rule as `is_parity_command`. Tested. Any arguments (for example a release) are rejected for now; the follow-up PR adds them.
- **Profile list:** `ci_helpers.profiles()` reads the WGS manifest's `profiles` keys, as the chr22 tier reads its manifest. No profile names are hard-coded in the workflow.
- **Statuses:** `ci_helpers.statuses()` is generalised over a tier description (aggregate context, per-profile context prefix, record-name prefix, job key) rather than copied. `parity-tests.yml` keeps its current output exactly, and its existing tests stay green.
- **Concurrency:** one group for the whole workflow (`parity-wgs`) with `cancel-in-progress: false`, so a second request queues behind the running one. The non-command comment filtering from `parity-tests.yml` is reused so ordinary comments never enter the group.
- **Verdict:** confirmed as in `parity-tests.yml`. The aggregate is success only if all profile records say success **and** the matrix job result is success. A missing, malformed or duplicate record counts as `error`.
- **Security invariants, as in `parity-tests.yml`:**
  - No `${{ }}` in `run:` scripts of privileged jobs.
  - Write scopes (`statuses`, `pull-requests`, `issues`) only on `authorize` and `report`, which never check out PR code and run on GitHub-hosted runners.
  - `permissions: {}` at the top level.

## 2. Runner, image and data

### Runner
- **Label:** a dedicated `vepyr-wgs` label, so no other workflow schedules onto it.
- **Runner group:** restricted (in org settings) to the `vepyr` repository and to the workflow `.github/workflows/parity-wgs.yml`.
- **Docker:** available to the runner user.

### Image
`ci/wgs-runner/Dockerfile`:
- **Base:** `ubuntu:24.04`, pinned by digest.
- **Tools:** uv (pinned), Python 3.12, bcftools, tabix/bgzip, git, git-lfs, ca-certificates.
- **vepyr:** not baked in. Each job installs the PR's wheel into a fresh venv inside the container.
- **Build:** `ci/wgs-runner/build-image.sh` builds it on the host and tags it `vepyr-wgs:<sha256 of the Dockerfile>`. The workflow references that exact tag. The image is never pulled from a registry at job time, so a PR cannot substitute it. Because the tag is derived from the Dockerfile, changing the Dockerfile requires a rebuild on the host, and the job fails with a clear message if the tag is missing.

### Data, staged once on the host at `/data/vepyr`
Mounted read-only as `/data` (`DATA_VEPYR_DIR=/data`), in the layout `run_comparison.py` already resolves:

```
input/   HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz(+.tbi)
         Homo_sapiens.GRCh38.dna.primary_assembly.fa(+.fai)
cache/   116_GRCh38_{ensembl,merged,refseq}/   HF revisions = e2e-testing/golden/116/chr22/manifest.json caches
output/  116/HG002_annotated_wgs_everything_hgvs_{vep,merged,refseq}.vcf.gz(+.tbi)   VEP 116.0 WGS goldens
STAGED.json   path, size, sha256 of every file + the source revisions
```

About 110 GB in total (caches ~105 GB, goldens and inputs ~5 GB).

**Committed manifest** `e2e-testing/golden/wgs-manifest.json` records:
- for each profile, the golden path, sha256, size, record count and expected strict and canonical body md5 per contig;
- the HG002 input and FASTA identities;
- the cache revisions.

It is generated once from the maintainer's verified local copies (`~/workspace/data_vepyr`) by a new `e2e-testing/scripts/prepare_wgs_manifest.py`.

**`ci/wgs-runner/stage-data.sh`**, run once by a maintainer on the host and re-run when the manifest changes:
- downloads the parquet caches from Hugging Face at the manifest revisions;
- downloads the FASTA from Ensembl and verifies it against `porting-tests/PINS.toml`;
- downloads the HG002 VCF from GIAB and verifies it against the manifest;
- rsyncs the goldens from a maintainer-provided source and verifies every golden's sha256 against the manifest;
- writes `STAGED.json`.

**Per-job data check** (in the profile job, before annotating): compare `STAGED.json` with the committed manifest (revisions, golden digests, file sizes). On mismatch the job errors with "data on runner differs from the manifest — re-run ci/wgs-runner/stage-data.sh". It does not re-hash 110 GB; the read-only mount is what keeps PR code from changing the data.

### Per-job scratch
- **Location:** `/scratch/vepyr-wgs/<run_id>-<attempt>-<profile>` on the host, mounted read-write into the container as `/work`.
- **Created and deleted:** by the job; a final `if: always()` step removes it.
- **Budget:** at most about 5 GB per job, once the harness fix below is in.

## 3. Harness change: compressed reference slices

`run_comparison.py --comparison-mode md5` currently writes each contig's VEP reference slice uncompressed (`vcfio.slice_vep` → `vep_<chrom>_<suffix>.vcf`). For the whole genome that is about 29 GB per profile and the dominant disk cost. Input slices are already bgzf.

**Change:**
- `slice_vep` writes `vep_<chrom>_<suffix>.vcf.gz` (bgzf; streamed through `bgzip`, never materialising the plain body).
- The md5 path reads it through `vcfio.open_text`, which already handles bgzf.
- Cached-slice reuse (the `source.json` sidecar) keeps working.
- An existing uncompressed slice from an older run is ignored and regenerated as `.vcf.gz`.

**Proof the change is safe:**
- Existing unit tests for `slice_vep`, `md5_concordance` and the comparison CLI pass, updated for the new extension.
- A new test asserts that the strict and canonical body digests from a bgzf slice equal those from the plain slice for the same records, including CRLF-free LF bytes and the `ignore_csq_order` profiles.
- A chr22 run of all 10 profiles before and after the change gives identical md5 summaries.

This also cuts local WGS runs' scratch from about 35 GB to about 5 GB per profile.

## 4. Failure handling

- **Per-profile verdict.** Each profile job writes `result-wgs-<profile>.json` (`success` / `failure` / `error` plus the strict and canonical md5 lines and the record count).
- **Failure artifacts.** On failure the job uploads that profile's `reports/` and md5 summary, never the full VCFs.
- **Stale data or missing image** is reported as `error` with a remediation message. It is never reported as a parity failure.
- **Timeouts.** 120 min per profile job and 20 h for the workflow run. The runner executes one job at a time, so the three profiles run one after another. The expected total is about 1 h at `--workers 8` (measured and recorded during the first run).
- **Cancellation.** A cancelled run leaves the aggregate as `error`, via `report` with `if: !cancelled()`, mirroring parity-tests. Scratch is removed by the `always()` step.

## 5. Isolation: accepted residual risk

`container:` jobs run their steps in a fresh container, but two things stay on the host and are reused between jobs: the runner agent and its working directory (`_work`). A malicious fork PR, once an admin has run `/parity-wgs` on it, could leave files there that a later job reads.

Mitigations:
- The data mount is read-only.
- No secrets are present on the runner.
- The job token has `contents: read` only.
- Privileged jobs never run on this machine.
- Each profile job's first and last steps delete `_work` contents and the job scratch.
- The runner group is restricted to this one workflow.

`result.json` is written by PR code, so a malicious PR can claim success; the admin's review before `/parity-wgs` is the defence (same as parity-tests).

The admin's review before `/parity-wgs` stays the main defence. If this proves insufficient, the upgrade is ephemeral runners in containers, which keep the same workflow shape with a different runner setup.

## 6. Setup and documentation

`ci/wgs-runner/README.md` (maintainer, once, on runner 4682):
1. Add the `vepyr-wgs` label and enable Docker for the runner user.
2. Restrict the runner group to the repository and the workflow.
3. Run `build-image.sh`.
4. Run `stage-data.sh --goldens-from <host:path>`.
5. Run a smoke check: `ci/wgs-runner/check.sh`, which validates the image tag, `STAGED.json` against the manifest, and free disk.

The root README's "Continuous integration" section gets a "Whole-genome parity (`/parity-wgs`)" paragraph, and PR #174's tier table is not changed.

## 7. Testing

- **Unit:**
  - `parse_parity_wgs_command`
  - the generalised `statuses()`, with the existing parity-tests expectations unchanged plus WGS contexts and release suffixes
  - `prepare_wgs_manifest.py`, on a small synthetic layout
  - the harness bgzf-slice tests from section 3
- **Before merge (GitHub only runs dispatch and comment workflows from master):**
  - A temporary push-triggered probe workflow on this branch runs `profile` for one profile (`merged`) on 4682, end to end, through the container, the read-only mount, the data check, the harness and the result JSON.
  - A second probe run deliberately corrupts that profile's expected md5 in a throwaway branch and must fail only that job.
  - Both probes are removed before merge.
- **After merge:**
  - `/parity-wgs` on dummy PR #175 runs the three profiles.
  - A non-admin `/parity-wgs` is refused.
  - Status creator is `github-actions[bot]`.

## Open risks

- **Runtime is unmeasured.** The ~1 h estimate is from local runs, and the timeouts get adjusted after the first probe.
- **Golden/cache pairing.** The 116 WGS goldens were produced against the Ensembl 116 cache; the parquet caches pinned for chr22 (2026-10-07 revisions) must reproduce them. If a profile mismatches on first run, that is a genuine parity finding to report, not something to paper over by changing goldens.
- **Runner availability.** If 4682 is offline, `/parity-wgs` jobs queue until it is back (GitHub cancels a job after 24 h unassigned). `report` then posts `error`.
