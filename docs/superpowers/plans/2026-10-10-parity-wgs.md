# Whole-genome parity on the self-hosted runner (`/parity-wgs`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An admin comments `/parity-wgs` on a PR and the HG002 whole-genome md5 parity gate runs for profiles `ensembl`, `merged` and `refseq` (VEP 116) on self-hosted runner 4682, reporting `parity/wgs` on the PR head.

**Architecture:** A new workflow `parity-wgs.yml` reuses the `/parity` trust pattern: command, authorize, build-wheel and report run on GitHub-hosted runners, and one profile job per profile runs on the self-hosted machine. Each profile job does only trusted work on the host (checkout, artifact transfer, cleanup). It executes all PR code inside a fresh, locked-down `docker run` of a locally built image, with the staged data mounted read-only. `run_comparison.py` md5 mode is changed to write compressed reference slices so per-job scratch stays small.

**Tech Stack:** GitHub Actions (issue_comment, workflow_dispatch, self-hosted runner), Docker, Python 3.12 stdlib, uv, bcftools/tabix, Hugging Face Hub, rsync.

**Spec:** `docs/superpowers/specs/2026-10-10-parity-wgs-design.md`

## Global Constraints

- Scope: release 116, profiles `ensembl`, `merged`, `refseq`, whole genome chr1–22, strict + canonical body md5 against `output/116/HG002_annotated_wgs_everything_hgvs_{vep,merged,refseq}.vcf.gz`.
- Trigger: a PR comment that is exactly `/parity-wgs` (trailing whitespace allowed, leading not), or `workflow_dispatch` with input `pr`. Arguments are rejected.
- Statuses: aggregate `parity/wgs`, informational `parity/wgs/<profile>`. Not added to `REQUIRED` in `ci/apply_branch_protection.py`.
- Fork PRs are allowed once an admin issues the command.
- Admin check covers `github.actor` and `github.triggering_actor`. The workflow runs only from `refs/heads/master`. There is no `${{ }}` inside `run:` scripts of privileged jobs. Top-level `permissions: {}`.
- Write scopes (`statuses`, `pull-requests`, `issues`) only on `authorize` and `report`, which run on `ubuntu-latest` and never check out PR code.
- Self-hosted label set: `[self-hosted, linux, x64, vepyr-wgs]`. Image tag `vepyr-wgs:1` is a constant in the workflow and must equal `ci/wgs-runner/IMAGE`.
- Data root on the host is `/data/vepyr`, mounted read-only at `/data`, with `DATA_VEPYR_DIR=/data`. Per-job scratch is `/scratch/vepyr-wgs/<run_id>-<run_attempt>-<profile>`, mounted at `/work` and deleted at the end.
- Concurrency: group `parity-wgs`, `cancel-in-progress: false`. Profile jobs run one at a time (`max-parallel: 1`).
- Timeouts: 120 min per profile job, run-level jobs ≤ 10 min, workflow bounded by job timeouts.
- Harness: md5 mode writes VEP reference slices as bgzf `vep_<chrom>_<suffix>.vcf.gz`; digests must be byte-identical to the plain-slice digests.
- Branch `ci/parity-wgs` starts from #174's head. The PR targets `ci/four-tier-testing` until #174 merges, then is retargeted to `master` before merging. A stacked squash into a dead base branch is a known trap.
- Commits: Conventional Commits, trailer `Co-Authored-By: <model that wrote it> <noreply@anthropic.com>`.

## Review Focus

1. **Stale or missing staged data must fail as `error`, not as a parity mismatch.** A maintainer who forgot to re-stage after a manifest change expects "re-run stage-data.sh", not a red parity verdict. Owned by Task 3 (`check` tests: missing file, size change, revision change), surfaced in Task 5.
2. **A cancelled or timed-out profile job must never yield `parity/wgs` success.** Owned by Task 2 (`wgs_statuses` tests: missing record, job result `cancelled`).
3. **An old uncompressed slice from a pre-change run must not be reused.** A maintainer re-running locally with a warm work dir expects fresh `.vcf.gz` slices, not a stale `.vcf`. Owned by Task 1 (test: a pre-existing `vep_chr1_x.vcf` is ignored and a `.vcf.gz` is produced).
4. **Leftovers from an earlier job on the persistent host must not leak into the next job.** Owned by Task 5 (clean steps before checkout and in `always()`). Verified in Task 7 by a probe that plants a file and checks it is gone.
5. **`/parity-wgs` must not trigger `/parity`, and `/parity` must not trigger `/parity-wgs`.** `startsWith('/parity')` matches both. Owned by Task 2 (helper tests: `is_parity_command('/parity-wgs')` is False). Task 5 uses a `/parity-wgs`-specific filter.

---

## File Structure

```
e2e-testing/scripts/comparison/vcfio.py        # MODIFY: slice_vep writes bgzf
tests/test_comparison_vcfio.py                 # MODIFY: slice tests for .vcf.gz, digest equality
ci/ci_helpers.py                               # MODIFY: is_parity_wgs_command, tier-generic statuses, wgs-statuses CLI
tests/test_ci_helpers.py                       # MODIFY
e2e-testing/scripts/wgs_manifest.py            # NEW: prepare (manifest from local data), record-staged, check
tests/test_wgs_manifest.py                     # NEW
e2e-testing/golden/wgs-manifest.json           # NEW: generated from ~/workspace/data_vepyr
ci/wgs-runner/IMAGE                            # NEW: "vepyr-wgs:1"
ci/wgs-runner/Dockerfile                       # NEW
ci/wgs-runner/build-image.sh                   # NEW
ci/wgs-runner/stage-data.sh                    # NEW
ci/wgs-runner/check.sh                         # NEW
ci/wgs-runner/run-profile.sh                   # NEW: entrypoint executed INSIDE the container
ci/wgs-runner/README.md                        # NEW: one-time setup on runner 4682
.github/workflows/parity-wgs.yml               # NEW
tests/test_parity_wgs_workflow.py              # NEW: static checks (image tag, labels, no ${{ }} in privileged run)
README.md                                      # MODIFY: "Whole-genome parity (/parity-wgs)"
```

## Setup (before Task 1)

- [ ] Worktree `.worktrees/parity-wgs` on branch `ci/parity-wgs` already exists. It holds the spec commits on top of `c44ea3f`. Work there. `unset CONDA_PREFIX; uv sync`.
- [ ] Unstaged `Cargo.lock`/`uv.lock` drift may appear from `uv run`. Never commit it.

---

### Task 1: Harness writes compressed VEP reference slices

**Files:**
- Modify: `e2e-testing/scripts/comparison/vcfio.py` (`slice_vep`, around lines 357–411)
- Test: `tests/test_comparison_vcfio.py`

**Interfaces:**
- Produces: `vcfio.slice_vep(vep_vcf, chrom, out_dir, suffix, force=False) -> str` now returns `<out_dir>/vep_<chrom>_<suffix>.vcf.gz` (bgzf). The sidecar name `vep_<chrom>_<suffix>.source.json` is unchanged. Callers (`cli.py` md5 and field modes) already read through `vcfio.open_text`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_comparison_vcfio.py`)

```python
def test_slice_vep_writes_bgzf(indexed_multi_contig, tmp_path):
    out = vcfio.slice_vep(str(indexed_multi_contig), "chr1", str(tmp_path), "merged")
    assert out.endswith("vep_chr1_merged.vcf.gz")
    assert vcfio.is_bgzf(out)
    assert not (tmp_path / "vep_chr1_merged.vcf").exists()
    assert vcfio.count_data_lines(out) == 2


def test_slice_vep_ignores_a_stale_plain_slice(indexed_multi_contig, tmp_path):
    stale = tmp_path / "vep_chr1_merged.vcf"
    stale.write_text("##stale\n")
    out = vcfio.slice_vep(str(indexed_multi_contig), "chr1", str(tmp_path), "merged")
    assert out.endswith(".vcf.gz") and vcfio.count_data_lines(out) == 2


def test_slice_vep_linear_scan_writes_bgzf(tmp_path):
    src = tmp_path / "bare.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "22\t100\t.\tA\tT\t50\tPASS\t.\n"
    )
    out = vcfio.slice_vep(str(src), "chr22", str(tmp_path / "w"), "merged")
    assert vcfio.is_bgzf(out) and vcfio.count_data_lines(out) == 1


@pytest.mark.parametrize("mode", ["strict", "canonical"])
@pytest.mark.parametrize("ignore_csq_order", [False, True])
def test_bgzf_slice_digest_equals_plain_digest(indexed_multi_contig, tmp_path, mode, ignore_csq_order):
    import md5_concordance

    gz = vcfio.slice_vep(str(indexed_multi_contig), "chr1", str(tmp_path / "gz"), "m")
    plain = tmp_path / "plain.vcf"
    with vcfio.open_text(gz) as fh:
        plain.write_text(fh.read())
    a = md5_concordance.digest_vcf(gz, mode, ignore_csq_order)
    b = md5_concordance.digest_vcf(str(plain), mode, ignore_csq_order)
    assert a.body == b.body
```

Check the top of the test file. If `md5_concordance` is not importable there, add `sys.path.insert(0, str(<repo>/e2e-testing/scripts))` the way `tests/test_comparison_cli.py` does; copy its exact idiom. Also confirm the `Digest` field name with `grep -n "class Digest" -A8 e2e-testing/scripts/md5_concordance.py`, and use the real attribute if it is not `body`.

Update the existing slice tests that hard-code the plain path: `test_slice_vep_regenerates_when_the_reference_source_changes` keeps its sidecar assertion. Any assertion comparing against `"vep_chr1_merged.vcf"` must now use `.vcf.gz`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_comparison_vcfio.py -k "slice_vep or bgzf_slice" -v`
Expected: the new tests fail. Paths end in `.vcf`, and `is_bgzf` is False.

- [ ] **Step 3: Implement**

In `slice_vep`:
- `out_path = os.path.join(out_dir, f"vep_{chrom}_{suffix}.vcf.gz")`.
- Reuse check unchanged (`os.path.exists(out_path) and not force and previous == source`).
- **Indexed path:** stream `tabix -H` then the body for the first matching alias into `bgzip -c > out_path`. Use `subprocess.Popen(["bgzip", "-c"], stdin=PIPE, stdout=fh)`, writing the header bytes and body bytes to stdin. Count `n` as today.
- **Linear-scan path:** open `bgzip -c` the same way and write the matching text lines encoded as UTF-8.
- Write to `out_path + ".tmp"` and `os.replace` it at the end, so an interrupted run never leaves a truncated slice that the reuse check would accept.
- Do not delete a pre-existing plain `.vcf` from older runs. It is simply not used.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_comparison_vcfio.py tests/test_comparison_cli.py tests/test_verify_parity_gate.py tests/test_md5_concordance.py -q`
Expected: all pass.

- [ ] **Step 5: chr22 equality check against the real goldens (local, maintainer data)**

```bash
for p in ensembl merged refseq merged_flag_pick merged_flag_pick_allele merged_flag_pick_allele_gene merged_pick_filter merged_pick_allele merged_per_gene merged_pick_allele_gene; do
  uv run python e2e-testing/scripts/run_comparison.py --release 116 --profile "$p" --chroms 22 \
    --comparison-mode md5 --md5-mode both --bgzf --output-dir "$SCRATCH/t1-after" >/dev/null || echo "FAIL $p"
done
grep -h 'MATCH\|DIFF' "$SCRATCH"/t1-after/reports/*md5* | sort | uniq -c
```

Expected: no `FAIL` lines, and every profile reports `body MATCH` for strict and canonical, as before the change. Compare against the same loop run on `git stash`-free `HEAD~1` into `$SCRATCH/t1-before` if any doubt. Record the per-profile summary lines in the report.

- [ ] **Step 6: Commit**

```bash
git add e2e-testing/scripts/comparison/vcfio.py tests/test_comparison_vcfio.py
git commit -m "perf(e2e): write VEP reference slices as bgzf in md5 mode"
```

---

### Task 2: Command helper and tier-generic statuses

**Files:**
- Modify: `ci/ci_helpers.py`
- Test: `tests/test_ci_helpers.py`

**Interfaces:**
- Produces:
  - `ci_helpers.is_parity_wgs_command(text: str) -> bool`. CLI: `is-parity-wgs-command [--env VAR]`, exit 0/1.
  - `ci_helpers.wgs_statuses(results_dir: Path, profiles: list[str], job_result: str) -> list[tuple[str, str, str]]`. CLI: `wgs-statuses --results-dir D --profiles JSON --job RESULT`, prints TSV.
  - Result records are named `wgs/<profile>`, in files `result-wgs-<profile>.json`. Contexts are `parity/wgs/<profile>` and the aggregate `parity/wgs`.
  - Existing `statuses()` output is unchanged.

- [ ] **Step 1: Write the failing tests** (append)

```python
@pytest.mark.parametrize("text", ["/parity-wgs", "/parity-wgs\n", "/parity-wgs \r\n"])
def test_is_parity_wgs_command_accepts(text):
    assert ci_helpers.is_parity_wgs_command(text)


@pytest.mark.parametrize(
    "text", ["", "/parity", " /parity-wgs", "/parity-wgs 115", "/parity-wgsx", "`/parity-wgs`", "/parity-wgs\nplease"]
)
def test_is_parity_wgs_command_rejects(text):
    assert not ci_helpers.is_parity_wgs_command(text)


def test_parity_and_wgs_commands_do_not_overlap():
    assert not ci_helpers.is_parity_command("/parity-wgs")
    assert not ci_helpers.is_parity_wgs_command("/parity")


def test_wgs_statuses_all_green(tmp_path):
    for p in ("ensembl", "merged", "refseq"):
        write_result(tmp_path, f"wgs/{p}", "success", "md5 strict: body MATCH")
    rows = as_dict(ci_helpers.wgs_statuses(tmp_path, ["ensembl", "merged", "refseq"], "success"))
    assert rows["parity/wgs"] == ("success", "3 profiles pass")
    assert rows["parity/wgs/merged"][0] == "success"


def test_wgs_statuses_missing_record_is_error(tmp_path):
    write_result(tmp_path, "wgs/ensembl", "success")
    rows = as_dict(ci_helpers.wgs_statuses(tmp_path, ["ensembl", "merged"], "failure"))
    assert rows["parity/wgs/merged"][0] == "error"
    assert rows["parity/wgs"][0] != "success"


def test_wgs_statuses_cancelled_job_never_success(tmp_path):
    for p in ("ensembl", "merged"):
        write_result(tmp_path, f"wgs/{p}", "success")
    rows = as_dict(ci_helpers.wgs_statuses(tmp_path, ["ensembl", "merged"], "cancelled"))
    assert rows["parity/wgs"][0] == "error"


def test_wgs_statuses_empty_profiles_is_error(tmp_path):
    rows = as_dict(ci_helpers.wgs_statuses(tmp_path, [], "success"))
    assert rows["parity/wgs"][0] == "error"


def test_wgs_statuses_cli(tmp_path, capsys):
    write_result(tmp_path, "wgs/merged", "success")
    assert ci_helpers.main(["wgs-statuses", "--results-dir", str(tmp_path),
                            "--profiles", '["merged"]', "--job", "success"]) == 0
    ctxs = {line.split("\t")[0] for line in capsys.readouterr().out.strip().splitlines()}
    assert ctxs == {"parity/wgs/merged", "parity/wgs"}
```

`write_result` and `as_dict` already exist in this file (from the parity statuses tests). `write_result` derives the filename from the name with `/` replaced by `-`, so `wgs/merged` becomes `result-wgs-merged.json`. Verify that before relying on it.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_ci_helpers.py -k "wgs" -v` (prefix `env -u CONDA_PREFIX` if needed).
Expected: fails with `AttributeError`.

- [ ] **Step 3: Implement**

Refactor without changing `statuses()` output:

```python
def _tier_rows(
    records: dict[str, dict],
    duplicates: set[str],
    *,
    context: str,
    record_prefix: str,
    profiles: list[str],
    job_result: str,
) -> list[tuple[str, str, str]]:
    """Per-profile rows plus a job-confirmed aggregate for one matrix tier."""
```

- Move the per-profile loop, aggregate and `confirmed()` logic for integration out of `statuses()` into `_tier_rows`. The per-record `state()` logic (duplicates, missing, non-verdict conclusion) becomes a module-level `_record_state(records, duplicates, name)`, used by both. An empty profile list gives `(context, "error", "profile list missing (profiles job failed)")`.
- `statuses()` keeps its vep-parity row and calls `_tier_rows(..., context="parity/integration", record_prefix="integration/", profiles=profiles, job_result=job_results.get("integration", "unknown"))`.
- `wgs_statuses(results_dir, profiles, job_result)` validates profile names with `PROFILE_NAME`, loads records with `_load_records`, returns `_tier_rows(..., context="parity/wgs", record_prefix="wgs/", ...)`, and clips descriptions with `_clip`.
- Add `PARITY_WGS_COMMAND = "/parity-wgs"`. `is_parity_wgs_command(text)` uses the same body as `is_parity_command` (`text.rstrip(" \t\r\n") == PARITY_WGS_COMMAND`). Factor a private `_is_command(text, command)` used by both.
- CLI: `is-parity-wgs-command` mirrors `is-parity-command` (`--env`). `wgs-statuses` takes `--results-dir`, `--profiles` (parsed by `parse_profiles`) and `--job` (required).

- [ ] **Step 4: Run the whole helper suite**

Run: `uv run pytest tests/test_ci_helpers.py -q`
Expected: all pass, including every pre-existing `statuses` test, unchanged.

- [ ] **Step 5: Commit**

```bash
git add ci/ci_helpers.py tests/test_ci_helpers.py
git commit -m "ci: /parity-wgs command helper and tier-generic statuses"
```

---

### Task 3: WGS manifest tool and committed manifest

**Files:**
- Create: `e2e-testing/scripts/wgs_manifest.py`
- Create: `tests/test_wgs_manifest.py`
- Create: `e2e-testing/golden/wgs-manifest.json` (generated)

**Interfaces:**
- Produces a CLI, `python3 e2e-testing/scripts/wgs_manifest.py`, with three subcommands:
  - `prepare --data DIR --out e2e-testing/golden/wgs-manifest.json`. Run once, from the maintainer's verified data.
  - `record-staged --data DIR --manifest M --out DIR/STAGED.json`. Run by `stage-data.sh`.
  - `check --data DIR --manifest M`. Exit 0 if consistent. Exit 3 with one line per problem, ending with `re-run ci/wgs-runner/stage-data.sh`.
- Library functions:
  - `build_manifest(data: Path, chr22_manifest: Path, profiles: dict[str, str]) -> dict`
  - `record_staged(data: Path, manifest: dict) -> dict`
  - `check_staged(data: Path, manifest: dict, staged: dict) -> list[str]`
- Manifest schema (version 1):

```json
{
  "schema_version": 1,
  "release": "116",
  "profiles": {
    "ensembl": {"golden": "output/116/HG002_annotated_wgs_everything_hgvs_vep.vcf.gz", "sha256": "...", "size": 0},
    "merged":  {"golden": "output/116/HG002_annotated_wgs_everything_hgvs_merged.vcf.gz", "sha256": "...", "size": 0},
    "refseq":  {"golden": "output/116/HG002_annotated_wgs_everything_hgvs_refseq.vcf.gz", "sha256": "...", "size": 0}
  },
  "inputs": {"input/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz": {"sha256": "...", "size": 0}},
  "caches": {"ensembl": {"repo_id": "...", "revision": "..."}, "merged": {}, "refseq": {}}
}
```

  - Each golden's `.tbi` is listed in `inputs` with its own digest.
  - `caches` is copied from `e2e-testing/golden/116/chr22/manifest.json` (`repo_id`, `revision` per flavour), so the WGS and chr22 tiers pin the same caches.
  - The FASTA is pinned by `porting-tests/PINS.toml`, and the manifest records only its relative path: `"fasta": "input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"`.
- `STAGED.json` schema:
  - `{"manifest_sha256": "<sha256 of the manifest bytes>"`
  - `"files": {relpath: {"sha256", "size"}}` for goldens, `.tbi` files and inputs
  - `"cache_files": {relpath: size}` for every file under `cache/116_GRCh38_<flavour>/`
  - `"caches": {flavour: revision}}`
- `check_staged` reports a problem when:
  - `staged["manifest_sha256"]` differs from the current manifest (message: manifest changed since staging);
  - a manifest file is missing from `staged["files"]`, or its staged sha256 differs from the manifest;
  - a file's current size on disk differs from the staged size (truncated or replaced);
  - a `cache_files` entry is missing on disk or has changed size;
  - a staged cache revision differs from the manifest.

  It does not re-hash: the read-only mount protects contents.

- [ ] **Step 1: Write the failing tests** (`tests/test_wgs_manifest.py`)

```python
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "e2e-testing" / "scripts"))
import wgs_manifest  # noqa: E402


def make_layout(root: Path) -> None:
    (root / "output/116").mkdir(parents=True)
    for name in ("vep", "merged", "refseq"):
        p = root / f"output/116/HG002_annotated_wgs_everything_hgvs_{name}.vcf.gz"
        p.write_bytes(f"golden-{name}".encode())
        (p.parent / (p.name + ".tbi")).write_bytes(b"tbi")
    (root / "input").mkdir()
    (root / "input/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz").write_bytes(b"vcf")
    (root / "input/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz.tbi").write_bytes(b"vtbi")
    for flavour in ("ensembl", "merged", "refseq"):
        d = root / f"cache/116_GRCh38_{flavour}/variation"
        d.mkdir(parents=True)
        (d / "chr1.parquet").write_bytes(b"x" * 10)


@pytest.fixture
def chr22_manifest(tmp_path):
    m = tmp_path / "chr22.json"
    m.write_text(json.dumps({"caches": {f: {"repo_id": f"biodatageeks/vepyr_116_GRCh38_{f}",
                                              "revision": f"rev-{f}", "files": {}}
                                          for f in ("ensembl", "merged", "refseq")}}))
    return m


PROFILES = {"ensembl": "vep", "merged": "merged", "refseq": "refseq"}


def test_build_manifest_records_goldens_and_caches(tmp_path, chr22_manifest):
    make_layout(tmp_path / "data")
    m = wgs_manifest.build_manifest(tmp_path / "data", chr22_manifest, PROFILES)
    assert set(m["profiles"]) == {"ensembl", "merged", "refseq"}
    g = m["profiles"]["merged"]
    assert g["golden"] == "output/116/HG002_annotated_wgs_everything_hgvs_merged.vcf.gz"
    assert g["sha256"] == hashlib.sha256(b"golden-merged").hexdigest()
    assert m["caches"]["refseq"] == {"repo_id": "biodatageeks/vepyr_116_GRCh38_refseq", "revision": "rev-refseq"}


def test_check_passes_on_fresh_staging(tmp_path, chr22_manifest):
    data = tmp_path / "data"
    make_layout(data)
    m = wgs_manifest.build_manifest(data, chr22_manifest, PROFILES)
    staged = wgs_manifest.record_staged(data, m)
    assert wgs_manifest.check_staged(data, m, staged) == []


def test_check_detects_missing_truncated_and_revision_change(tmp_path, chr22_manifest):
    data = tmp_path / "data"
    make_layout(data)
    m = wgs_manifest.build_manifest(data, chr22_manifest, PROFILES)
    staged = wgs_manifest.record_staged(data, m)
    (data / "output/116/HG002_annotated_wgs_everything_hgvs_refseq.vcf.gz").unlink()
    (data / "cache/116_GRCh38_merged/variation/chr1.parquet").write_bytes(b"short")
    m2 = json.loads(json.dumps(m))
    m2["caches"]["ensembl"]["revision"] = "rev-new"
    problems = wgs_manifest.check_staged(data, m2, staged)
    text = "\n".join(problems)
    assert "refseq.vcf.gz" in text
    assert "chr1.parquet" in text
    assert "manifest changed since staging" in text or "ensembl" in text


def test_check_detects_wrong_golden_digest(tmp_path, chr22_manifest):
    data = tmp_path / "data"
    make_layout(data)
    m = wgs_manifest.build_manifest(data, chr22_manifest, PROFILES)
    staged = wgs_manifest.record_staged(data, m)
    m["profiles"]["merged"]["sha256"] = "0" * 64
    assert any("merged" in p for p in wgs_manifest.check_staged(data, m, staged))


def test_cli_check_exit_codes(tmp_path, chr22_manifest, capsys):
    data = tmp_path / "data"
    make_layout(data)
    mpath = tmp_path / "wgs.json"
    assert wgs_manifest.main(["prepare", "--data", str(data), "--chr22-manifest", str(chr22_manifest),
                              "--out", str(mpath)]) == 0
    assert wgs_manifest.main(["record-staged", "--data", str(data), "--manifest", str(mpath),
                              "--out", str(data / "STAGED.json")]) == 0
    assert wgs_manifest.main(["check", "--data", str(data), "--manifest", str(mpath)]) == 0
    (data / "STAGED.json").unlink()
    assert wgs_manifest.main(["check", "--data", str(data), "--manifest", str(mpath)]) == 3
    assert "stage-data.sh" in capsys.readouterr().err
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_wgs_manifest.py -v`
Expected: `ModuleNotFoundError: wgs_manifest`.

- [ ] **Step 3: Implement `wgs_manifest.py`**

Stdlib only: `argparse`, `hashlib`, `json`, `pathlib`, `sys`.
- **Constants:** `PROFILES = {"ensembl": "vep", "merged": "merged", "refseq": "refseq"}` (profile to golden suffix, matching `comparison/profiles.py` basenames `HG002_annotated_wgs_everything_hgvs_<suffix>`); `DEFAULT_CHR22 = <repo>/e2e-testing/golden/116/chr22/manifest.json`; `VCF = "input/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"`; `FASTA = "input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"`.
- **`_sha256(path)`:** streams the file in 8 MiB chunks.
- **`build_manifest`:** builds the schema above.
- **`record_staged`:**
  - computes `manifest_sha256` over `json.dumps(manifest, indent=2, sort_keys=True)` bytes. The `prepare` command writes exactly those bytes plus `\n`, and `check` re-reads the file and hashes `json.dumps(json.load(f), indent=2, sort_keys=True)` so formatting differences cannot cause false alarms;
  - computes sha256 and size for every golden, `.tbi` and input;
  - records the size of every file under each `cache/116_GRCh38_<flavour>/`;
  - records `caches` revisions copied from the manifest. `stage-data.sh` only calls this after downloading those revisions.
- **`main`:** subcommands as in Interfaces. `check` prints problems to stderr and returns 3. If `STAGED.json` is missing it reports `"no STAGED.json under <data> — re-run ci/wgs-runner/stage-data.sh"`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_wgs_manifest.py -v`
Expected: all pass.

- [ ] **Step 5: Generate the committed manifest from the maintainer's data**

```bash
uv run python e2e-testing/scripts/wgs_manifest.py prepare \
  --data ~/workspace/data_vepyr \
  --chr22-manifest e2e-testing/golden/116/chr22/manifest.json \
  --out e2e-testing/golden/wgs-manifest.json
python3 -m json.tool e2e-testing/golden/wgs-manifest.json | head -40
```

Expected: three profiles with real sha256s and sizes in the gigabyte range. If `input/` under `~/workspace/data_vepyr` lacks the benchmark VCF, it is at the data root (`~/workspace/data_vepyr/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz`, the legacy location `comparison/profiles.py` falls back to). In that case pass `--data` such that `input/<name>` resolves, for example via a temporary symlink farm in `$SCRATCH`, and say so in the report. Do not move the maintainer's files.

- [ ] **Step 6: Commit**

```bash
git add e2e-testing/scripts/wgs_manifest.py tests/test_wgs_manifest.py e2e-testing/golden/wgs-manifest.json
git commit -m "test(e2e): WGS golden manifest and staged-data check"
```

---

### Task 4: Runner kit (image, staging, check, in-container entrypoint, README)

**Files:**
- Create: `ci/wgs-runner/IMAGE`, `Dockerfile`, `build-image.sh`, `stage-data.sh`, `check.sh`, `run-profile.sh`, `README.md`

**Interfaces:**
- Consumes: `wgs_manifest.py` CLI (Task 3), `run_comparison.py` (Task 1).
- Produces:
  - **`IMAGE`:** contains exactly `vepyr-wgs:1`.
  - **`run-profile.sh`:** the in-container contract. `run-profile.sh <profile> <wheel-path>`, with env `DATA_VEPYR_DIR=/data`, `WORK=/work` and the repo checkout at `/src` (read-only).
    - **Exit codes:** 0 = parity match, 1 = mismatch, 3 = stale data (from `check`), anything else = error.
    - **Outputs:** writes `/work/result.json` (`result_record` shape: `name: "wgs/<profile>"`) and `/work/summary.txt` (the md5 lines).

- [ ] **Step 1: Write the files**

`ci/wgs-runner/IMAGE`:
```
vepyr-wgs:1
```

`ci/wgs-runner/Dockerfile`:
```dockerfile
# Job image for /parity-wgs on the self-hosted runner. Built locally on the
# runner by build-image.sh and never pulled at job time. vepyr itself is not
# baked in: each job installs the PR's wheel into a fresh venv under /work.
FROM ubuntu:24.04@sha256:<pin the current digest: docker buildx imagetools inspect ubuntu:24.04>
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3.12 python3.12-venv ca-certificates curl bcftools tabix git \
 && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/local/bin/uv
ENV UV_NO_CACHE=1 PYTHONDONTWRITEBYTECODE=1
```

Resolve both pins at implementation time:
- the ubuntu digest, via `docker buildx imagetools inspect ubuntu:24.04 --format '{{json .Manifest.Digest}}'`;
- a released `ghcr.io/astral-sh/uv` tag that exists, checked with `docker manifest inspect`.

Write the real values; no angle-bracket placeholders may remain in the committed file.

`ci/wgs-runner/build-image.sh`:
```bash
#!/usr/bin/env bash
# Build the /parity-wgs job image on the runner host. Re-run after changing
# the Dockerfile, and bump IMAGE (and parity-wgs.yml's WGS_IMAGE) together.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tag="$(cat "$here/IMAGE")"
docker build --pull -t "$tag" -f "$here/Dockerfile" "$here"
docker run --rm "$tag" bash -c 'python3.12 --version && uv --version && bcftools --version | head -1 && tabix --version | head -1'
echo "built $tag"
```

`ci/wgs-runner/run-profile.sh` (executed inside the container):
```bash
#!/usr/bin/env bash
# /parity-wgs: one profile, inside a fresh container. Repo at /src (read-only),
# data at /data (read-only), scratch at /work. See ci/wgs-runner/README.md.
set -uo pipefail
profile="$1"; wheel="$2"
cd /src
python3.12 e2e-testing/scripts/wgs_manifest.py check --data /data --manifest e2e-testing/golden/wgs-manifest.json
rc=$?
if [ "$rc" -ne 0 ]; then
  python3.12 ci/ci_helpers.py result --name "wgs/$profile" --outcome error \
    --summary "staged data differs from the manifest; re-run ci/wgs-runner/stage-data.sh" --out /work/result.json
  exit 3
fi
uv venv --python python3.12 /work/venv >/dev/null
uv pip install --python /work/venv/bin/python "$wheel" "huggingface_hub==1.28.0"
export PATH="/work/venv/bin:$PATH"
/work/venv/bin/python e2e-testing/scripts/run_comparison.py --release 116 --profile "$profile" \
  --comparison-mode md5 --md5-mode both --bgzf --workers "$(nproc)" \
  --output-dir /work/out 2>&1 | tee /work/run.log
rc=${PIPESTATUS[0]}
grep -E 'md5 (strict|canonical)|PASS|FAIL' /work/run.log | tail -50 > /work/summary.txt || true
case "$rc" in 0) outcome=success ;; 1) outcome=failure ;; *) outcome=error ;; esac
python3.12 ci/ci_helpers.py result --name "wgs/$profile" --outcome "$outcome" \
  --summary "$(grep -m1 'md5 strict' /work/summary.txt | xargs || echo "exit $rc")" --out /work/result.json
exit "$rc"
```

Before finalising, verify the `run_comparison.py` exit-code semantics: what does it return for a md5 mismatch versus a crash? Read `e2e-testing/scripts/comparison/cli.py` around its final `return`. Also check whether `--workers` requires a tabix-indexed input; the README says within-contig workers need tabix, which the staged input has. Adjust the `case` to the real codes. If mismatch and crash both return 1, treat 1 as `failure` only when `summary.txt` contains a `DIFF`/`MISMATCH` md5 line, and as `error` otherwise.

`ci/wgs-runner/stage-data.sh`:
- Usage: `stage-data.sh --goldens-from <rsync-source> [--data /data/vepyr]`, with `set -euo pipefail`.
- Steps:
  1. `mkdir -p` the layout.
  2. For each flavour, run `hf download <repo_id> --repo-type dataset --revision <revision> --local-dir $DATA/cache/116_GRCh38_<flavour>`, with repo and revision read from `e2e-testing/golden/wgs-manifest.json` via `python3 -c`. It needs the `hf` CLI: `uvx --from huggingface_hub hf`.
  3. Download the FASTA `.gz` from the URL in `porting-tests/PINS.toml` `[grch38_fasta]`. Verify its Ensembl BSD `sum` against `ensembl_sum` and gunzip it. Write the `.fai` with `porting-tests/tools/run_tests/fetch.py`'s `write_fai`, imported via `PYTHONPATH=porting-tests/tools python3 -c ...`, so no samtools is needed. Then check the sha256 of the `.fa` against `PINS.toml` `sha`.
  4. Fetch the HG002 VCF and `.tbi` with `curl` from GIAB `https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/NISTv4.2.1/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz{,.tbi}`, and verify sha256 against the manifest `inputs`.
  5. `rsync -a --progress "$GOLDENS_FROM/" "$DATA/output/116/"`, limited to the three goldens and their `.tbi` with `--include`/`--exclude` rules, then verify each sha256 against the manifest.
  6. `python3 e2e-testing/scripts/wgs_manifest.py record-staged --data "$DATA" --manifest e2e-testing/golden/wgs-manifest.json --out "$DATA/STAGED.json"`.
  7. `chmod -R a-w "$DATA"` (best effort), then print the totals.

`ci/wgs-runner/check.sh`:
```bash
#!/usr/bin/env bash
# Smoke check of the runner host: image present, staged data consistent, disk.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; repo="$(cd "$here/../.." && pwd)"
data="${1:-/data/vepyr}"
tag="$(cat "$here/IMAGE")"
docker image inspect "$tag" >/dev/null || { echo "image $tag missing: run build-image.sh"; exit 1; }
python3 "$repo/e2e-testing/scripts/wgs_manifest.py" check --data "$data" --manifest "$repo/e2e-testing/golden/wgs-manifest.json"
mkdir -p /scratch/vepyr-wgs
avail=$(df -Pk /scratch/vepyr-wgs | awk 'NR==2 {print $4}')
[ "$avail" -gt $((20*1024*1024)) ] || { echo "less than 20 GB free under /scratch"; exit 1; }
echo "runner OK: $tag, data consistent, $((avail/1024/1024)) GB free"
```

`ci/wgs-runner/README.md` covers the one-time setup on runner 4682 (spec section 6):
1. Install Docker, add the runner user to the `docker` group and restart the runner service.
2. Add the runner label `vepyr-wgs` (org settings → Actions → Runners → 4682).
3. Restrict the runner group to repository `biodatageeks/vepyr` and workflow `.github/workflows/parity-wgs.yml@refs/heads/master`.
4. Create `/data/vepyr` and `/scratch/vepyr-wgs`, owned by the runner user.
5. In a checkout of `master`, run `ci/wgs-runner/build-image.sh`.
6. Run `ci/wgs-runner/stage-data.sh --goldens-from <user@host:~/workspace/data_vepyr/output/116>`.
7. Run `ci/wgs-runner/check.sh`.

It also has sections on re-staging when `wgs-manifest.json` changes, rebuilding the image when the Dockerfile changes (bump `IMAGE` and the workflow's `WGS_IMAGE` together), and the accepted residual risk (spec section 5).

- [ ] **Step 2: Static checks**

```bash
chmod +x ci/wgs-runner/*.sh
docker run --rm -v "$PWD:/mnt" -w /mnt koalaman/shellcheck:v0.10.0 ci/wgs-runner/*.sh
grep -n '<' ci/wgs-runner/Dockerfile   # expect no angle-bracket placeholders
```

Expected: shellcheck is clean, and grep prints nothing from the Dockerfile.

- [ ] **Step 3: Build the image locally (amd64) and dry-run the entrypoint on chr22**

```bash
ci/wgs-runner/build-image.sh
```

Apple Silicon note: build with `--platform linux/amd64` only if running it. Native arm64 is fine for a dry run, because the image has no vepyr; it installs the wheel. To dry-run `run-profile.sh` locally, use an arm64 Linux wheel built with `uv run maturin build --release --out "$SCRATCH/whl"` inside a Linux container, or skip this step and note that Task 7 covers it on the real runner. Running the full WGS locally is not required.

- [ ] **Step 4: Commit**

```bash
git add ci/wgs-runner
git commit -m "ci: self-hosted runner kit for /parity-wgs (image, staging, check, entrypoint)"
```

---

### Task 5: `parity-wgs.yml` workflow

**Files:**
- Create: `.github/workflows/parity-wgs.yml`
- Create: `tests/test_parity_wgs_workflow.py`

**Interfaces:**
- Consumes:
  - `is-parity-wgs-command`, `wgs-statuses`, `find-wheel`, `profiles` (Tasks 1–2);
  - `wgs-manifest.json` (Task 3), whose profile keys are read by `ci_helpers.profiles`;
  - `ci/wgs-runner/IMAGE` and `run-profile.sh` (Task 4).
- Produces: statuses `parity/wgs`, `parity/wgs/<profile>`; artifacts `result-wgs-<profile>` and `wgs-<profile>-reports`.

- [ ] **Step 1: Write the static test** (`tests/test_parity_wgs_workflow.py`)

```python
"""Static guarantees of parity-wgs.yml that review must not have to re-derive."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WF = (ROOT / ".github/workflows/parity-wgs.yml").read_text()


def job_block(name: str) -> str:
    m = re.search(rf"^  {name}:\n(.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)", WF, re.S | re.M)
    assert m, name
    return m.group(1)


def test_image_tag_matches_runner_kit():
    tag = (ROOT / "ci/wgs-runner/IMAGE").read_text().strip()
    assert f"WGS_IMAGE: {tag}" in WF


def test_only_profile_job_uses_self_hosted():
    assert WF.count("vepyr-wgs]") == 1
    assert "self-hosted" in job_block("profile")


def test_privileged_jobs_have_no_expressions_in_run_scripts():
    for job in ("authorize", "report"):
        block = job_block(job)
        for run in re.findall(r"run: \|\n((?:\s{10,}.*\n)+)", block):
            assert "${{" not in run, (job, run)


def test_write_scopes_only_on_authorize_and_report():
    for job in ("command", "build-wheel", "profile"):
        block = job_block(job)
        assert "statuses: write" not in block and "issues: write" not in block and "pull-requests: write" not in block


def test_top_level_permissions_empty_and_master_only():
    assert re.search(r"^permissions: \{\}$", WF, re.M)
    assert "refs/heads/master" in job_block("authorize")


def test_queue_not_cancel():
    assert re.search(r"cancel-in-progress: false", WF)
    assert "max-parallel: 1" in job_block("profile")
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_parity_wgs_workflow.py -v`
Expected: fails with `FileNotFoundError`.

- [ ] **Step 3: Write the workflow**

Base it on the current `.github/workflows/parity-tests.yml`: read it in full and copy its patterns, comments style, admin and master checks, reply logic and env-only scripts. Differences:

```yaml
name: Parity tests (whole genome)

# Admin-only whole-genome md5 parity (HG002, VEP 116, profiles in
# e2e-testing/golden/wgs-manifest.json) on the self-hosted runner.
#   gh workflow run parity-wgs.yml -f pr=123
#   a PR comment that is exactly `/parity-wgs`
# issue_comment always runs the default branch's copy of this workflow.
# PR code runs only inside a fresh `docker run` of the locally built image,
# with the staged data mounted read-only. See ci/wgs-runner/README.md.

on:
  workflow_dispatch:
    inputs:
      pr:
        description: Pull request number
        required: true
        type: string
  issue_comment:
    types: [created]

permissions: {}

env:
  WGS_IMAGE: vepyr-wgs:1

# One whole-genome run at a time on the machine; a second request waits.
# Non-command comments get their own group so they never queue behind a run.
concurrency:
  group: >-
    ${{
      (github.event_name == 'workflow_dispatch'
        || (github.event.issue.pull_request
            && startsWith(github.event.comment.body, '/parity-wgs')
            && contains(fromJSON('["OWNER","COLLABORATOR"]'), github.event.comment.author_association)))
      && 'parity-wgs'
      || format('parity-wgs-ignored-{0}', github.run_id)
    }}
  cancel-in-progress: false
```

Jobs:
- **`command`:** as in parity-tests, but with `if: ... startsWith(github.event.comment.body, '/parity-wgs')` and `is-parity-wgs-command`.
- **`authorize`:** as in parity-tests. Pending status for `parity/wgs` only. The reply text reads "Whole-genome parity started on `$SHA` …". The refusal reply uses `/parity-wgs`. The `outputs` add `profiles`, read with `python3 ci/ci_helpers.py profiles e2e-testing/golden/wgs-manifest.json` from a sparse checkout of `ci` and `e2e-testing/golden` at the workflow's own ref (master). The profile list must come from master, not from the PR.
- **`build-wheel`:** identical to parity-tests (`sccache: false`).
- **`profile`:**

```yaml
  profile:
    needs: [authorize, build-wheel]
    name: wgs (${{ matrix.profile }})
    runs-on: [self-hosted, linux, x64, vepyr-wgs]
    timeout-minutes: 120
    permissions:
      contents: read
    strategy:
      fail-fast: false
      max-parallel: 1
      matrix:
        profile: ${{ fromJSON(needs.authorize.outputs.profiles) }}
    env:
      PROFILE: ${{ matrix.profile }}
      SCRATCH: /scratch/vepyr-wgs/${{ github.run_id }}-${{ github.run_attempt }}-${{ matrix.profile }}
    steps:
      - name: Clean leftovers from earlier jobs
        run: |
          find "$GITHUB_WORKSPACE" -mindepth 1 -delete 2>/dev/null || true
          rm -rf "$SCRATCH" && mkdir -p "$SCRATCH"
      - uses: actions/checkout@v4
        with:
          ref: ${{ needs.authorize.outputs.sha }}
          lfs: false
          persist-credentials: false
      - uses: actions/download-artifact@v4
        with:
          name: wheels-manylinux-x86_64
          path: ${{ env.SCRATCH }}/dist
      - id: run
        name: Whole-genome parity in a fresh container
        run: |
          docker image inspect "$WGS_IMAGE" >/dev/null || { echo "::error::image $WGS_IMAGE missing on runner; run ci/wgs-runner/build-image.sh"; exit 3; }
          # No checked-out (PR) code runs on the host: shell only here.
          shopt -s nullglob
          wheels=("$SCRATCH"/dist/vepyr-*manylinux*x86_64*.whl)
          [ "${#wheels[@]}" -eq 1 ] || { echo "::error::expected one x86_64 wheel, got ${#wheels[@]}"; exit 3; }
          wheel="${wheels[0]}"
          docker run --rm --network bridge \
            --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
            --user "$(id -u):$(id -g)" --read-only --tmpfs /tmp:rw,size=4g \
            -e DATA_VEPYR_DIR=/data -e HOME=/work -e WORK=/work \
            -v /data/vepyr:/data:ro -v "$SCRATCH":/work:rw -v "$GITHUB_WORKSPACE":/src:ro \
            "$WGS_IMAGE" bash /src/ci/wgs-runner/run-profile.sh "$PROFILE" "/work/dist/$(basename "$wheel")"
      - name: Step summary
        if: always()
        run: |
          { echo "### wgs ($PROFILE)"; cat "$SCRATCH/summary.txt" 2>/dev/null || echo "did not run"; } >> "$GITHUB_STEP_SUMMARY"
          cp "$SCRATCH/result.json" "result-wgs-$PROFILE.json" 2>/dev/null \
            || printf '{"name":"wgs/%s","conclusion":"error","summary":"no result written"}\n' "$PROFILE" > "result-wgs-$PROFILE.json"
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: result-wgs-${{ matrix.profile }}
          path: result-wgs-${{ matrix.profile }}.json
      - uses: actions/upload-artifact@v4
        if: failure()
        with:
          name: wgs-${{ matrix.profile }}-reports
          path: |
            ${{ env.SCRATCH }}/out/reports
            ${{ env.SCRATCH }}/summary.txt
            ${{ env.SCRATCH }}/run.log
          if-no-files-found: ignore
      - name: Remove scratch and workspace
        if: always()
        run: |
          rm -rf "$SCRATCH"
          find "$GITHUB_WORKSPACE" -mindepth 1 -delete 2>/dev/null || true
```

The host never executes checked-out (PR) code in this job: the wheel is found with a shell glob and the fallback result is written with `printf`. Add a test to `tests/test_parity_wgs_workflow.py` that asserts `job_block("profile")` contains no `python3 ci/` and no `python3 e2e-testing/`.

- **`report`:** as in parity-tests, `if: ${{ !cancelled() && needs.authorize.result == 'success' }}`.
  - `env`: `PROFILES: ${{ needs.authorize.outputs.profiles }}` and `PROFILE_RESULT: ${{ needs.profile.result }}`.
  - It runs `python3 ci/ci_helpers.py wgs-statuses --results-dir results --profiles "$PROFILES" --job "$PROFILE_RESULT"`, posts each row, and replies (comment route) with the `parity/wgs` row only, plus the run link. Use the code-span sanitising from parity-tests.

- [ ] **Step 4: Run the static test and actionlint**

```bash
uv run pytest tests/test_parity_wgs_workflow.py -v
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7 <copy the three -ignore flags from .github/workflows/ci.yml lint job>
```

Expected: all tests pass, and actionlint is clean. actionlint may warn that `vepyr-wgs` is an unknown runner label. If so, add `.github/actionlint.yaml` with `self-hosted-runner: { labels: [vepyr-wgs] }`; that is the supported way.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/parity-wgs.yml tests/test_parity_wgs_workflow.py .github/actionlint.yaml
git commit -m "ci: admin /parity-wgs whole-genome parity on the self-hosted runner"
```

---

### Task 6: Documentation

**Files:**
- Modify: `README.md` ("Continuous integration" section)

- [ ] **Step 1: Add a subsection after the `/parity` text**

```markdown
### Whole-genome parity (`/parity-wgs`)

An admin comment that is exactly `/parity-wgs` (or
`gh workflow run parity-wgs.yml -f pr=<n>`) runs the HG002 GRCh38
whole-genome md5 parity gate against the Ensembl VEP 116.0 golden VCFs for
the `ensembl`, `merged` and `refseq` profiles on the self-hosted runner.
It reports `parity/wgs` (and `parity/wgs/<profile>`) on the PR head. Runs
queue one at a time, take about an hour, and are not a required check
unless added to branch protection. PR code runs only inside a fresh,
locked-down container with the staged data mounted read-only. Runner setup
and the accepted residual risk are in `ci/wgs-runner/README.md`.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document /parity-wgs"
```

---

### Task 7: Exercise on runner 4682, then open the PR

`parity-wgs.yml` cannot run before it is on master. A temporary push-triggered probe exercises the `profile` job on the real runner.

**Prerequisite (maintainer, on the runner):** this task needs the one-time setup from `ci/wgs-runner/README.md` steps 1–7 done on 4682. The executor cannot do that: it has no access to the host and the goldens live on the maintainer's machine. **Stop and ask the maintainer** to complete the setup and confirm that `check.sh` printed `runner OK`. Give them the exact README steps. Continue only after confirmation.

**Files:**
- Create, then delete: `.github/workflows/zz-wgs-probe.yml`

- [ ] **Step 1: Add the probe**

```yaml
name: wgs-probe (temporary, remove before merge)
on:
  push:
    branches: [ci/parity-wgs, probe/wgs-negative]
permissions:
  contents: read
env:
  WGS_IMAGE: vepyr-wgs:1
jobs:
  build-wheel:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: PyO3/maturin-action@v1
        with: {target: x86_64, args: --release --out dist --find-interpreter, sccache: false, manylinux: auto}
      - uses: actions/upload-artifact@v4
        with: {name: wheels-manylinux-x86_64, path: dist}
```

Then copy the `profile` job from `parity-wgs.yml` verbatim, with these changes:
- `needs: build-wheel`
- `matrix.profile: [merged]`
- `ref: ${{ github.sha }}` instead of the authorize output

Add a first step that plants a file in the workspace, `touch "$GITHUB_WORKSPACE/planted-by-earlier-job"`, after the clean step. Add one more job, `leak-check`, with `needs: profile`, on the same runner labels. It asserts `planted-by-earlier-job` is absent from `$GITHUB_WORKSPACE` and that `/scratch/vepyr-wgs` holds no directory from this run. This tests Review Focus 4.

- [ ] **Step 2: Push and watch** (the push is to this feature branch only)

```bash
git add .github/workflows/zz-wgs-probe.yml && git commit -m "ci: temporary wgs probe (to be removed)"
git push -u origin ci/parity-wgs
gh run list -R biodatageeks/vepyr --branch ci/parity-wgs --workflow wgs-probe --limit 1
```

Expected:
- `wgs (merged)` green, with the step summary showing `md5 strict: body MATCH` and `md5 canonical: body MATCH` across all contigs;
- `leak-check` green.

Record the wall time of `wgs (merged)` and set the README estimate and the 120-minute timeout from it: timeout at 2× the measured time, rounded up to 30 minutes.

- [ ] **Step 3: Negative probe**

Create branch `probe/wgs-negative` from this branch. In it, edit `run-profile.sh` to pass `--vep /data/output/116/HG002_annotated_wgs_everything_hgvs_refseq.vcf.gz` for `merged`, comparing merged output against the refseq golden, which must mismatch. Push. Expected:
- `wgs (merged)` fails with `failure`, not `error`;
- the summary shows `DIFF`/`MISMATCH` md5 lines;
- the reports artifact is uploaded.

Delete the branch afterwards with `git push origin --delete probe/wgs-negative`.

- [ ] **Step 4: Remove the probe and open the PR**

```bash
git rm .github/workflows/zz-wgs-probe.yml && git commit -m "ci: remove temporary wgs probe"
git push
gh pr create --draft --base ci/four-tier-testing --head ci/parity-wgs \
  --title "ci: whole-genome parity on the self-hosted runner (/parity-wgs)" --body-file "$SCRATCH/wgs-pr-body.md"
```

The PR body covers:
- the summary;
- a note that the base is `ci/four-tier-testing` until #174 merges, then it is retargeted to `master` before merging (stacked-PR trap);
- the probe evidence: run ids, merged WGS wall time, negative attribution, leak check;
- the runner setup done;
- the post-merge checklist: `/parity-wgs` on dummy PR #175 as admin (statuses, replies, creator `github-actions[bot]`), the same from a non-admin (refused), and a second request while one is running (it queues).

It ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

---

## Self-Review Notes

- **Spec coverage:**
  - §1 trigger and trust: Tasks 2 and 5.
  - §2 runner, image and data: Tasks 3 and 4.
  - §3 harness: Task 1.
  - §4 failures: Tasks 2, 4 and 5.
  - §5 isolation: Tasks 4, 5 and 7.
  - §6 setup and docs: Tasks 4 and 6.
  - §7 testing: Tasks 1–5 and 7.
- **Deviations from the spec, each justified:**
  - **`docker run` instead of `container:`.** GitHub's job-container start runs `docker pull`, which fails for an image that exists only on the host. An explicit `docker run` also keeps all PR code off the host, which is stronger than `container:`.
  - **Image tag.** A versioned constant (`vepyr-wgs:1`) replaces a Dockerfile hash, because job-level expressions cannot hash files before checkout. A test keeps `IMAGE` and the workflow in sync.
  - **Manifest scope.** The manifest records golden sha256 and size, not per-contig expected md5. Golden identity by sha256 already fixes the expected digests, which are computed at run time.
  - **Profile list source.** It comes from master's copy of the manifest (authorize, sparse checkout), not from the PR, so a PR cannot shrink the profile set.
- **Human dependency:** Task 7 blocks on the maintainer's one-time runner setup. That is an explicit stop, not a guess.
