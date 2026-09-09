---
name: vepyr-fix
description: Apply a vepyr correctness or performance fix end to end across datafusion-bio-formats, datafusion-bio-functions and vepyr. Capture a perf and md5 parity baseline before any edit, implement, open one draft PR per repo with the pin cascade, drive the codex and claude review loop to green, then re-verify against the baseline before declaring anything ready. It never merges. It hands green, verified PRs to a human for the final review and leaves the merge and the post-merge re-pin to them. Use this whenever the user wants to fix, implement or land a VEP parity bug, act on a filed vepyr issue, change the annotation engine or the VCF reader, or asks for PRs across the engine repos, even when they name only one repo, because a pin bump in the others is almost always required.
---

# Applying a vepyr fix

A vepyr fix usually spans three repos, and the two things that decide whether it
landed well are measured on this machine rather than argued about: byte-level
md5 parity against Ensembl VEP, and wall/phase timings compared to the same
host's own earlier numbers. Capture both **before** touching code, so the
end-state comparison means something. A baseline taken after the change is not
a baseline.

Harnesses this skill drives, all already in the repo:
`performance-tests/vepyr/scripts/` (worker scaling) and
`e2e-testing/scripts/` (md5 concordance and the parity gate).

## Scope

**In scope:** one fix, or a small coherent group of fixes, that changes
annotation behaviour or the VCF reader — the shape of the issues filed as
`biodatageeks/vepyr#92`–`#99`.

**Out of scope:** merging. This skill takes the work to the point where a human
can review and merge it, and stops there — see step 8. Also out of scope:
adding a plugin (use the `vep-add-plugin` skill), rebuilding or republishing
caches, and cutting a release. Those have their own gates and their own
runbooks.

## Before you start

```bash
export DATA_VEPYR_DIR=~/workspace/data_vepyr   # required, no default; every path below hangs off it
uptime                                          # this host is SHARED — see Measurement traps
df -g "$DATA_VEPYR_DIR"                         # plain output above 1 worker wants ~65 GiB free

RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
```

That last line is the build every measured run uses (`docs/performance.md`,
`docs/developers.md`). It is a release build with native CPU instructions, so a
benchmark taken against a `maturin develop` tree is measuring a different
binary and is not comparable with anything. Use the same command, with the same
`RUSTFLAGS`, for the baseline in step 1 and the re-verification in step 8 — a
flag that differs between the bookends silently turns a codegen difference into
an apparent regression or win.

## Workflow

### 1. Capture the baseline — both halves, before any edit

Put everything in one run directory so the final comparison is a diff, not a
memory exercise:

```bash
ROOT=$(git rev-parse --show-toplevel)
RUN="$ROOT/e2e-testing/results/fix-$(date +%Y%m%d-%H%M)/baseline" && mkdir -p "$RUN"
```

Resolve `RUN` to an absolute path. The steps below change directory, and a
relative run directory would put the redirections somewhere under the script
directory that was never created.

**Performance.** The trace is the signal that matters; wall clock and memory are
coarser alarms. Export the tracing variables, because the runner does not set
them, then drive the Python runner directly — the shell wrappers cannot express
the separate archive directories and the record floor this needs:

```bash
export VEP_PIPELINE_TRACE=1 VEP_ENGINE_PROFILE=1
cd "$ROOT/performance-tests/vepyr/scripts"

sweep() {  # $1 = archive dir, rest = worker counts — one invocation each
  local archive="$1"; shift
  local status=0
  for w in "$@"; do
    uv run python run_vepyr_worker_scaling.py \
      --input-vcf "$DATA_VEPYR_DIR/input/HG002_normalized.vcf.gz" \
      --cache-dir "$DATA_VEPYR_DIR/cache/116_GRCh38_merged" --cache-type merged \
      --reference-fasta "$DATA_VEPYR_DIR/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa" \
      --ssd-output-dir /tmp/vepyr-perf --archive-dir "$archive" \
      --expected-records 4096123 --minimum-free-gib 65 --force --workers "$w" \
      || status=$?
    # Each WGS output is ~29 GB (`output_bytes: 29418788410` in the checked-in
    # performance-tests metrics) and the gate never reads it — only `summary.tsv`
    # and the per-worker traces. This has to happen BETWEEN workers: the runner
    # re-checks free space before each one (`run_vepyr_worker_scaling.py:266-272`),
    # so a single `--workers 8 1` call would archive 29 GB and then fail worker 1
    # on the space check it just invalidated.
    rm -f "$archive"/*.vcf "$archive"/*.vcf.gz
    [ "$status" -eq 0 ] || break
  done
  return "$status"   # the rm must not become the function's exit status
}

sweep "$RUN/warmup" 8    || exit 1   # DISCARD: first run of a session reads ~35% slow at 8 workers
sweep "$RUN/archive" 8 1 || exit 1   # measured
```

One worker per invocation is what makes the cleanup effective, and saving the
runner's status is what stops a failed sweep from looking successful — `rm`
would otherwise be the last command and supply the function's exit code.

If you have a second volume, put the archive on it and add
`--require-separate-filesystems`. The runner checks free space per worker
against the SSD directory, so archiving onto the same filesystem it is
measuring recovers nothing between workers — it prints a note saying so
(`run_vepyr_worker_scaling.py:242-244`). The `rm` above is what makes a
single-volume machine viable.

`--force` on the measured sweep is load-bearing. The runner refuses to start
when any SSD or archive artifact for that worker count already exists
(`run_vepyr_worker_scaling.py:289-293`), so after a warm-up at 8 workers the
measured sweep would die before annotating anything. The separate archive
directory keeps the discarded warm-up out of the measured summary.

The whole sweep is about 15 minutes, so none of this is a costly ceremony.

Everything the gate reads lands under `$RUN/archive`:

| Evidence | Where |
|---|---|
| wall and peak RSS per worker count | `summary.tsv`, columns `annotation_seconds` and `max_rss_kb` |
| `[VEP_PIPELINE_TRACE]` phase lines | `merged_workers<N>.stderr.txt` |

The trace is **not** on the runner's own stderr. Each annotation child's stderr
goes to its own artifact, which is then archived
(`run_vepyr_worker_scaling.py:330-333`, `:346`), so redirecting the runner's
stderr captures only its progress chatter. Confirm the traces are really there
before trusting any later comparison, because an empty trace file silently
compares equal to another empty one and reads as "no regression":

```bash
for f in "$RUN"/archive/merged_workers*.stderr.txt; do
  n=$(grep -c VEP_PIPELINE_TRACE "$f" || true)
  echo "$f: $n trace lines"
  [ "$n" -gt 0 ] || { echo "no trace lines in $f — VEP_PIPELINE_TRACE was not set"; exit 1; }
done
```

`4096123` is the record count of the normalized HG002 input, which is the
`bcftools norm -m -both` output of the GIAB benchmark (`docs/testing-vep.md`).
Annotating raw multi-allelic input is a different measurement and a different
correctness question, so keep to the normalized file.

**Quality.** Hash the bodies in strict mode. `canonical` is the default and it
rewrites QUAL and sorts INFO keys before hashing, which is useful for triage
and wrong for a parity claim:

Order matters here, and not for a cosmetic reason. Both comparison modes write
to the same per-contig report path (`comparison/cli.py:466`), and md5 mode
leaves `comparison` null because that field is only populated in field mode
(`:418-419`). `verify_parity_gate.py:398-400` rejects a report whose
`comparison` is not a dict, so running md5 first makes the gate fail with
"comparison is missing or null" even when parity is perfect. Run field mode,
gate on it, then take the digests:

```bash
set -o pipefail   # without this, tee's success hides a nonzero exit below
cd "$ROOT/e2e-testing/scripts"

uv run python run_comparison.py --release 116 --chroms all --bgzf \
  | tee "$RUN/field.out" || exit 1             # populates `comparison` in each report
uv run python verify_parity_gate.py --release 116 --profile merged --chroms 1-22 \
  | tee "$RUN/gate.out" || exit 1

uv run python run_comparison.py --release 116 --chroms all \
  --comparison-mode md5 --md5-mode strict --bgzf | tee "$RUN/md5.out" || exit 1
cp -r "$ROOT/e2e-testing/reports" "$RUN/reports"
```

`set -o pipefail` is not decoration. `run_comparison.py` returns 1 when any
contig mismatches (`comparison/cli.py:695`) and `verify_parity_gate.py` exits
nonzero on a gate failure, but a plain `cmd | tee f` reports tee's status, so
without it the recipe sails past exactly the failures that should stop it and
declares a red baseline green. `pipefail` alone is not enough either: it makes
the failure visible in the pipeline's status but does not stop the block, and
the trailing `cp` would then supply a zero exit for the whole thing. Hence the
explicit `|| exit 1` after each pipeline — run the block as a script, not by
pasting it into a shell you care about.

The md5 pass overwrites the reports the gate just read, so copy them into the
run directory, and re-run field mode before ever re-running the gate against
that directory. There is no `--report-dir` on `run_comparison.py` to keep the
two apart — the path is fixed at `cli.py:588`.

The two scripts take contigs differently: `run_comparison.py` wants names or
`all`, while `verify_parity_gate.py` also expands numeric ranges like `1-22`.
Passing `1-22` to the former treats it as one contig name and compares nothing.

`md5_concordance.py` hashes header and body separately and excludes the `##VEP=`
and `##datafusion-bio-function-vep=` provenance lines, so a body match is a real
record-for-record match and a header mismatch alone is usually just provenance.
Use `--pair VEP_VCF VEPYR_VCF --explain` on a single chromosome when a body
digest differs and you need the offending records.

**Gate:** the baseline body digests must already match. If they do not, stop and
say so — you cannot attribute a later mismatch to your fix when the starting
point was already red.

### 2. Reproduce the defect with a test that fails now

Write the failing test before the fix, in the repo that owns the behaviour. The
issues filed for this project already carry proposed tests with positive
controls; reuse them rather than inventing new ones. A fix whose test never
failed proves nothing about the fix.

Note which harness can even see the defect. The chr1 golden fixture holds 91
SNVs and 9 indels with no MNV, no multi-allelic site and no `ALT=.` record, so a
green golden gate is not evidence for those paths — add a fixture that contains
the shape you are fixing.

### 3. Implement in dependency order

`datafusion-bio-formats` → `datafusion-bio-functions` → `vepyr`. While
iterating, point the downstream repos at your local checkouts with a temporary
Cargo `[patch]` rather than pushing to get a rev, and remember that the patch is
scratch state that must not reach a PR.

While iterating locally, `env -u CONDA_PREFIX uv run maturin develop` is the
faster rebuild. Both `VIRTUAL_ENV` and `CONDA_PREFIX` are set on this machine
and maturin refuses that combination, and the refusal is quiet: the previously
built extension in `.venv` keeps serving, so you test old code believing it is
new. Rebuild with the native release command before any timing.

Lint through pre-commit, never directly — `uv run ruff` finds nothing in
`.venv` and falls through to pyenv:

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
uv run pre-commit run ruff --all-files     # note: the hook passes --fix and rewrites code
```

### 4. Open one PR per repo, with the pin cascade

Open upstream first and pin each downstream PR to the **head commit** of the PR
below it, so reviewers see a tree that builds:

| PR | pins |
|---|---|
| formats | nothing |
| functions | formats PR head |
| vepyr | functions PR head |

Write the body from a file. `gh pr edit` reports success and changes nothing, so
edit through the API and read the body back to confirm:

Open them as drafts. The gates in step 7 have not run yet, so a PR that looks
ready before it is verified invites a review of unproven work — and `gh pr
ready` in step 8 is then the honest signal that it has been. Draft PRs still
fire `claude-code-review.yml`, which has no draft guard, so the review loop is
not delayed by this.

```bash
set -o pipefail

# Record the number each repo's PR got. They differ, and reusing one number
# across the three comments on unrelated PRs — or silently acts on the wrong one.
PRS=""
for r in datafusion-bio-formats datafusion-bio-functions vepyr; do
  url=$(gh pr create --draft --repo "biodatageeks/$r" \
          --title "..." --body-file "/tmp/pr-$r.md") || exit 1
  PRS="$PRS $r:${url##*/}"
done
echo "PRS=$PRS"   # e.g. " datafusion-bio-formats:41 datafusion-bio-functions:207 vepyr:100"

# gh pr edit reports success and changes nothing, so patch through the API and
# read the body back. `| head` would otherwise mask a failed fetch with its own
# clean exit, which is why pipefail is set above.
for e in $PRS; do
  r=${e%%:*}; n=${e##*:}
  gh api -X PATCH "repos/biodatageeks/$r/pulls/$n" -f body="$(cat "/tmp/pr-$r.md")" >/dev/null || exit 1
  gh api "repos/biodatageeks/$r/pulls/$n" --jq .body | head -5 || exit 1
done
```

### 5. Ask both reviewers

Each repo runs `claude-code-review.yml` automatically when a PR opens or gets
new commits. The two on-demand reviewers answer comments:

```bash
for e in $PRS; do
  r=${e%%:*}; n=${e##*:}
  gh pr comment "$n" --repo "biodatageeks/$r" --body "@codex review"  || exit 1
  gh pr comment "$n" --repo "biodatageeks/$r" --body "@claude review" || exit 1
done
```

Guard both. If the codex request fails and the claude one succeeds, an unguarded
loop still returns zero and the workflow walks into its "both reviewers" gate
with only one of them having looked.

`@codex review` draws a reply from `chatgpt-codex-connector[bot]` and `@claude
review` from `claude[bot]`. Codex has no committed workflow in any of the three
repos — it is a GitHub App on the repo — so if no reply arrives, check the app
rather than hunting for a broken workflow file.

### 6. Iterate to green under a monitor

Rather than polling by hand, arm one persistent monitor that emits a line
whenever a check concludes or a new bot comment lands, then work on the findings
as they arrive:

```bash
prev=""
while true; do
  cur=$(for r in datafusion-bio-formats datafusion-bio-functions vepyr; do
          gh pr checks <n> --repo "biodatageeks/$r" --json name,bucket \
            --jq ".[] | select(.bucket!=\"pending\") | \"$r \(.name): \(.bucket)\"" 2>/dev/null
          gh pr view <n> --repo "biodatageeks/$r" --json comments \
            --jq ".comments[] | select(.author.login|test(\"bot\")) | \"$r comment \(.createdAt)\"" 2>/dev/null
        done | sort)
  comm -13 <(echo "$prev") <(echo "$cur")
  prev=$cur
  sleep 120
done
```

Address each finding on its merits. Reviewer feedback here is frequently about
measurement rather than code, and a bot can be wrong — verify a claim against
the source before acting on it, and say so in the thread when you disagree.
Push, then re-request **both** reviewers on the repos you pushed to. Neither
re-runs itself on a push, and codex in particular has no synchronize-triggered
workflow at all, so without a fresh `@codex review` it never sees the commits
you are actually proposing:

```bash
for e in $PRS; do
  r=${e%%:*}; n=${e##*:}
  gh pr comment "$n" --repo "biodatageeks/$r" --body "@codex review"  || exit 1
  gh pr comment "$n" --repo "biodatageeks/$r" --body "@claude review" || exit 1
done
```

**Re-pin downstream after every upstream push.** This is the step that is easy
to skip and expensive to miss. A review fix landing on the formats branch moves
its head, but the functions PR still pins the commit from step 4, and vepyr
still pins the old functions head. All three PRs then go green independently
while the tree a reviewer reads — and the tree step 7 measures — silently
excludes the fixes you just made. So after any upstream push, walk the cascade
in dependency order before re-requesting review or running a gate:

```bash
formats=$(gh pr view <n> --repo biodatageeks/datafusion-bio-formats --json headRefOid --jq .headRefOid)
# bump the formats rev in datafusion-bio-functions, cargo check, commit, push
functions=$(gh pr view <n> --repo biodatageeks/datafusion-bio-functions --json headRefOid --jq .headRefOid)
# bump the functions rev in vepyr, rebuild, commit, push
```

Confirm each pin resolves to the commit you meant before moving on, because a
pin that silently kept its old value is indistinguishable from a green run.

**Gate:** green means, simultaneously across all three PRs, that every check has
concluded green and no bot finding is left unanswered. One repo going green
while another has an open finding is not green.

### 7. Re-verify against the baseline, on the stacked branches

Run step 1 again, byte for byte the same commands and the same `RUSTFLAGS`, into
a `final/` directory beside `baseline/`, then set `BASE` and `FINAL` to the two
absolute run directories for the comparisons below. Same host, same session shape, warm-up
discarded again. Nothing is merged at this point, so measure the vepyr PR branch
with its pin still on the functions PR head — that stack is exactly what a
reviewer will read. Check first that every pin points at its upstream PR's
current head, per step 6: measuring a stale cascade produces numbers for code
nobody is going to merge.

**Quality gate:** every body digest matches in strict mode, and
`verify_parity_gate.py` exits 0.

**Performance gate**, three metrics, each at both 1 and 8 workers:

| Metric | Source | Bar |
|---|---|---|
| phase durations | `[VEP_PIPELINE_TRACE]` in `archive/merged_workers<N>.stderr.txt` | no phase regresses >5% |
| total wall | `annotation_seconds` in `summary.tsv` | within 10% |
| peak RSS | `max_rss_kb` in `summary.tsv` | within 10% |

One command checks all three and returns a verdict:

```bash
uv run python "$ROOT/.claude/skills/vepyr-fix/scripts/compare_runs.py" \
  "$BASE/archive" "$FINAL/archive" || exit 1
```

Absolute path on purpose: step 1 leaves the shell in `$ROOT/e2e-testing/scripts`,
and a relative path would resolve under that directory and fail before comparing
anything. Guarding the sweeps the same way matters for the same reason — a
`sweep` that propagates its status is no use if the caller discards it, which
would let a failed warm-up be followed by a measured run that returns 0 for the
whole block.

Its exit status is the gate: 0 when every bar holds, 1 when any is exceeded, and
nonzero rather than a pass when there is nothing to compare. It ends on a
`VERDICT:` line, so the result is one thing to read rather than three tables to
weigh up.

All three metrics live in one command deliberately. Three separate tables and a
human deciding is how a regression gets waved through, and every gate in this
skill has to be able to fail on its own. The script sums each
`(worker, stage, event, metric)` duration from the per-worker traces, ignores
phases under 50 ms where relative swings are meaningless, and reads
`annotation_seconds` and `max_rss_kb` per worker from `summary.tsv`. Two
deliberate refusals: a phase present on only one side fails, because that is a
shape change in the pipeline rather than noise; and an empty
`annotation_seconds` is an error rather than a zero, because it means the run
did not report, not that it was instant.

A byte diff could not do this. Every `*_ms` value moves slightly between real
runs, so exact equality rejects changes far below the 5% allowance, and the
traces are per worker, so concatenating them would let a regression at 8 workers
cancel against a speed-up at 1.

Read RSS with the floor in mind. A large fixed allocation dominates the peak, so
a change that adds real memory to the variable part can still come in under a
percentage bar on the total. If RSS moved at all, say by how much in absolute
terms rather than only as a ratio, and check whether it moved at one worker
count and not the other — that asymmetry usually points at per-worker buffers
rather than at a shared structure.

Report the outcome as a table of baseline against final for both worker counts,
and state plainly which gate passed and which did not. If wall time moved but no
phase did, suspect the host rather than the change, and say that instead of
claiming a win.

### 8. Hand off for human review — do not merge

Merging is the human's call, always, even when all three PRs are green and both
gates passed. Green means the work is ready to be judged, not that it is
approved. So take each PR out of draft, post the evidence, and stop:

```bash
for e in $PRS; do
  r=${e%%:*}; n=${e##*:}
  gh pr ready "$n" --repo "biodatageeks/$r" || exit 1
  gh pr comment "$n" --repo "biodatageeks/$r" --body-file /tmp/handoff.md || exit 1
done
```

Guard `gh pr ready` too. A hand-off that half succeeds — one PR still a draft
while its comment claims the stack is ready — is worse than one that fails
outright, because nobody goes looking.

The hand-off comment carries what a reviewer needs and cannot easily rederive:
the baseline-against-final table for both worker counts, which gates passed,
the md5 mode used, the commit each pin points at, and every bot finding with
how it was addressed. Then say in the conversation that the PRs are ready and
name what is left for a person to do:

- merge in dependency order, formats then functions then vepyr;
- after each merge, rewrite the downstream pin from the PR head to the **merge
  commit**, so no merged tree points at a commit that only ever existed on a
  branch.

Leave both to them. Do not merge, do not enable auto-merge, and do not push the
post-merge re-pin ahead of the merge it depends on. If the reviewer asks you to
merge, that is a fresh instruction for that specific merge and does not
generalise to the rest of the stack.

## Measurement traps

Each of these has cost a full cycle before now.

- **The host is shared.** A concurrent build elsewhere on the machine doubled
  wall time and inflated CPU-seconds. Check `uptime` before trusting any
  timing, and re-run rather than reasoning about a number taken under load.
- **Discard the first run.** About 35% slow at 8 workers, every session.
- **Attribute by ablation, not by phase boundary.** Deriving cause from RSS
  deltas at phase boundaries produced four wrong attributions in one session.
  Flip the flag and re-measure instead.
- **Peak RSS needs a subprocess per configuration.** An in-process sweep reports
  a cumulative high-water mark, not the cost of each configuration.
  `run_vepyr_worker_scaling.py` already gets this right — it reads one child's
  usage through `os.wait4` rather than `RUSAGE_CHILDREN`, which would carry every
  earlier worker count's peak into the later ones, and it normalises the macOS
  bytes against Linux kilobytes difference. So read `max_rss_kb` from its
  `summary.tsv` instead of measuring memory yourself.
- **Prefer the trace to the wall.** Phase durations settled in two runs what 24
  whole-genome wall-clock runs could not.
- **Build the bookends identically.** Same command, same `RUSTFLAGS`. A native
  release build against a dev build is not a regression, it is a different
  binary.
- **Numbers compare only within one host.** The perf README documents server
  paths under another user's home; those numbers are not comparable with this
  Mac's.
