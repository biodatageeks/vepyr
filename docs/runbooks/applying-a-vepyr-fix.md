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
cargo fmt || exit 1
cargo clippy --all-targets -- -D warnings || exit 1
uv run pre-commit run ruff --all-files || exit 1   # the hook passes --fix and rewrites code
```

Guard each one. Chained with `&&` and followed by the lint, a clippy failure
would be overwritten by the lint's clean exit and the block would report
success with the Rust side still red.

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
BRANCH="fix/your-slug"   # the same branch name in all three repos, already pushed in each

PRS=""
for r in datafusion-bio-formats datafusion-bio-functions vepyr; do
  # --head is required here. --repo picks the target repository, but --head still
  # defaults to the CURRENT branch, so a loop running in one working tree would
  # propose this checkout's branch to the other two repos — failing, or worse,
  # opening a PR for the wrong branch.
  url=$(gh pr create --draft --repo "biodatageeks/$r" --head "$BRANCH" \
          --title "..." --body-file "/tmp/pr-$r.md") || exit 1
  PRS="$PRS biodatageeks/$r:${url##*/}"
done
# Owner-qualified on purpose: `gh --repo` requires OWNER/REPO and rejects a bare
# name, and every consumer below — including tools/vepyr-fix/handoff.sh — takes
# the repository straight out of this list rather than rebuilding it.
echo "PRS=$PRS"   # e.g. " biodatageeks/datafusion-bio-formats:41 biodatageeks/vepyr:100"

# gh pr edit reports success and changes nothing, so patch through the API and
# read the body back. `| head` would otherwise mask a failed fetch with its own
# clean exit, which is why pipefail is set above.
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}; r=${repo##*/}
  gh api -X PATCH "repos/$repo/pulls/$n" -f body="$(cat "/tmp/pr-$r.md")" >/dev/null || exit 1
  gh api "repos/$repo/pulls/$n" --jq .body > "/tmp/readback-$r.md" || exit 1
  diff -q "/tmp/pr-$r.md" "/tmp/readback-$r.md" \
    || { echo "$r: PR body does not match what was sent"; exit 1; }
done
```

Compare the whole body, not a prefix. The point of the read-back is to catch a
PATCH that silently kept the old text, and printing the first few lines proves
only that the fetch worked — a body that lost everything below the prefix would
still look right.

### 5. Ask both reviewers

Each repo runs `claude-code-review.yml` automatically when a PR opens or gets
new commits. The two on-demand reviewers answer comments:

```bash
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}
  gh pr comment "$n" --repo "$repo" --body "@codex review"  || exit 1
  gh pr comment "$n" --repo "$repo" --body "@claude review" || exit 1
done
```

Guard both. If the codex request fails and the claude one succeeds, an unguarded
loop still returns zero and the workflow walks into its "both reviewers" gate
with only one of them having looked.

`@codex review` draws a reply from `chatgpt-codex-connector[bot]` and `@claude
review` from `claude[bot]`. Codex has no committed workflow in any of the three
repos — it is a GitHub App on the repo — so if no reply arrives, check the app
rather than hunting for a broken workflow file.

### 6. Iterate to green, watching for three signals

Three things are worth waking for, and nothing else: a check that did not
pass, new review activity, and the run reaching fully concluded. If your harness
can watch a command in the background and notify you per line, use it. If it
cannot, run this loop in a second terminal, or poll it by hand on the same
interval — the requirement is the signals, not the mechanism:

```bash
prev=""
while true; do
  cur=""; failed=""
  for e in $PRS; do
    repo=${e%%:*}; n=${e##*:}; r=${repo##*/}

    # statusCheckRollup, NOT `gh pr checks`: that command exits 8 while any check
    # is pending and nonzero when one fails, so its exit status reports the checks
    # rather than the query, and treating it as a query failure would print an
    # error through the whole normal review lifecycle.
    checks=$(gh pr view "$n" --repo "$repo" --json statusCheckRollup \
      --jq '.statusCheckRollup[] | "'"$r"' \(.name): \(if (.conclusion // "") == "" then (.status // .state // "UNKNOWN") else .conclusion end)"') \
      || failed="$failed checks:$r"

    # Two endpoints, because they hold different things: a bot's inline findings
    # are PULL REQUEST REVIEW comments, while `gh pr view --json comments` returns
    # only the issue-comment thread. Polling just the latter yields a green
    # snapshot with every finding invisible.
    findings=$(gh api --paginate "repos/$repo/pulls/$n/comments" \
      --jq '.[] | select(.user.login|test("\\[bot\\]")) | "'"$r"' finding \(.id)"') \
      || failed="$failed findings:$r"
    remarks=$(gh api --paginate "repos/$repo/issues/$n/comments" \
      --jq '.[] | select(.user.login|test("\\[bot\\]")) | "'"$r"' comment \(.id)"') \
      || failed="$failed comments:$r"

    # Third endpoint, and the only one that can show a CLEAN pass. A reviewer
    # with nothing to say submits a review carrying no inline comment, so it
    # appears in neither list above. Keyed by commit, so "codex has looked at
    # the current head" becomes an observable event rather than an assumption.
    reviews=$(gh api --paginate "repos/$repo/pulls/$n/reviews" \
      --jq '.[] | select(.user.login|test("\\[bot\\]")) | "'"$repo"'#'"$n"' review \(.user.login) \(.commit_id[0:7]) \(.state) id=\(.id) body_chars=\((.body//"")|length)"') \
      || failed="$failed reviews:$r"

    cur="$cur$checks
$findings
$remarks
$reviews
"
  done

  # A watcher that has lost authentication must not look like a quiet green stack.
  [ -n "$failed" ] && echo "QUERY FAILED:$failed"
  cur=$(echo "$cur" | sort -u)
  comm -13 <(echo "$prev") <(echo "$cur")
  prev=$cur
  sleep 120
done
```

`--paginate` on both comment queries, not decoration: the API returns 30 items
per page and a PR under active review passes that quickly, after which the
newest findings are the ones that fall off. A loop that silently reads only the
first page reports green while the findings it has not fetched go unaddressed. The per-item `--jq` filters concatenate cleanly across pages; a
filter like `length` would not, because it emits one value per page.

Emit pending checks as well as concluded ones. Dropping them makes a check that
is still running — or wedged — indistinguishable from one that does not exist,
and the hand-off in step 8 would then see nothing wrong. Note the fallback has
to test for an empty string explicitly: a pending `CheckRun` carries
`conclusion: ""` and `status: "IN_PROGRESS"`, and `//` falls back only on `null`
or `false`, so `.conclusion // .status` yields the empty string and silently
loses the state.

Carry the review id and its body length, not just the state. A reviewer can put
a finding in the review body itself, and `COMMENTED` with feedback then looks
identical to `COMMENTED` with nothing to say — the loop would report that the bot
reviewed the head while the finding stayed invisible. The length says
whether there is anything to read and the id says where to read it:

```bash
# Each review line carries its own repo#number, so this needs nothing from the
# watch loop's shell — which matters, because that loop is occupying one.
gh api "repos/<owner/repo>/pulls/<number>/reviews/<id>" --jq .body
```

Address the review directly by id rather than listing and filtering: a single
object needs no `--paginate`, so a review past the first page cannot go missing
from the lookup the way it could from a list.

Step 6's gate asks whether both reviewers have examined the current head, and
only the reviews endpoint can answer that: a reviewer with no findings leaves a
review and no comments at all, so a comments-only loop sees silence and
cannot tell a clean pass from a reviewer that never ran. Including the commit in
that line is what makes "looked at *this* head" checkable, rather than "looked
at some head once".

Five traps here, each of which makes the watch lie in a different way. A
literal `<n>` is not a placeholder to the shell but input redirection from a file
called `n`, so the query fails and a trailing `sort` reports success. `gh pr
checks` exits 8 whenever a check is pending and nonzero when one fails, so its
status describes the checks and not the query — guard on it and the watch
cries failure through the entire normal lifecycle. And review findings are not
issue comments: on a PR of this kind the two endpoints differ by a wide margin,
so polling only `gh pr view --json comments` reaches a green snapshot with every
finding unseen.

**Re-pin downstream after every upstream push.** This is the step that is easy
to skip and expensive to miss. A review fix landing on the formats branch moves
its head, but the functions PR still pins the commit from step 4, and vepyr
still pins the old functions head. All three PRs then go green independently
while the tree a reviewer reads — and the tree step 7 measures — silently
excludes the fixes you just made. So after any upstream push, walk the cascade
in dependency order before re-requesting review or running a gate:

```bash
# PRS entries are owner-qualified, so match on the repository name after the
# last slash — passing a bare name must still resolve, or the head lookups
# below silently get an empty selector and gh falls back to the current branch.
num() {
  for e in $PRS; do
    repo=${e%%:*}
    [ "${repo##*/}" = "$1" ] || [ "$repo" = "$1" ] || continue
    echo "${e##*:}"; return
  done
  echo "no PR recorded for $1" >&2; return 1
}

formats=$(gh pr view "$(num datafusion-bio-formats)" \
  --repo biodatageeks/datafusion-bio-formats --json headRefOid --jq .headRefOid) || exit 1
# bump the formats rev in datafusion-bio-functions, cargo check, commit, push
functions=$(gh pr view "$(num datafusion-bio-functions)" \
  --repo biodatageeks/datafusion-bio-functions --json headRefOid --jq .headRefOid) || exit 1
# bump the functions rev in vepyr, rebuild, commit, push
```

Confirm each pin resolves to the commit you meant before moving on, because a
pin that silently kept its old value is indistinguishable from a green run.

Only once the whole cascade is pushed, re-request both reviewers. Neither
re-runs itself on a push, and codex has no synchronize-triggered workflow at
all, so without a fresh `@codex review` it never sees the commits you are
proposing — and asking before the pin commits exist means it reviews a head you
are about to replace:

```bash
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}
  gh pr comment "$n" --repo "$repo" --body "@codex review"  || exit 1
  gh pr comment "$n" --repo "$repo" --body "@claude review" || exit 1
done
```

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
uv run python "$ROOT/tools/vepyr-fix/compare_runs.py" \
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
bash "$ROOT/tools/vepyr-fix/handoff.sh" "$PRS" /tmp/handoff.md
```

This is a script rather than a block of shell in a document because the ordering
is the whole point and it took three review rounds to get right. It runs five
phases and exits nonzero at the first that fails:

1. **Refuse while anything is red or running.** Nothing leaves draft otherwise.
2. **Snapshot, then mark ready.** The current check-run set and the current bot
   feedback are both recorded first.
3. **Wait for the run that marking ready started.** `ready_for_review` is one of
   `claude-code-review.yml`'s trigger events, so a gate that runs before this
   says nothing about the check it causes — and GitHub takes a moment to
   register the new run, so a poll that accepts the first green rollup it sees
   is reading the *previous* result. It waits for the check-run set to change
   before accepting any green. A query failure aborts rather than retrying,
   because half an hour of silent polling is what a broken credential looks like
   otherwise.
4. **Refuse if that run produced new feedback.** A review can conclude green
   while carrying a finding, so the check status cannot answer step 6's
   requirement that nothing is left unanswered. The bot comment and review ids
   are compared against the snapshot.
5. **Only then post the hand-off comment.**

Change it under test. `tools/vepyr-fix/tests/test_handoff.sh` drives all five
phases against a stubbed `gh`, so the mutating ones can be exercised without
touching a real pull request — which is why they were wrong for three rounds
running. Run it after any edit:

```bash
tools/vepyr-fix/tests/test_handoff.sh
```

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
