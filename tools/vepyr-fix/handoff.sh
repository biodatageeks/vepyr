#!/usr/bin/env bash
# Take a stack of PRs from "green in draft" to "handed off", in the only order
# that is actually safe.
#
#   handoff.sh "<owner/repo:number> ..." /path/to/handoff.md
#
# Entries must be owner-qualified: `gh --repo` requires [HOST/]OWNER/REPO and
# rejects a bare name outright, so a bare entry aborts phase 1 every time.
#
# The ordering matters more than any individual check. Marking a PR ready
# re-triggers the review workflow (claude-code-review.yml lists
# ready_for_review among its events), so a gate that runs before that says
# nothing about the check it causes. And a review can conclude successfully
# while still carrying a finding, so a green rollup is not the same as no
# open feedback.
#
# Exit 0 only when every PR is ready, every check concluded green, and no bot
# feedback arrived that this run has not seen.
set -o pipefail

PRS=${1:?usage: handoff.sh "<owner/repo:number> ..." <handoff-body-file>}
BODY=${2:?usage: handoff.sh "<owner/repo:number> ..." <handoff-body-file>}
[ -r "$BODY" ] || { echo "cannot read handoff body: $BODY" >&2; exit 2; }

for e in $PRS; do
  case ${e%%:*} in
    */*) ;;
    *) echo "entry '$e' is not owner-qualified; gh --repo needs OWNER/REPO" >&2; exit 2;;
  esac
done

# Snapshot keys must include the repository: two repos can hand out the same PR
# number, and keying on the number alone lets the later one overwrite the
# earlier one's signature and feedback.
key() { echo "$1" | tr '/:' '__'; }

POLL=${HANDOFF_POLL_SECONDS:-30}
TRIES=${HANDOFF_MAX_POLLS:-60}

# 0 every check concluded green; 1 not yet; 2 the query itself failed.
gate() {
  local repo=$1 n=$2 roll
  roll=$(gh pr view "$n" --repo "$repo" --json statusCheckRollup \
    --jq '.statusCheckRollup[] | "\(.name)\t\(if (.conclusion // "") == "" then (.status // .state // "UNKNOWN") else .conclusion end)"') || return 2
  [ -n "$roll" ] || { echo "$repo#$n: no checks reported at all" >&2; return 2; }
  echo "$roll" | awk -F'\t' -v id="$repo#$n" '
    $2=="SUCCESS"||$2=="SKIPPED"||$2=="NEUTRAL"{next}
    {print id": "$1" is "$2; bad++} END{exit bad?1:0}'
}

# Identity of the current set of check runs. Marking ready must change this
# before a green rollup means anything: GitHub takes a moment to register the
# new run, and until it does the rollup still shows the previous green.
sig() {
  gh pr view "$2" --repo "$1" --json statusCheckRollup \
    --jq '[.statusCheckRollup[] | "\(.name)@\(.startedAt // "")"] | sort | join(",")'
}

# Bot findings, meaning INLINE review comments. Deliberately not review
# submissions: a reviewer with nothing to say still submits one (codex posts a
# fixed summary body either way), so counting those would refuse every clean
# run and the happy path could never complete.
#
# The gap this leaves is a bot that puts a finding only in a review body and
# never inline. Phase 4 prints new review ids for that reason, so they can be
# read, but does not fail on them.
#
# Each query is checked on its own: in `{ a; b; } | sort` the group's status is
# b's and sort succeeds regardless, so a failed first query would otherwise pass
# silently as an empty snapshot.
findings() {
  local repo=$1 n=$2 out
  out=$(gh api --paginate "repos/$repo/pulls/$n/comments" \
    --jq '.[]|select(.user.login|test("\\[bot\\]"))|.id') || return 2
  echo "$out" | sort -u
}

reviews_of() {
  local repo=$1 n=$2 out
  out=$(gh api --paginate "repos/$repo/pulls/$n/reviews" \
    --jq '.[]|select(.user.login|test("\\[bot\\]"))|.id') || return 2
  echo "$out" | sort -u
}

# --- 1. nothing leaves draft while anything is red or still running ----------
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}
  gate "$repo" "$n" || { echo "refusing: $repo#$n is not green" >&2; exit 1; }
done

# --- 2. snapshot, then mark ready -------------------------------------------
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}; k=$(key "$e")
  s=$(sig "$repo" "$n") || exit 2
  eval "sig_${k}=\$s"
  findings   "$repo" "$n" > "/tmp/handoff-$k.findings.before" || exit 2
  reviews_of "$repo" "$n" > "/tmp/handoff-$k.reviews.before"  || exit 2
  gh pr ready "$n" --repo "$repo" || exit 1
done

# --- 3. wait for the ready-triggered run to appear, then to go green ---------
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}; k=$(key "$e")
  eval "before=\$sig_${k}"
  seen_new=0
  for _ in $(seq "$TRIES"); do
    now=$(sig "$repo" "$n") || { echo "$repo#$n: rollup query failed" >&2; exit 2; }
    [ "$now" != "$before" ] && seen_new=1
    if [ "$seen_new" = 1 ]; then
      gate "$repo" "$n" >/dev/null 2>&1; status=$?
      # A query failure is not a pending state: retrying it for half an hour
      # turns a broken credential into a silent stall.
      [ "$status" -eq 2 ] && { gate "$repo" "$n"; echo "$repo#$n: query failed, not retrying" >&2; exit 2; }
      [ "$status" -eq 0 ] && break
    fi
    sleep "$POLL"
  done
  [ "$seen_new" = 1 ] || { echo "$repo#$n: no post-ready check ever appeared" >&2; exit 1; }
  gate "$repo" "$n" || { echo "$repo#$n did not settle green after ready_for_review" >&2; exit 1; }
done

# --- 4. refuse if that run produced feedback nobody has answered -------------
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}; k=$(key "$e")
  findings   "$repo" "$n" > "/tmp/handoff-$k.findings.after" || exit 2
  reviews_of "$repo" "$n" > "/tmp/handoff-$k.reviews.after"  || exit 2

  new_reviews=$(comm -13 "/tmp/handoff-$k.reviews.before" "/tmp/handoff-$k.reviews.after")
  [ -n "$new_reviews" ] && echo "$repo#$n: reviews submitted since ready: $(echo "$new_reviews" | tr '\n' ' ')"

  new=$(comm -13 "/tmp/handoff-$k.findings.before" "/tmp/handoff-$k.findings.after") || exit 2
  if [ -n "$new" ]; then
    echo "$repo#$n: new inline findings arrived after ready_for_review:" >&2
    echo "$new" >&2
    echo "address them and re-run; a green check is not the same as no findings" >&2
    exit 1
  fi
done

# --- 5. only now say it is ready --------------------------------------------
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}
  gh pr comment "$n" --repo "$repo" --body-file "$BODY" || exit 1
done
echo "handed off: $PRS"
