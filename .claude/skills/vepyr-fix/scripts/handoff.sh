#!/usr/bin/env bash
# Take a stack of PRs from "green in draft" to "handed off", in the only order
# that is actually safe.
#
#   handoff.sh "<repo:number> ..." /path/to/handoff.md
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

PRS=${1:?usage: handoff.sh "<repo:number> ..." <handoff-body-file>}
BODY=${2:?usage: handoff.sh "<repo:number> ..." <handoff-body-file>}
[ -r "$BODY" ] || { echo "cannot read handoff body: $BODY" >&2; exit 2; }

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

# Every bot finding and review, inline or review-body. A review that concludes
# green can still carry feedback, so the count is what decides, not the check.
feedback() {
  { gh api --paginate "repos/$1/pulls/$2/comments" --jq '.[]|select(.user.login|test("\\[bot\\]"))|.id'
    gh api --paginate "repos/$1/pulls/$2/reviews"  --jq '.[]|select(.user.login|test("\\[bot\\]"))|.id'
  } | sort -u
}

# --- 1. nothing leaves draft while anything is red or still running ----------
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}
  gate "$repo" "$n" || { echo "refusing: $repo#$n is not green" >&2; exit 1; }
done

# --- 2. snapshot, then mark ready -------------------------------------------
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}
  eval "sig_${n}=\$(sig '$repo' '$n')" || exit 2
  feedback "$repo" "$n" > "/tmp/handoff-feedback-$n.before" || exit 2
  gh pr ready "$n" --repo "$repo" || exit 1
done

# --- 3. wait for the ready-triggered run to appear, then to go green ---------
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}
  eval "before=\$sig_${n}"
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
  repo=${e%%:*}; n=${e##*:}
  feedback "$repo" "$n" > "/tmp/handoff-feedback-$n.after" || exit 2
  if ! new=$(comm -13 "/tmp/handoff-feedback-$n.before" "/tmp/handoff-feedback-$n.after") || [ -n "$new" ]; then
    echo "$repo#$n: new bot feedback arrived after ready_for_review:" >&2
    echo "$new" >&2
    echo "address it and re-run; a green check is not the same as no findings" >&2
    exit 1
  fi
done

# --- 5. only now say it is ready --------------------------------------------
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}
  gh pr comment "$n" --repo "$repo" --body-file "$BODY" || exit 1
done
echo "handed off: $PRS"
