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
# Any character that is not [A-Za-z0-9_], so the key is safe as a FILENAME.
# It is deliberately never used as a shell variable name: repository names
# contain hyphens, `eval "sig_a-b=..."` is not an assignment but a command, and
# its failure is easy to leave unchecked — which is how a bogus signature gets
# compared against and the stale pre-ready green gets accepted.
key() { printf '%s' "$1" | tr -c 'A-Za-z0-9_' '_'; }

POLL=${HANDOFF_POLL_SECONDS:-30}
TRIES=${HANDOFF_MAX_POLLS:-60}
# Recognising the reviewer's routine summary needs BOTH tests, because each
# alone lets one real case through:
#   * containment alone exempts a body carrying the marker AND a finding;
#   * "matches some body seen before" alone exempts a finding the reviewer
#     repeats verbatim, since that text is already in the history.
# So a new body is clean only when it carries the marker AND matches, in full,
# a body this PR has already shown — with commit shas and timestamps normalised
# out so the same template on a later commit still matches.
CLEAN_BODY_MARKER=${HANDOFF_CLEAN_BODY_MARKER:-codex-pull-request-review-summary}
normalise_body() {
  sed -E -e 's/[0-9a-f]{7,40}/SHA/g' \
         -e 's/[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z?/TS/g' \
         -e 's/[[:space:]]+/ /g' -e 's/^ //' -e 's/ $//'
}

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

# id + base64 body. A review body can carry a finding, but bots also post a
# fixed summary body on every pass, so the discriminator is whether the body is
# one we have seen before on this PR — not whether it is empty.
review_bodies() {
  local repo=$1 n=$2 out
  out=$(gh api --paginate "repos/$repo/pulls/$n/reviews" \
    --jq '.[]|select(.user.login|test("\\[bot\\]"))|"\(.id)\t\(.body|@base64)"') || return 2
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
  sig "$repo" "$n" > "/tmp/handoff-$k.sig.before" || exit 2
  findings       "$repo" "$n" > "/tmp/handoff-$k.findings.before" || exit 2
  reviews_of     "$repo" "$n" > "/tmp/handoff-$k.reviews.before"  || exit 2
  review_bodies  "$repo" "$n" > "/tmp/handoff-$k.bodies.before"   || exit 2

  # Whether this PR was still a draft decides what phase 3 may require. Marking
  # an already-ready PR ready again causes no transition, so no new check run
  # appears — waiting for one would hang a rerun after refused feedback.
  draft=$(gh pr view "$n" --repo "$repo" --json isDraft --jq .isDraft) || exit 2
  echo "$draft" > "/tmp/handoff-$k.wasdraft"
  gh pr ready "$n" --repo "$repo" || exit 1
done

# --- 3. wait for the ready-triggered run to appear, then to go green ---------
for e in $PRS; do
  repo=${e%%:*}; n=${e##*:}; k=$(key "$e")
  before=$(cat "/tmp/handoff-$k.sig.before") || exit 2
  # Only a draft->ready transition triggers a fresh run. On a rerun the PR is
  # already ready, so there is nothing new to wait for and the current state is
  # what a reviewer sees.
  if [ "$(cat "/tmp/handoff-$k.wasdraft")" != "true" ]; then
    echo "$repo#$n: already ready, no new run to await"
    gate "$repo" "$n" || { echo "$repo#$n is not green" >&2; exit 1; }
    continue
  fi
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

  # Inspect the body of every review that is NEW by id. Identity decides what
  # has been considered; the body decides whether it says anything.
  review_bodies "$repo" "$n" > "/tmp/handoff-$k.bodies.after" || exit 2
  cut -f1 "/tmp/handoff-$k.bodies.before" | sort -u > "/tmp/handoff-$k.seenreviews"
  # The templates this PR has already shown, normalised. A new body is clean
  # only if it matches one of them in full.
  : > "/tmp/handoff-$k.templates"
  while IFS=$(printf '\t') read -r _ b64; do
    [ -n "$b64" ] || continue
    printf '%s' "$b64" | base64 --decode 2>/dev/null | normalise_body >> "/tmp/handoff-$k.templates"
  done < "/tmp/handoff-$k.bodies.before"
  sort -u "/tmp/handoff-$k.templates" -o "/tmp/handoff-$k.templates"

  body_finding=0
  while IFS=$(printf '\t') read -r rid b64; do
    [ -n "$rid" ] || continue
    grep -qx "$rid" "/tmp/handoff-$k.seenreviews" && continue
    body=$(printf '%s' "$b64" | base64 --decode 2>/dev/null)
    [ -z "$body" ] && continue
    norm=$(printf '%s' "$body" | normalise_body)
    if printf '%s' "$body" | grep -qF "$CLEAN_BODY_MARKER" \
       && grep -qxF "$norm" "/tmp/handoff-$k.templates"; then
      continue
    fi
    echo "$repo#$n: review $rid carries a body that is not this PR's routine summary:" >&2
    printf '%s\n' "$body" | head -8 >&2
    body_finding=1
  done < "/tmp/handoff-$k.bodies.after"
  if [ "$body_finding" = 1 ]; then
    echo "address it and re-run; only inline findings are matched automatically" >&2
    exit 1
  fi

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
