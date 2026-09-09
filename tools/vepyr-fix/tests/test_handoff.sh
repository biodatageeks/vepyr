#!/usr/bin/env bash
# Exercises every phase of handoff.sh against a stubbed gh, so the mutating
# phases can be tested without touching a real pull request. That is the whole
# point: phases 2-5 were previously unverifiable, and every defect in them
# survived until a reviewer found it.
#
#   tools/vepyr-fix/tests/test_handoff.sh
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
SCRIPT=$HERE/../handoff.sh
PATH="$HERE/bin:$PATH"
export HANDOFF_POLL_SECONDS=0 HANDOFF_MAX_POLLS=6
pass=0; fail=0

green()   { echo '{"statusCheckRollup":[{"name":"lint","conclusion":"SUCCESS","status":"COMPLETED","startedAt":"'"$1"'"}]}'; }
pending() { echo '{"statusCheckRollup":[{"name":"lint","conclusion":"","status":"IN_PROGRESS","startedAt":"'"$1"'"}]}'; }
ids()     { printf '['; local sep=""; for i in "$@"; do printf '%s{"id":%s,"user":{"login":"codex[bot]"},"body":"x"}' "$sep" "$i"; sep=","; done; printf ']\n'; }

setup() { D=$(mktemp -d); export FAKE_DIR=$D; echo 0 > "$D/step"; : > "$D/calls.log"; }
calls() { grep -c "^$1" "$FAKE_DIR/calls.log" 2>/dev/null || true; }
check() { # name, expected, actual
  if [ "$2" = "$3" ]; then pass=$((pass+1)); echo "  ok   $1"
  else fail=$((fail+1)); echo "  FAIL $1: expected '$2' got '$3'"; fi
}

echo "1. refuses while a check is pending, and marks nothing ready"
setup; pending T1 > "$D/rollup.0.json"; ids > "$D/comments.0.json"; ids > "$D/reviews.0.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 1" 1 "$rc"; check "no ready call" 0 "$(calls ready)"; check "no comment" 0 "$(calls comment)"

echo "2. happy path: green, ready, new run appears green, no findings, comment posted"
setup; green T1 > "$D/rollup.0.json"; green T2 > "$D/rollup.1.json"
ids > "$D/comments.0.json"; ids 1 > "$D/reviews.0.json"; ids 1 2 > "$D/reviews.1.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 0" 0 "$rc"; check "marked ready" 1 "$(calls ready)"; check "comment posted" 1 "$(calls comment)"
check "clean review not treated as feedback" 0 "$(grep -c 'new inline findings' <<<"$out")"

echo "3. a new inline finding after ready refuses, and posts no comment"
setup; green T1 > "$D/rollup.0.json"; green T2 > "$D/rollup.1.json"
ids > "$D/comments.0.json"; ids 9 > "$D/comments.1.json"; ids > "$D/reviews.0.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 1" 1 "$rc"; check "marked ready" 1 "$(calls ready)"; check "no comment" 0 "$(calls comment)"

echo "4. bare repo entry is refused up front"
setup; green T1 > "$D/rollup.0.json"
out=$(bash "$SCRIPT" "r:100" "$0" 2>&1); rc=$?
check "exit 2" 2 "$rc"; check "names the reason" 1 "$(grep -c 'owner-qualified' <<<"$out")"

echo "5. hyphenated repo name survives snapshot keying"
setup; green T1 > "$D/rollup.0.json"; green T2 > "$D/rollup.1.json"
ids > "$D/comments.0.json"; ids > "$D/reviews.0.json"
out=$(bash "$SCRIPT" "o/datafusion-bio-formats:41" "$0" 2>&1); rc=$?
check "exit 0" 0 "$rc"; check "no command-not-found" 0 "$(grep -c 'command not found' <<<"$out")"

echo "6. rerun when the PR is already ready still completes"
setup; green T1 > "$D/rollup.0.json"; ids > "$D/comments.0.json"; ids > "$D/reviews.0.json"
echo 1 > "$D/already_ready"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 0" 0 "$rc"; check "comment posted" 1 "$(calls comment)"

echo "7. a review body carrying a finding refuses"
setup; green T1 > "$D/rollup.0.json"; green T2 > "$D/rollup.1.json"
ids > "$D/comments.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"## Codex Review Summary"}]' > "$D/reviews.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"## Codex Review Summary"},{"id":2,"user":{"login":"codex[bot]"},"body":"P1: this is broken"}]' > "$D/reviews.1.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 1" 1 "$rc"; check "no comment" 0 "$(calls comment)"

echo
echo "passed=$pass failed=$fail"
[ "$fail" -eq 0 ]
