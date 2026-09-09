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
# Routine reviews carry the reviewer's summary template, which is what a clean
# pass looks like. A fixture body of "x" would be a finding under the body rule,
# so it must not be the default here.
CLEAN='<!-- codex-pull-request-review-summary --> no findings'
ids()     { printf '['; local sep=""; for i in "$@"; do printf '%s{"id":%s,"user":{"login":"codex[bot]"},"body":"%s"}' "$sep" "$i" "$CLEAN"; sep=","; done; printf ']\n'; }

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

echo "8. phase 3 polls a post-ready check from pending to green"
setup; green T1 > "$D/rollup.0.json"; pending T2 > "$D/rollup.1.json"; green T2 > "$D/rollup.3.json"
ids > "$D/comments.0.json"; ids > "$D/reviews.0.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 0" 0 "$rc"; check "comment posted" 1 "$(calls comment)"
check "actually waited (more than one rollup read)" 1 "$([ "$(cat "$D/rstep")" -gt 2 ] && echo 1 || echo 0)"

echo "9. a repeated finding body is refused even though the text was seen before"
setup; green T1 > "$D/rollup.0.json"; green T2 > "$D/rollup.1.json"
ids > "$D/comments.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"P1: this is broken"}]' > "$D/reviews.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"P1: this is broken"},{"id":2,"user":{"login":"codex[bot]"},"body":"P1: this is broken"}]' > "$D/reviews.1.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 1" 1 "$rc"; check "no comment" 0 "$(calls comment)"

echo "10. the clean summary template is not treated as a finding"
setup; green T1 > "$D/rollup.0.json"; green T2 > "$D/rollup.1.json"
ids > "$D/comments.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"<!-- codex-pull-request-review-summary -->\nall clear"}]' > "$D/reviews.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"<!-- codex-pull-request-review-summary -->\nall clear"},{"id":2,"user":{"login":"codex[bot]"},"body":"<!-- codex-pull-request-review-summary -->\nall clear"}]' > "$D/reviews.1.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 0" 0 "$rc"; check "comment posted" 1 "$(calls comment)"

echo "11. a finding appended to the clean marker is still refused"
setup; green T1 > "$D/rollup.0.json"; green T2 > "$D/rollup.1.json"
ids > "$D/comments.0.json"; ids 1 > "$D/reviews.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"'"$CLEAN"'"},{"id":2,"user":{"login":"codex[bot]"},"body":"<!-- codex-pull-request-review-summary -->\nP1: still broken"}]' > "$D/reviews.1.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 1" 1 "$rc"; check "no comment" 0 "$(calls comment)"

echo "12. the template with only volatile parts changed is still clean"
setup; green T1 > "$D/rollup.0.json"; green T2 > "$D/rollup.1.json"
ids > "$D/comments.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"<!-- codex-pull-request-review-summary --> reviewed abc1234 at 2026-09-09T10:00:00Z"}]' > "$D/reviews.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"<!-- codex-pull-request-review-summary --> reviewed abc1234 at 2026-09-09T10:00:00Z"},{"id":2,"user":{"login":"codex[bot]"},"body":"<!-- codex-pull-request-review-summary --> reviewed def5678 at 2026-09-09T11:22:33Z"}]' > "$D/reviews.1.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 0" 0 "$rc"; check "comment posted" 1 "$(calls comment)"

echo "13. a MULTILINE historical template does not exempt a later finding"
setup; green T1 > "$D/rollup.0.json"; green T2 > "$D/rollup.1.json"
ids > "$D/comments.0.json"
ML='<!-- codex-pull-request-review-summary -->\n## Summary\nno findings'
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"'"$ML"'"}]' > "$D/reviews.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"'"$ML"'"},{"id":2,"user":{"login":"codex[bot]"},"body":"<!-- codex-pull-request-review-summary -->\nP1: still broken"}]' > "$D/reviews.1.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 1" 1 "$rc"; check "no comment" 0 "$(calls comment)"

echo "14. a MULTILINE template repeated is still clean"
setup; green T1 > "$D/rollup.0.json"; green T2 > "$D/rollup.1.json"
ids > "$D/comments.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"'"$ML"'"}]' > "$D/reviews.0.json"
echo '[{"id":1,"user":{"login":"codex[bot]"},"body":"'"$ML"'"},{"id":2,"user":{"login":"codex[bot]"},"body":"'"$ML"'"}]' > "$D/reviews.1.json"
out=$(bash "$SCRIPT" "o/r:100" "$0" 2>&1); rc=$?
check "exit 0" 0 "$rc"; check "comment posted" 1 "$(calls comment)"

echo
echo "passed=$pass failed=$fail"
[ "$fail" -eq 0 ]
