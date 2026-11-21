"""Static guarantees of parity-caches.yml: the ones parity-wgs.yml gives, mirrored."""

import json
import re
from pathlib import Path

import pytest

from tests.test_parity_wgs_workflow import PURGE, _step, job_block, run_scripts

ROOT = Path(__file__).resolve().parents[1]
WF = (ROOT / ".github/workflows/parity-caches.yml").read_text()
WGS = (ROOT / ".github/workflows/parity-wgs.yml").read_text()
PARITY_TESTS = (ROOT / ".github/workflows/parity-tests.yml").read_text()
RUN_SCRIPT = (ROOT / "ci/wgs-runner/run-cache-parity.sh").read_text()


def block(name: str) -> str:
    return job_block(name, WF)


def test_workflow_parses_and_has_the_five_jobs():
    # PyYAML comes with pre-commit and mkdocs, not as a declared dependency.
    yaml = pytest.importorskip("yaml")
    workflow = yaml.safe_load(WF)
    assert list(workflow["jobs"]) == [
        "command",
        "authorize",
        "build-wheel",
        "cache",
        "report",
    ]
    triggers = workflow[True]  # YAML reads the key `on` as a boolean
    assert set(triggers) == {"workflow_dispatch", "issue_comment"}
    assert set(triggers["workflow_dispatch"]["inputs"]) == {"pr", "sha", "select"}
    assert workflow["permissions"] == {}


def test_image_tag_matches_runner_kit():
    tag = (ROOT / "ci/wgs-runner/IMAGE").read_text().strip()
    assert f"WGS_IMAGE: {tag}" in WF


def test_only_cache_job_uses_self_hosted_with_the_wgs_labels():
    labels = "runs-on: [self-hosted, linux, x64, vepyr-wgs]"
    assert WF.count("vepyr-wgs]") == 1
    assert labels in block("cache")
    assert labels in job_block("profile", WGS)


def test_command_job_only_reacts_to_parity_caches_comments():
    command_if = block("command").split("runs-on:")[0]
    assert "startsWith(github.event.comment.body, '/parity-caches')" in command_if
    assert "github.event.issue.pull_request" in command_if
    assert "parity-caches-command --env COMMENT_BODY" in block("command")
    # A dispatch input reaches authorize only as plain words.
    assert '"${SELECT_INPUT//[^0-9A-Za-z_ ]/}"' in block("command")
    assert '"${SHA_INPUT//[^0-9A-Za-z]/}"' in block("command")


def test_other_parity_workflows_ignore_parity_caches_comments():
    head = PARITY_TESTS[: PARITY_TESTS.index("\njobs:\n")]
    conc = head[head.index("concurrency:") :]
    ignore = "!startsWith(github.event.comment.body, '/parity-caches')"
    assert ignore in conc
    assert ignore in job_block("command", PARITY_TESTS).split("runs-on:")[0]
    # `/parity-caches` does not start with `/parity-wgs`.
    wgs_if = job_block("command", WGS).split("runs-on:")[0]
    assert "startsWith(github.event.comment.body, '/parity-wgs')" in wgs_if


def test_authorization_is_that_of_parity_wgs():
    """Admin-only for actor and triggering actor, master only, pinned fork head."""
    mine, theirs = block("authorize"), job_block("authorize", WGS)

    def steps(text: str) -> str:
        """From the master-only check to the end of the SHA resolution."""
        first = text.index("- name: Only master's workflow may run this")
        return text[first : text.index("      # The workflow's own ref")]

    assert steps(mine).replace("/parity-caches", "/parity-wgs") == steps(theirs)
    pr_step = mine[mine.index("id: pr") : mine.index("sparse-checkout")]
    assert 'for who in "$ACTOR" "$TRIGGERING_ACTOR"; do' in mine
    assert 'if [ "$perm" != "admin" ]; then' in mine
    assert '[ "$REF" = "refs/heads/master" ]' in mine
    assert "PIN: ${{ needs.command.outputs.pin }}" in mine
    assert '[[ "$sha" == "$pin"* ]]' in pr_step
    assert 'elif [ "$head_repo" != "$REPO" ]; then' in pr_step
    assert "fork PR: pin the head you reviewed with \\`/parity-caches" in pr_step
    assert pr_step.index('echo "sha=$sha"') > pr_step.index("fork PR: pin")


def test_targets_come_from_masters_pins_and_manifest():
    authorize = block("authorize")
    checkout = authorize[authorize.index("sparse-checkout-cone-mode") :]
    for path in (
        "/ci/",
        "/e2e-testing/golden/cache-parity-manifest.json",
        "/src/vepyr/_published.py",
    ):
        assert path in checkout
    # The sparse checkout has no `ref:`, so it is the workflow's own (master).
    assert "ref:" not in checkout.split("- id: targets")[0]
    assert "python3 ci/ci_helpers.py cache-targets" in authorize
    assert "--published src/vepyr/_published.py" in authorize
    assert '--select "$SELECT"' in authorize
    cache = block("cache")
    assert "matrix: ${{ fromJSON(needs.authorize.outputs.matrix) }}" in cache
    assert "timeout-minutes: ${{ matrix.timeout }}" in cache
    assert "MEMORY: ${{ matrix.memory }}" in cache
    assert '--memory "$MEMORY" --memory-swap "$MEMORY"' in cache


def test_privileged_jobs_have_no_expressions_in_run_scripts():
    for job in ("authorize", "report"):
        scripts = run_scripts(block(job))
        assert scripts, job
        for run in scripts:
            assert "${{" not in run, (job, run)


def test_write_scopes_only_on_authorize_and_report():
    for job in ("command", "build-wheel", "cache"):
        text = block(job)
        assert (
            "statuses: write" not in text
            and "issues: write" not in text
            and "pull-requests: write" not in text
        )
    assert re.search(r"^permissions: \{\}$", WF, re.M)


def test_no_concurrency_group_runner_serialises():
    assert not re.search(r"^\s*concurrency:", WF, re.M)
    assert "cancel-in-progress" not in WF
    assert "max-parallel: 4" in block("cache")
    assert "fail-fast: false" in block("cache")


def test_build_wheel_job_is_the_parity_wgs_one():
    def steps(text: str) -> str:
        return text.split("\n  #")[0]  # without the next job's leading comment

    assert steps(block("build-wheel")) == steps(job_block("build-wheel", WGS))
    assert "sccache: false" in block("build-wheel")


def test_cache_job_runs_no_checked_out_code_on_host():
    text = block("cache")
    assert "python3 ci/" not in text
    assert "python3 e2e-testing/" not in text
    assert "bash /src/ci/wgs-runner/run-cache-parity.sh" in text


def test_container_is_hardened_like_the_wgs_one():
    def docker_run(text: str) -> str:
        start = text.index("docker run --rm")
        return text[start : text.index("\n      - ", start)]

    mine, theirs = docker_run(block("cache")), docker_run(job_block("profile", WGS))
    for flag in (
        "--network bridge",
        "--cap-drop ALL",
        "--security-opt no-new-privileges",
        "--pids-limit 4096",
        '--user "$(id -u):$(id -g)"',
        "--read-only",
        "--tmpfs /tmp:rw,size=4g",
        "-v /data/vepyr:/data:ro",
        '-v "$SCRATCH":/work:rw',
        '-v "$GITHUB_WORKSPACE":/src:ro',
        "--label vepyr-wgs=1",
        '--name "$CONTAINER"',
    ):
        assert flag in mine and flag in theirs, flag
    clean = _step(block("cache"), "Clean leftovers from earlier jobs")
    assert "docker ps -aq --filter label=vepyr-wgs=1 | xargs -r docker rm -f" in clean


def test_collect_step_is_the_parity_wgs_one():
    """Kill the container, purge scratch, then copy capped regular files only."""
    mine = _step(block("cache"), "Collect results")
    theirs = _step(job_block("profile", WGS), "Collect results")
    same = (
        mine.replace("caches/%s", "wgs/%s")
        .replace("result-caches-$TARGET", "result-wgs-$PROFILE")
        .replace('"$TARGET"', '"$PROFILE"')
    )
    summary = '          { echo "### '
    assert same.split(summary)[0] == theirs.split(summary)[0]
    kill = mine.index('docker rm -f "$CONTAINER"')
    assert kill < mine.index(PURGE) < mine.index('"$SCRATCH/')
    after_run = block("cache")[block("cache").index("docker run --rm") :]
    assert "env.SCRATCH" not in after_run  # nothing uploaded from scratch
    uploads = re.findall(
        r"uses: actions/upload-artifact@v4.*?path: (.*?)\n(?=\s*(?:if-no|- |#|$))",
        block("cache"),
        re.S,
    )
    assert len(uploads) == 2
    for path in uploads:
        assert path.startswith("${{ env.RES }}/"), path


def test_report_posts_statuses_from_masters_helper():
    report = block("report")
    assert "sparse-checkout: ci" in report
    assert "caches-statuses --results-dir results" in report
    assert (
        '--targets "$TARGETS" --job "$CACHE_RESULT" --aggregate "$AGGREGATE"' in report
    )
    assert "CACHE_RESULT: ${{ needs.cache.result }}" in report
    assert "pattern: result-caches-*" in report


def test_run_script_tells_mismatch_from_crash_by_the_verdict_line():
    # cache_parity.py ends its summary with PASS:, FAIL: or ERROR:; a Python
    # crash also exits 1, so run-cache-parity.sh believes only those lines.
    parity = (ROOT / "e2e-testing/scripts/cache_parity.py").read_text()
    for marker in ('f"PASS: ', 'f"FAIL: ', 'f"ERROR: '):
        assert marker in parity
    assert "grep -m1 -E '^(PASS|FAIL|ERROR): ' /work/summary.txt" in RUN_SCRIPT
    assert 'if [[ "$verdict" == FAIL:* ]]; then outcome=failure; code=1' in RUN_SCRIPT
    assert 'if [[ "$verdict" == PASS:* ]]; then outcome=success; code=0' in RUN_SCRIPT
    assert '--name "caches/$target"' in RUN_SCRIPT
    assert "e2e-testing/golden/cache-parity-manifest.json" in RUN_SCRIPT
    assert "--report /work/out/reports/cache-parity.json" in RUN_SCRIPT


def test_manifest_limits_fit_the_workflow():
    manifest = json.loads(
        (ROOT / "e2e-testing/golden/cache-parity-manifest.json").read_text()
    )
    for kind in ("core", "plugin"):
        assert re.fullmatch(r"[1-9][0-9]{0,3}g", manifest["limits"][kind]["memory"])
    assert '[[ "$MEMORY" =~ ^[1-9][0-9]{0,3}g$ ]]' in block("cache")
    opt_in = {
        name for name, entry in manifest["plugins"].items() if entry.get("opt_in")
    }
    assert opt_in == {"cadd", "spliceai"}
