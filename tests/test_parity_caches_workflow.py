"""Static guarantees of parity-caches.yml.

Authorization, the job container and the result collection are those of
parity-wgs.yml, mirrored. Unlike parity-wgs.yml, every job runs on the
self-hosted hosts, the wheel build included.
"""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.test_parity_wgs_workflow import PURGE, _step, job_block, run_scripts

ROOT = Path(__file__).resolve().parents[1]
WF = (ROOT / ".github/workflows/parity-caches.yml").read_text()
WGS = (ROOT / ".github/workflows/parity-wgs.yml").read_text()
PARITY_TESTS = (ROOT / ".github/workflows/parity-tests.yml").read_text()
PUBLISH = (ROOT / ".github/workflows/publish_to_pypi.yml").read_text()
JOBS = ("command", "authorize", "build-wheel", "cache", "report")
CLEAN = "Clean leftovers from earlier jobs"
RUN_SCRIPT = (ROOT / "ci/wgs-runner/run-cache-parity.sh").read_text()
CHECK_SCRIPT = (ROOT / "ci/wgs-runner/check.sh").read_text()


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


def test_every_job_runs_on_the_self_hosted_hosts():
    """No job of this workflow may use a GitHub-hosted runner."""
    labels = "[self-hosted, linux, x64, vepyr-wgs]"
    jobs = re.findall(r"^  ([a-z][a-z0-9-]*):\n", WF[WF.index("\njobs:\n") :], re.M)
    assert tuple(jobs) == JOBS
    for job in jobs:
        assert re.findall(r"^ +runs-on: (.*)$", block(job), re.M) == [labels], job
    assert len(re.findall(r"^ +runs-on:", WF, re.M)) == len(JOBS)
    for hosted in ("ubuntu-", "macos-", "windows-"):
        assert hosted not in WF, hosted
    # The labels of the /parity-wgs profile job, which keeps its hosted jobs.
    assert f"runs-on: {labels}" in job_block("profile", WGS)
    assert WGS.count("vepyr-wgs]") == 1


def test_every_job_first_empties_the_workspace_and_fails_closed():
    """The hosts persist: no step may start in a directory PR code wrote."""
    for job in JOBS:
        steps = block(job).split("    steps:\n", 1)[1]
        assert steps.startswith(f"      - name: {CLEAN}\n"), job
        clean = _step(block(job), CLEAN)
        order = [
            clean.index(line)
            for line in (
                "set -euo pipefail",
                # One runner per host is what makes the removal below safe.
                "runners=$(pgrep -cx Runner.Listener || true)",
                '[ "${runners:-0}" -le 1 ] || { echo "::error::$runners runner'
                " services on the host of $RUNNER_NAME; /parity-caches needs one"
                ' runner per host"; exit 1; }',
                "docker ps -aq --filter label=vepyr-wgs=1 | xargs -r docker rm -f",
                'if ! find "$GITHUB_WORKSPACE" -mindepth 1 -delete 2>/dev/null; then',
                'docker run --rm --network none -v "$GITHUB_WORKSPACE:/w"'
                ' --entrypoint find "$BUILD_IMAGE" /w -mindepth 1 -delete',
                # A workspace find cannot read counts as not empty.
                'rest=$(find "$GITHUB_WORKSPACE" -mindepth 1 -print -quit)'
                " || rest=unreadable",
                '[ -z "$rest" ] || { echo "::error::workspace not empty or not'
                ' readable after clean-up"; exit 1; }',
            )
        ]
        assert order == sorted(order), job
        # Nothing but the count of runners may fail without failing the step
        # (pgrep exits 1 when it counts none).
        assert clean.count("|| true") == 1, job
        assert "-delete 2>/dev/null || true" not in clean, job


def test_only_the_build_and_cache_jobs_check_out_the_pr():
    for job in ("command", "authorize", "report"):
        text = block(job)
        assert text.count("uses: actions/checkout@v4") == 1, job
        # No `ref:` key: the workflow's own ref, which authorize pins to master.
        assert not re.search(r"^\s+ref:", text, re.M), job
        assert "sparse-checkout" in text, job
    for job in ("build-wheel", "cache"):
        text = block(job)
        assert text.count("uses: actions/checkout@v4") == 1, job
        assert "ref: ${{ needs.authorize.outputs.sha }}" in text, job
    # No job leaves its token in .git/config on a persistent host.
    assert WF.count("persist-credentials: false") == len(JOBS)
    assert WF.count("uses: actions/checkout@v4") == len(JOBS)


def test_jobs_check_the_host_tools_they_call():
    """A missing python3 would otherwise read as "not a /parity-caches command"."""
    python = "python3 -c 'import sys; sys.exit(sys.version_info < (3, 10))'"
    for job in ("command", "authorize", "report"):
        assert python in _step(block(job), CLEAN), job
    for job in ("authorize", "report"):
        assert "command -v gh >/dev/null ||" in _step(block(job), CLEAN), job


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


def test_no_job_has_expressions_in_run_scripts():
    for job in JOBS:
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


def test_wheel_is_built_in_the_image_publish_pins_by_digest():
    image = re.search(r"^  BUILD_IMAGE: (\S+)$", WF, re.M).group(1)
    assert re.fullmatch(
        r"quay\.io/pypa/manylinux2014_x86_64@sha256:[0-9a-f]{64}", image
    )
    # The image publish_to_pypi.yml builds the x86_64 release wheel in, on the
    # same hosts: bump both digests together.
    assert f"container: {image}\n" in PUBLISH
    build = _step(block("build-wheel"), "Build the wheel in the pinned container")
    assert "uses: PyO3/maturin-action@v1" in build
    assert "container: ${{ env.BUILD_IMAGE }}" in build
    assert "sccache: false" in build
    # With `container:` set the input picks nothing (see publish_to_pypi.yml).
    assert "manylinux:" not in build
    # The wheel /parity-wgs tests: same target, profile and artifact.
    theirs = job_block("build-wheel", WGS)
    for line in (
        "target: x86_64",
        "args: --release --out dist --find-interpreter",
        "name: wheels-manylinux-x86_64",
    ):
        assert line in block("build-wheel") and line in theirs, line


def test_build_container_is_named_labelled_and_given_nothing_of_the_host():
    job = block("build-wheel")
    assert (
        "CONTAINER: vepyr-build-${{ github.run_id }}-${{ github.run_attempt }}" in job
    )
    build = _step(job, "Build the wheel in the pinned container")
    options = re.search(r"docker-options: >-\n((?: {12}\S.*\n?)+)", build).group(1)
    for flag in (
        "--name ${{ env.CONTAINER }}",
        "--label vepyr-wgs=1",
        "--security-opt no-new-privileges",
        "--cap-drop NET_RAW",
        "--cap-drop MKNOD",
        "--pids-limit 4096",
    ):
        assert flag in options, flag
    for flag in (
        "--privileged",
        "--cap-add",
        "--volume",
        "--mount",
        "--network",
        "--pid",
        "--user",
        "-v ",
        "-e ",
        "docker.sock",
    ):
        assert flag not in options.replace("--pids-limit", ""), flag
    # maturin-action forwards an ssh agent and mounts the runner user's git
    # credentials when it finds them, and calls sudo on build paths.
    assert 'SSH_AUTH_SOCK: ""' in build
    assert "XDG_CONFIG_HOME: ${{ runner.temp }}/no-config" in build
    assert 'echo "$RUNNER_TEMP/no-sudo" >> "$GITHUB_PATH"' in _step(job, CLEAN)


def test_build_job_runs_no_checked_out_code_on_host():
    def code(script: str) -> str:
        lines = script.splitlines()
        return "\n".join(ln for ln in lines if not ln.lstrip().startswith("#"))

    job = block("build-wheel")
    scripts = [code(run) for run in run_scripts(job)]
    assert len(scripts) == 4
    words = r"\b(python3?|cargo|maturin|pip3?|uv|make|bash|source)\b"
    for script in scripts:
        assert not re.search(words, script), script
    names = (
        f"- name: {CLEAN}",
        "uses: actions/checkout@v4",
        "- name: Refuse a checkout that holds a symbolic link",
        "uses: PyO3/maturin-action@v1",
        "- name: Collect the wheel",
        "uses: actions/upload-artifact@v4",
        "- name: Delete the root-owned build tree",
    )
    positions = [job.index(name) for name in names]
    assert positions == sorted(positions)
    link = _step(job, "Refuse a checkout that holds a symbolic link")
    assert '-path "$GITHUB_WORKSPACE/.git" -prune -o -type l -print -quit' in link


def test_repository_holds_no_symbolic_link():
    """build-wheel refuses a checkout that holds one."""
    git = shutil.which("git")
    if git is None or not (ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    listed = subprocess.run(
        [git, "-C", str(ROOT), "ls-files", "-s"], capture_output=True, text=True
    )
    if listed.returncode != 0:
        pytest.skip("git ls-files failed")
    assert [ln for ln in listed.stdout.splitlines() if ln.startswith("120000 ")] == []


def test_wheel_is_uploaded_from_a_host_made_directory():
    """dist/ is written by PR code as root: one checked regular file leaves it."""
    job = block("build-wheel")
    collect = _step(job, "Collect the wheel")
    kill = collect.index('docker rm -f "$CONTAINER"')
    verify = collect.index('docker ps -aq --filter "name=^${CONTAINER}')
    assert kill < verify < collect.index('"$ws"/dist/')
    checks = (
        'if [ ! -d "$ws/dist" ] || [ -L "$ws/dist" ]; then fail',
        '[ "${#wheels[@]}" -eq 1 ] || fail',
        '[[ "$name" =~ ^vepyr-[0-9A-Za-z._+-]+\\.whl$ ]] || fail',
        'if [ ! -f "$wheel" ] || [ -L "$wheel" ]; then fail',
        '[ "$(realpath -e -- "$wheel")" = "$ws/dist/$name" ] || fail',
        '[ "$size" -le $((512 * 1024 * 1024)) ] || fail',
    )
    copy = collect.index('cp -- "$wheel" "$RES/$name"')
    for check in checks:
        assert collect.index(check) < copy, check
    upload = job[job.index("uses: actions/upload-artifact@v4") :]
    assert "path: ${{ env.RES }}\n" in upload
    assert "if-no-files-found: error" in upload
    assert "path: dist" not in job
    # The same glob finds the wheel in the cache job.
    assert "vepyr-*manylinux*x86_64*.whl" in collect
    assert "vepyr-*manylinux*x86_64*.whl" in block("cache")


def test_build_tree_is_deleted_as_root_even_after_a_failure():
    delete = _step(block("build-wheel"), "Delete the root-owned build tree")
    assert "if: always()" in delete
    kill = delete.index('docker rm -f "$CONTAINER"')
    verify = delete.index('docker ps -aq --filter "name=^${CONTAINER}')
    wipe = delete.index('--entrypoint find "$BUILD_IMAGE" /w -mindepth 1 -delete')
    empty = delete.index("workspace not empty or not readable after clean-up")
    assert kill < verify < wipe < empty


def test_cache_job_runs_no_checked_out_code_on_host():
    text = block("cache")
    assert "python3 ci/" not in text
    assert "python3 e2e-testing/" not in text
    assert "bash /src/ci/wgs-runner/run-cache-parity.sh" in text


def test_container_is_hardened_like_the_wgs_one():
    def docker_run(text: str) -> str:
        start = text.index('docker run --rm --name "$CONTAINER"')
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
    clean = _step(block("cache"), CLEAN)
    assert "docker ps -aq --filter label=vepyr-wgs=1 | xargs -r docker rm -f" in clean


def test_collect_step_is_the_parity_wgs_one():
    """Kill the container, purge scratch, then copy capped regular files only."""
    mine = _step(block("cache"), "Collect results")
    theirs = _step(job_block("profile", WGS), "Collect results")
    same = (
        mine.replace("caches/%s", "wgs/%s")
        .replace("result-caches-$TARGET", "result-wgs-$PROFILE")
        .replace('"$TARGET"', '"$PROFILE"')
        .replace(
            "          # A fenced block keeps the summary's column alignment.\n", ""
        )
    )
    summary = '          { echo "### '
    assert same.split(summary)[0] == theirs.split(summary)[0]
    kill = mine.index('docker rm -f "$CONTAINER"')
    assert kill < mine.index(PURGE) < mine.index('"$SCRATCH/')
    job_run = block("cache").index('docker run --rm --name "$CONTAINER"')
    after_run = block("cache")[job_run:]
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


def test_run_script_ends_a_failed_set_up_step_as_a_named_error():
    # Without `set -e` a failed `uv venv` or `uv pip install` would only show
    # later, as a missing interpreter (127) or a failed import.
    assert "\nset -uo pipefail\n" in RUN_SCRIPT
    function = RUN_SCRIPT[RUN_SCRIPT.index("setup_failed() {") :]
    function = function[: function.index("\n}\n")]
    assert 'echo "ERROR: $1" | tee /work/summary.txt' in function
    assert '--name "caches/$target" --outcome error' in function
    assert "--out /work/result.json" in function
    assert function.rstrip().endswith("exit 4")
    venv = RUN_SCRIPT.index("uv venv --python python3.12 /work/venv")
    install = RUN_SCRIPT.index("uv pip install --python /work/venv/bin/python")
    run = RUN_SCRIPT.index("cache_parity.py run")
    assert RUN_SCRIPT.index("setup_failed() {") < venv < install < run
    for start, end, message in (
        (venv, install, "uv venv could not create the Python 3.12 environment"),
        (install, run, "uv pip install failed for the wheel and its dependencies"),
    ):
        step = RUN_SCRIPT[start:end]
        # The step's own exit code, not that of the `tee` behind it.
        assert '2>&1 | tee -a /work/run.log\n[ "${PIPESTATUS[0]}" -eq 0 ] \\\n' in step
        assert f'|| setup_failed "{message} (see run.log)"' in step
    assert '[ -f "$wheel" ] || setup_failed "wheel not found: $wheel"' in RUN_SCRIPT
    # run.log holds the set-up output too: the comparison appends to it.
    assert RUN_SCRIPT.count("| tee /work/run.log") == 0
    assert RUN_SCRIPT.count("| tee -a /work/run.log") == 3


def test_check_script_checks_the_cache_parity_inputs():
    assert 'selectors="${2:-}"' in CHECK_SCRIPT
    call = CHECK_SCRIPT[CHECK_SCRIPT.index('ci_helpers.py" cache-inputs') :]
    call = call[: call.index("\nelse\n")]
    for argument in (
        '--data "$data"',
        "--scratch /scratch/vepyr-wgs",
        '--published "$repo/src/vepyr/_published.py"',
        '--manifest "$repo/e2e-testing/golden/cache-parity-manifest.json"',
        '--select "$selectors"',
    ):
        assert argument in call, argument
    # Only on a host where stage-cache-inputs.sh has staged something: a host
    # that serves /parity-wgs alone still passes.
    guard = 'if [ -d "$data/vep-cache" ] || [ -d "$data/plugin-cache" ] || [ -d "$data/plugin-source" ]; then'
    assert guard in CHECK_SCRIPT
    check = CHECK_SCRIPT.index('ci_helpers.py" cache-inputs')
    assert CHECK_SCRIPT.index(guard) < check < CHECK_SCRIPT.index('echo "runner OK')


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
