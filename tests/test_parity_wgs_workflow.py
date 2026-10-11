"""Static guarantees of parity-wgs.yml that review must not have to re-derive."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WF = (ROOT / ".github/workflows/parity-wgs.yml").read_text()
PARITY_TESTS = (ROOT / ".github/workflows/parity-tests.yml").read_text()
PURGE = r'find "$SCRATCH" -mindepth 1 \( -type l -o ! -type f ! -type d \) -delete'


def job_block(name: str, wf: str = WF) -> str:
    m = re.search(
        rf"^  {name}:\n(.*?)(?=^  [a-z][a-z0-9-]*:\n|^\S|\Z)", wf, re.S | re.M
    )
    assert m, name
    return m.group(1)


def run_scripts(block: str) -> list[str]:
    """Every `run:` body in a job block, inline or block scalar."""
    lines = block.splitlines()
    scripts = []
    for i, line in enumerate(lines):
        m = re.match(r"^(\s*)(?:- )?run:\s*(.*)$", line)
        if not m:
            continue
        indent, rest = len(m.group(1)), m.group(2)
        if rest and rest[0] not in "|>":
            scripts.append(rest)
            continue
        body = []
        for nxt in lines[i + 1 :]:
            if nxt.strip() and len(nxt) - len(nxt.lstrip()) <= indent:
                break
            body.append(nxt)
        scripts.append("\n".join(body))
    return scripts


def test_run_scripts_parser_sees_inline_and_block_runs():
    block = "    steps:\n      - run: echo ${{ x }}\n      - run: |\n          a\n          b\n      - uses: y\n"
    assert run_scripts(block) == ["echo ${{ x }}", "          a\n          b"]


def test_image_tag_matches_runner_kit():
    tag = (ROOT / "ci/wgs-runner/IMAGE").read_text().strip()
    assert f"WGS_IMAGE: {tag}" in WF


def test_only_profile_job_uses_self_hosted():
    assert WF.count("vepyr-wgs]") == 1
    assert "self-hosted" in job_block("profile")


def test_privileged_jobs_have_no_expressions_in_run_scripts():
    for job in ("authorize", "report"):
        scripts = run_scripts(job_block(job))
        assert scripts, job
        for run in scripts:
            assert "${{" not in run, (job, run)


def test_write_scopes_only_on_authorize_and_report():
    for job in ("command", "build-wheel", "profile"):
        block = job_block(job)
        assert (
            "statuses: write" not in block
            and "issues: write" not in block
            and "pull-requests: write" not in block
        )


def test_top_level_permissions_empty_and_master_only():
    assert re.search(r"^permissions: \{\}$", WF, re.M)
    assert "refs/heads/master" in job_block("authorize")


def test_no_concurrency_group_runner_serialises():
    # A group keeps one pending run, so a third request would evict a queued one.
    assert not re.search(r"^\s*concurrency:", WF, re.M)
    assert "cancel-in-progress" not in WF
    assert "max-parallel: 1" in job_block("profile")


def test_profile_job_runs_no_checked_out_code_on_host():
    block = job_block("profile")
    assert "python3 ci/" not in block
    assert "python3 e2e-testing/" not in block


def test_profile_job_purges_scratch_before_reading_it():
    block = job_block("profile")
    after_run = block[block.index("docker run --rm") :]
    purge = after_run.index(PURGE)
    reads = [
        after_run.find(s)
        for s in ('"$SCRATCH/', "$SCRATCH/out", "env.SCRATCH }}/")
        if after_run.find(s) != -1
    ]
    assert reads and purge < min(reads)
    assert "env.SCRATCH" not in after_run  # nothing uploaded from scratch


def test_profile_job_writes_and_uploads_only_outside_workspace():
    block = job_block("profile")
    for run in run_scripts(block):
        for target in re.findall(r"(?<![0-9&])>>?\s*(\S+)", run):
            assert target.startswith(('"$', "/dev/null", "&")), (target, run)
    uploads = re.findall(
        r"uses: actions/upload-artifact@v4.*?path: (.*?)\n(?=\s*(?:if-no|- |$))",
        block,
        re.S,
    )
    assert len(uploads) == 2
    for path in uploads:
        assert path.startswith("${{ env.RES }}/"), path
    assert 'mktemp -d "$RUNNER_TEMP/' in block


def test_profile_container_has_memory_cap():
    assert "--memory 56g --memory-swap 56g" in job_block("profile")


def test_parity_tests_ignores_parity_wgs_comments():
    head = PARITY_TESTS[: PARITY_TESTS.index("\njobs:\n")]
    conc = head[head.index("concurrency:") :]
    assert "!startsWith(github.event.comment.body, '/parity-wgs')" in conc
    command_if = job_block("command", PARITY_TESTS).split("runs-on:")[0]
    assert "!startsWith(github.event.comment.body, '/parity-wgs')" in command_if


def _step(block: str, name: str) -> str:
    start = block.index(f"- name: {name}")
    nxt = block.find("\n      - ", start + 1)
    return block[start : nxt if nxt != -1 else len(block)]


def test_collect_kills_container_before_touching_scratch():
    collect = _step(job_block("profile"), "Collect results")
    kill = collect.index('docker rm -f "$CONTAINER"')
    verify = collect.index('docker ps -aq --filter "name=^${CONTAINER}')
    assert kill < verify < collect.index(PURGE)
    assert collect.index(PURGE) < collect.index('"$SCRATCH/')


def test_container_is_named_and_labelled_via_env():
    block = job_block("profile")
    assert (
        "CONTAINER: vepyr-wgs-${{ github.run_id }}-${{ github.run_attempt }}-${{ matrix.profile }}"
        in block
    )
    assert 'docker run --rm --name "$CONTAINER" --label vepyr-wgs=1' in block


def test_leftover_containers_removed_at_job_start():
    clean = _step(job_block("profile"), "Clean leftovers from earlier jobs")
    assert "docker ps -aq --filter label=vepyr-wgs=1 | xargs -r docker rm -f" in clean


def test_take_has_realpath_guard_and_total_cap():
    collect = _step(job_block("profile"), "Collect results")
    assert 'real_scratch=$(realpath -e -- "$SCRATCH")' in collect
    assert 'real=$(realpath -e -- "$1")' in collect
    assert 'case "$real" in "$real_scratch"/*)' in collect
    assert "budget=$((200 * 1024 * 1024))" in collect


def test_run_profile_outcome_strings_match_run_comparison():
    # run-profile.sh tells a mismatch (failure) from a crash (error) by these
    # exact run_comparison lines; rewording either turns mismatches into errors.
    script = (ROOT / "ci/wgs-runner/run-profile.sh").read_text()
    cli = (ROOT / "e2e-testing/scripts/comparison/cli.py").read_text()
    for marker in ("FAIL: body digest differs", "FAILED contigs"):
        assert marker in script
        assert marker in cli
    assert "grep -q 'FAIL: body digest differs' /work/run.log" in script
