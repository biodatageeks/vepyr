"""Static guarantees of parity-wgs.yml that review must not have to re-derive."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WF = (ROOT / ".github/workflows/parity-wgs.yml").read_text()


def job_block(name: str) -> str:
    m = re.search(rf"^  {name}:\n(.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)", WF, re.S | re.M)
    assert m, name
    return m.group(1)


def test_image_tag_matches_runner_kit():
    tag = (ROOT / "ci/wgs-runner/IMAGE").read_text().strip()
    assert f"WGS_IMAGE: {tag}" in WF


def test_only_profile_job_uses_self_hosted():
    assert WF.count("vepyr-wgs]") == 1
    assert "self-hosted" in job_block("profile")


def test_privileged_jobs_have_no_expressions_in_run_scripts():
    for job in ("authorize", "report"):
        block = job_block(job)
        for run in re.findall(r"run: \|\n((?:\s{10,}.*\n)+)", block):
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


def test_queue_not_cancel():
    assert re.search(r"cancel-in-progress: false", WF)
    assert "max-parallel: 1" in job_block("profile")


def test_profile_job_runs_no_checked_out_code_on_host():
    block = job_block("profile")
    assert "python3 ci/" not in block
    assert "python3 e2e-testing/" not in block
