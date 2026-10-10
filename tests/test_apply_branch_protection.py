import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ci"))
import apply_branch_protection as abp  # noqa: E402

CURRENT = {  # trimmed GET .../branches/master/protection, 2026-10-10
    "required_pull_request_reviews": {
        "dismiss_stale_reviews": False,
        "require_code_owner_reviews": False,
        "require_last_push_approval": False,
        "required_approving_review_count": 1,
    },
    "enforce_admins": {"enabled": False},
    "required_linear_history": {"enabled": False},
    "allow_force_pushes": {"enabled": False},
    "allow_deletions": {"enabled": False},
    "required_conversation_resolution": {"enabled": False},
    "block_creations": {"enabled": False},
    "lock_branch": {"enabled": False},
    "allow_fork_syncing": {"enabled": False},
}


def test_payload_preserves_existing_settings():
    payload = abp.build_payload(CURRENT)
    reviews = payload["required_pull_request_reviews"]
    assert reviews["required_approving_review_count"] == 1
    assert payload["enforce_admins"] is False
    assert payload["allow_force_pushes"] is False
    assert payload["restrictions"] is None
    for key in ("block_creations", "lock_branch", "allow_fork_syncing"):
        assert payload[key] is False


def test_payload_carries_enabled_flags():
    keys = (
        "enforce_admins",
        "required_linear_history",
        "allow_force_pushes",
        "allow_deletions",
        "required_conversation_resolution",
        "block_creations",
        "lock_branch",
        "allow_fork_syncing",
    )
    payload = abp.build_payload({**CURRENT, **{k: {"enabled": True} for k in keys}})
    for key in keys:
        assert payload[key] is True, key


def test_payload_requires_every_tier():
    checks = abp.build_payload(CURRENT)["required_status_checks"]["checks"]
    contexts = {c["context"] for c in checks}
    for needed in (
        "lint",
        "test-rust",
        "vep-parity-checks",
        "windows-tests (3.12)",
        "linux-arm64-tests (3.12)",
        "nfcore / nfcore-module",
        "nfcore / nfcore-subworkflow-parity",
        "parity/vep-parity",
        "parity/integration",
    ):
        assert needed in contexts
    assert {
        f"linux-tests (x86_64, {v})" for v in ("3.10", "3.11", "3.12", "3.13", "3.14")
    } <= contexts
    assert not any(c.startswith("parity/integration/") for c in contexts)


def test_checks_come_from_github_actions():
    checks = abp.build_payload(CURRENT)["required_status_checks"]["checks"]
    assert {c["app_id"] for c in checks} == {abp.GITHUB_ACTIONS_APP_ID}
