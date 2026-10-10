"""Require the four CI tiers on master, keeping the other protection settings.

Carried over from the current protection: the pull-request review settings
(dismiss_stale_reviews, require_code_owner_reviews, require_last_push_approval,
required_approving_review_count), enforce_admins, required_linear_history,
allow_force_pushes, allow_deletions, required_conversation_resolution,
block_creations, lock_branch and allow_fork_syncing. Replaced: the required
status checks (REQUIRED below, strict=False) and push restrictions (none).
Signed-commit protection has its own endpoint and is left untouched.

Dry run by default (prints the PUT payload). --apply sends it. Apply only after
each check below has reported at least once: GitHub can only require a check
it has already seen.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

REPO = "biodatageeks/vepyr"
BRANCH = "master"
GITHUB_ACTIONS_APP_ID = 15368
REQUIRED = [
    "lint",
    "test-rust",
    *(f"linux-tests (x86_64, {v})" for v in ("3.10", "3.11", "3.12", "3.13", "3.14")),
    "windows-tests (3.12)",
    "linux-arm64-tests (3.12)",
    "vep-parity-checks",
    "nfcore / nfcore-module",
    "nfcore / nfcore-subworkflow-parity",
    "parity/vep-parity",
    "parity/integration",
]


def _enabled(current: dict, key: str) -> bool:
    return bool((current.get(key) or {}).get("enabled", False))


def build_payload(current: dict) -> dict:
    reviews = current.get("required_pull_request_reviews")
    if reviews is not None:
        reviews = {
            k: reviews[k]
            for k in (
                "dismiss_stale_reviews",
                "require_code_owner_reviews",
                "require_last_push_approval",
                "required_approving_review_count",
            )
            if k in reviews
        }
    return {
        "required_status_checks": {
            "strict": False,
            "checks": [
                {"context": c, "app_id": GITHUB_ACTIONS_APP_ID} for c in REQUIRED
            ],
        },
        "enforce_admins": _enabled(current, "enforce_admins"),
        "required_pull_request_reviews": reviews,
        "restrictions": None,
        "required_linear_history": _enabled(current, "required_linear_history"),
        "allow_force_pushes": _enabled(current, "allow_force_pushes"),
        "allow_deletions": _enabled(current, "allow_deletions"),
        "required_conversation_resolution": _enabled(
            current, "required_conversation_resolution"
        ),
        "block_creations": _enabled(current, "block_creations"),
        "lock_branch": _enabled(current, "lock_branch"),
        "allow_fork_syncing": _enabled(current, "allow_fork_syncing"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="send the payload (default: print it)"
    )
    args = parser.parse_args(argv)
    path = f"repos/{REPO}/branches/{BRANCH}/protection"
    current = json.loads(subprocess.check_output(["gh", "api", path]))
    payload = build_payload(current)
    print(json.dumps(payload, indent=2))
    if args.apply:
        subprocess.run(
            ["gh", "api", "-X", "PUT", path, "--input", "-"],
            input=json.dumps(payload),
            text=True,
            check=True,
        )
        print(f"applied to {REPO}@{BRANCH}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
