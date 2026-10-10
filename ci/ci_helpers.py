"""Small helpers for the CI workflows in .github/workflows/.

Each subcommand prints its answer on stdout. When its input is not what the
workflow expects, it prints a message on stderr and exits 1, so a workflow
step fails at the cause instead of later.
Stdlib only: it runs on a bare runner before any environment exists.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

WHEEL = re.compile(r"^vepyr-(?P<version>[^-]+)-[^-]+-[^-]+-.*manylinux.*x86_64\.whl$")
CONCLUSIONS = {"success": "success", "failure": "failure"}


class CiError(Exception):
    """An input the workflow cannot proceed with."""


def find_wheel(directory: Path) -> Path:
    """The one manylinux x86_64 vepyr wheel under ``directory``, as an absolute path.

    Absolute because callers may run from another working directory (the porting
    run changes into porting-tests/ before it reads the wheel).
    """
    found = sorted(
        p.resolve() for p in directory.rglob("vepyr-*.whl") if WHEEL.match(p.name)
    )
    if len(found) != 1:
        names = [p.name for p in found]
        raise CiError(
            f"expected exactly one manylinux x86_64 vepyr wheel under {directory}, "
            f"found {len(found)}: {names}"
        )
    return found[0]


def wheel_version(wheel: Path) -> str:
    match = WHEEL.match(wheel.name)
    if not match:
        raise CiError(f"not a manylinux x86_64 vepyr wheel: {wheel.name}")
    return match["version"]


def result_record(name: str, outcome: str, summary: str) -> dict:
    """A verdict for the dispatch reporter; anything but success/failure is an error."""
    return {
        "name": name,
        "conclusion": CONCLUSIONS.get(outcome, "error"),
        "summary": summary,
    }


def profiles(manifest: Path) -> list[str]:
    """Integration profiles = the golden manifest's ``profiles`` keys."""
    names = sorted(json.loads(manifest.read_text()).get("profiles") or {})
    if not names:
        raise CiError(f"no profiles in {manifest}")
    return names


def conda_specs(environment_yml: Path) -> list[str]:
    """The module's conda dependencies minus vepyr (installed from the wheel).

    A line-level reader for the nf-core environment.yml layout (no PyYAML on a
    bare runner): ``- spec`` items under the top-level ``dependencies:`` key.
    """
    specs, in_deps = [], False
    for line in environment_yml.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not line.startswith((" ", "-")) or stripped == "---":
            in_deps = stripped == "dependencies:"
            continue
        if in_deps and stripped.startswith("- "):
            spec = stripped[2:].strip()
            name = spec.split("::")[-1].split("=")[0].split("<")[0].split(">")[0]
            if name != "vepyr":
                specs.append(spec)
    if not specs:
        raise CiError(f"no non-vepyr dependencies in {environment_yml}")
    return specs


def set_snapshot_version(text: str, version: str) -> str:
    """Point every ``[process, "vepyr", version]`` entry at ``version``.

    Only the vepyr version changes; md5s and every other entry stay strict.
    """
    data = json.loads(text)
    hits = 0

    def walk(node):
        nonlocal hits
        if isinstance(node, list):
            if (
                len(node) == 3
                and all(isinstance(x, str) for x in node)
                and node[1] == "vepyr"
            ):
                node[2] = version
                hits += 1
                return
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            for item in node.values():
                walk(item)

    walk(data)
    if hits == 0:
        raise CiError('no [process, "vepyr", version] entry in the snapshot')
    return json.dumps(data, indent=4) + "\n"


STATUS_LIMIT = 140  # GitHub's commit status description limit


def _clip(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= STATUS_LIMIT else text[: STATUS_LIMIT - 1] + "…"


def statuses(results_dir: Path, profiles: list[str]) -> list[tuple[str, str, str]]:
    """Commit statuses for one dispatch run; a missing verdict is an error."""
    records = {}
    for path in sorted(results_dir.rglob("result-*.json")):
        record = json.loads(path.read_text())
        records[record["name"]] = record

    def state(name: str) -> tuple[str, str]:
        record = records.get(name)
        if record is None:
            return "error", "no result reported (job cancelled, timed out or never ran)"
        return record["conclusion"], record.get("summary") or record["conclusion"]

    rows = [("parity/porting", *state("porting"))]
    if not profiles:
        rows.append(
            (
                "parity/integration",
                "error",
                "profile list missing (profiles job failed)",
            )
        )
    else:
        bad = []
        for profile in profiles:
            conclusion, summary = state(f"integration/{profile}")
            rows.append((f"parity/integration/{profile}", conclusion, summary))
            if conclusion != "success":
                bad.append(profile)
        if bad:
            rows.append(
                (
                    "parity/integration",
                    "failure",
                    f"{len(bad)} of {len(profiles)} profiles not passing: {', '.join(bad)}",
                )
            )
        else:
            rows.append(
                ("parity/integration", "success", f"{len(profiles)} profiles pass")
            )
    return [(ctx, st, _clip(desc)) for ctx, st, desc in rows]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ci_helpers.py")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("find-wheel")
    p.add_argument("directory", type=Path)
    p = sub.add_parser("wheel-version")
    p.add_argument("wheel", type=Path)
    p = sub.add_parser("result")
    p.add_argument("--name", required=True)
    p.add_argument("--outcome", required=True)
    p.add_argument("--summary", default="")
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("profiles")
    p.add_argument("manifest", type=Path)
    p = sub.add_parser("conda-specs")
    p.add_argument("environment_yml", type=Path)
    p = sub.add_parser("snap-version")
    p.add_argument("--snap", type=Path, required=True)
    p.add_argument("--version", required=True)
    p = sub.add_parser("statuses")
    p.add_argument("--results-dir", type=Path, required=True)
    p.add_argument("--profiles", default="[]", help="JSON list (may be empty)")
    args = parser.parse_args(argv)
    try:
        if args.command == "find-wheel":
            print(find_wheel(args.directory))
        elif args.command == "wheel-version":
            print(wheel_version(args.wheel))
        elif args.command == "result":
            record = result_record(args.name, args.outcome, args.summary)
            args.out.write_text(json.dumps(record) + "\n")
        elif args.command == "profiles":
            print(json.dumps(profiles(args.manifest)))
        elif args.command == "conda-specs":
            print(" ".join(conda_specs(args.environment_yml)))
        elif args.command == "snap-version":
            args.snap.write_text(
                set_snapshot_version(args.snap.read_text(), args.version)
            )
        elif args.command == "statuses":
            names = json.loads(args.profiles or "[]")
            for ctx, st, desc in statuses(args.results_dir, names):
                print(f"{ctx}\t{st}\t{desc}")
    except CiError as exc:
        print(f"ci_helpers: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
