"""Small helpers for the CI workflows in .github/workflows/.

Each subcommand prints its answer on stdout. When its input is not what the
workflow expects, it prints a message on stderr and exits 1, so a workflow
step fails at the cause instead of later.
Stdlib only: it runs on a bare runner before any environment exists.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import shutil
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
    """WGS profiles, including any separately declared ClinVar profiles."""
    data = json.loads(manifest.read_text())
    core = set(data.get("profiles") or {})
    clinvar = set(data.get("clinvar_profiles") or {})
    preferred = [name for name in ("merged", "ensembl", "refseq") if name in core]
    names = preferred + sorted(core - set(preferred)) + sorted(clinvar)
    if not names:
        raise CiError(f"no profiles in {manifest}")
    for name in names:
        # fullmatch: ``$`` alone would accept a trailing newline.
        if not PROFILE_NAME.fullmatch(name):
            raise CiError(f"invalid profile name: {name!r}")
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


PROFILE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")
VERDICTS = ("success", "failure")


def parse_profiles(text: str) -> list[str]:
    """The profiles JSON from the (untrusted) integration job; '' means none."""
    try:
        names = json.loads(text or "[]")
    except json.JSONDecodeError as exc:
        raise CiError(f"profiles is not valid JSON: {exc}") from exc
    if not isinstance(names, list):
        raise CiError("profiles must be a JSON list")
    for name in names:
        if not isinstance(name, str) or not PROFILE_NAME.match(name):
            raise CiError(f"invalid profile name: {name!r}")
    return names


def _load_records(results_dir: Path) -> tuple[dict[str, dict], set[str]]:
    """Valid result records by name, plus names reported more than once."""
    records: dict[str, dict] = {}
    duplicates: set[str] = set()
    for path in sorted(results_dir.rglob("result-*.json")):
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if not (
            isinstance(record, dict)
            and isinstance(record.get("name"), str)
            and isinstance(record.get("conclusion"), str)
        ):
            continue
        if record["name"] in records:
            duplicates.add(record["name"])
        records[record["name"]] = record
    return records, duplicates


def _record_state(
    records: dict[str, dict], duplicates: set[str], name: str
) -> tuple[str, str]:
    if name in duplicates:
        return "error", f"duplicate result for {name}"
    record = records.get(name)
    if record is None:
        return "error", "no result reported (job cancelled, timed out or never ran)"
    conclusion = record["conclusion"]
    summary = record.get("summary")
    summary = summary if isinstance(summary, str) and summary else conclusion
    if conclusion not in VERDICTS:
        return "error", summary
    return conclusion, summary


def _tier_rows(
    records: dict[str, dict],
    duplicates: set[str],
    *,
    context: str,
    record_prefix: str,
    profiles: list[str],
    job_result: str,
    error_wins: bool = False,
    noun: str = "profiles",
) -> list[tuple[str, str, str]]:
    """Per-profile rows plus a job-confirmed aggregate for one matrix tier.

    With `error_wins`, any profile in `error` makes the aggregate `error`, so
    stale data or a crash is never reported as a parity failure.
    """
    if not profiles:
        return [(context, "error", "profile list missing (profiles job failed)")]
    rows = []
    bad = []
    for profile in profiles:
        conclusion, summary = _record_state(
            records, duplicates, f"{record_prefix}{profile}"
        )
        rows.append((f"{context}/{profile}", conclusion, summary))
        if conclusion != "success":
            bad.append(profile)
    errored = [p for p, (_, st, _) in zip(profiles, rows) if st == "error"]
    if error_wins and errored:
        aggregate = (
            "error",
            f"{len(errored)} of {len(profiles)} {noun} errored: {', '.join(errored)}",
        )
    elif bad:
        aggregate = (
            "failure",
            f"{len(bad)} of {len(profiles)} {noun} not passing: {', '.join(bad)}",
        )
    else:
        aggregate = ("success", f"{len(profiles)} {noun} pass")
    if aggregate[0] == "success" and job_result != "success":
        aggregate = ("error", f"job result {job_result} contradicts verdict")
    rows.append((context, *aggregate))
    return rows


def _check_profiles(profiles: list[str]) -> None:
    for name in profiles:
        if not PROFILE_NAME.match(name):
            raise CiError(f"invalid profile name: {name!r}")


def statuses(
    results_dir: Path, profiles: list[str], job_results: dict[str, str]
) -> list[tuple[str, str, str]]:
    """Commit statuses for one dispatch run; a missing verdict is an error.

    Result files come from jobs that ran PR code, so they are untrusted: a
    success is only believed when the tier job's own result agrees.
    """
    _check_profiles(profiles)
    records, duplicates = _load_records(results_dir)
    vep = _record_state(records, duplicates, "vep-parity")
    if vep[0] == "success" and job_results.get("vep-parity", "unknown") != "success":
        vep = (
            "error",
            f"job result {job_results.get('vep-parity', 'unknown')} "
            "contradicts verdict",
        )
    rows = [("parity/vep-parity", *vep)]
    rows += _tier_rows(
        records,
        duplicates,
        context="parity/integration",
        record_prefix="integration/",
        profiles=profiles,
        job_result=job_results.get("integration", "unknown"),
    )
    return [(ctx, st, _clip(desc)) for ctx, st, desc in rows]


def wgs_statuses(
    results_dir: Path, profiles: list[str], job_result: str
) -> list[tuple[str, str, str]]:
    """Commit statuses for one /parity-wgs run, same trust rules as statuses()."""
    _check_profiles(profiles)
    records, duplicates = _load_records(results_dir)
    rows = _tier_rows(
        records,
        duplicates,
        context="parity/wgs",
        record_prefix="wgs/",
        profiles=profiles,
        job_result=job_result,
        error_wins=True,
    )
    return [(ctx, st, _clip(desc)) for ctx, st, desc in rows]


PARITY_COMMAND = "/parity"


def _is_command(text: str, command: str) -> bool:
    return text.rstrip(" \t\r\n") == command


def is_parity_command(text: str) -> bool:
    """True only for a comment that is exactly the /parity command.

    Trailing ASCII whitespace is ignored (str.rstrip() would also drop Unicode
    spaces such as NBSP); leading whitespace is not, matching the workflow's
    startsWith(body, '/parity') filter. Anything else, including arguments,
    quoting or a second line, is not the command.
    """
    return _is_command(text, PARITY_COMMAND)


PARITY_WGS_COMMAND = "/parity-wgs"


# A pinned commit: 7-40 lowercase hex, matched as a prefix of the PR head.
PINNED_SHA = re.compile(r"[0-9a-f]{7,40}")


def parity_wgs_pin(text: str) -> str | None:
    """Pinned SHA of a /parity-wgs comment: "" when bare, None if not the command.

    The command is exactly `/parity-wgs` or `/parity-wgs <sha>`; fork PRs must
    pin the head the admin reviewed, so a push after the comment cannot run.
    """
    text = text.rstrip(" \t\r\n")
    if text == PARITY_WGS_COMMAND:
        return ""
    head, sep, pin = text.partition(" ")
    if head == PARITY_WGS_COMMAND and sep and PINNED_SHA.fullmatch(pin):
        return pin
    return None


def is_parity_wgs_command(text: str) -> bool:
    """True only for `/parity-wgs`, optionally followed by one pinned SHA."""
    return parity_wgs_pin(text) is not None


PARITY_CACHES_COMMAND = "/parity-caches"
CACHE_SELECTOR = re.compile(r"[a-z][a-z0-9_]*")
MAX_CACHE_SELECTORS = 16
PLUGIN_PREFIX = "plugin_"
COMMIT = re.compile(r"[0-9a-f]{40}")
MEMORY_LIMIT = re.compile(r"[1-9][0-9]{0,3}g")


def parity_caches_command(text: str) -> tuple[str, list[str]] | None:
    """Pinned SHA and selectors of a /parity-caches comment; None if not the command.

    The command is `/parity-caches [<sha>] [<selector> ...]` on one line, the
    words separated by single spaces. The SHA is pinned as for /parity-wgs and
    must come first: a first word of 7-40 hex characters is the pin, never a
    selector. Selectors (`core`, `plugins`, a cache type, a plugin name) are
    only checked for shape here; `cache_targets` resolves them.
    """
    head, *words = text.rstrip(" \t\r\n").split(" ")
    if head != PARITY_CACHES_COMMAND:
        return None
    pin = ""
    if words and PINNED_SHA.fullmatch(words[0]):
        pin, words = words[0], words[1:]
    if len(words) > MAX_CACHE_SELECTORS:
        return None
    if any(not CACHE_SELECTOR.fullmatch(word) for word in words):
        return None
    return pin, words


def published_pins(published_py: Path) -> dict[tuple[int, str, str], str]:
    """`PINS` of src/vepyr/_published.py, read without importing vepyr."""
    for node in ast.parse(published_py.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.AnnAssign):
            target = node.target
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
        else:
            continue
        if not (isinstance(target, ast.Name) and target.id == "PINS" and node.value):
            continue
        try:
            pins = ast.literal_eval(node.value)
        except ValueError as exc:
            raise CiError(f"PINS in {published_py} is not a literal: {exc}") from exc
        if not isinstance(pins, dict):
            break
        for key, commit in pins.items():
            if not (
                isinstance(key, tuple)
                and len(key) == 3
                and isinstance(key[0], int)
                and isinstance(key[1], str)
                and isinstance(key[2], str)
                and isinstance(commit, str)
                and COMMIT.fullmatch(commit)
            ):
                raise CiError(f"unexpected PINS entry in {published_py}: {key!r}")
        return pins
    raise CiError(f"no PINS dictionary in {published_py}")


def _cache_limits(manifest: dict, target: str) -> dict:
    """Job timeout and container memory of one target, from the parity manifest."""
    is_plugin = target.startswith(PLUGIN_PREFIX)
    limits = dict(
        (manifest.get("limits") or {}).get("plugin" if is_plugin else "core") or {}
    )
    section = manifest.get("plugins" if is_plugin else "core") or {}
    entry = section.get(target.removeprefix(PLUGIN_PREFIX)) or {}
    limits.update({k: entry[k] for k in ("timeout_minutes", "memory") if k in entry})
    timeout, memory = limits.get("timeout_minutes"), limits.get("memory")
    if not (
        isinstance(timeout, int)
        and not isinstance(timeout, bool)
        and 1 <= timeout <= 7200
    ):
        raise CiError(f"{target}: timeout_minutes must be 1-7200, got {timeout!r}")
    if not (isinstance(memory, str) and MEMORY_LIMIT.fullmatch(memory)):
        raise CiError(f"{target}: memory must look like '22g', got {memory!r}")
    return {"timeout": timeout, "memory": memory}


def cache_targets(
    pins: dict[tuple[int, str, str], str], manifest: dict, selectors: list[str]
) -> dict:
    """The /parity-caches matrix: one target per pinned cache and plugin cache.

    Targets are the names pinned for the manifest's release and assembly: a
    cache type, or `plugin_<name>`. A plugin the manifest marks `opt_in` runs
    only when a selector names it. `aggregate` is true when the selection
    covers every default target, so `parity/caches` may speak for the whole
    set; a narrower run reports only its own targets.
    """
    release, assembly = manifest.get("release"), manifest.get("assembly")
    names = sorted(n for (r, a, n) in pins if r == release and a == assembly)
    for name in names:
        if not PROFILE_NAME.fullmatch(name):
            raise CiError(f"invalid cache name: {name!r}")
    core = [n for n in names if not n.startswith(PLUGIN_PREFIX)]
    plugins = [n for n in names if n.startswith(PLUGIN_PREFIX)]
    if not names:
        raise CiError(f"no cache is pinned for release {release} {assembly}")
    entries = manifest.get("plugins") or {}
    opt_in = {
        n
        for n in plugins
        if (entries.get(n.removeprefix(PLUGIN_PREFIX)) or {}).get("opt_in")
    }
    default = [n for n in names if n not in opt_in]
    chosen: set[str] = set() if selectors else set(default)
    for selector in selectors:
        if not CACHE_SELECTOR.fullmatch(selector):
            raise CiError(f"invalid selector: {selector!r}")
        if selector == "core":
            chosen.update(core)
        elif selector == "plugins":
            chosen.update(n for n in plugins if n not in opt_in)
        elif selector in names:
            chosen.add(selector)
        elif PLUGIN_PREFIX + selector in plugins:
            chosen.add(PLUGIN_PREFIX + selector)
        else:
            known = [
                "core",
                "plugins",
                *core,
                *(p.removeprefix(PLUGIN_PREFIX) for p in plugins),
            ]
            raise CiError(
                f"unknown selector '{selector}'; use one of: {', '.join(known)}"
            )
    targets = [n for n in (*core, *plugins) if n in chosen]
    if not targets:
        raise CiError("the selection is empty")
    return {
        "targets": targets,
        "aggregate": set(default) <= set(targets),
        "matrix": {
            "include": [{"target": n, **_cache_limits(manifest, n)} for n in targets]
        },
    }


def published_org(published_py: Path) -> str:
    """`HF_ORG` of src/vepyr/_published.py, read without importing vepyr."""
    for node in ast.parse(published_py.read_text(encoding="utf-8")).body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "HF_ORG"
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
            and PROFILE_NAME.fullmatch(node.value.value)
        ):
            return node.value.value
    raise CiError(f"no HF_ORG string in {published_py}")


def _load(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def cache_inputs(
    data: Path,
    org: str,
    pins: dict[tuple[int, str, str], str],
    manifest: dict,
    targets: list[str],
    scratch_free_gb: float | None = None,
) -> tuple[list[str], list[str]]:
    """What /parity-caches jobs for `targets` would not find on this host.

    Returns `(problems, warnings)`. A problem is what ends a job with exit
    code 3 before it builds (e2e-testing/scripts/cache_parity.py: the
    `.revision` marker of a staged core cache, the raw Ensembl VEP cache, the
    staged plugin sources, the free scratch space). A warning is what the job
    makes up for: a published plugin cache it downloads.
    """
    release, assembly = manifest.get("release"), manifest.get("assembly")
    problems: list[str] = []
    warnings: list[str] = []

    def add(message: str) -> None:
        if message not in problems:
            problems.append(message)

    def staged_core(cache_type: str, why: str) -> None:
        cache = data / "cache" / f"{release}_{assembly}_{cache_type}"
        pin = pins.get((release, assembly, cache_type))
        repo_id = f"{org}/vepyr_{release}_{assembly}_{cache_type}"
        want = {"repo_id": repo_id, "revision": pin}
        if pin is None:
            add(f"{cache_type} ({why}) has no pinned revision")
        elif _load(cache / ".revision") != want:
            add(
                f"{cache} ({why}) is not staged at {want['repo_id']}@{pin}: re-run "
                "ci/wgs-runner/stage-data.sh"
            )

    def scratch(target: str, entry: dict) -> None:
        need = entry.get("scratch_gb")
        if need and scratch_free_gb is not None and scratch_free_gb < need:
            add(
                f"{target} needs {need} GiB of scratch; {scratch_free_gb:.0f} GiB "
                "are free"
            )

    restage = "re-run ci/wgs-runner/stage-cache-inputs.sh"
    for target in targets:
        if not target.startswith(PLUGIN_PREFIX):
            entry = (manifest.get("core") or {}).get(target) or {}
            staged_core(target, "the published cache to compare with")
            raw = entry.get("raw")
            if not raw:
                add(f"{target} has no raw cache path in the parity manifest")
            elif not (data / raw / "info.txt").is_file():
                add(f"raw Ensembl VEP cache missing: {data / raw}/info.txt; {restage}")
            scratch(target, entry)
            continue
        plugin = target.removeprefix(PLUGIN_PREFIX)
        entry = (manifest.get("plugins") or {}).get(plugin) or {}
        staged_core(
            manifest.get("plugin_tiering_cache", "merged"), "tiers of plugin caches"
        )
        pin = pins.get((release, assembly, target))
        repo_id = f"{org}/vepyr_{release}_{assembly}_{target}"
        cache = data / "plugin-cache" / f"{release}_{assembly}" / "plugin" / plugin
        sources = data / "plugin-source" / plugin
        published = _load(cache / "manifest.json")
        if _load(cache / ".revision") != {"repo_id": repo_id, "revision": pin}:
            warnings.append(
                f"published {plugin} cache is not staged at {repo_id}@{pin}: the job "
                f"downloads it; {restage} to avoid that"
            )
            # The job reads the source names from the downloaded manifest.json.
            if not sources.is_dir() or not any(sources.iterdir()):
                add(f"no staged source file for {plugin} under {sources}; {restage}")
        elif not isinstance(published, dict) or not published.get("sources"):
            add(f"{cache}/manifest.json lists no sources; {restage}")
        else:
            for source in published["sources"]:
                names = [source.get("file"), (source.get("index") or {}).get("file")]
                for name in filter(None, names):
                    if not (sources / name).is_file():
                        add(
                            f"staged plugin source missing: {sources / name}; {restage}"
                        )
        scratch(target, entry)
    return problems, warnings


def caches_statuses(
    results_dir: Path, targets: list[str], job_result: str, aggregate: bool
) -> list[tuple[str, str, str]]:
    """Commit statuses for one /parity-caches run, same trust rules as statuses().

    Without `aggregate` (a run of selected targets) there is no `parity/caches`
    row. The per-target rows then carry the verdict, so they are not believed
    when every one claims success but the matrix job did not succeed.
    """
    _check_profiles(targets)
    records, duplicates = _load_records(results_dir)
    rows = _tier_rows(
        records,
        duplicates,
        context="parity/caches",
        record_prefix="caches/",
        profiles=targets,
        job_result=job_result,
        error_wins=True,
        noun="targets",
    )
    if not aggregate and targets:
        rows = rows[:-1]
        if job_result != "success" and all(state == "success" for _, state, _ in rows):
            contradiction = f"job result {job_result} contradicts verdict"
            rows = [(ctx, "error", contradiction) for ctx, _, _ in rows]
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
    p.add_argument("--vep-parity-job", required=True, help="needs.vep-parity.result")
    p.add_argument("--integration-job", required=True, help="needs.integration.result")
    p = sub.add_parser("is-parity-command", help="exit 0 iff the text is /parity")
    p.add_argument("--env", help="read the text from this variable, not stdin")
    p = sub.add_parser(
        "is-parity-wgs-command", help="exit 0 iff the text is /parity-wgs"
    )
    p.add_argument("--env", help="read the text from this variable, not stdin")
    p = sub.add_parser(
        "parity-wgs-pin",
        help="print the pinned SHA of a /parity-wgs comment (empty if bare); "
        "exit 1 if the text is not the command",
    )
    p.add_argument("--env", help="read the text from this variable, not stdin")
    p = sub.add_parser("wgs-statuses")
    p.add_argument("--results-dir", type=Path, required=True)
    p.add_argument("--profiles", default="[]", help="JSON list (may be empty)")
    p.add_argument("--job", required=True, help="needs.<job>.result")
    p = sub.add_parser(
        "parity-caches-command",
        help="print pin= and select= of a /parity-caches comment; "
        "exit 1 if the text is not the command",
    )
    p.add_argument("--env", help="read the text from this variable, not stdin")
    p = sub.add_parser(
        "cache-targets", help="print targets=, matrix= and aggregate= for a selection"
    )
    p.add_argument("--published", type=Path, required=True, help="_published.py")
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--select", default="", help="selectors separated by spaces")
    p = sub.add_parser(
        "cache-inputs",
        help="exit 1 and list what /parity-caches jobs would not find on this host",
    )
    p.add_argument("--data", type=Path, required=True, help="staged data root")
    p.add_argument("--published", type=Path, required=True, help="_published.py")
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--select", default="", help="selectors separated by spaces")
    p.add_argument("--scratch", type=Path, help="directory the jobs use as scratch")
    p = sub.add_parser("caches-statuses")
    p.add_argument("--results-dir", type=Path, required=True)
    p.add_argument("--targets", default="[]", help="JSON list (may be empty)")
    p.add_argument("--job", required=True, help="needs.<job>.result")
    p.add_argument("--aggregate", required=True, choices=["true", "false"])
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
            names = parse_profiles(args.profiles)
            jobs = {
                "vep-parity": args.vep_parity_job,
                "integration": args.integration_job,
            }
            for ctx, st, desc in statuses(args.results_dir, names, jobs):
                print(f"{ctx}\t{st}\t{desc}")
        elif args.command == "is-parity-command":
            text = os.environ.get(args.env, "") if args.env else sys.stdin.read()
            if not is_parity_command(text):
                raise CiError("not the /parity command")
        elif args.command == "is-parity-wgs-command":
            text = os.environ.get(args.env, "") if args.env else sys.stdin.read()
            if not is_parity_wgs_command(text):
                raise CiError("not the /parity-wgs command")
        elif args.command == "parity-wgs-pin":
            text = os.environ.get(args.env, "") if args.env else sys.stdin.read()
            pin = parity_wgs_pin(text)
            if pin is None:
                raise CiError("not the /parity-wgs command")
            print(pin)
        elif args.command == "wgs-statuses":
            names = parse_profiles(args.profiles)
            for ctx, st, desc in wgs_statuses(args.results_dir, names, args.job):
                print(f"{ctx}\t{st}\t{desc}")
        elif args.command == "parity-caches-command":
            text = os.environ.get(args.env, "") if args.env else sys.stdin.read()
            parsed = parity_caches_command(text)
            if parsed is None:
                raise CiError("not the /parity-caches command")
            print(f"pin={parsed[0]}")
            print(f"select={' '.join(parsed[1])}")
        elif args.command == "cache-targets":
            try:
                manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise CiError(f"cannot read {args.manifest}: {exc}") from exc
            selection = cache_targets(
                published_pins(args.published), manifest, args.select.split()
            )
            print(f"targets={json.dumps(selection['targets'])}")
            print(f"matrix={json.dumps(selection['matrix'])}")
            print(f"aggregate={'true' if selection['aggregate'] else 'false'}")
        elif args.command == "cache-inputs":
            try:
                manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise CiError(f"cannot read {args.manifest}: {exc}") from exc
            pins = published_pins(args.published)
            targets = cache_targets(pins, manifest, args.select.split())["targets"]
            free = None
            if args.scratch is not None:
                try:
                    free = shutil.disk_usage(args.scratch).free / 2**30
                except OSError as exc:
                    raise CiError(f"cannot read {args.scratch}: {exc}") from exc
            problems, warnings = cache_inputs(
                args.data, published_org(args.published), pins, manifest, targets, free
            )
            for warning in warnings:
                print(f"WARNING: {warning}", file=sys.stderr)
            if problems:
                raise CiError(
                    f"{len(problems)} cache parity input(s) missing for "
                    f"{', '.join(targets)}:\n- " + "\n- ".join(problems)
                )
            print(f"cache parity inputs present for: {' '.join(targets)}")
        elif args.command == "caches-statuses":
            names = parse_profiles(args.targets)
            rows = caches_statuses(
                args.results_dir, names, args.job, args.aggregate == "true"
            )
            for ctx, st, desc in rows:
                print(f"{ctx}\t{st}\t{desc}")
    except CiError as exc:
        print(f"ci_helpers: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
