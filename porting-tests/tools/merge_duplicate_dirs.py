"""Merge duplicate data-test directories into one directory each (#238).

One-off migration, idempotent and re-runnable (e.g. after #237 re-blesses the
legacy ``ensembl`` directories)::

    tools/merge_duplicate_dirs [--exclude-flavour F ...] [--index FILE] [--dry-run]
                               [DIR]

``DIR`` (default ``tests/data``) is grouped by the comparison key of
:mod:`check_unique_dirs` (input body, oracle body, ``[vepyr]``,
``[[vepyr_run]]``, ``[vep] command``). For every group of two or more:

* the *kept* directory is the first one in sort order (owner decision on #238);
  its ``input.vcf`` and ``expected_output.vcf`` are not touched;
* every member's test becomes a ``[[tests]]`` table of the kept
  ``test.toml``: a single-test member contributes ``id`` = its directory
  name, its top-level ``description`` and its ``[origin]`` keys; a member that
  already has ``[[tests]]`` tables contributes them unchanged. The kept
  member's own tests come first, the others follow in sort order;
* the kept ``test.toml`` loses ``[origin]``, gets a generated top-level
  ``description`` and the ``[[tests]]`` tables at its end; every other line
  (``[input]``, ``[vep]``, ``[vepyr]``, ``[compare]``, comments) is kept as is;
* the other directories are deleted.

``--exclude-flavour F`` leaves every group alone that holds a directory whose
``[vepyr] flavour`` is ``F`` (#238 merges the ``merged`` groups first and the
legacy ``ensembl`` pair after #237). ``--index FILE`` regenerates the data-test
index with ``tools/build_test_index`` after a merge.

Before anything is written, and again after, the tool verifies that the set of
test ids (directory names for single-test directories) and the set of
distinct (input body md5, oracle body md5) pairs are unchanged and that every
kept directory's ``input.vcf`` / ``expected_output.vcf`` bytes are unchanged.

Exit codes: ``0`` merged (or nothing to merge); ``1`` a verification failed or a
directory cannot be read or merged; ``2`` bad usage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tomllib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any, Final

from check_unique_dirs import (
    EXPECTED_VCF,
    INPUT_VCF,
    TEST_TOML,
    UnreadableDir,
    body_md5,
    duplicate_groups,
    test_dirs,
)

__all__ = ["MergeError", "Snapshot", "main", "merge_group", "render_test_toml"]

DEFAULT_ROOT: Final[Path] = Path("tests/data")
BUILD_INDEX: Final[Path] = Path(__file__).resolve().parent / "build_test_index"
ORIGIN_KEYS: Final[tuple[str, ...]] = ("vep_test",)
"""``[origin]`` keys, in the order a ``[[tests]]`` table lists them."""
TEST_ORDER: Final[tuple[str, ...]] = (
    "id",
    "description",
    *ORIGIN_KEYS,
)
"""Scalar ``[[tests]]`` keys in render order; ``focus`` (a table) comes last."""
_HEADER: Final[re.Pattern[str]] = re.compile(r"^\s*\[\[?\s*([^\]]+?)\s*\]\]?\s*(#.*)?$")
_DESCRIPTION: Final[re.Pattern[str]] = re.compile(r"^description\s*=")


class MergeError(Exception):
    """A group cannot be merged safely (exit code 1)."""


type NamedTest = dict[str, Any]


def _load(test_dir: Path) -> dict[str, Any]:
    """Parse ``test_dir/test.toml``."""
    try:
        return tomllib.loads((test_dir / TEST_TOML).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise MergeError(f"{test_dir / TEST_TOML}: {exc}") from exc


def tests_of(test_dir: Path, doc: Mapping[str, Any]) -> list[NamedTest]:
    """Return the tests one directory stands for.

    Args:
        test_dir: The data-test directory.
        doc: Its parsed ``test.toml``.

    Returns:
        Its ``[[tests]]`` tables, or the one test made of its directory
        name, top-level ``description`` and ``[origin]`` keys.

    Raises:
        MergeError: Neither ``[origin]`` nor ``[[tests]]`` is present.
    """
    if props := doc.get("tests"):
        return [dict(p) for p in props]
    origin = doc.get("origin")
    if not isinstance(origin, dict):
        raise MergeError(f"{test_dir}: no [origin] and no [[tests]]")
    return [
        {
            "id": test_dir.name,
            "description": doc.get("description", ""),
            **{k: origin[k] for k in ORIGIN_KEYS if k in origin},
        }
    ]


def _value(value: Any) -> str:
    """Render a TOML value: string, integer, boolean, array or inline table."""
    match value:
        case bool():
            return "true" if value else "false"
        case int():
            return str(value)
        case str():
            return json.dumps(value, ensure_ascii=False)
        case list():
            return "[" + ", ".join(_value(v) for v in value) + "]"
        case dict():
            return (
                "{ " + ", ".join(f"{k} = {_value(v)}" for k, v in value.items()) + " }"
            )
        case _:
            raise MergeError(f"cannot render {value!r} as TOML")


def _render_test(prop: NamedTest) -> str:
    """Render one ``[[tests]]`` table (known keys first, others in order)."""
    keys = [k for k in TEST_ORDER if k in prop] + [
        k for k in prop if k not in TEST_ORDER
    ]
    return "[[tests]]\n" + "".join(f"{k} = {_value(prop[k])}\n" for k in keys)


def _sections(lines: Sequence[str]) -> Iterable[tuple[str | None, int, int]]:
    """Yield ``(header name, start, end)`` for the preamble and each table."""
    starts = [i for i, ln in enumerate(lines) if _HEADER.match(ln)]
    bounds = [0, *starts, len(lines)]
    for start, end in pairwise(bounds):
        m = _HEADER.match(lines[start]) if start in starts else None
        yield (m.group(1) if m else None), start, end


def render_test_toml(text: str, tests: Sequence[NamedTest], description: str) -> str:
    """Rewrite a ``test.toml`` into a multi-test one, line by line.

    ``[origin]`` and any ``[[tests]]`` table (with its sub-tables) are
    removed, the top-level ``description`` line is replaced, and ``tests``
    are appended at the end. All other lines are kept byte for byte.

    Args:
        text: The kept directory's ``test.toml``.
        tests: Every test of the merged group, in order.
        description: The new top-level description.

    Returns:
        The new document; it is parsed back and checked by the caller.
    """
    lines = text.splitlines(keepends=True)
    kept: list[str] = []
    for name, start, end in _sections(lines):
        if name is not None and (name == "origin" or name.startswith("test")):
            continue
        chunk = lines[start:end]
        if name is None:
            chunk = [
                f"description = {_value(description)}\n"
                if _DESCRIPTION.match(ln)
                else ln
                for ln in chunk
            ]
        kept.extend(chunk)
    while kept and not kept[-1].strip():
        kept.pop()
    if kept and not kept[-1].endswith("\n"):
        kept[-1] += "\n"
    return "".join(kept) + "".join("\n" + _render_test(p) for p in tests)


def describe(n: int) -> str:
    """The generated top-level description of a merged directory."""
    return (
        f"One comparison covering {n} VEP assertion tests, one [[tests]] "
        "each: vepyr's output body must equal native VEP's (expected_output.vcf)."
    )


@dataclass(frozen=True, slots=True)
class Snapshot:
    """What a merge must preserve across a data root.

    Attributes:
        ids: Every test id (sorted, with repeats).
        pairs: The distinct (input body md5, oracle body md5) pairs.
        files: sha256 of every ``input.vcf`` / ``expected_output.vcf`` by path.
    """

    ids: tuple[str, ...]
    pairs: frozenset[tuple[str, str]]
    files: Mapping[Path, str]

    @classmethod
    def take(cls, root: Path) -> Snapshot:
        """Measure ``root``."""
        ids: list[str] = []
        pairs: set[tuple[str, str]] = set()
        files: dict[Path, str] = {}
        for d in test_dirs(root):
            ids.extend(p["id"] for p in tests_of(d, _load(d)))
            pairs.add((body_md5(d / INPUT_VCF), body_md5(d / EXPECTED_VCF)))
            for name in (INPUT_VCF, EXPECTED_VCF):
                files[d / name] = hashlib.sha256((d / name).read_bytes()).hexdigest()
        return cls(tuple(sorted(ids)), frozenset(pairs), files)


def merge_group(group: Sequence[Path], *, dry_run: bool = False) -> Path:
    """Merge one duplicate group into its first directory.

    Args:
        group: The directories, sorted by name; ``group[0]`` is kept.
        dry_run: Only verify and report; write and delete nothing.

    Returns:
        The kept directory.

    Raises:
        MergeError: NamedTest ids collide, the rewritten ``test.toml`` does not
            parse back to the intended content, or a ``[compare] body_md5``
            disagrees within the group.
    """
    kept, *others = group
    docs = {d: _load(d) for d in group}
    reasons = [docs[d].get("skip_reason") for d in group]
    if any(reason != reasons[0] for reason in reasons[1:]):
        raise MergeError(f"skip_reason differs within the group: {group}")
    md5s = {d: docs[d].get("compare", {}).get("body_md5") for d in group}
    if len(set(md5s.values())) != 1:
        raise MergeError(f"[compare] body_md5 differs within the group: {md5s}")
    tests = [p for d in group for p in tests_of(d, docs[d])]
    ids = [p["id"] for p in tests]
    if len(set(ids)) != len(ids):
        raise MergeError(f"{kept}: test ids collide: {sorted(ids)}")
    text = (kept / TEST_TOML).read_text(encoding="utf-8")
    new_text = render_test_toml(text, tests, describe(len(tests)))
    new_doc = tomllib.loads(new_text)
    want = {k: v for k, v in docs[kept].items() if k not in {"origin", "tests"}}
    want["description"] = describe(len(tests))
    want["tests"] = tests
    if new_doc != want:
        raise MergeError(f"{kept / TEST_TOML}: the rewrite does not round-trip")
    if not dry_run:
        (kept / TEST_TOML).write_text(new_text, encoding="utf-8")
        for d in others:
            shutil.rmtree(d)
    return kept


def _flavours(d: Path) -> set[str]:
    """Every ``[vepyr]`` / ``[[vepyr_run]]`` flavour of ``d``."""
    doc = _load(d)
    tables = [doc.get("vepyr", {}), *doc.get("vepyr_run", [])]
    return {t["flavour"] for t in tables if "flavour" in t}


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point; see the module docstring for the exit codes."""
    parser = argparse.ArgumentParser(
        prog="tools/merge_duplicate_dirs", description=__doc__.splitlines()[0]
    )
    parser.add_argument(
        "dir", nargs="?", type=Path, default=DEFAULT_ROOT, metavar="DIR"
    )
    parser.add_argument(
        "--exclude-flavour",
        action="append",
        default=[],
        metavar="F",
        help="leave groups holding a directory with [vepyr] flavour F alone",
    )
    parser.add_argument("--index", type=Path, help="regenerate this index afterwards")
    parser.add_argument("--dry-run", action="store_true", help="write nothing")
    args = parser.parse_args(argv)
    root: Path = args.dir
    if not root.is_dir():
        print(f"merge_duplicate_dirs: {root}: not a directory", file=sys.stderr)
        return 1
    excluded = set(args.exclude_flavour)
    try:
        before = Snapshot.take(root)
        groups = duplicate_groups(test_dirs(root))
        chosen = [g for g in groups if not any(_flavours(d) & excluded for d in g)]
        for group in groups:
            verb = "merge" if group in chosen else "skip (excluded flavour)"
            print(
                f"{verb} {len(group)}: {group[0].name} <- "
                + " ".join(d.name for d in group[1:])
            )
        kept = [merge_group(g, dry_run=args.dry_run) for g in chosen]
        after = Snapshot.take(root)
    except (MergeError, UnreadableDir, OSError) as exc:
        print(f"merge_duplicate_dirs: FAIL {exc}", file=sys.stderr)
        return 1
    problems: list[str] = []
    if before.ids != after.ids:
        problems.append("the multiset of test ids changed")
    if before.pairs != after.pairs:
        problems.append("the set of distinct (input, oracle) body md5 pairs changed")
    for d in kept:
        for name in (INPUT_VCF, EXPECTED_VCF):
            if before.files[d / name] != after.files.get(d / name):
                problems.append(f"{d / name} bytes changed")
    if problems:
        print("merge_duplicate_dirs: FAIL " + "; ".join(problems), file=sys.stderr)
        return 1
    print(
        f"merge_duplicate_dirs: {'would merge' if args.dry_run else 'merged'} "
        f"{len(chosen)} group(s), {sum(len(g) - 1 for g in chosen)} directories "
        "removed; "
        f"{len(after.ids)} test ids and {len(after.pairs)} distinct pairs unchanged"
    )
    if args.index is not None and not args.dry_run and chosen:
        cmd = [str(BUILD_INDEX), "--root", str(root), "--out", str(args.index)]
        return subprocess.run(cmd, check=False).returncode
    return 0


if __name__ == "__main__":
    sys.exit(main())
