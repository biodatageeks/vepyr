"""Fail when two data-test directories make the same comparison (#238).

::

    tools/check_unique_dirs [DIR]        # DIR defaults to tests/data

A data-test directory is one comparison: vepyr's output body against the body of
``expected_output.vcf`` (the *body* of a VCF is every line not starting with
``#``). Two directories are *duplicates* when they share the comparison key

* the md5 of the body of ``input.vcf``,
* the md5 of the body of ``expected_output.vcf``,
* the ``[vepyr]`` table, the ``[[vepyr_run]]`` list and ``[vep] command``.

Header lines are not part of the key, so two directories whose oracles differ
only in VEP's header are duplicates (the runner compares bodies only). Several
VEP assertions that one comparison covers belong in one directory as
``[[tests]]`` tables of its ``test.toml``, not in copies of the directory.

``DIR`` is a data root: every immediate subdirectory holding a ``test.toml`` is
a data-test. One ``DUPLICATE <n> <dir> <dir> ...`` line is printed per group of
directories sharing a key (names sorted), then a summary line.

Exit codes: ``0`` no duplicates and at least one directory; ``1`` a duplicate
group, a directory whose files cannot be read, no data-test directory, or
``DIR`` not a directory; ``2`` bad usage (argparse).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tomllib
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

__all__ = [
    "DirKey",
    "UnreadableDir",
    "body_md5",
    "duplicate_groups",
    "key_of",
    "main",
    "test_dirs",
]

DEFAULT_ROOT: Final[Path] = Path("tests/data")
INPUT_VCF: Final[str] = "input.vcf"
EXPECTED_VCF: Final[str] = "expected_output.vcf"
TEST_TOML: Final[str] = "test.toml"


class UnreadableDir(Exception):
    """A data-test directory whose files cannot produce a comparison key."""


@dataclass(frozen=True, slots=True, order=True)
class DirKey:
    """The comparison a data-test directory makes.

    Attributes:
        input_md5: md5 of the body of ``input.vcf``.
        oracle_md5: md5 of the body of ``expected_output.vcf``.
        config: Canonical JSON of ``[vepyr]``, ``[[vepyr_run]]`` and
            ``[vep] command``.
    """

    input_md5: str
    oracle_md5: str
    config: str


def body_md5(path: Path) -> str:
    """Return the md5 of the lines of ``path`` not starting with ``#``.

    Lines are hashed as stored, terminators included (the runner's body md5).

    Args:
        path: A VCF file.

    Returns:
        Lower-case hex digest.

    Raises:
        OSError: The file cannot be read.
    """
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as fh:
        for line in fh:
            if not line.startswith(b"#"):
                digest.update(line)
    return digest.hexdigest()


def key_of(test_dir: Path) -> DirKey:
    """Compute the comparison key of one data-test directory.

    Args:
        test_dir: A directory holding ``input.vcf``, ``expected_output.vcf`` and
            ``test.toml``.

    Returns:
        Its :class:`DirKey`.

    Raises:
        UnreadableDir: A file is missing or unreadable, or ``test.toml`` is not
            valid TOML.
    """
    try:
        doc = tomllib.loads((test_dir / TEST_TOML).read_text(encoding="utf-8"))
        vep = doc.get("vep")
        config = json.dumps(
            {
                "vepyr": doc.get("vepyr"),
                "vepyr_run": doc.get("vepyr_run"),
                "vep_command": vep.get("command") if isinstance(vep, dict) else None,
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        return DirKey(
            input_md5=body_md5(test_dir / INPUT_VCF),
            oracle_md5=body_md5(test_dir / EXPECTED_VCF),
            config=config,
        )
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise UnreadableDir(f"{test_dir}: {exc}") from exc


def test_dirs(root: Path) -> list[Path]:
    """Every immediate subdirectory of ``root`` holding a ``test.toml``, sorted."""
    return sorted(d for d in root.iterdir() if d.is_dir() and (d / TEST_TOML).is_file())


def duplicate_groups(dirs: Sequence[Path]) -> list[list[Path]]:
    """Group ``dirs`` by comparison key and return the groups of two or more.

    Args:
        dirs: Data-test directories.

    Returns:
        Each group sorted by directory name; the groups sorted by size
        (descending), then by first directory name (the issue's G01..G17 order).

    Raises:
        UnreadableDir: A directory's key cannot be computed.
    """
    by_key: defaultdict[DirKey, list[Path]] = defaultdict(list)
    for d in dirs:
        by_key[key_of(d)].append(d)
    groups = [sorted(g, key=lambda p: p.name) for g in by_key.values() if len(g) > 1]
    return sorted(groups, key=lambda g: (-len(g), g[0].name))


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point; see the module docstring for output and exit codes."""
    parser = argparse.ArgumentParser(
        prog="tools/check_unique_dirs", description=__doc__.splitlines()[0]
    )
    parser.add_argument(
        "dir",
        nargs="?",
        type=Path,
        default=DEFAULT_ROOT,
        metavar="DIR",
        help=f"data root (default: {DEFAULT_ROOT})",
    )
    root: Path = parser.parse_args(argv).dir
    if not root.is_dir():
        print(f"check_unique_dirs: {root}: not a directory", file=sys.stderr)
        return 1
    dirs = test_dirs(root)
    if not dirs:
        print(f"check_unique_dirs: no data-test directory in {root}", file=sys.stderr)
        return 1
    try:
        groups = duplicate_groups(dirs)
    except UnreadableDir as exc:
        print(f"check_unique_dirs: FAIL cannot read {exc}", file=sys.stderr)
        return 1
    for group in groups:
        print(f"DUPLICATE {len(group)} " + " ".join(d.name for d in group))
    redundant = sum(len(g) - 1 for g in groups)
    print(
        f"check_unique_dirs: {len(dirs)} directories, {len(dirs) - redundant} distinct "
        f"comparisons, {len(groups)} duplicate group(s)"
    )
    return 1 if groups else 0


if __name__ == "__main__":
    sys.exit(main())
