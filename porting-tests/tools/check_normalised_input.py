"""Check that every committed data-test input is already normalised.

``tools/normalize_input`` (#85) is the only allowed producer of a data-test's
``input.vcf`` and of the ``[input]`` table in its ``test.toml``. This check
(#89) re-runs it on each committed ``input.vcf`` -- raw inputs are not
committed (#90), and re-normalising a normalised file is a fixed point -- and
compares the result with what is committed::

    ./check_normalised_input [DATA_DIR]        # DATA_DIR defaults to tests/data

``DATA_DIR`` may also be one data-test directory (it holds a ``test.toml``
itself); then only that test is checked (#166). Otherwise, for every
immediate subdirectory of ``DATA_DIR`` that holds a ``test.toml``,
the committed ``test.toml`` is copied into a fresh temporary directory, the
normaliser writes ``input.vcf`` and rewrites ``[input]`` there, and both files
are compared with the committed ones: ``input.vcf`` byte for byte,
``test.toml`` as a whole. One ``OK <dir>`` or ``MISMATCH <dir>`` line is
printed per test, followed by a unified diff for each mismatch. A normaliser
that exits 0 but writes no ``input.vcf`` is a ``MISMATCH`` too, never an
exception. CI runs this in
the ``input-normalised-check`` workflow
(``.github/workflows/input-normalised-check.yml``).

What it verifies: each ``input.vcf`` is already normalised (a fixed point of
``tools/normalize_input``) and ``[input]`` is what that script writes. It does
not verify the raw source file: raw inputs are not committed (#90); raw
provenance is tracked in #111.

Nothing is ever written under ``DATA_DIR``.

Known limit: re-normalising an already normalised ``input.vcf`` cannot detect
one built from the wrong raw file; that follows from #90 and is accepted.

Exit codes: ``0`` every test is ``OK`` and at least one was found; ``1`` any
mismatch, any normaliser failure (wrong bcftools included), no test found, or
``DATA_DIR`` not a directory; ``2`` bad usage.
"""

from __future__ import annotations

import argparse
import difflib
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

__all__ = [
    "CheckResult",
    "Status",
    "check_all",
    "check_test",
    "discover",
    "main",
    "run_normalize_input",
]

_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
NORMALIZE_INPUT: Final[Path] = _ROOT / "tools" / "normalize_input"
"""The one normaliser; run with this interpreter (it has no dependencies)."""

DEFAULT_DATA_DIR: Final[Path] = Path("tests/data")
INPUT_VCF: Final[str] = "input.vcf"
TEST_TOML: Final[str] = "test.toml"

type Normalizer = Callable[[Path, Path], subprocess.CompletedProcess[str]]
"""``(committed input.vcf, scratch test dir) -> finished process``."""


class Status(StrEnum):
    """Verdict for one data-test directory."""

    OK = "OK"
    MISMATCH = "MISMATCH"


@dataclass(frozen=True, slots=True, kw_only=True)
class CheckResult:
    """Outcome of re-normalising one data-test.

    Attributes:
        test_dir: The committed data-test directory.
        status: ``OK`` when both files match, ``MISMATCH`` otherwise.
        details: Unified diffs and/or the normaliser's error output; empty
            when ``status`` is ``OK``.
    """

    test_dir: Path
    status: Status
    details: str = ""


def run_normalize_input(raw: Path, test_dir: Path) -> subprocess.CompletedProcess[str]:
    """Run ``tools/normalize_input <raw> <test_dir>`` and capture its output.

    Args:
        raw: The committed ``input.vcf`` to re-normalise.
        test_dir: Scratch directory that receives ``input.vcf`` and ``test.toml``.

    Returns:
        The finished process; the caller judges its return code.
    """
    return subprocess.run(
        [sys.executable, str(NORMALIZE_INPUT), str(raw), str(test_dir)],
        capture_output=True,
        text=True,
        check=False,
    )


def discover(data_dir: Path) -> list[Path]:
    """List the data-test directories selected by ``data_dir``.

    Args:
        data_dir: Root of the data-tests, e.g. ``tests/data``, or one
            data-test directory.

    Returns:
        ``[data_dir]`` when ``data_dir`` itself holds a ``test.toml`` (single
        directory mode, #166); otherwise every immediate subdirectory holding
        a ``test.toml``, sorted by name.
    """
    if (data_dir / TEST_TOML).is_file():
        return [data_dir]
    return sorted(p.parent for p in data_dir.glob(f"*/{TEST_TOML}") if p.is_file())


def _unified(expected: bytes, actual: bytes, *, name: str) -> str:
    """Render a unified diff of two file contents, committed first.

    Args:
        expected: Committed bytes.
        actual: Bytes the normaliser produced.
        name: File name used in the diff headers.

    Returns:
        The diff text, or a one-line note when the bytes differ but the decoded
        lines do not (e.g. only an encoding or line-ending difference).
    """
    diff = "".join(
        difflib.unified_diff(
            expected.decode("utf-8", "replace").splitlines(keepends=True),
            actual.decode("utf-8", "replace").splitlines(keepends=True),
            fromfile=f"committed/{name}",
            tofile=f"normalize_input/{name}",
        )
    )
    return diff or f"{name}: bytes differ but no line-level difference is shown\n"


def check_test(
    test_dir: Path, *, normalize: Normalizer = run_normalize_input
) -> CheckResult:
    """Re-normalise one data-test in a scratch directory and compare.

    Args:
        test_dir: Committed data-test directory; only read, never written.
        normalize: The normaliser runner; injectable for tests.

    Returns:
        ``OK`` if ``input.vcf`` and ``test.toml`` both match byte for byte,
        otherwise ``MISMATCH`` with diffs or the normaliser's error.
    """
    committed_vcf = test_dir / INPUT_VCF
    committed_toml = test_dir / TEST_TOML
    with tempfile.TemporaryDirectory(prefix="check_normalised_input.") as tmp:
        scratch = Path(tmp)
        shutil.copyfile(committed_toml, scratch / TEST_TOML)
        done = normalize(committed_vcf, scratch)
        if done.returncode != 0:
            return CheckResult(
                test_dir=test_dir,
                status=Status.MISMATCH,
                details=(
                    f"tools/normalize_input exited {done.returncode}\n"
                    f"{done.stderr.rstrip()}\n"
                ),
            )
        if not (scratch / INPUT_VCF).is_file():
            return CheckResult(
                test_dir=test_dir,
                status=Status.MISMATCH,
                details=f"tools/normalize_input exited 0 but wrote no {INPUT_VCF}\n",
            )
        diffs = [
            _unified(want, got, name=name)
            for name in (INPUT_VCF, TEST_TOML)
            if (want := (test_dir / name).read_bytes())
            != (got := (scratch / name).read_bytes())
        ]
    return CheckResult(
        test_dir=test_dir,
        status=Status.MISMATCH if diffs else Status.OK,
        details="".join(diffs),
    )


def check_all(
    data_dir: Path, *, normalize: Normalizer = run_normalize_input
) -> Iterator[CheckResult]:
    """Check every data-test under ``data_dir``, in name order.

    Args:
        data_dir: Root of the data-tests.
        normalize: The normaliser runner; injectable for tests.

    Yields:
        One result per discovered test directory.
    """
    for test_dir in discover(data_dir):
        yield check_test(test_dir, normalize=normalize)


def _parse_args(argv: Sequence[str] | None) -> Path:
    """Parse the command line.

    Args:
        argv: Arguments without the program name; ``None`` means ``sys.argv``.

    Returns:
        The data directory to check.
    """
    parser = argparse.ArgumentParser(
        prog="check_normalised_input",
        description=(
            "Verify every data-test input.vcf and test.toml [input] table is "
            "what tools/normalize_input produces."
        ),
        allow_abbrev=False,
    )
    parser.add_argument(
        "data_dir",
        nargs="?",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=(
            f"directory of data-tests, or one data-test directory "
            f"(default: {DEFAULT_DATA_DIR})"
        ),
    )
    return parser.parse_args(argv).data_dir


def main(
    argv: Sequence[str] | None = None, *, normalize: Normalizer = run_normalize_input
) -> int:
    """Entry point.

    Args:
        argv: Arguments without the program name; ``None`` means ``sys.argv``.
        normalize: The normaliser runner; injectable for tests.

    Returns:
        ``0`` if at least one test was found and all are ``OK``, else ``1``.
    """
    data_dir = _parse_args(argv)
    if not data_dir.is_dir():
        print(f"check_normalised_input: not a directory: {data_dir}", file=sys.stderr)
        return 1
    found = failed = 0
    for result in check_all(data_dir, normalize=normalize):
        found += 1
        print(f"{result.status} {result.test_dir}", flush=True)
        if result.status is Status.MISMATCH:
            failed += 1
            print(result.details, end="", flush=True)
    if found == 0:
        print(
            f"check_normalised_input: no data-test (*/{TEST_TOML}) under {data_dir}",
            file=sys.stderr,
        )
        return 1
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
