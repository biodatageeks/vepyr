"""Check the structural invariants of committed data-test directories (#164).

``./bless --check`` compares the oracle's body md5, ``./check_normalised_input``
checks that ``input.vcf`` is a fixed point of the normaliser and the loader
parses ``test.toml``. None of them looks at the shape of a directory; this
tool does::

    ./check_test_dir [DIR]        # DIR defaults to tests/data

``DIR`` is either a data root (every immediate subdirectory holding a
``test.toml`` is checked) or one data-test directory (``DIR`` itself holds a
``test.toml``). Five checks run on each directory, all of them every time:

``files``
    The directory holds exactly ``input.vcf``, ``expected_output.vcf`` and
    ``test.toml``, each non-empty, and nothing else.
``input-records``
    ``input.vcf`` has at least one record (an empty input gives an empty
    oracle body and a test that passes vacuously).
``order``
    POS ascends (non-strictly) within each contig of ``input.vcf`` and each
    contig is one contiguous block.
``oracle-meta``
    ``expected_output.vcf`` has exactly one ``##VEP=`` line and ``[vep] image``
    in ``test.toml`` is ``ensemblorg/ensembl-vep@sha256:<64 hex>``.
``one-to-one``
    The oracle body is the input's records minus those VEP 116.2 drops, in
    input order, compared on columns 1-5 (CHROM, POS, ID, REF, ALT, verbatim).
    The expectation is derived from the input (#193): a record whose every ALT
    allele is ``.`` is dropped (VEP skips it without ``--allow_non_variant``,
    ``Parser/VCF.pm`` L259 at the pin); every other record must appear exactly
    once. A lost, extra, reordered or substituted line fails, and so does an
    input whose records are all dropped (nothing would be compared). Mandatory:
    there is no option and no ``test.toml`` key to skip it (owner decision,
    #164).

One ``OK <dir>`` line is printed per passing directory, or one
``FAIL <dir> <check>: <detail>`` line per failing check (all of them, never
only the first). A file that cannot be read fails every check that needs it; a
directory that cannot be listed or inspected is a ``files`` FAIL, never skipped.
Records are read by :mod:`vcf_records`, the repo's one VCF record reader.

Not checked here: the ``test.toml`` schema (the loader, #155), the mode
(#155), the body md5 (``./bless --check``, #133), REF vs FASTA (#96).

Nothing is ever written.

Exit codes: ``0`` every directory is OK and at least one was found; ``1`` any
FAIL, no test found, or ``DIR`` not a directory; ``2`` bad usage.
"""

from __future__ import annotations

import argparse
import re
import stat
import sys
import tomllib
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Final

from vcf_records import VcfFormatError, VcfRecord, read_records

__all__ = [
    "CheckId",
    "Failure",
    "check_dir",
    "discover",
    "main",
]

DEFAULT_DATA_DIR: Final[Path] = Path("tests/data")
INPUT_VCF: Final[str] = "input.vcf"
EXPECTED_VCF: Final[str] = "expected_output.vcf"
TEST_TOML: Final[str] = "test.toml"
FILES: Final[tuple[str, ...]] = (INPUT_VCF, EXPECTED_VCF, TEST_TOML)
IMAGE_RE: Final[re.Pattern[str]] = re.compile(
    r"ensemblorg/ensembl-vep@sha256:[0-9a-f]{64}"
)
VEP_LINE: Final[str] = "##VEP="


class CheckId(StrEnum):
    """The five structural checks, in the order they are reported."""

    FILES = "files"
    INPUT_RECORDS = "input-records"
    ORDER = "order"
    ORACLE_META = "oracle-meta"
    ONE_TO_ONE = "one-to-one"


@dataclass(frozen=True, slots=True, kw_only=True)
class Failure:
    """One failing check of one directory.

    Attributes:
        check: Which check failed.
        detail: Human-readable reason, one line.
    """

    check: CheckId
    detail: str

    def line(self, test_dir: Path) -> str:
        """Render the ``FAIL <dir> <check>: <detail>`` output line."""
        return f"FAIL {test_dir} {self.check}: {self.detail}"


type Loaded[T] = T | str
"""A successfully read value, or the reason it could not be read."""


def _records(path: Path) -> Loaded[list[VcfRecord]]:
    """Read the records of ``path``, or say why that is impossible."""
    try:
        return read_records(path)
    except (OSError, UnicodeDecodeError, VcfFormatError) as e:
        return f"cannot read {path.name}: {e}"


def _check_files(test_dir: Path) -> str | None:
    """Exactly the three files, each a non-empty regular file, nothing else."""
    missing = [n for n in FILES if not (test_dir / n).is_file()]
    empty = [
        n for n in FILES if n not in missing and (test_dir / n).stat().st_size == 0
    ]
    extra = sorted(p.name for p in test_dir.iterdir() if p.name not in FILES)
    problems = [
        f"{label} {names}"
        for label, names in (("missing", missing), ("empty", empty), ("extra", extra))
        if names
    ]
    return "; ".join(problems) or None


def _check_input_records(records: Loaded[list[VcfRecord]]) -> str | None:
    """At least one input record."""
    match records:
        case str(reason):
            return reason
        case []:
            return f"{INPUT_VCF} has no records"
        case _:
            return None


def _check_order(records: Loaded[list[VcfRecord]]) -> str | None:
    """POS ascends within a contig; each contig is one contiguous block."""
    if isinstance(records, str):
        return records
    finished: set[str] = set()
    for prev, cur in pairwise(records):
        if cur.chrom != prev.chrom:
            if cur.chrom in finished:
                return f"contig {cur.chrom} interleaved ({INPUT_VCF} line {cur.line})"
            finished.add(prev.chrom)
        elif cur.pos < prev.pos:
            where = f"{INPUT_VCF} line {cur.line}"
            return f"{cur.chrom}:{cur.pos} after {cur.chrom}:{prev.pos} ({where})"
    return None


def _check_oracle_meta(test_dir: Path) -> str | None:
    """Exactly one ``##VEP=`` line and a digest-pinned ``[vep] image``."""
    problems: list[str] = []
    try:
        with (test_dir / EXPECTED_VCF).open(encoding="utf-8") as fh:
            n = sum(1 for ln in fh if ln.startswith(VEP_LINE))
        if n != 1:
            problems.append(f"{n} {VEP_LINE} lines in {EXPECTED_VCF}, want 1")
    except (OSError, UnicodeDecodeError) as e:
        problems.append(f"cannot read {EXPECTED_VCF}: {e}")
    try:
        with (test_dir / TEST_TOML).open("rb") as fh:
            vep = tomllib.load(fh).get("vep")
        image = vep.get("image") if isinstance(vep, dict) else None
        if not isinstance(image, str) or not IMAGE_RE.fullmatch(image):
            problems.append(
                f"[vep] image = {image!r}, want ensemblorg/ensembl-vep@sha256:<64 hex>"
            )
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as e:
        problems.append(f"cannot read {TEST_TOML}: {e}")
    return "; ".join(problems) or None


type RecordKey = tuple[str, int, str, str, str]
"""Columns 1-5 of a record: CHROM, POS, ID, REF, ALT, verbatim."""


def _droppable(r: VcfRecord) -> bool:
    """Whether VEP 116.2 drops ``r``: every ALT allele is ``.``.

    VEP skips such a record unless ``--allow_non_variant`` is given
    (``Parser/VCF.pm`` L259 at the pin), and the one VEP command does not give
    it. A record with any other ALT allele (``A,.``, ``*``, ``<DEL>``, REF==ALT)
    is kept (measured against VEP 116.2, #193).
    """
    return all(a == "." for a in r.alt.split(","))


def _key(r: VcfRecord) -> RecordKey:
    """Columns 1-5 of ``r``."""
    return (r.chrom, r.pos, r.id, r.ref, r.alt)


def _show(k: RecordKey) -> str:
    """Render a record key as ``CHROM:POS ID REF>ALT``."""
    chrom, pos, rid, ref, alt = k
    return f"{chrom}:{pos} {rid} {ref}>{alt}"


def _check_one_to_one(
    records: Loaded[list[VcfRecord]], oracle: Loaded[list[VcfRecord]]
) -> str | None:
    """The oracle body is the input minus the ALT-``.`` records, in order.

    Columns 1-5 are compared line by line; the first difference is reported.
    An empty expectation fails: a test with nothing to compare passes vacuously.
    """
    if isinstance(records, str):
        return records
    if isinstance(oracle, str):
        return oracle
    want = [_key(r) for r in records if not _droppable(r)]
    got = [_key(r) for r in oracle]
    counts = (
        f"(input {len(records)} records, ALT '.' dropped "
        f"{len(records) - len(want)}, oracle {len(got)} body lines)"
    )
    if not records:
        return f"input has no records; nothing to compare {counts}"
    if not want:
        return f"no input record survives the ALT '.' drop; nothing to compare {counts}"
    if want == got:
        return None
    i = next(
        (n for n, (w, g) in enumerate(zip(want, got, strict=False)) if w != g),
        min(len(want), len(got)),
    )
    if i == len(got):
        return f"oracle has no line for input {_show(want[i])} {counts}"
    expected = _show(want[i]) if i < len(want) else "no more lines"
    return f"oracle line {i + 1} is {_show(got[i])}, want {expected} {counts}"


def check_dir(test_dir: Path) -> list[Failure]:
    """Run all five checks on one data-test directory; read only.

    Args:
        test_dir: The data-test directory (holding ``test.toml``).

    Returns:
        One :class:`Failure` per failing check, in :class:`CheckId` order;
        empty when the directory passes.
    """
    try:
        next(iter(test_dir.iterdir()), None)
    except OSError as e:
        return [Failure(check=CheckId.FILES, detail=f"cannot read directory: {e}")]
    records = _records(test_dir / INPUT_VCF)
    oracle = _records(test_dir / EXPECTED_VCF)
    checks: dict[CheckId, Callable[[], str | None]] = {
        CheckId.FILES: lambda: _check_files(test_dir),
        CheckId.INPUT_RECORDS: lambda: _check_input_records(records),
        CheckId.ORDER: lambda: _check_order(records),
        CheckId.ORACLE_META: lambda: _check_oracle_meta(test_dir),
        CheckId.ONE_TO_ONE: lambda: _check_one_to_one(records, oracle),
    }
    return [
        Failure(check=cid, detail=detail)
        for cid, run in checks.items()
        if (detail := run()) is not None
    ]


def _may_be_test_dir(d: Path) -> bool:
    """Whether ``d`` holds a ``test.toml`` or cannot be inspected.

    A directory whose ``test.toml`` cannot be stat'ed (e.g. no permission) counts
    as a data-test so that :func:`check_dir` reports it as FAIL instead of it
    being skipped silently.
    """
    try:
        return stat.S_ISREG((d / TEST_TOML).stat().st_mode)
    except (FileNotFoundError, NotADirectoryError):
        return False
    except OSError:
        return True


def _is_dir_or_uninspectable(p: Path) -> bool:
    """Whether ``p`` is a directory (symlinks followed) or cannot be stat'ed."""
    try:
        return stat.S_ISDIR(p.stat().st_mode)
    except FileNotFoundError:
        return False
    except OSError:
        return True


def discover(root: Path) -> list[Path]:
    """Resolve ``DIR`` into the data-test directories to check.

    Walks ``root`` explicitly (no glob, which swallows errors): anything that
    cannot be listed or inspected is returned, so it is reported as FAIL and
    never skipped.

    Args:
        root: A data-test directory, or a data root holding them.

    Returns:
        ``[root]`` if ``root`` holds a ``test.toml`` or cannot be listed, else
        every immediate subdirectory of ``root`` that holds one or cannot be
        inspected, sorted by name.
    """
    if _may_be_test_dir(root):
        return [root]
    try:
        entries = sorted(root.iterdir())
    except OSError:
        return [root]
    return [p for p in entries if _is_dir_or_uninspectable(p) and _may_be_test_dir(p)]


def _report(dirs: Sequence[Path]) -> Iterator[tuple[str, bool]]:
    """Yield ``(output line, ok)`` for every directory, in order."""
    for d in dirs:
        failures = check_dir(d)
        if not failures:
            yield f"OK {d}", True
        for f in failures:
            yield f.line(d), False


def _parser() -> argparse.ArgumentParser:
    """Build the command-line parser (deliberately without any skip option)."""
    p = argparse.ArgumentParser(
        prog="check_test_dir",
        description="Check the structural invariants of data-test directories (#164).",
    )
    p.add_argument(
        "dir",
        nargs="?",
        type=Path,
        default=DEFAULT_DATA_DIR,
        metavar="DIR",
        help=f"data root or one data-test directory (default: {DEFAULT_DATA_DIR})",
    )
    return p


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point.

    Args:
        argv: Arguments without the program name; ``None`` reads ``sys.argv``.

    Returns:
        The exit code (see the module docstring). Bad usage exits ``2`` via
        :class:`SystemExit` raised by argparse.
    """
    root: Path = _parser().parse_args(argv).dir
    if not root.is_dir():
        print(f"check_test_dir: {root}: not a directory", file=sys.stderr)
        return 1
    dirs = discover(root)
    if not dirs:
        print(
            f"check_test_dir: no data-test directory (with {TEST_TOML}) in {root}",
            file=sys.stderr,
        )
        return 1
    ok = True
    for line, passed in _report(dirs):
        print(line)
        ok &= passed
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
