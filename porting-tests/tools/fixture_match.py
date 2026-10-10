"""``tools/fixture_match``: first N records of an input equal a fixture's (#171).

A data-test issue that names an upstream fixture (a VCF file, or a Rust
``const VCF: &str = "...";`` literal in a VEP test source) needs proof that the
first ``N`` records of ``input.vcf`` are the fixture's. This tool compares
CHROM, POS, ID, REF and ALT::

    tools/fixture_match --input PATH --fixture SRC --records N \\
        [--rust-const NAME] [--by-pos] [--negative-control]

``PATH`` is a VCF (plain or gzip, detected by magic bytes) or a data-test
directory (its ``input.vcf``). ``SRC`` is a local path or an ``http(s)://`` URL
(no ``git:<repo>:<rev>:<path>`` spec). Records are read with the shared
:mod:`vcf_records`.

Output: one ``PASS|FAIL fixture-match first N: <detail>`` line. With
``--negative-control`` the first input record's ALT is mutated in memory (``A``
appended), a mismatch is expected (``PASS|FAIL fixture-match-negative: ...``)
and a ``PASS|FAIL summary: k/2 checks passed`` line follows.

Exit codes: 0 match, 1 mismatch (including fewer than ``N`` records on either
side, or a failed negative control), 2 usage or unreadable/invalid input,
3 network error or anything unexpected.
"""

from __future__ import annotations

import argparse
import gzip
import re
import sys
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass, replace
from enum import IntEnum
from pathlib import Path
from typing import Final

from vcf_records import VcfFormatError, VcfRecord, iter_records

__all__ = [
    "Check",
    "Exit",
    "FetchError",
    "InputError",
    "main",
    "match_first",
    "read_fixture",
    "read_input",
    "rust_const_vcf",
]

PROG: Final[str] = "fixture_match"
GZIP_MAGIC: Final[bytes] = b"\x1f\x8b"
FETCH_TIMEOUT_S: Final[float] = 60.0
ESCAPES: Final[dict[str, str]] = {"n": "\n", "t": "\t"}
"""Rust escapes that map to something other than the escaped character itself."""

type Key = tuple[str, int, str, str, str]


class Exit(IntEnum):
    """Process exit codes (see the module docstring)."""

    OK = 0
    FAIL = 1
    USAGE = 2
    ERROR = 3


class InputError(Exception):
    """Unreadable or invalid input or fixture (exit 2)."""


class FetchError(Exception):
    """A fixture URL cannot be fetched (exit 3)."""


@dataclass(frozen=True, slots=True)
class Check:
    """Outcome of one check, rendered as dt renders its checks."""

    name: str
    ok: bool
    detail: str = ""

    def line(self) -> str:
        """The paste-ready ``PASS``/``FAIL`` line."""
        tail = f": {self.detail}" if self.detail else ""
        return f"{'PASS' if self.ok else 'FAIL'} {self.name}{tail}"


def key(rec: VcfRecord) -> Key:
    """The compared columns: CHROM, POS, ID, REF, ALT."""
    return (rec.chrom, rec.pos, rec.id, rec.ref, rec.alt)


def decode(data: bytes, where: str) -> str:
    """UTF-8 text of ``data``, gunzipped first if it starts with the gzip magic bytes.

    Raises:
        InputError: Corrupt gzip data or text that is not UTF-8.
    """
    try:
        if data.startswith(GZIP_MAGIC):
            data = gzip.decompress(data)
        return data.decode("utf-8")
    except (gzip.BadGzipFile, EOFError, UnicodeDecodeError, OSError) as e:
        raise InputError(
            f"{where}: not a plain or gzip-compressed UTF-8 text ({e})"
        ) from e


def records(text: str, where: str) -> list[VcfRecord]:
    """Records of VCF ``text`` via :func:`vcf_records.iter_records`.

    Raises:
        InputError: A body line is not a VCF record.
    """
    try:
        return list(iter_records(text.splitlines(), where=where))
    except VcfFormatError as e:
        raise InputError(str(e)) from e


def read_path(path: Path) -> bytes:
    """Bytes of a local file.

    Raises:
        InputError: The file is missing or unreadable.
    """
    try:
        return path.expanduser().read_bytes()
    except OSError as e:
        raise InputError(f"{path}: cannot read: {e.strerror or e}") from e


def read_input(target: Path) -> list[VcfRecord]:
    """Records of a VCF, or of ``<dir>/input.vcf`` when ``target`` is a directory."""
    path = target / "input.vcf" if target.is_dir() else target
    return records(decode(read_path(path), str(path)), str(path))


def read_source(spec: str) -> str:
    """Text of a fixture ``spec``: an ``http(s)://`` URL or a local path.

    Raises:
        FetchError: The URL cannot be fetched.
        InputError: A ``git:`` spec, or a path that cannot be read or decoded.
    """
    match spec.partition(":"):
        case ("http" | "https", ":", _):
            try:
                with urllib.request.urlopen(spec, timeout=FETCH_TIMEOUT_S) as resp:
                    data: bytes = resp.read()
            except OSError as e:  # URLError, HTTPError, timeouts
                raise FetchError(f"cannot fetch {spec}: {e}") from e
        case ("git", ":", _):
            raise InputError(
                f"{spec!r}: git:<repo>:<rev>:<path> specs are not supported; "
                "pass a local path or a raw http(s) URL"
            )
        case _:
            data = read_path(Path(spec))
    return decode(data, spec)


def rust_const_vcf(source: str, name: str) -> str:
    """Extract the value of a Rust ``const NAME: &str = "...";`` literal.

    Handles ``\\``-newline continuations (which also swallow the next line's
    leading whitespace) and the escapes ``\\n``, ``\\t``, ``\\\\`` and ``\\"``.

    Raises:
        InputError: No such const, or the const is a raw string (``r"..."``,
            ``r#"..."#``), which is not supported.
    """
    head = rf"const\s+{re.escape(name)}\s*:\s*&(?:'static\s+)?str\s*=\s*"
    if re.search(head + r'r#*"', source):
        raise InputError(f"`const {name}` is a raw string literal: not supported")
    m = re.search(head + r'"((?:[^"\\]|\\.)*)"', source, re.S)
    if m is None:
        raise InputError(f'no `const {name}: &str = "..."` in fixture')
    body = re.sub(r"\\\n\s*", "", m.group(1))
    return re.sub(r"\\(.)", lambda e: ESCAPES.get(e.group(1), e.group(1)), body)


def read_fixture(spec: str, rust_const: str | None = None) -> list[VcfRecord]:
    """Records of the fixture ``spec`` (or of its Rust const ``rust_const``)."""
    text = read_source(spec)
    return records(rust_const_vcf(text, rust_const) if rust_const else text, spec)


def match_first(
    inp: Sequence[VcfRecord], fix: Sequence[VcfRecord], n: int, *, by_pos: bool = False
) -> Check:
    """CHROM, POS, ID, REF, ALT of the first ``n`` input records equal the fixture's.

    Positional (i-th vs i-th) by default; with ``by_pos`` each input record is
    compared with the fixture record at the same CHROM:POS (the fixture may hold
    other rows).
    """
    name = f"fixture-match first {n}"
    fix_keys: list[Key] = [key(r) for r in fix]
    if by_pos:
        index = {k[:2]: k for k in fix_keys}
        fix_keys = [
            index.get((r.chrom, r.pos), (r.chrom, r.pos, "<absent>", "", ""))
            for r in inp[:n]
        ]
    if len(inp) < n or len(fix_keys) < n:
        return Check(
            name, False, f"input has {len(inp)}, fixture {len(fix_keys)} records"
        )
    for i, (a, b) in enumerate(zip(map(key, inp[:n]), fix_keys[:n], strict=True), 1):
        if a != b:
            return Check(name, False, f"record {i}: input {a} != fixture {b}")
    return Check(name, True, "CHROM,POS,ID,REF,ALT identical")


def negative_control(
    inp: Sequence[VcfRecord],
    fix: Sequence[VcfRecord],
    n: int,
    positive: Check,
    *,
    by_pos: bool = False,
) -> Check:
    """A copy with ``A`` appended to the first ALT must not match (run after a PASS)."""
    name = "fixture-match-negative"
    if not positive.ok:
        return Check(name, False, f"not run: positive check {positive.name!r} failed")
    mutated = [replace(inp[0], alt=inp[0].alt + "A"), *inp[1:]]
    neg = match_first(mutated, fix, n, by_pos=by_pos)
    return Check(name, not neg.ok, f"mutated copy -> {neg.line()}")


def positive_int(text: str) -> int:
    """argparse type: an integer >= 1."""
    try:
        value = int(text)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"{text!r} is not an integer") from e
    if value < 1:
        raise argparse.ArgumentTypeError(f"{value} < 1: need at least one record")
    return value


def parser() -> argparse.ArgumentParser:
    """Build the CLI (argparse exits 2 on bad usage)."""
    p = argparse.ArgumentParser(
        prog=f"tools/{PROG}",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--input",
        type=Path,
        required=True,
        help="VCF (plain or gzip) or data-test directory",
    )
    p.add_argument("--fixture", required=True, help="local path or http(s) URL")
    p.add_argument("--records", type=positive_int, required=True, help="N >= 1")
    p.add_argument(
        "--rust-const",
        metavar="NAME",
        help='fixture is Rust source; take `const NAME: &str = "..."`',
    )
    p.add_argument(
        "--by-pos",
        action="store_true",
        help="match fixture rows by CHROM:POS, not by order",
    )
    p.add_argument(
        "--negative-control",
        action="store_true",
        help="also check that a mutated first ALT fails, and print a summary",
    )
    return p


def run(args: argparse.Namespace) -> Exit:
    """Compare, print the check lines and return the exit code."""
    inp = read_input(args.input)
    fix = read_fixture(args.fixture, args.rust_const)
    checks = [match_first(inp, fix, args.records, by_pos=args.by_pos)]
    if args.negative_control:
        checks.append(
            negative_control(inp, fix, args.records, checks[0], by_pos=args.by_pos)
        )
    for c in checks:
        print(c.line())
    failed = [c.name for c in checks if not c.ok]
    if args.negative_control:
        print(
            f"{'FAIL' if failed else 'PASS'} summary: "
            f"{len(checks) - len(failed)}/{len(checks)} checks passed"
            + (f"; failed: {', '.join(failed)}" if failed else "")
        )
    return Exit.FAIL if failed else Exit.OK


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point; returns 0 match, 1 mismatch, 2 usage/input, 3 network/unexpected."""
    args = parser().parse_args(argv)
    try:
        return int(run(args))
    except InputError as e:
        print(f"{PROG}: error: {e}", file=sys.stderr)
        return Exit.USAGE
    except FetchError as e:
        print(f"{PROG}: network error: {e}", file=sys.stderr)
        return Exit.ERROR
    except Exception as e:
        print(f"{PROG}: unexpected {type(e).__name__}: {e}", file=sys.stderr)
        return Exit.ERROR


if __name__ == "__main__":
    sys.exit(main())
