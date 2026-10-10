"""The one reader of VCF body records for repo tooling (#161, #164).

Tools that need the fixed columns of a VCF's records (CHROM, POS, ID, REF,
ALT) import this module instead of splitting lines themselves::

    from vcf_records import read_records

    for rec in read_records(Path("tests/data/<slug>/input.vcf")):
        print(rec.chrom, rec.pos)

A *body line* is any line that does not start with ``#``. Every body line must
be a tab-separated record with at least the five fixed columns and a decimal
POS; anything else (a blank line included) raises :class:`VcfFormatError`
with the file and 1-based line number. Only plain-text UTF-8 VCFs are read:
committed data-test files are never compressed.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

__all__ = ["VcfFormatError", "VcfRecord", "iter_records", "read_records"]


class VcfFormatError(ValueError):
    """A body line that is not a tab-separated VCF record."""


@dataclass(frozen=True, slots=True, kw_only=True)
class VcfRecord:
    """The five fixed identifying columns of one VCF record.

    Attributes:
        chrom: CHROM, verbatim (``21``, not normalised to ``chr21``).
        pos: POS, 1-based.
        id: ID, verbatim (``.`` when absent).
        ref: REF allele, verbatim.
        alt: ALT allele(s), verbatim (comma-separated if several).
        line: 1-based line number of the record in its file.
    """

    chrom: str
    pos: int
    id: str
    ref: str
    alt: str
    line: int


def iter_records(lines: Iterable[str], *, where: str = "<vcf>") -> Iterator[VcfRecord]:
    """Parse the body lines of VCF text, skipping ``#`` header lines.

    Args:
        lines: The file's lines, terminators optional.
        where: Name used in error messages (usually the file path).

    Yields:
        One :class:`VcfRecord` per body line, in file order.

    Raises:
        VcfFormatError: A body line has fewer than five tab-separated columns
            or a POS that is not a non-negative decimal integer.
    """
    for lineno, raw in enumerate(lines, 1):
        if raw.startswith("#"):
            continue
        text = raw.rstrip("\r\n")
        cols = text.split("\t")
        match cols:
            case [chrom, pos, id_, ref, alt, *_] if pos.isdecimal() and pos.isascii():
                yield VcfRecord(
                    chrom=chrom, pos=int(pos), id=id_, ref=ref, alt=alt, line=lineno
                )
            case _:
                raise VcfFormatError(
                    f"{where}:{lineno}: not a tab-separated VCF record "
                    f"(CHROM POS ID REF ALT): {text[:60]!r}"
                )


def read_records(path: Path) -> list[VcfRecord]:
    """Read every record of a plain-text VCF file.

    Args:
        path: The VCF to read.

    Returns:
        The records in file order (empty if the file has only header lines).

    Raises:
        OSError: The file cannot be read.
        UnicodeDecodeError: The file is not UTF-8 text.
        VcfFormatError: A body line is not a record (see :func:`iter_records`).
    """
    with path.open(encoding="utf-8", newline="") as fh:
        return list(iter_records(fh, where=str(path)))
