"""Tests for ``tools/vcf_records.py``, the shared VCF record reader (#164)."""

from __future__ import annotations

from pathlib import Path

import pytest

from vcf_records import VcfFormatError, VcfRecord, iter_records, read_records


def test_reads_fixed_columns_and_skips_headers(tmp_path: Path) -> None:
    """Header lines are skipped; the five fixed columns and the line number are kept."""
    p = tmp_path / "x.vcf"
    p.write_text(
        "##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\n21\t100\trs1\tG\tT,C\t.\t.\t.\n22\t5\t.\tA\tG\n"
    )
    assert read_records(p) == [
        VcfRecord(chrom="21", pos=100, id="rs1", ref="G", alt="T,C", line=3),
        VcfRecord(chrom="22", pos=5, id=".", ref="A", alt="G", line=4),
    ]


def test_header_only_file_has_no_records(tmp_path: Path) -> None:
    """A file of header lines yields an empty list, not an error."""
    p = tmp_path / "x.vcf"
    p.write_text("##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\n")
    assert read_records(p) == []


def test_crlf_terminators_are_stripped() -> None:
    """A CRLF line ending does not leak into ALT."""
    (r,) = iter_records(["21\t1\t.\tA\tG\r\n"])
    assert r.alt == "G"


@pytest.mark.parametrize(
    "line",
    [
        pytest.param("21 100 . G T\n", id="spaces_not_tabs"),
        pytest.param("21\t100\t.\tG\n", id="four_columns"),
        pytest.param("21\tx\t.\tG\tT\n", id="non_numeric_pos"),
        pytest.param("21\t-1\t.\tG\tT\n", id="negative_pos"),
        pytest.param("\n", id="blank_line"),
    ],
)
def test_malformed_body_line_raises_with_location(line: str) -> None:
    """A body line that is not a record raises with ``where:line``."""
    with pytest.raises(VcfFormatError, match=r"^f\.vcf:2: "):
        list(iter_records(["#h\n", line], where="f.vcf"))
