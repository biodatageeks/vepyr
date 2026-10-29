"""Percent-encoded input INFO/FORMAT values in the annotated VCF (vepyr#207).

ClinVar writes a `;` inside an INFO value as `%3B` and an `=` as `%3D`, as VCF
4.3 §1.2 requires. Ensembl VEP copies the input line and appends CSQ
(`OutputFactory/VCF.pm:314-355` at ensembl-vep `release/116.0`), so the input's
bytes come back unchanged. vepyr decodes the escapes on read, which is right
for the DataFrame, and has to encode them again when it writes the VCF:
written back raw, a `;` ends the INFO field early and a `,` adds a list element.

CSQ is the engine's own, already escaped by the VEP rules (`p.Ala125%3D`), so
it must not be encoded a second time.

Before VCF 4.3 an INFO value may hold no whitespace, so under a 4.1 or 4.2
header (ClinVar declares 4.1) a space decoded from `%20` is encoded again. VCF
4.3 allows spaces, and there they stay literal.

The input is the committed HG002 chr22 fixture's cache and FASTA, with
chr22:16592176 A>G: a synonymous change, so its CSQ carries `%3D` itself.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import vepyr

DATA = Path(__file__).parent / "data" / "hg002_chr22"
CACHE = DATA / "cache"
FASTA = DATA / "chr22.fa.gz"


def _lfs_pointer(path: Path) -> bool:
    with open(path, "rb") as handle:
        return handle.read(24).startswith(b"version https://git-lfs")


_FIXTURE_FILES = [FASTA, *sorted(CACHE.glob("*/chr22.parquet"))]
_MISSING = [str(p) for p in _FIXTURE_FILES if not p.exists() or _lfs_pointer(p)]
pytestmark = pytest.mark.skipif(
    bool(_MISSING), reason=f"hg002_chr22 fixture not fetched from LFS: {_MISSING}"
)

HEADER = """##contig=<ID=chr22>
##INFO=<ID=CLNDN,Number=.,Type=String,Description="Disease names">
##INFO=<ID=CLNVI,Number=.,Type=String,Description="Variant identifiers">
##INFO=<ID=GENEINFO,Number=1,Type=String,Description="Gene">
##INFO=<ID=P,Number=1,Type=String,Description="Percent">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=FS,Number=1,Type=String,Description="Sample string">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1
"""

# Escapes the writer must restore: every reserved character, in a list and in a
# scalar, plus a FORMAT `:`. Literal `:` and `|` in INFO stay literal.
RESERVED = [
    (
        "CLNDN=Lung_disease%3B|not_provided;CLNVI=BIC:5817&base_change%3DC_to_G;"
        "GENEINFO=G%3B1:23;P=50%25",
        "0/1:x%3Ay",
    ),
    ("CLNDN=a%2Cb,c;CLNVI=t(12%3B21)(p13%3Bq22);GENEINFO=X%3DY;P=100%25", "0/1:a%2Cb"),
    # Line controls are escaped too: a decoded tab would split the record.
    ("GENEINFO=tab%09here;P=cr%0Dlf%0A", "0/1:t%09"),
]

# An escaped space, in a scalar and in a list element (ClinVar 51220,
# `CLNVI=...base_change%3Dins%20A`).
ESCAPED_SPACE = ("GENEINFO=base_change%3Dins%20A;CLNVI=a%20b,c;P=x", "0/1:x")

# Inputs that the typed values alone cannot restore. Formats #272 carries the
# original INFO text, so these four INFO cases now round-trip exactly. The
# FORMAT sample semicolon remains outside that raw-INFO carry and stays an
# expected failure.
UNRESTORABLE = {
    "space_from_vcf_4_3": ("VCFv4.3", "GENEINFO=base_change%3Dins%20A;P=x", "0/1:x"),
    "plus": ("VCFv4.2", "GENEINFO=a%2Bb;P=x", "0/1:x"),
    "lower_case": ("VCFv4.2", "GENEINFO=a%3bb;P=x", "0/1:x"),
    "format_semicolon": ("VCFv4.2", "GENEINFO=x;P=x", "0/1:p%3Bq"),
    "literal_percent": ("VCFv4.2", "GENEINFO=x;P=50%", "0/1:x"),
}


def _vcf(records: list[tuple[str, str]], file_format: str) -> str:
    lines = [
        f"chr22\t16592176\tp{i}\tA\tG\t.\t.\t{info}\tGT:FS\t{sample}\n"
        for i, (info, sample) in enumerate(records, 1)
    ]
    return f"##fileformat={file_format}\n" + HEADER + "".join(lines)


def _annotate_vcf(
    tmp_path: Path, records: list[tuple[str, str]], file_format: str = "VCFv4.2"
) -> list[list[str]]:
    src = tmp_path / "input.vcf"
    src.write_text(_vcf(records, file_format))
    out = tmp_path / "output.vcf"
    vepyr.annotate(
        str(src),
        str(CACHE),
        reference_fasta=str(FASTA),
        output_vcf=str(out),
        show_progress=False,
    )
    return [
        line.split("\t")
        for line in out.read_text().splitlines()
        if line and not line.startswith("#")
    ]


def _input_info(record: list[str]) -> str:
    # CSQ is appended after the input's own keys, so the input INFO is
    # everything before the last `;CSQ=`.
    return record[7].rsplit(";CSQ=", 1)[0]


def test_reserved_escapes_are_written_back_byte_for_byte(tmp_path):
    records = _annotate_vcf(tmp_path, RESERVED)

    assert [(_input_info(r), r[9]) for r in records] == RESERVED


def test_csq_is_not_encoded_a_second_time(tmp_path):
    records = _annotate_vcf(tmp_path, RESERVED)

    for record in records:
        csq = record[7].rsplit(";CSQ=", 1)[1]
        assert "p.Ala125%3D" in csq
        assert "%25" not in csq
        blocks = csq.split(",")
        assert len(blocks) > 1
        assert not any("%2C" in block for block in blocks)


def test_the_lazyframe_keeps_the_decoded_values(tmp_path):
    src = tmp_path / "input.vcf"
    src.write_text(_vcf(RESERVED, "VCFv4.2"))
    df = vepyr.annotate(
        str(src), str(CACHE), reference_fasta=str(FASTA), show_progress=False
    ).collect()

    assert df["CLNDN"].to_list()[:2] == [["Lung_disease;|not_provided"], ["a,b", "c"]]
    assert df["GENEINFO"].to_list() == ["G;1:23", "X=Y", "tab\there"]
    assert df["P"].to_list() == ["50%", "100%", "cr\rlf\n"]


@pytest.mark.parametrize("file_format", ["VCFv4.1", "VCFv4.2"])
def test_an_escaped_space_is_written_back_before_vcf_4_3(tmp_path, file_format):
    (written,) = _annotate_vcf(tmp_path, [ESCAPED_SPACE], file_format)

    assert (_input_info(written), written[9]) == ESCAPED_SPACE


def test_a_literal_space_stays_literal_from_vcf_4_3(tmp_path):
    record = ("GENEINFO=base_change%3Dins A;CLNVI=a b,c;P=x", "0/1:x")
    (written,) = _annotate_vcf(tmp_path, [record], "VCFv4.3")

    assert (_input_info(written), written[9]) == record


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            "format_semicolon",
            marks=pytest.mark.xfail(
                strict=True,
                reason="the raw source carry covers INFO, not FORMAT sample text",
            ),
        ),
        "literal_percent",
        "lower_case",
        "plus",
        "space_from_vcf_4_3",
    ],
)
def test_unrestorable_escapes_are_written_back_byte_for_byte(tmp_path, case):
    file_format, *record = UNRESTORABLE[case]
    (written,) = _annotate_vcf(tmp_path, [tuple(record)], file_format)

    assert (_input_info(written), written[9]) == tuple(record)
