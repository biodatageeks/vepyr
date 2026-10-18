"""Percent-encoded input INFO/FORMAT values in the annotated VCF (vepyr#207).

ClinVar writes a `;` inside an INFO value as `%3B` and an `=` as `%3D`, as VCF
4.3 §1.2 requires. Ensembl VEP copies the input line and appends CSQ
(`OutputFactory/VCF.pm:314-355` at ensembl-vep `release/116.0`), so the input's
bytes come back unchanged. vepyr decodes the escapes on read, which is right
for the DataFrame, and has to encode them again when it writes the VCF:
written back raw, a `;` ends the INFO field early and a `,` adds a list element.

CSQ is the engine's own, already escaped by the VEP rules (`p.Ala125%3D`), so
it must not be encoded a second time.

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

HEADER = """##fileformat=VCFv4.2
##contig=<ID=chr22>
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

# Escapes the re-encoding writer cannot restore: an unreserved character
# (`%20`, `%2B`, a `;` in a sample column) comes back decoded and a lower-case
# escape upper-cased. Exact bytes need the raw input text carried through, the
# follow-up to #207. One case each, so a partial follow-up shows up as an XPASS.
UNRESTORABLE = {
    "space": ("GENEINFO=base_change%3Dins%20A;P=x", "0/1:x"),
    "plus": ("GENEINFO=a%2Bb;P=x", "0/1:x"),
    "lower_case": ("GENEINFO=a%3bb;P=x", "0/1:x"),
    "format_semicolon": ("GENEINFO=x;P=x", "0/1:p%3Bq"),
}


def _vcf(records: list[tuple[str, str]]) -> str:
    lines = [
        f"chr22\t16592176\tp{i}\tA\tG\t.\t.\t{info}\tGT:FS\t{sample}\n"
        for i, (info, sample) in enumerate(records, 1)
    ]
    return HEADER + "".join(lines)


def _annotate_vcf(tmp_path: Path, records: list[tuple[str, str]]) -> list[list[str]]:
    src = tmp_path / "input.vcf"
    src.write_text(_vcf(records))
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
    src.write_text(_vcf(RESERVED))
    df = vepyr.annotate(
        str(src), str(CACHE), reference_fasta=str(FASTA), show_progress=False
    ).collect()

    assert df["CLNDN"].to_list()[:2] == [["Lung_disease;|not_provided"], ["a,b", "c"]]
    assert df["GENEINFO"].to_list() == ["G;1:23", "X=Y", "tab\there"]
    assert df["P"].to_list() == ["50%", "100%", "cr\rlf\n"]


@pytest.mark.xfail(
    strict=True,
    reason="vepyr#207 follow-up: escapes of unreserved characters and lower-case "
    "escapes need the raw input text carried through",
)
@pytest.mark.parametrize("case", sorted(UNRESTORABLE))
def test_unrestorable_escapes_are_written_back_byte_for_byte(tmp_path, case):
    record = UNRESTORABLE[case]
    (written,) = _annotate_vcf(tmp_path, [record])

    assert (_input_info(written), written[9]) == record
