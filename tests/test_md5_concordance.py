from __future__ import annotations

from pathlib import Path
import sys

import pytest

SCRIPTS_DIR = Path(__file__).parents[1] / "e2e-testing" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import md5_concordance as mc  # noqa: E402

RECORD = "chr21\t100\t.\tA\tT\t50\tPASS\tDP=1;CSQ=T|missense\tGT\t0/1\n"

VEP_HEADER = """##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##VEP="v116" time="2026-08-23 09:00:00" cache="/opt/vep/.vep"
##VEP-command-line='vep --everything'
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tHG002
"""

VEPYR_HEADER = """##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##datafusion-bio-function-vep="0.15.0" cache="/home/me/cache" tool="vepyr 0.3.0"
##datafusion-bio-function-vep-command-line='{"engine":"datafusion-bio-function-vep"}'
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tHG002
"""


def write(tmp_path: Path, name: str, header: str, record: str = RECORD) -> str:
    path = tmp_path / name
    path.write_text(header + record)
    return str(path)


def test_each_sides_own_provenance_is_left_out_of_the_header_digest(tmp_path):
    """Both tools stamp the header with run-specific provenance — wall-clock
    time, absolute cache paths, tool versions — that can never match. Only
    VEP's was excluded, so vepyr's own lines reported as a header difference
    against output that is otherwise identical."""
    vep = mc.digest_vcf(write(tmp_path, "vep.vcf", VEP_HEADER), "strict")
    vepyr = mc.digest_vcf(write(tmp_path, "vepyr.vcf", VEPYR_HEADER), "strict")

    assert vep.header == vepyr.header
    assert vep.header_lines == vepyr.header_lines == 3


def test_excluded_provenance_does_not_hide_a_real_header_difference(tmp_path):
    other = VEPYR_HEADER.replace('Description="Depth"', 'Description="Read depth"')
    vep = mc.digest_vcf(write(tmp_path, "vep.vcf", VEP_HEADER), "strict")
    vepyr = mc.digest_vcf(write(tmp_path, "vepyr.vcf", other), "strict")

    assert vep.header != vepyr.header
    only_vep, only_vepyr = mc.diff_headers(
        write(tmp_path, "vep2.vcf", VEP_HEADER),
        write(tmp_path, "vepyr2.vcf", other),
    )
    assert only_vep == ['##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">']
    assert only_vepyr == [
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="Read depth">'
    ]


def test_provenance_lines_never_reach_the_body_digest(tmp_path):
    vep = mc.digest_vcf(write(tmp_path, "vep.vcf", VEP_HEADER), "strict")
    vepyr = mc.digest_vcf(write(tmp_path, "vepyr.vcf", VEPYR_HEADER), "strict")

    assert vep.body == vepyr.body
    assert vep.records == vepyr.records == 1


# ---------------------------------------------------------------------------
# review findings on the comparator itself (vepyr#45)
# ---------------------------------------------------------------------------

DECLARATIONS = """##fileformat=VCFv4.2
##reference=GRCh38
##contig=<ID=chr21,length=46709983>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
"""
HISTORY = "##bcftools_normCommand=norm -m -both -o out.vcf in.vcf\n"
COLUMNS = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tHG002\n"


def pair_files(tmp_path, vep_header, vepyr_header, vep_body=RECORD, vepyr_body=RECORD):
    vep = tmp_path / "vep_chr21.vcf"
    vepyr = tmp_path / "vepyr_chr21.vcf"
    vep.write_bytes((vep_header + COLUMNS).encode() + vep_body.encode())
    vepyr.write_bytes((vepyr_header + COLUMNS).encode() + vepyr_body.encode())
    return str(vep), str(vepyr)


def test_a_directory_holding_only_one_side_is_rejected(tmp_path):
    """A contig whose run produced only one side used to be skipped silently,
    so the gate could report PASS while omitting the chromosome that failed."""
    complete = tmp_path / "fast_chr21"
    complete.mkdir()
    pair_files(complete, DECLARATIONS, DECLARATIONS)

    half = tmp_path / "fast_chr22"
    half.mkdir()
    (half / "vep_chr22.vcf").write_text(DECLARATIONS + COLUMNS + RECORD)

    with pytest.raises(mc.ConcordanceError) as excinfo:
        mc.discover_pairs(tmp_path, "vep_*.vcf*", "vepyr_*.vcf*")
    message = str(excinfo.value)
    assert "fast_chr22" in message
    assert "vepyr" in message


def test_a_directory_holding_neither_side_is_still_skipped(tmp_path):
    """`_shared/` and friends carry inputs, not outputs; they are not runs."""
    shared = tmp_path / "_shared"
    shared.mkdir()
    (shared / "normalized.vcf").write_text(DECLARATIONS + COLUMNS + RECORD)

    complete = tmp_path / "fast_chr21"
    complete.mkdir()
    pair_files(complete, DECLARATIONS, DECLARATIONS)

    pairs = mc.discover_pairs(tmp_path, "vep_*.vcf*", "vepyr_*.vcf*")
    assert [p.label for p in pairs] == ["chr21"]


def test_a_differing_declaration_fails_the_gate(tmp_path):
    """Header declarations define how the record bytes are to be read, so two
    files whose bodies match byte for byte are still not interchangeable when
    an ##INFO definition disagrees."""
    changed = DECLARATIONS.replace('Description="Depth"', 'Description="Read depth"')
    vep, vepyr = pair_files(tmp_path, DECLARATIONS, changed)
    assert mc.main(["--pair", vep, vepyr, "--mode", "strict"]) == 1


def test_a_differing_input_provenance_line_does_not_fail_the_gate(tmp_path):
    """`##bcftools_normCommand` records how the *input* was produced. It is
    reported, because a difference means the two sides were normalized
    differently, but it does not change how any record is interpreted."""
    vep, vepyr = pair_files(tmp_path, DECLARATIONS + HISTORY, DECLARATIONS)
    assert mc.main(["--pair", vep, vepyr, "--mode", "strict"]) == 0


def test_strict_mode_does_not_normalize_line_endings(tmp_path):
    """Strict mode hashes the record bytes as-is; reading through universal
    newlines made a CRLF file and an LF file digest identically."""
    vep, vepyr = pair_files(
        tmp_path, DECLARATIONS, DECLARATIONS, vepyr_body=RECORD.replace("\n", "\r\n")
    )
    assert mc.main(["--pair", vep, vepyr, "--mode", "strict"]) == 1


def test_canonical_mode_still_ignores_line_endings(tmp_path):
    """Line-ending style is exactly the kind of cosmetic serialization
    difference canonical mode exists to normalize."""
    vep, vepyr = pair_files(
        tmp_path, DECLARATIONS, DECLARATIONS, vepyr_body=RECORD.replace("\n", "\r\n")
    )
    assert mc.main(["--pair", vep, vepyr, "--mode", "canonical"]) == 0
