from __future__ import annotations

from pathlib import Path
import sys

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
