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


# Ensembl VEP emits the selected CSQ entries of --per_gene and --pick_allele_gene
# by iterating Perl hashes, so their order changes from one VEP run to the next
# (vepyr#138). Profiles marked ignore_csq_order hash them order-insensitively.
TWO_GENE_CSQ = "T|missense|GENE_A,T|intron|GENE_B"
SWAPPED_CSQ = "T|intron|GENE_B,T|missense|GENE_A"


def record_with_csq(csq: str, info_prefix: str = "DP=1;") -> str:
    return f"chr21\t100\t.\tA\tT\t50\tPASS\t{info_prefix}CSQ={csq}\tGT\t0/1\n"


def test_csq_entry_order_differs_by_default(tmp_path):
    for mode in ("strict", "canonical"):
        vep = mc.digest_vcf(
            write(tmp_path, "vep.vcf", VEP_HEADER, record_with_csq(TWO_GENE_CSQ)),
            mode,
        )
        vepyr = mc.digest_vcf(
            write(tmp_path, "vepyr.vcf", VEPYR_HEADER, record_with_csq(SWAPPED_CSQ)),
            mode,
        )
        assert vep.body != vepyr.body, mode


def test_ignore_csq_order_matches_reordered_entries(tmp_path):
    for mode in ("strict", "canonical"):
        vep = mc.digest_vcf(
            write(tmp_path, "vep.vcf", VEP_HEADER, record_with_csq(TWO_GENE_CSQ)),
            mode,
            ignore_csq_order=True,
        )
        vepyr = mc.digest_vcf(
            write(tmp_path, "vepyr.vcf", VEPYR_HEADER, record_with_csq(SWAPPED_CSQ)),
            mode,
            ignore_csq_order=True,
        )
        assert vep.body == vepyr.body, mode


def test_ignore_csq_order_still_catches_a_content_difference(tmp_path):
    changed = "T|intron|GENE_B,T|synonymous|GENE_A"
    for mode in ("strict", "canonical"):
        vep = mc.digest_vcf(
            write(tmp_path, "vep.vcf", VEP_HEADER, record_with_csq(TWO_GENE_CSQ)),
            mode,
            ignore_csq_order=True,
        )
        vepyr = mc.digest_vcf(
            write(tmp_path, "vepyr.vcf", VEPYR_HEADER, record_with_csq(changed)),
            mode,
            ignore_csq_order=True,
        )
        assert vep.body != vepyr.body, mode


def test_ignore_csq_order_keeps_a_missing_or_duplicated_entry_visible(tmp_path):
    """Sorting must not collapse duplicates: an extra copy of an entry is a
    content difference, not an order difference."""
    duplicated = "T|missense|GENE_A,T|intron|GENE_B,T|intron|GENE_B"
    vep = mc.digest_vcf(
        write(tmp_path, "vep.vcf", VEP_HEADER, record_with_csq(TWO_GENE_CSQ)),
        "strict",
        ignore_csq_order=True,
    )
    vepyr = mc.digest_vcf(
        write(tmp_path, "vepyr.vcf", VEPYR_HEADER, record_with_csq(duplicated)),
        "strict",
        ignore_csq_order=True,
    )
    assert vep.body != vepyr.body


def test_ignore_csq_order_leaves_everything_but_csq_byte_exact_in_strict(tmp_path):
    """Only the CSQ entry order is relaxed; strict must still see other INFO
    key order, which canonical mode normalises separately."""
    vep = mc.digest_vcf(
        write(
            tmp_path,
            "vep.vcf",
            VEP_HEADER,
            record_with_csq(TWO_GENE_CSQ, "DP=1;AF=0.5;"),
        ),
        "strict",
        ignore_csq_order=True,
    )
    vepyr = mc.digest_vcf(
        write(
            tmp_path,
            "vepyr.vcf",
            VEPYR_HEADER,
            record_with_csq(SWAPPED_CSQ, "AF=0.5;DP=1;"),
        ),
        "strict",
        ignore_csq_order=True,
    )
    assert vep.body != vepyr.body


def test_sort_csq_entries_handles_csq_as_the_only_or_middle_key():
    assert mc.sort_csq_entries(record_with_csq(TWO_GENE_CSQ, "")) == record_with_csq(
        SWAPPED_CSQ, ""
    )
    middle = "chr21\t100\t.\tA\tT\t50\tPASS\tDP=1;CSQ=b,a;AF=1\tGT\t0/1\n"
    assert mc.sort_csq_entries(middle) == (
        "chr21\t100\t.\tA\tT\t50\tPASS\tDP=1;CSQ=a,b;AF=1\tGT\t0/1\n"
    )
    no_csq = "chr21\t100\t.\tA\tT\t50\tPASS\tDP=1\tGT\t0/1\n"
    assert mc.sort_csq_entries(no_csq) == no_csq


def test_explain_names_a_csq_order_difference():
    vep = record_with_csq(TWO_GENE_CSQ).rstrip("\n")
    vepyr = record_with_csq(SWAPPED_CSQ).rstrip("\n")
    assert mc.classify_difference(vep, vepyr) == ["CSQ order"]


def test_compare_passes_ignore_csq_order_through(tmp_path):
    pair = mc.Pair(
        "chr21",
        write(tmp_path, "vep.vcf", VEP_HEADER, record_with_csq(TWO_GENE_CSQ)),
        write(tmp_path, "vepyr.vcf", VEPYR_HEADER, record_with_csq(SWAPPED_CSQ)),
    )
    assert not mc.compare(pair, "strict").body_match
    assert mc.compare(pair, "strict", ignore_csq_order=True).body_match


def test_ignore_csq_order_in_a_sites_only_vcf(tmp_path):
    """No FORMAT/sample columns: INFO is last and ends with the newline."""
    sites = "chr21\t100\t.\tA\tT\t50\tPASS\tCSQ={}\n"
    for mode in ("strict", "canonical"):
        vep = mc.digest_vcf(
            write(tmp_path, "vep.vcf", VEP_HEADER, sites.format(TWO_GENE_CSQ)),
            mode,
            ignore_csq_order=True,
        )
        vepyr = mc.digest_vcf(
            write(tmp_path, "vepyr.vcf", VEPYR_HEADER, sites.format(SWAPPED_CSQ)),
            mode,
            ignore_csq_order=True,
        )
        assert vep.body == vepyr.body, mode
