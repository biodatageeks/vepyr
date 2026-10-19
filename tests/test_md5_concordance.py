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


# Ensembl VEP 116.0 joins a co-located variant's allele-specific clinical
# significances with `join(';', keys(%clin_sigs))`, unsorted, so a CLIN_SIG
# with two or more values (`%3B`-separated in CSQ) changes order between runs
# and between the CSQ entries of one record (vepyr#238).
CLIN_SIG_HEADER = (
    "##fileformat=VCFv4.2\n"
    '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations '
    'from Ensembl VEP. Format: Allele|Consequence|Feature|CLIN_SIG|PHENO">\n'
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
)
CLIN_SIG_VEPYR = "A|missense|ENST01|risk_factor%3Brisk_factor&benign|1"
CLIN_SIG_VEP = "A|missense|ENST01|risk_factor&benign%3Brisk_factor|1"
CLIN_SIG_DATA = Path(__file__).parent / "data" / "clin_sig_order"


def clin_sig_record(*entries: str, pos: int = 100) -> str:
    return f"chr22\t{pos}\t.\tG\tA\t.\t.\tCSQ={','.join(entries)}\n"


def clin_sig_digests(tmp_path, vep_record, vepyr_record, mode, **kw):
    vep = mc.digest_vcf(
        write(tmp_path, "vep.vcf", CLIN_SIG_HEADER, vep_record), mode, **kw
    )
    vepyr = mc.digest_vcf(
        write(tmp_path, "vepyr.vcf", CLIN_SIG_HEADER, vepyr_record), mode, **kw
    )
    return vep, vepyr


def test_clin_sig_group_order_is_ignored_by_canonical_but_not_by_strict(tmp_path):
    """The regression test #238 asks for: two CSQ lines differing only in the
    order of CLIN_SIG's %3B groups share a canonical digest and keep different
    strict digests. Strict is the byte-parity claim and must stay untouched."""
    vep_record = clin_sig_record(CLIN_SIG_VEP)
    vepyr_record = clin_sig_record(CLIN_SIG_VEPYR)

    vep, vepyr = clin_sig_digests(tmp_path, vep_record, vepyr_record, "canonical")
    assert vep.body == vepyr.body

    vep, vepyr = clin_sig_digests(tmp_path, vep_record, vepyr_record, "strict")
    assert vep.body != vepyr.body
    assert vep.clin_sig_reordered == vepyr.clin_sig_reordered == {}


def test_ignore_csq_order_also_ignores_clin_sig_group_order_in_strict(tmp_path):
    """--ignore-csq-order exists for VEP's Perl hash order, and CLIN_SIG is the
    same defect inside an entry. The two are applied together, so entries whose
    CLIN_SIG order differs still sort into the same entry order."""
    other = "A|intron|ENST02||"
    vep_record = clin_sig_record(other, CLIN_SIG_VEP)
    vepyr_record = clin_sig_record(CLIN_SIG_VEPYR, other)
    vep, vepyr = clin_sig_digests(
        tmp_path, vep_record, vepyr_record, "strict", ignore_csq_order=True
    )
    assert vep.body == vepyr.body


def test_clin_sig_sort_keeps_real_differences_visible(tmp_path):
    """Only %3B groups move. The '&' order inside a group comes from the cache,
    and a missing, extra or changed value is a content difference."""
    vep_record = clin_sig_record(CLIN_SIG_VEP)
    for vepyr_value in (
        "risk_factor%3Bbenign&risk_factor",  # '&' order inside a group
        "risk_factor&benign",  # a group missing
        "risk_factor%3Brisk_factor%3Brisk_factor&benign",  # a group duplicated
        "risk_factor%3Bpathogenic&benign",  # a value changed
    ):
        vepyr_record = clin_sig_record(f"A|missense|ENST01|{vepyr_value}|1")
        vep, vepyr = clin_sig_digests(tmp_path, vep_record, vepyr_record, "canonical")
        assert vep.body != vepyr.body, vepyr_value


def test_clin_sig_sort_touches_only_the_clin_sig_column():
    """A %3B in any other sub-field is left in place, and a VCF whose CSQ
    declares no CLIN_SIG is never rewritten."""
    index = mc.csq_field_index(CLIN_SIG_HEADER.splitlines()[1])
    assert index == 3
    line = clin_sig_record("A|b%3Ba|ENST01|z%3By|c%3Ba")
    assert mc.sort_clin_sig(line, index) == clin_sig_record(
        "A|b%3Ba|ENST01|y%3Bz|c%3Ba"
    )
    assert mc.sort_clin_sig(line, None) == line
    assert mc.csq_field_index('##INFO=<ID=DP,Description="Format: CLIN_SIG">') is None
    assert (
        mc.csq_field_index(
            '##INFO=<ID=CSQ,Number=.,Type=String,Description="Format: Allele|PHENO">'
        )
        is None
    )
    # A short entry (fewer sub-fields than the header declares) is left alone.
    assert mc.sort_clin_sig(clin_sig_record("A|x%3Bw"), index) == clin_sig_record(
        "A|x%3Bw"
    )


def test_canonical_record_sorts_clin_sig_when_given_its_index():
    line = clin_sig_record(CLIN_SIG_VEP).rstrip("\n")
    assert mc.canonical_record(line) == line
    assert mc.canonical_record(line, 3) == clin_sig_record(CLIN_SIG_VEPYR).rstrip("\n")


def test_compare_reports_clin_sig_order_records_as_vep_nondeterministic(tmp_path):
    """Records that concord only because of the sort are counted and named,
    never silently folded into the plain matches."""
    vep_body = (
        clin_sig_record(CLIN_SIG_VEP, CLIN_SIG_VEPYR, pos=100)  # one entry flipped
        + clin_sig_record(CLIN_SIG_VEPYR, pos=200)  # identical, multi-valued
        + clin_sig_record(CLIN_SIG_VEPYR, pos=300)  # only vepyr out of order
        + clin_sig_record(CLIN_SIG_VEP, pos=400)  # identical, both reordered
    )
    vepyr_body = (
        clin_sig_record(CLIN_SIG_VEPYR, CLIN_SIG_VEPYR, pos=100)
        + clin_sig_record(CLIN_SIG_VEPYR, pos=200)
        + clin_sig_record(CLIN_SIG_VEP, pos=300)
        + clin_sig_record(CLIN_SIG_VEP, pos=400)
    )
    pair = mc.Pair(
        "chr22",
        write(tmp_path, "vep.vcf", CLIN_SIG_HEADER, vep_body),
        write(tmp_path, "vepyr.vcf", CLIN_SIG_HEADER, vepyr_body),
    )

    canonical = mc.compare(pair, "canonical")
    assert canonical.body_match
    assert canonical.clin_sig_order_only == 2
    assert any("vepyr#238" in note for note in canonical.notes)

    strict = mc.compare(pair, "strict")
    assert not strict.body_match
    assert strict.clin_sig_order_only == 0

    relaxed = mc.compare(pair, "strict", ignore_csq_order=True)
    assert relaxed.body_match
    assert relaxed.clin_sig_order_only == 2


def test_explain_names_a_clin_sig_order_difference():
    vep = clin_sig_record(CLIN_SIG_VEP).rstrip("\n")
    vepyr = clin_sig_record(CLIN_SIG_VEPYR).rstrip("\n")
    assert mc.classify_difference(vep, vepyr) == ["INFO VALUES"]
    assert mc.classify_difference(vep, vepyr, (3, 3)) == [
        "CLIN_SIG order (VEP-nondeterministic, vepyr#238)"
    ]


def test_main_prints_the_vep_nondeterministic_count(tmp_path, capsys):
    vep = write(tmp_path, "vep.vcf", CLIN_SIG_HEADER, clin_sig_record(CLIN_SIG_VEP))
    vepyr = write(
        tmp_path, "vepyr.vcf", CLIN_SIG_HEADER, clin_sig_record(CLIN_SIG_VEPYR)
    )
    assert mc.main(["--pair", vep, vepyr]) == 0
    out = capsys.readouterr().out
    assert "VEP-nondeterministic: 1 record(s)" in out
    assert mc.main(["--pair", vep, vepyr, "--mode", "strict"]) == 1


def test_real_clinvar_record_22_26666332(tmp_path):
    """ClinVar 2691795 (22:26666332 G>A), cut from the e2e/clinvar-parity
    fixture chr22/hgvs to its first four CSQ entries: two carry VEP's flipped
    `risk_factor&benign%3Brisk_factor`, two match vepyr. CLIN_SIG is the only
    difference, so canonical concords and names the record nondeterministic."""
    pair = mc.Pair(
        "chr22",
        str(CLIN_SIG_DATA / "vep.vcf"),
        str(CLIN_SIG_DATA / "vepyr.vcf"),
    )
    assert not mc.compare(pair, "strict").body_match
    canonical = mc.compare(pair, "canonical")
    assert canonical.body_match
    assert canonical.clin_sig_order_only == 1
    assert dict(mc.explain(pair.vep, pair.vepyr, 0)) == {
        "CLIN_SIG order (VEP-nondeterministic, vepyr#238)": 1
    }


def test_explain_names_csq_order_plus_clin_sig_order():
    other = "A|intron|ENST02||"
    vep = clin_sig_record(other, CLIN_SIG_VEP).rstrip("\n")
    vepyr = clin_sig_record(CLIN_SIG_VEPYR, other).rstrip("\n")
    assert mc.classify_difference(vep, vepyr, (3, 3)) == [
        "CSQ order + CLIN_SIG order (VEP-nondeterministic, vepyr#238)"
    ]
