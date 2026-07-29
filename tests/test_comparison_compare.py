import json
import itertools
import subprocess

import pytest

from comparison import compare

HEADER = (
    "##fileformat=VCFv4.2\n"
    '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations. '
    'Format: Allele|Consequence|IMPACT|Feature">\n'
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
)

MATCHING = HEADER + (
    "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|missense_variant|MODERATE|ENST01\n"
    "chr1\t200\t.\tG\tC\t50\tPASS\tCSQ=C|synonymous_variant|LOW|ENST01\n"
)

DIFFERING = HEADER + (
    "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|stop_gained|HIGH|ENST01\n"
    "chr1\t200\t.\tG\tC\t50\tPASS\tCSQ=C|synonymous_variant|LOW|ENST01\n"
)


def _write(tmp_path, name, body, compressed):
    plain = tmp_path / name
    plain.write_text(body)
    if not compressed:
        return str(plain)
    gz = tmp_path / (name + ".gz")
    with open(gz, "wb") as fh:
        subprocess.run(["bgzip", "-c", str(plain)], stdout=fh, check=True)
    return str(gz)


@pytest.mark.parametrize(
    "vepyr_gz,vep_gz", list(itertools.product([False, True], repeat=2))
)
def test_compare_is_identical_across_all_compression_combinations(
    tmp_path, vepyr_gz, vep_gz
):
    """Regression for the bare open() that raised UnicodeDecodeError on bgzf refs."""
    a = _write(tmp_path, "vepyr.vcf", MATCHING, vepyr_gz)
    b = _write(tmp_path, "vep.vcf", MATCHING, vep_gz)
    result = compare.compare_vcfs(a, b, "combo")
    assert result["variants_compared"] == 2
    assert result["variants_only_in_vepyr"] == 0
    assert result["variants_only_in_vep"] == 0
    assert result["field_mismatch_counts"] == {}
    assert result["field_match_rates"]["Consequence"] == 100.0


def test_compare_counts_field_mismatches(tmp_path):
    a = _write(tmp_path, "vepyr.vcf", MATCHING, False)
    b = _write(tmp_path, "vep.vcf", DIFFERING, False)
    result = compare.compare_vcfs(a, b, "diff")
    assert result["field_mismatch_counts"]["Consequence"] == 1
    assert result["field_mismatch_counts"]["IMPACT"] == 1
    assert (
        result["field_mismatch_examples"]["Consequence"][0]["vepyr"]
        == "missense_variant"
    )
    assert result["equality_bucket_counts"]["both_nonempty_unequal"] == 2
    assert result["field_equality_counts"]["Consequence"]["both_nonempty_unequal"] == 1


def test_compare_can_ignore_vep_hash_order_csq_order(tmp_path):
    two_entries = HEADER + (
        "chr1\t100\t.\tA\tT\t50\tPASS\t"
        "CSQ=T|missense_variant|MODERATE|ENST01,T|intron_variant|MODIFIER|ENST02\n"
    )
    reordered = HEADER + (
        "chr1\t100\t.\tA\tT\t50\tPASS\t"
        "CSQ=T|intron_variant|MODIFIER|ENST02,T|missense_variant|MODERATE|ENST01\n"
    )
    a = _write(tmp_path, "vepyr.vcf", two_entries, False)
    b = _write(tmp_path, "vep.vcf", reordered, False)

    strict = compare.compare_vcfs(a, b, "strict")
    assert strict["csq_order_mismatch"] == 1
    assert strict["csq_order_ignored"] == 0

    lenient = compare.compare_vcfs(a, b, "lenient", ignore_csq_order=True)
    assert lenient["csq_order_mismatch"] == 0
    assert lenient["csq_order_ignored"] == 1
    assert lenient["csq_order_ignore_reason"] == (
        compare.VEP_HASH_ORDER_PICK_IGNORE_REASON
    )


def test_compare_reports_variants_present_in_only_one_side(tmp_path):
    extra = (
        MATCHING
        + "chr1\t300\t.\tT\tA\t50\tPASS\tCSQ=A|intron_variant|MODIFIER|ENST01\n"
    )
    a = _write(tmp_path, "vepyr.vcf", extra, False)
    b = _write(tmp_path, "vep.vcf", MATCHING, False)
    result = compare.compare_vcfs(a, b, "extra")
    assert result["variants_only_in_vepyr"] == 1
    assert result["variants_only_in_vep"] == 0


def test_mismatch_ledger_is_uncapped_and_content_hashed(tmp_path):
    records_a = []
    records_b = []
    for position in range(1, 13):
        prefix = f"chr1\t{position}\t.\tA\tT\t50\tPASS\tCSQ="
        records_a.append(prefix + "T|missense_variant|MODERATE|ENST01\n")
        records_b.append(prefix + "T|stop_gained|HIGH|ENST01\n")
    a = _write(tmp_path, "vepyr.vcf", HEADER + "".join(records_a), False)
    b = _write(tmp_path, "vep.vcf", HEADER + "".join(records_b), False)
    ledger = tmp_path / "mismatches.jsonl"

    result = compare.compare_vcfs(a, b, "uncapped", mismatch_ledger_path=str(ledger))

    rows = [json.loads(line) for line in ledger.read_text().splitlines()]
    assert result["field_mismatch_counts"] == {"Consequence": 12, "IMPACT": 12}
    assert len(result["field_mismatch_examples"]["Consequence"]) == 10
    assert len(rows) == 24
    assert result["mismatch_ledger"]["rows"] == 24
    assert len(result["mismatch_ledger"]["sha256"]) == 64
    assert {row["kind"] for row in rows} == {"field_mismatch"}


def test_entry_pairing_uses_allele_identity_before_feature(tmp_path):
    header = (
        "##fileformat=VCFv4.2\n"
        '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations. '
        'Format: Allele|ALLELE_NUM|Consequence|IMPACT|Feature">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
    )
    vepyr = header + (
        "chr1\t100\t.\tA\tT,G\t50\tPASS\t"
        "CSQ=T|1|missense_variant|MODERATE|ENST01,"
        "G|2|missense_variant|MODERATE|ENST01\n"
    )
    vep = header + (
        "chr1\t100\t.\tA\tT,G\t50\tPASS\t"
        "CSQ=G|2|missense_variant|HIGH|ENST01,"
        "T|1|missense_variant|MODERATE|ENST01\n"
    )
    a = _write(tmp_path, "vepyr.vcf", vepyr, False)
    b = _write(tmp_path, "vep.vcf", vep, False)
    ledger = tmp_path / "alleles.jsonl"

    result = compare.compare_vcfs(
        a,
        b,
        "alleles",
        ignore_csq_order=True,
        mismatch_ledger_path=str(ledger),
    )

    assert result["field_mismatch_counts"] == {"IMPACT": 1}
    mismatch = json.loads(ledger.read_text().splitlines()[0])
    assert mismatch["allele"] == "G"
    assert mismatch["allele_num"] == "2"
    assert mismatch["feature"] == "ENST01"
    assert mismatch["duplicate_ordinal"] == 1


def test_unmatched_csq_entry_is_structural_not_a_shifted_field_mismatch(tmp_path):
    with_extra = HEADER + (
        "chr1\t100\t.\tA\tT\t50\tPASS\t"
        "CSQ=T|intron_variant|MODIFIER|ENST00,"
        "T|missense_variant|MODERATE|ENST01\n"
    )
    without_extra = HEADER + (
        "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|missense_variant|MODERATE|ENST01\n"
    )
    a = _write(tmp_path, "vepyr.vcf", with_extra, False)
    b = _write(tmp_path, "vep.vcf", without_extra, False)
    ledger = tmp_path / "entries.jsonl"

    result = compare.compare_vcfs(a, b, "entry-extra", mismatch_ledger_path=str(ledger))

    assert result["field_mismatch_counts"] == {}
    assert result["csq_entries_only_in_vepyr"] == 1
    rows = [json.loads(line) for line in ledger.read_text().splitlines()]
    assert [row["kind"] for row in rows] == ["csq_entry_only_in_vepyr"]
    assert rows[0]["feature"] == "ENST00"
