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


def test_compare_treats_chr_prefixed_and_bare_contigs_as_the_same_key(tmp_path):
    bare_contigs = MATCHING.replace("\nchr1\t", "\n1\t")
    a = _write(tmp_path, "vepyr.vcf", MATCHING, False)
    b = _write(tmp_path, "vep.vcf", bare_contigs, False)

    result = compare.compare_vcfs(a, b, "contig-alias")

    assert result["variants_compared"] == 2
    assert result["variants_only_in_vepyr"] == 0
    assert result["variants_only_in_vep"] == 0
    assert result["field_mismatch_counts"] == {}


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


def test_mismatch_ledger_closes_when_comparison_raises(monkeypatch):
    closed = []

    class FakeLedger:
        def __init__(self, path):
            assert path == "/tmp/mismatches.jsonl"

        def emit(self, _record):
            pass

        def close(self):
            closed.append(True)
            return {"path": None, "rows": 0, "sha256": ""}

    monkeypatch.setattr(compare, "_MismatchLedger", FakeLedger)
    monkeypatch.setattr(compare.vcfio, "count_data_lines", lambda _path: 0)
    monkeypatch.setattr(compare, "_get_csq_fields", lambda _path: [])
    monkeypatch.setattr(
        compare,
        "_extract_keyed_csq",
        lambda _path: (_ for _ in ()).throw(RuntimeError("injected")),
    )

    with pytest.raises(RuntimeError, match="injected"):
        compare.compare_vcfs(
            "vepyr.vcf",
            "vep.vcf",
            "failure",
            mismatch_ledger_path="/tmp/mismatches.jsonl",
        )

    assert closed == [True]


PLUGIN_HEADER = (
    "##fileformat=VCFv4.2\n"
    '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations. '
    'Format: Allele|Feature|CADD_RAW|DS_AG|CLNSIG">\n'
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
)


@pytest.mark.parametrize(
    "vepyr_value,vep_value",
    [
        ("0", "0.00"),
        ("0.57985", "0.579850"),
        ("1e-05", "0.00001"),
        ("0.1&0.25", "0.10&0.250"),
        ("", "."),
    ],
)
def test_representation_only_differences_are_not_mismatches(
    tmp_path, vepyr_value, vep_value
):
    """Decimal padding and VEP's '.' marker are the same datum, not a mismatch."""
    a = _write(
        tmp_path,
        "vepyr.vcf",
        PLUGIN_HEADER + f"chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|ENST01|{vepyr_value}||\n",
        False,
    )
    b = _write(
        tmp_path,
        "vep.vcf",
        PLUGIN_HEADER + f"chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|ENST01|{vep_value}||\n",
        False,
    )
    result = compare.compare_vcfs(a, b, "format")

    assert result["field_mismatch_counts"] == {}
    assert result["field_format_mismatch_counts"] == {"CADD_RAW": 1}
    assert result["field_match_rates"]["CADD_RAW"] == 100.0
    # Absorbed into the match rate, but the equality buckets stay strict so the
    # parity gate can still see the difference.
    assert result["field_equality_counts"]["CADD_RAW"]["both_nonempty_equal"] == 0
    assert result["mismatch_ledger"]["rows"] == 0


@pytest.mark.parametrize(
    "vepyr_value,vep_value",
    [
        ("0.5", "0.6"),
        ("0", "0.000001"),
        ("0.1&0.2", "0.1&0.2&0.3"),
        ("PATHOGENIC", "BENIGN"),
        ("0.1", "high"),
        # Everything below is absorbed by float() and must not be: two
        # integers past 2**53 that share a double, a zero-padded identifier,
        # stray whitespace, Python's '_' digit separator, and two distinct
        # large exponents that both become inf.
        ("12345678901234567", "12345678901234568"),
        ("01", "1"),
        (" 1", "1"),
        ("1_0", "10"),
        ("1e400", "1e999"),
        # VEP's "." marks absence; a "." from vepyr is an output defect.
        (".", ""),
    ],
)
def test_real_value_differences_are_still_mismatches(tmp_path, vepyr_value, vep_value):
    """Equivalence is exact Decimal over canonical numerics, nothing looser."""
    a = _write(
        tmp_path,
        "vepyr.vcf",
        PLUGIN_HEADER + f"chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|ENST01|{vepyr_value}||\n",
        False,
    )
    b = _write(
        tmp_path,
        "vep.vcf",
        PLUGIN_HEADER + f"chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|ENST01|{vep_value}||\n",
        False,
    )
    result = compare.compare_vcfs(a, b, "real")

    assert result["field_mismatch_counts"] == {"CADD_RAW": 1}
    assert result["field_format_mismatch_counts"] == {}
    assert result["mismatch_ledger"]["rows"] == 1


def test_ampersand_order_still_wins_over_format_equivalence(tmp_path):
    """An order-only difference keeps its own counter rather than being reclassified."""
    a = _write(
        tmp_path,
        "vepyr.vcf",
        PLUGIN_HEADER + "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|ENST01|0.1&0.2||\n",
        False,
    )
    b = _write(
        tmp_path,
        "vep.vcf",
        PLUGIN_HEADER + "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|ENST01|0.2&0.1||\n",
        False,
    )
    result = compare.compare_vcfs(a, b, "order")

    assert result["field_order_mismatch_counts"] == {"CADD_RAW": 1}
    assert result["field_format_mismatch_counts"] == {}


def test_absorbed_differences_never_print_the_all_match_banner(tmp_path, capsys):
    """'100%' is what an operator greps for; byte differences must not claim it."""
    a = _write(
        tmp_path,
        "vepyr.vcf",
        PLUGIN_HEADER + "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|ENST01|0||\n",
        False,
    )
    b = _write(
        tmp_path,
        "vep.vcf",
        PLUGIN_HEADER + "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|ENST01|0.00||\n",
        False,
    )
    compare.compare_vcfs(a, b, "banner")

    out = capsys.readouterr().out
    assert "match at 100%!" not in out
    assert "not byte parity" in out
    assert "CADD_RAW" in out


def test_the_all_match_banner_still_prints_on_real_parity(tmp_path, capsys):
    a = _write(tmp_path, "vepyr.vcf", MATCHING, False)
    b = _write(tmp_path, "vep.vcf", MATCHING, False)
    compare.compare_vcfs(a, b, "parity")

    assert "match at 100%!" in capsys.readouterr().out


def test_values_equivalent_absorbs_order_and_format_together():
    """A field can differ in token order AND numeric padding at once.

    The caller's order check compares raw strings, so padding defeats it, and
    the position-wise equivalence pass is defeated by the reordering. Neither
    difference is a value difference, so the pair must still be equivalent.
    """
    assert compare._values_equivalent("0.10&0.20", "0.2&0.1")
    assert compare._values_equivalent("1.0&2.00&3", "3.000&1&2")


def test_multiset_equivalence_still_rejects_real_differences():
    # A genuinely different value must not be absorbed by the multiset pairing.
    assert not compare._values_equivalent("0.10&0.20", "0.2&0.3")
    # Differing token counts stay a real difference.
    assert not compare._values_equivalent("0.1&0.2", "0.2&0.1&0.1")
    # Duplicates must pair one-to-one, not collapse.
    assert not compare._values_equivalent("0.1&0.1", "0.1&0.2")
    assert compare._values_equivalent("0.1&0.10", "0.100&0.1")
    # Non-numeric tokens still compare by identity only.
    assert not compare._values_equivalent("foo&bar", "foo&baz")
    assert compare._values_equivalent("bar&foo", "foo&bar") is True
