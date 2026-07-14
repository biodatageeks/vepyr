"""Tests for :mod:`vepyr.parity` — the shared VEP-vs-vepyr CSQ comparator.

The comparator is the arbiter of two gates:

* the WGS benchmark (``e2e-testing/scripts/run_annotation_fast.py``), and
* the plugin-parity gate (``vepyr-plugins``), which restricts the comparison to
  a single plugin's CSQ fields.

These tests are written against real VCF pairs (a real ``##INFO=<ID=CSQ`` header
with a ``Format:`` spec, real pipe-delimited CSQ entries) written into
``tmp_path`` — not stubs — because the file/CSQ parsing is part of what is
being asserted.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vepyr.parity import ComparisonResult, compare_csq_fields, compare_vcfs

# A realistic (trimmed) VEP CSQ format: core fields plus the two AlphaMissense
# plugin fields, which is exactly the shape the plugin-parity gate sees.
CSQ_FORMAT = (
    "Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|"
    "HGVSc|HGVSp|SIFT|PolyPhen|am_class|am_pathogenicity"
)

_HEADER_TEMPLATE = "\n".join(
    [
        "##fileformat=VCFv4.2",
        "##contig=<ID=chr1,length=248956422>",
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
        '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence '
        'annotations from Ensembl VEP. Format: {csq_format}">',
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tHG002",
    ]
)


def write_vcf(
    path: Path,
    records: list[tuple[str, int, str, str, list[str]]],
    csq_format: str = CSQ_FORMAT,
) -> Path:
    """Write a VCF whose INFO column carries a VEP-style ``CSQ`` annotation.

    Mirrors the shape of the real benchmark VCFs (``tests/data/golden``): 10
    columns, i.e. INFO followed by FORMAT and a sample column.

    Args:
        path: Destination path.
        records: ``(chrom, pos, ref, alt, csq_entries)`` tuples, where each CSQ
            entry is a raw pipe-delimited string matching ``csq_format``.
        csq_format: The pipe-delimited field spec advertised in the CSQ header.

    Returns:
        ``path``, for chaining.
    """
    lines = [_HEADER_TEMPLATE.format(csq_format=csq_format)]
    for chrom, pos, ref, alt, entries in records:
        info = f"DP=30;CSQ={','.join(entries)}" if entries else "DP=30"
        lines.append(
            f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t50\tPASS\t{info}\tGT:DP\t1/1:30"
        )
    path.write_text("\n".join(lines) + "\n")
    return path


def csq(
    *,
    allele: str = "G",
    consequence: str = "missense_variant",
    impact: str = "MODERATE",
    symbol: str = "GENE1",
    gene: str = "ENSG00000001",
    feature: str = "ENST00000001",
    biotype: str = "protein_coding",
    hgvsc: str = "ENST00000001.1:c.100A>G",
    hgvsp: str = "ENSP00000001.1:p.Lys34Arg",
    sift: str = "tolerated(0.21)",
    polyphen: str = "benign(0.012)",
    am_class: str = "likely_benign",
    am_pathogenicity: str = "0.0812",
) -> str:
    """Build one pipe-delimited CSQ entry matching :data:`CSQ_FORMAT`."""
    return "|".join(
        [
            allele,
            consequence,
            impact,
            symbol,
            gene,
            "Transcript",
            feature,
            biotype,
            hgvsc,
            hgvsp,
            sift,
            polyphen,
            am_class,
            am_pathogenicity,
        ]
    )


def realistic_records() -> list[tuple[str, int, str, str, list[str]]]:
    """A few dozen records, incl. a multi-CSQ-entry variant and empty fields."""
    records: list[tuple[str, int, str, str, list[str]]] = []
    for i in range(30):
        pos = 100_000 + i * 137
        records.append(
            (
                "chr1",
                pos,
                "A",
                "G",
                [csq(feature=f"ENST{i:011d}", am_pathogenicity=f"0.{i:04d}")],
            )
        )
    # A variant annotated against three overlapping transcripts.
    records.append(
        (
            "chr1",
            200_000,
            "C",
            "T",
            [
                csq(allele="T", feature="ENST00000900", am_pathogenicity="0.9101"),
                csq(allele="T", feature="ENST00000901", am_pathogenicity="0.4400"),
                csq(allele="T", feature="ENST00000902", am_pathogenicity="0.1234"),
            ],
        )
    )
    # An intron variant: AlphaMissense emits nothing for non-missense sites.
    records.append(
        (
            "chr1",
            300_000,
            "G",
            "A",
            [
                csq(
                    allele="A",
                    consequence="intron_variant",
                    impact="MODIFIER",
                    feature="ENST00000903",
                    hgvsp="",
                    sift="",
                    polyphen="",
                    am_class="",
                    am_pathogenicity="",
                )
            ],
        )
    )
    return records


def test_identical_vcfs_agree(tmp_path: Path) -> None:
    """A file compared against a byte-identical copy has zero mismatches."""
    records = realistic_records()
    truth = write_vcf(tmp_path / "vep.vcf", records)
    test = write_vcf(tmp_path / "vepyr.vcf", records)

    result = compare_csq_fields(truth, test)

    assert isinstance(result, ComparisonResult)
    assert result.keys_compared == len(records)
    assert result.keys_only_in_test == 0
    assert result.keys_only_in_truth == 0
    assert result.entry_count_mismatch == 0
    assert result.field_mismatches == dict.fromkeys(result.fields_compared, 0)
    assert result.over_emissions == dict.fromkeys(result.fields_compared, 0)
    assert "am_pathogenicity" in result.fields_compared
    assert result.is_clean


def test_field_mismatch_reports_key_and_both_values(tmp_path: Path) -> None:
    """A differing field is reported with the variant key AND both values."""
    records = realistic_records()
    drifted = [
        (chrom, pos, ref, alt, entries)
        if pos != 200_000
        else (
            chrom,
            pos,
            ref,
            alt,
            [
                csq(allele="T", feature="ENST00000900", am_pathogenicity="0.9101"),
                csq(allele="T", feature="ENST00000901", am_pathogenicity="0.4400"),
                # vepyr disagrees with VEP on this transcript's score.
                csq(allele="T", feature="ENST00000902", am_pathogenicity="0.5678"),
            ],
        )
        for chrom, pos, ref, alt, entries in records
    ]
    truth = write_vcf(tmp_path / "vep.vcf", records)
    test = write_vcf(tmp_path / "vepyr.vcf", drifted)

    result = compare_csq_fields(truth, test, fields=["am_class", "am_pathogenicity"])

    assert result.field_mismatches["am_pathogenicity"] == 1
    assert result.field_mismatches["am_class"] == 0
    assert not result.is_clean

    (example,) = result.field_mismatch_examples["am_pathogenicity"]
    assert example.key == "chr1\t200000\tC\tT"
    assert example.truth == "0.1234"
    assert example.test == "0.5678"


def test_over_emission_is_a_mismatch(tmp_path: Path) -> None:
    """Populating a field VEP left empty is a FAILURE, not a bonus.

    VEP emits no AlphaMissense call for an intron variant. If vepyr emits one
    anyway, that is over-emission: the plugin fired where upstream did not.
    """
    records = realistic_records()
    over_emitting = [
        (chrom, pos, ref, alt, entries)
        if pos != 300_000
        else (
            chrom,
            pos,
            ref,
            alt,
            [
                csq(
                    allele="A",
                    consequence="intron_variant",
                    impact="MODIFIER",
                    feature="ENST00000903",
                    hgvsp="",
                    sift="",
                    polyphen="",
                    am_class="likely_benign",
                    am_pathogenicity="0.0500",
                )
            ],
        )
        for chrom, pos, ref, alt, entries in records
    ]
    truth = write_vcf(tmp_path / "vep.vcf", records)
    test = write_vcf(tmp_path / "vepyr.vcf", over_emitting)

    result = compare_csq_fields(truth, test, fields=["am_class", "am_pathogenicity"])

    assert not result.is_clean
    assert result.field_mismatches == {"am_class": 1, "am_pathogenicity": 1}
    assert result.over_emissions == {"am_class": 1, "am_pathogenicity": 1}

    (example,) = result.field_mismatch_examples["am_pathogenicity"]
    assert example.key == "chr1\t300000\tG\tA"
    assert example.truth == ""
    assert example.test == "0.0500"


def test_field_restriction_ignores_drift_in_other_fields(tmp_path: Path) -> None:
    """Restricting to a plugin's fields ignores drift in the core fields."""
    records = realistic_records()
    core_drift = [
        (
            chrom,
            pos,
            ref,
            alt,
            [
                entry.replace("tolerated(0.21)", "deleterious(0.01)")
                for entry in entries
            ],
        )
        for chrom, pos, ref, alt, entries in records
    ]
    truth = write_vcf(tmp_path / "vep.vcf", records)
    test = write_vcf(tmp_path / "vepyr.vcf", core_drift)

    plugin_only = compare_csq_fields(
        truth, test, fields=["am_class", "am_pathogenicity"]
    )
    assert plugin_only.fields_compared == ("am_class", "am_pathogenicity")
    assert plugin_only.field_mismatches == {"am_class": 0, "am_pathogenicity": 0}
    assert plugin_only.is_clean

    # The same drift is a hard failure for the unrestricted (core) verdict.
    everything = compare_csq_fields(truth, test)
    assert everything.field_mismatches["SIFT"] > 0
    assert not everything.is_clean


def test_requested_field_missing_from_test_header_is_reported(tmp_path: Path) -> None:
    """A plugin field vepyr never emits must not silently pass the gate."""
    records = realistic_records()
    truth = write_vcf(tmp_path / "vep.vcf", records)
    # vepyr's CSQ header stops before the plugin fields: nothing to compare.
    core_format = CSQ_FORMAT.rsplit("|am_class|", 1)[0]
    test = write_vcf(
        tmp_path / "vepyr.vcf",
        [
            (chrom, pos, ref, alt, ["|".join(e.split("|")[:-2]) for e in entries])
            for chrom, pos, ref, alt, entries in records
        ],
        csq_format=core_format,
    )

    result = compare_csq_fields(truth, test, fields=["am_class", "am_pathogenicity"])

    assert result.fields_compared == ()
    assert result.fields_missing_from_test == ("am_class", "am_pathogenicity")
    assert result.fields_missing_from_truth == ()
    assert not result.is_clean


def test_entry_count_mismatch_is_reported(tmp_path: Path) -> None:
    """Emitting fewer CSQ entries than VEP is an entry-count mismatch."""
    records = realistic_records()
    dropped = [
        (chrom, pos, ref, alt, entries if pos != 200_000 else entries[:2])
        for chrom, pos, ref, alt, entries in records
    ]
    truth = write_vcf(tmp_path / "vep.vcf", records)
    test = write_vcf(tmp_path / "vepyr.vcf", dropped)

    result = compare_csq_fields(truth, test, fields=["am_pathogenicity"])

    assert result.entry_count_mismatch == 1
    assert result.entry_count_match == len(records) - 1
    assert not result.is_clean


def test_entry_order_mismatch_is_strict_by_default_and_can_be_ignored(
    tmp_path: Path,
) -> None:
    """Same entries in a different comma order: strict by default, ignorable.

    ``ignore_entry_order`` exists only for VEP's per_gene / pick_allele_gene
    Perl-hash emission order; the reason is recorded on the result.
    """
    entries = [
        csq(allele="T", feature="ENST00000900"),
        csq(allele="T", feature="ENST00000901"),
    ]
    truth = write_vcf(tmp_path / "vep.vcf", [("chr1", 200_000, "C", "T", entries)])
    test = write_vcf(
        tmp_path / "vepyr.vcf", [("chr1", 200_000, "C", "T", list(reversed(entries)))]
    )

    strict = compare_csq_fields(truth, test)
    assert strict.entry_order_mismatch == 1
    assert strict.entry_order_ignored == 0
    assert strict.entry_order_ignore_reason is None
    assert not strict.is_clean
    # Entries are paired by Feature before field comparison, so field values
    # still match: only the emission order differs.
    assert strict.field_mismatches["Feature"] == 0

    lenient = compare_csq_fields(truth, test, ignore_entry_order=True)
    assert lenient.entry_order_mismatch == 0
    assert lenient.entry_order_ignored == 1
    assert "per_gene" in (lenient.entry_order_ignore_reason or "")
    assert "pick_allele_gene" in (lenient.entry_order_ignore_reason or "")
    assert lenient.is_clean


def test_ampersand_order_only_difference_is_not_a_mismatch(tmp_path: Path) -> None:
    """``a&b`` vs ``b&a`` in a multi-valued field counts as a match (order-only)."""
    truth = write_vcf(
        tmp_path / "vep.vcf",
        [
            (
                "chr1",
                100,
                "A",
                "G",
                [csq(consequence="splice_region_variant&intron_variant")],
            )
        ],
    )
    test = write_vcf(
        tmp_path / "vepyr.vcf",
        [
            (
                "chr1",
                100,
                "A",
                "G",
                [csq(consequence="intron_variant&splice_region_variant")],
            )
        ],
    )

    result = compare_csq_fields(truth, test, fields=["Consequence"])

    assert result.field_mismatches["Consequence"] == 0
    assert result.field_order_mismatches["Consequence"] == 1
    (example,) = result.field_order_mismatch_examples["Consequence"]
    assert example.truth == "splice_region_variant&intron_variant"
    assert example.test == "intron_variant&splice_region_variant"


def test_keys_only_in_one_side_are_counted(tmp_path: Path) -> None:
    """Variants present on only one side are counted, not compared."""
    truth = write_vcf(
        tmp_path / "vep.vcf",
        [("chr1", 100, "A", "G", [csq()]), ("chr1", 200, "A", "G", [csq()])],
    )
    test = write_vcf(
        tmp_path / "vepyr.vcf",
        [("chr1", 100, "A", "G", [csq()]), ("chr1", 300, "A", "G", [csq()])],
    )

    result = compare_csq_fields(truth, test)

    assert result.keys_compared == 1
    assert result.keys_only_in_truth == 1
    assert result.keys_only_in_test == 1


def test_compare_vcfs_returns_the_legacy_report_dict(tmp_path: Path, capsys) -> None:
    """The e2e benchmark's ``compare_vcfs`` still returns its JSON report shape."""
    records = realistic_records()
    truth = write_vcf(tmp_path / "vep.vcf", records)
    test = write_vcf(tmp_path / "vepyr.vcf", records)

    report = compare_vcfs(str(test), str(truth), "chr1")

    assert report["variants_compared"] == len(records)
    assert report["csq_entry_count_mismatch"] == 0
    assert report["field_mismatch_counts"] == {}
    assert report["csq_order_ignore_reason"] is None
    assert report["field_match_rates"]["am_pathogenicity"] == 100.0
    assert "ALL" in capsys.readouterr().out


@pytest.mark.parametrize("suffix", [".vcf", ".vcf.gz"])
def test_gzipped_inputs_are_read_transparently(tmp_path: Path, suffix: str) -> None:
    """Comparison works on plain and gzipped VCFs alike."""
    import gzip

    records = realistic_records()
    truth = write_vcf(tmp_path / "vep.vcf", records)
    test = write_vcf(tmp_path / "vepyr.vcf", records)
    if suffix == ".vcf.gz":
        for path in (truth, test):
            gz = path.with_suffix(".vcf.gz")
            gz.write_bytes(gzip.compress(path.read_bytes()))
        truth = truth.with_suffix(".vcf.gz")
        test = test.with_suffix(".vcf.gz")

    result = compare_csq_fields(truth, test)

    assert result.keys_compared == len(records)
    assert result.is_clean
