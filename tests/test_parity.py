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

from vepyr.parity import (
    MAX_EXAMPLES,
    ComparisonResult,
    compare_csq_fields,
    compare_vcfs,
)

# A realistic (trimmed) VEP CSQ format: core fields plus the two AlphaMissense
# plugin fields, which is exactly the shape the plugin-parity gate sees.
#
# ``Protein_position`` and ``Amino_acids`` are the core engine attributes that
# AlphaMissense's own discriminator (``{ref_aa}{Protein_position}{alt_aa}``) is
# built from, so they are the fields through which a *core* divergence
# masquerades as a *plugin* failure. They are here for that reason.
CSQ_FORMAT = (
    "Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|"
    "HGVSc|HGVSp|Protein_position|Amino_acids|SIFT|PolyPhen|"
    "am_class|am_pathogenicity"
)

#: The core fields whose divergence the plugin gate must not blame on a plugin.
#: ``ref``/``alt`` are absent because in this comparator they are not CSQ fields:
#: they are part of the variant key, so a disagreement there cannot pair.
CORE_FIELDS = ["Feature", "Consequence", "Amino_acids", "Protein_position"]

#: The plugin's own CSQ fields — the only ones its parity gate compares.
PLUGIN_FIELDS = ["am_class", "am_pathogenicity"]

_HEADER_LINES = [
    "##fileformat=VCFv4.2",
    "##contig=<ID=chr1,length=248956422>",
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
    '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence '
    'annotations from Ensembl VEP. Format: {csq_format}">',
]
_COLUMNS = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"


def write_vcf(
    path: Path,
    records: list[tuple[str, int, str, str, list[str]]],
    csq_format: str = CSQ_FORMAT,
    *,
    sites_only: bool = False,
) -> Path:
    """Write a VCF whose INFO column carries a VEP-style ``CSQ`` annotation.

    Args:
        path: Destination path.
        records: ``(chrom, pos, ref, alt, csq_entries)`` tuples, where each CSQ
            entry is a raw pipe-delimited string matching ``csq_format``.
        csq_format: The pipe-delimited field spec advertised in the CSQ header.
        sites_only: Emit an 8-column, sites-only VCF (INFO is the final column),
            as produced by running VEP over a sites-only region VCF. The default
            emits 10 columns (INFO + FORMAT + sample), mirroring the real WGS
            benchmark VCFs in ``tests/data/golden``.

    Returns:
        ``path``, for chaining.
    """
    header = [line.format(csq_format=csq_format) for line in _HEADER_LINES]
    header.append(_COLUMNS if sites_only else f"{_COLUMNS}\tFORMAT\tHG002")
    lines = ["\n".join(header)]
    for chrom, pos, ref, alt, entries in records:
        info = f"DP=30;CSQ={','.join(entries)}" if entries else "DP=30"
        row = f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t50\tPASS\t{info}"
        lines.append(row if sites_only else f"{row}\tGT:DP\t1/1:30")
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
    protein_position: str = "34",
    amino_acids: str = "K/R",
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
            protein_position,
            amino_acids,
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
                    protein_position="",
                    amino_acids="",
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
                    protein_position="",
                    amino_acids="",
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


# ── Sites-only (8-column) VCFs: the record separator is not a field value ────
#
# A region VEP run over a sites-only VCF emits 8 columns, so INFO is the final
# column and the line's "\n" sits immediately after the last CSQ field. Plugin
# fields are appended at the END of the CSQ Format string, which puts them
# exactly where a leaked line terminator would land.


def test_sites_only_last_csq_field_is_not_polluted_by_the_line_terminator(
    tmp_path: Path,
) -> None:
    """The final CSQ field of an 8-column VCF must not absorb the newline."""
    records = realistic_records()
    truth = write_vcf(tmp_path / "vep.vcf", records, sites_only=True)
    test = write_vcf(tmp_path / "vepyr.vcf", records, sites_only=True)

    result = compare_csq_fields(truth, test, fields=["am_pathogenicity"])

    assert result.is_clean
    # 30 single-entry variants + one 3-entry variant + one intron variant.
    assert result.field_totals["am_pathogenicity"] == 34


def test_sites_only_entry_order_difference_does_not_invent_a_field_mismatch(
    tmp_path: Path,
) -> None:
    """A pure CSQ entry-order difference must not manufacture a mismatch.

    The line terminator used to ride on whichever CSQ entry was emitted last;
    once entries are paired by Feature, it landed on *different* entries on the
    two sides and produced a phantom mismatch on the final field.
    """
    entries = [
        csq(allele="T", feature="ENST00000900", am_pathogenicity="0.9101"),
        csq(allele="T", feature="ENST00000901", am_pathogenicity="0.4400"),
    ]
    truth = write_vcf(
        tmp_path / "vep.vcf", [("chr1", 200_000, "C", "T", entries)], sites_only=True
    )
    test = write_vcf(
        tmp_path / "vepyr.vcf",
        [("chr1", 200_000, "C", "T", list(reversed(entries)))],
        sites_only=True,
    )

    result = compare_csq_fields(truth, test, fields=["am_class", "am_pathogenicity"])

    assert result.field_mismatches == {"am_class": 0, "am_pathogenicity": 0}
    assert result.field_mismatch_examples["am_pathogenicity"] == []
    # The entry-order difference itself is still reported, strictly.
    assert result.entry_order_mismatch == 1


def test_sites_only_over_emission_on_the_final_field_is_classified(
    tmp_path: Path,
) -> None:
    """An empty final VEP field must read as ``""``, so over-emission is seen."""
    truth = write_vcf(
        tmp_path / "vep.vcf",
        [("chr1", 300_000, "G", "A", [csq(am_class="", am_pathogenicity="")])],
        sites_only=True,
    )
    test = write_vcf(
        tmp_path / "vepyr.vcf",
        [
            (
                "chr1",
                300_000,
                "G",
                "A",
                [csq(am_class="likely_benign", am_pathogenicity="0.0500")],
            )
        ],
        sites_only=True,
    )

    result = compare_csq_fields(truth, test, fields=["am_class", "am_pathogenicity"])

    assert result.field_mismatches == {"am_class": 1, "am_pathogenicity": 1}
    assert result.over_emissions == {"am_class": 1, "am_pathogenicity": 1}

    (example,) = result.field_mismatch_examples["am_pathogenicity"]
    assert example.truth == ""
    assert example.test == "0.0500"
    assert example.is_over_emission


def test_sites_only_mismatch_examples_carry_no_stray_whitespace(
    tmp_path: Path,
) -> None:
    """Reported values are the values, not the values plus a line terminator."""
    truth = write_vcf(
        tmp_path / "vep.vcf",
        [("chr1", 100, "A", "G", [csq(am_pathogenicity="0.1234")])],
        sites_only=True,
    )
    test = write_vcf(
        tmp_path / "vepyr.vcf",
        [("chr1", 100, "A", "G", [csq(am_pathogenicity="0.5678")])],
        sites_only=True,
    )

    result = compare_csq_fields(truth, test, fields=["am_pathogenicity"])

    (example,) = result.field_mismatch_examples["am_pathogenicity"]
    assert example.truth == "0.1234"
    assert example.test == "0.5678"


def test_crlf_line_endings_do_not_pollute_the_final_csq_field(tmp_path: Path) -> None:
    """A CRLF-terminated sites-only VCF must not leak ``\\r`` into the last field."""
    records = [("chr1", 100, "A", "G", [csq(am_pathogenicity="0.1234")])]
    truth = write_vcf(tmp_path / "vep.vcf", records, sites_only=True)
    test = write_vcf(tmp_path / "vepyr.vcf", records, sites_only=True)
    test.write_bytes(test.read_bytes().replace(b"\n", b"\r\n"))

    result = compare_csq_fields(truth, test, fields=["am_pathogenicity"])

    assert result.field_mismatches["am_pathogenicity"] == 0
    assert result.is_clean


# ── A total annotation dropout must not read as a clean run ──────────────────


def test_variant_with_no_csq_at_all_in_test_is_counted_not_ignored(
    tmp_path: Path,
) -> None:
    """A variant vepyr left entirely unannotated is a defect, not a non-event.

    A plugin cache that annotates NOTHING would otherwise show a perfectly clean
    field table: with no CSQ to parse, the variant contributes no field totals
    and no entry counts.
    """
    records = realistic_records()
    truth = write_vcf(tmp_path / "vep.vcf", records)
    # vepyr emits the variants but annotates none of them.
    test = write_vcf(
        tmp_path / "vepyr.vcf",
        [(chrom, pos, ref, alt, []) for chrom, pos, ref, alt, _ in records],
    )

    result = compare_csq_fields(truth, test, fields=["am_class", "am_pathogenicity"])

    assert result.keys_compared == len(records)
    assert result.csq_missing_in_test == len(records)
    assert result.csq_missing_in_truth == 0
    # The blind spot: nothing lands in the field or entry-count buckets.
    assert result.field_mismatches == {"am_class": 0, "am_pathogenicity": 0}
    assert result.entry_count_mismatch == 0
    assert not result.is_clean


def test_variant_with_no_csq_at_all_in_truth_is_counted(tmp_path: Path) -> None:
    """The mirror case: vepyr annotated a variant VEP did not."""
    records = realistic_records()
    truth = write_vcf(
        tmp_path / "vep.vcf",
        [(chrom, pos, ref, alt, []) for chrom, pos, ref, alt, _ in records],
    )
    test = write_vcf(tmp_path / "vepyr.vcf", records)

    result = compare_csq_fields(truth, test)

    assert result.csq_missing_in_truth == len(records)
    assert result.csq_missing_in_test == 0
    assert not result.is_clean


def test_variant_unannotated_on_both_sides_is_agreement(tmp_path: Path) -> None:
    """Neither side annotating a variant is agreement, not a dropout."""
    records = [
        (chrom, pos, ref, alt, []) for chrom, pos, ref, alt, _ in realistic_records()
    ]
    truth = write_vcf(tmp_path / "vep.vcf", records)
    test = write_vcf(tmp_path / "vepyr.vcf", records)

    result = compare_csq_fields(truth, test)

    assert result.csq_missing_in_test == 0
    assert result.csq_missing_in_truth == 0
    assert result.is_clean


def test_dropouts_are_surfaced_in_the_benchmark_report(tmp_path: Path) -> None:
    """The e2e report must carry the dropout counters, not bury them."""
    records = realistic_records()
    truth = write_vcf(tmp_path / "vep.vcf", records)
    test = write_vcf(
        tmp_path / "vepyr.vcf",
        [(chrom, pos, ref, alt, []) for chrom, pos, ref, alt, _ in records],
    )

    report = compare_vcfs(str(test), str(truth), "chr1")

    assert report["csq_missing_in_vepyr"] == len(records)
    assert report["csq_missing_in_vep"] == 0


# ── The uncapped mismatch-key set: the exclusion set the plugin gate rests on ─
#
# The plugin gate's blame rule needs the set of variants where the CORE already
# disagrees, so those variants can be excluded before a plugin's own fields are
# judged. An *approximate* exclusion set is worse than none: it silently
# attributes core bugs to plugins on precisely the long tail it cannot see. The
# capped `examples` therefore cannot serve this purpose — hence `mismatch_keys`.


def key_of(chrom: str, pos: int, ref: str, alt: str) -> str:
    """The comparator's rendering of a variant key: ``CHROM\\tPOS\\tREF\\tALT``."""
    return f"{chrom}\t{pos}\t{ref}\t{alt}"


def test_mismatch_keys_holds_every_key_while_examples_stay_capped(
    tmp_path: Path,
) -> None:
    """Every mismatching key is retained; the examples remain a capped report.

    The whole point of the attribute: spread the mismatches across MORE keys
    than the example cap and the examples can no longer see them all. The
    exclusion set must.
    """
    n_mismatching = MAX_EXAMPLES * 3
    assert n_mismatching > MAX_EXAMPLES, "the cap must actually bite"

    positions = [100_000 + i * 137 for i in range(n_mismatching)]
    truth_records = [
        (
            "chr1",
            pos,
            "A",
            "G",
            [csq(feature=f"ENST{i:011d}", am_pathogenicity="0.1000")],
        )
        for i, pos in enumerate(positions)
    ]
    # vepyr disagrees on am_pathogenicity at EVERY one of them.
    test_records = [
        (
            "chr1",
            pos,
            "A",
            "G",
            [csq(feature=f"ENST{i:011d}", am_pathogenicity="0.9000")],
        )
        for i, pos in enumerate(positions)
    ]
    truth = write_vcf(tmp_path / "vep.vcf", truth_records)
    test = write_vcf(tmp_path / "vepyr.vcf", test_records)

    result = compare_csq_fields(truth, test, fields=PLUGIN_FIELDS)

    expected = {key_of("chr1", pos, "A", "G") for pos in positions}
    assert result.field_mismatches["am_pathogenicity"] == n_mismatching
    assert result.mismatch_keys["am_pathogenicity"] == expected
    assert len(result.mismatch_keys["am_pathogenicity"]) == n_mismatching

    # The examples are still a report, not a dump — unchanged, still capped.
    examples = result.field_mismatch_examples["am_pathogenicity"]
    assert len(examples) == MAX_EXAMPLES
    assert {ex.key for ex in examples} < result.mismatch_keys["am_pathogenicity"]

    # A compared field that never mismatched gets an empty set, not a KeyError.
    assert result.mismatch_keys["am_class"] == set()
    assert set(result.mismatch_keys) == set(result.fields_compared)


def test_mismatch_keys_deduplicates_a_variant_mismatching_in_several_entries(
    tmp_path: Path,
) -> None:
    """It is a set of *variants*, not of entry pairs: one bad variant, one key.

    The counter stays per-entry-pair (three mismatching transcripts on one
    variant are three mismatches); the key set collapses them to the one variant
    that must be excluded.
    """
    truth = write_vcf(
        tmp_path / "vep.vcf",
        [
            (
                "chr1",
                200_000,
                "C",
                "T",
                [
                    csq(allele="T", feature="ENST00000900", am_pathogenicity="0.11"),
                    csq(allele="T", feature="ENST00000901", am_pathogenicity="0.22"),
                    csq(allele="T", feature="ENST00000902", am_pathogenicity="0.33"),
                ],
            )
        ],
    )
    test = write_vcf(
        tmp_path / "vepyr.vcf",
        [
            (
                "chr1",
                200_000,
                "C",
                "T",
                [
                    csq(allele="T", feature="ENST00000900", am_pathogenicity="0.99"),
                    csq(allele="T", feature="ENST00000901", am_pathogenicity="0.88"),
                    csq(allele="T", feature="ENST00000902", am_pathogenicity="0.77"),
                ],
            )
        ],
    )

    result = compare_csq_fields(truth, test, fields=PLUGIN_FIELDS)

    assert result.field_mismatches["am_pathogenicity"] == 3
    assert result.mismatch_keys["am_pathogenicity"] == {
        key_of("chr1", 200_000, "C", "T")
    }


def test_mismatch_keys_excludes_order_only_differences(tmp_path: Path) -> None:
    """``a&b`` vs ``b&a`` counts as a MATCH, so its key is not an exclusion.

    The set must track exactly what :attr:`field_mismatches` counts — a key that
    only differs in multi-value order is not a disagreement, and excluding it
    would shrink the plugin gate for no reason.
    """
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

    assert result.field_order_mismatches["Consequence"] == 1
    assert result.field_mismatches["Consequence"] == 0
    assert result.mismatch_keys["Consequence"] == set()


def test_mismatch_keys_are_empty_on_a_clean_run(tmp_path: Path) -> None:
    """A clean comparison excludes nothing."""
    records = realistic_records()
    truth = write_vcf(tmp_path / "vep.vcf", records)
    test = write_vcf(tmp_path / "vepyr.vcf", records)

    result = compare_csq_fields(truth, test)

    assert result.is_clean
    assert result.mismatch_keys == dict.fromkeys(result.fields_compared, set())


def test_core_mismatch_keys_let_the_gate_attribute_blame_by_set_difference(
    tmp_path: Path,
) -> None:
    """The blame-attribution rule the plugin gate rests on, end to end.

    A plugin's CSQ fields are *derived* from core engine attributes:
    AlphaMissense keys its lookup on ``{ref_aa}{Protein_position}{alt_aa}``,
    built from ``Amino_acids`` and ``Protein_position``. So when the core
    diverges from VEP, the plugin is handed a wrong lookup key and its field
    comes out wrong through no fault of its own — and a naive diff blames the
    plugin.

    Two verdicts over the same pair of files, one set difference:

    * key A — the core agrees, the plugin's value is wrong: a GENUINE plugin bug;
    * key B — the core disagrees (wrong amino acids), so the plugin's value is
      wrong downstream of that: NOT the plugin's fault;
    * key C — everything agrees.

    The gate must find exactly ``{A}``.
    """
    key_a = ("chr1", 100, "A", "G")
    key_b = ("chr1", 200, "C", "T")
    key_c = ("chr1", 300, "G", "A")

    truth_records = [
        (
            *key_a,
            [
                csq(
                    feature="ENST00000A",
                    protein_position="34",
                    amino_acids="K/R",
                    am_class="likely_benign",
                    am_pathogenicity="0.1234",
                )
            ],
        ),
        (
            *key_b,
            [
                csq(
                    feature="ENST00000B",
                    protein_position="88",
                    amino_acids="E/K",
                    am_class="likely_pathogenic",
                    am_pathogenicity="0.9500",
                )
            ],
        ),
        (
            *key_c,
            [
                csq(
                    feature="ENST00000C",
                    protein_position="12",
                    amino_acids="A/T",
                    am_class="likely_benign",
                    am_pathogenicity="0.0500",
                )
            ],
        ),
    ]
    test_records = [
        # A: core identical, plugin score wrong — the plugin's own bug.
        (
            *key_a,
            [
                csq(
                    feature="ENST00000A",
                    protein_position="34",
                    amino_acids="K/R",
                    am_class="likely_benign",
                    am_pathogenicity="0.5678",
                )
            ],
        ),
        # B: the CORE diverges — wrong amino acids and protein position, so the
        # discriminator AlphaMissense looks up is wrong, so its score is wrong.
        # The plugin is downstream of a core bug, not the cause of it.
        (
            *key_b,
            [
                csq(
                    feature="ENST00000B",
                    protein_position="89",
                    amino_acids="E/Q",
                    am_class="likely_benign",
                    am_pathogenicity="0.2222",
                )
            ],
        ),
        # C: agreement.
        (
            *key_c,
            [
                csq(
                    feature="ENST00000C",
                    protein_position="12",
                    amino_acids="A/T",
                    am_class="likely_benign",
                    am_pathogenicity="0.0500",
                )
            ],
        ),
    ]
    truth = write_vcf(tmp_path / "vep.vcf", truth_records)
    test = write_vcf(tmp_path / "vepyr.vcf", test_records)

    # Verdict 1: the core fields — which variants does vepyr ALREADY get wrong?
    core = compare_csq_fields(truth, test, fields=CORE_FIELDS)
    # Verdict 2: the plugin's own fields, over the same pair of files.
    plugin = compare_csq_fields(truth, test, fields=PLUGIN_FIELDS)

    core_diverged = set().union(*core.mismatch_keys.values())
    plugin_failed = set().union(*plugin.mismatch_keys.values())

    assert core_diverged == {key_of(*key_b)}
    assert plugin_failed == {key_of(*key_a), key_of(*key_b)}

    # The rule. Exactly one genuine plugin failure — key A.
    genuine_plugin_failures = plugin_failed - core_diverged
    assert genuine_plugin_failures == {key_of(*key_a)}

    # And what the naive diff would have said: TWO plugin failures, one of them
    # a core bug wearing a plugin's name.
    assert plugin.field_mismatches["am_pathogenicity"] == 2


def test_mismatch_keys_is_not_added_to_the_report(tmp_path: Path) -> None:
    """A new attribute for programmatic consumers — NOT a report change.

    Every archived benchmark report reads :meth:`as_report_dict`'s shape, so the
    payload's key set is frozen. The printed report is likewise untouched.
    """
    records = realistic_records()
    truth = write_vcf(tmp_path / "vep.vcf", records)
    # Mismatch am_class on every annotated variant — far more keys than the cap.
    test = write_vcf(
        tmp_path / "vepyr.vcf",
        [
            (
                chrom,
                pos,
                ref,
                alt,
                [e.replace("|likely_benign|", "|likely_pathogenic|") for e in entries],
            )
            for chrom, pos, ref, alt, entries in records
        ],
    )

    report = compare_vcfs(str(test), str(truth), "chr1")

    assert set(report) == {
        "variants_compared",
        "variants_only_in_vepyr",
        "csq_order_mismatch",
        "csq_order_mismatch_examples",
        "csq_order_ignored",
        "csq_order_ignored_examples",
        "csq_order_ignore_reason",
        "variants_only_in_vep",
        "csq_missing_in_vepyr",
        "csq_missing_in_vep",
        "csq_entry_count_match",
        "csq_entry_count_mismatch",
        "field_match_rates",
        "field_mismatch_counts",
        "field_mismatch_examples",
        "field_order_mismatch_counts",
        "field_order_mismatch_examples",
    }
    # The report still carries only the CAPPED examples, and no key sets.
    assert len(report["field_mismatch_examples"]["am_class"]) == MAX_EXAMPLES  # type: ignore[index]
