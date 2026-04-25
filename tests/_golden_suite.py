"""Reusable golden-suite helpers for default and merged VEP cache tests."""

from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

DEFAULT_CSQ_FIELDS = [
    "Allele",
    "Consequence",
    "IMPACT",
    "SYMBOL",
    "Gene",
    "Feature_type",
    "Feature",
    "BIOTYPE",
    "EXON",
    "INTRON",
    "HGVSc",
    "HGVSp",
    "cDNA_position",
    "CDS_position",
    "Protein_position",
    "Amino_acids",
    "Codons",
    "Existing_variation",
    "DISTANCE",
    "STRAND",
    "FLAGS",
    "VARIANT_CLASS",
    "SYMBOL_SOURCE",
    "HGNC_ID",
    "CANONICAL",
    "MANE",
    "MANE_SELECT",
    "MANE_PLUS_CLINICAL",
    "TSL",
    "APPRIS",
    "CCDS",
    "ENSP",
    "SWISSPROT",
    "TREMBL",
    "UNIPARC",
    "UNIPROT_ISOFORM",
    "GENE_PHENO",
    "SIFT",
    "PolyPhen",
    "DOMAINS",
    "miRNA",
    "HGVS_OFFSET",
    "AF",
    "AFR_AF",
    "AMR_AF",
    "EAS_AF",
    "EUR_AF",
    "SAS_AF",
    "gnomADe_AF",
    "gnomADe_AFR_AF",
    "gnomADe_AMR_AF",
    "gnomADe_ASJ_AF",
    "gnomADe_EAS_AF",
    "gnomADe_FIN_AF",
    "gnomADe_MID_AF",
    "gnomADe_NFE_AF",
    "gnomADe_REMAINING_AF",
    "gnomADe_SAS_AF",
    "gnomADg_AF",
    "gnomADg_AFR_AF",
    "gnomADg_AMI_AF",
    "gnomADg_AMR_AF",
    "gnomADg_ASJ_AF",
    "gnomADg_EAS_AF",
    "gnomADg_FIN_AF",
    "gnomADg_MID_AF",
    "gnomADg_NFE_AF",
    "gnomADg_REMAINING_AF",
    "gnomADg_SAS_AF",
    "MAX_AF",
    "MAX_AF_POPS",
    "CLIN_SIG",
    "SOMATIC",
    "PHENO",
    "PUBMED",
    "MOTIF_NAME",
    "MOTIF_POS",
    "HIGH_INF_POS",
    "MOTIF_SCORE_CHANGE",
    "TRANSCRIPTION_FACTORS",
]

MERGED_CSQ_FIELDS = [
    *DEFAULT_CSQ_FIELDS[:36],
    "REFSEQ_MATCH",
    "SOURCE",
    "REFSEQ_OFFSET",
    "GIVEN_REF",
    "USED_REF",
    "BAM_EDIT",
    *DEFAULT_CSQ_FIELDS[36:],
]

PICK_CSQ_FIELDS = [
    *DEFAULT_CSQ_FIELDS[:21],
    "PICK",
    *DEFAULT_CSQ_FIELDS[21:],
]

MERGED_PICK_CSQ_FIELDS = [
    *PICK_CSQ_FIELDS[:37],
    "REFSEQ_MATCH",
    "SOURCE",
    "REFSEQ_OFFSET",
    "GIVEN_REF",
    "USED_REF",
    "BAM_EDIT",
    *PICK_CSQ_FIELDS[37:],
]

DEFAULT_DF_COMPARISON_FIELDS = [
    "SYMBOL",
    "IMPACT",
    "Gene",
    "Feature",
    "Feature_type",
    "BIOTYPE",
    "HGVSc",
    "HGVSp",
    "Existing_variation",
    "VARIANT_CLASS",
    "CANONICAL",
    "SIFT",
    "PolyPhen",
    "ENSP",
    "Consequence",
    "STRAND",
    "DISTANCE",
    "EXON",
    "INTRON",
    "AF",
    "AFR_AF",
    "gnomADe_AF",
    "gnomADg_AF",
    "MAX_AF",
    "CLIN_SIG",
    "SYMBOL_SOURCE",
    "DOMAINS",
]


@dataclass(frozen=True)
class GoldenConfig:
    name: str
    cache_dir: Path
    input_vcf: Path
    golden_vcf: Path
    reference_fasta: Path
    annotate_kwargs: dict[str, Any]
    csq_fields: list[str]
    df_comparison_fields: list[str]

    @property
    def vcf_comparison_fields(self) -> list[str]:
        return [field for field in self.csq_fields if field != "Allele"]

    @property
    def min_df_width(self) -> int:
        # VCF core fields + most_severe_consequence + ClinVar/COSMIC/dbSNP extras.
        return len(self.csq_fields) + 16


def _parse_vcf_csq(path: Path) -> dict[tuple[str, int, str, str], list[str]]:
    """Parse VCF into {(chrom, pos, ref, alt): [csq1, csq2, ...]}."""
    results = {}
    with open(path) as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            fields = line.rstrip("\n").split("\t")
            chrom, pos, ref, alt = fields[0], int(fields[1]), fields[3], fields[4]
            info = fields[7]
            csq_entries = []
            for part in info.split(";"):
                if part.startswith("CSQ="):
                    csq_entries = part[4:].split(",")
                    break
            results[(chrom, pos, ref, alt)] = csq_entries
    return results


def _parse_vcf_csq_field_order(path: Path) -> list[str] | None:
    """Extract CSQ field order from the VCF header, if present."""
    with open(path) as handle:
        for line in handle:
            if not line.startswith("#"):
                break
            match = re.search(r'Format: ([^"]+)', line)
            if match:
                return match.group(1).split("|")
    return None


def _normalize_chrom(chrom: str) -> str:
    return chrom.replace("chr", "")


def _lookup(key, mapping):
    """Look up a variant, trying both chr-prefixed and bare chromosome keys."""
    norm_key = (_normalize_chrom(key[0]), key[1], key[2], key[3])
    return mapping.get(key) or mapping.get(norm_key)


def _field_test_name(field: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", field.lower()).strip("_")


def install_golden_suite(namespace: dict[str, Any], config: GoldenConfig) -> None:
    """Install fixtures and tests for one golden dataset into a test module."""

    csq_index = {name: i for i, name in enumerate(config.csq_fields)}

    def csq_field(csq: str, field: str) -> str:
        idx = csq_index.get(field)
        if idx is None:
            return ""
        parts = csq.split("|")
        return parts[idx] if idx < len(parts) else ""

    @pytest.fixture(scope="module")
    def golden_annotations():
        return _parse_vcf_csq(config.golden_vcf)

    @pytest.fixture(scope="module")
    def golden_field_order():
        return _parse_vcf_csq_field_order(config.golden_vcf)

    @pytest.fixture(scope="module")
    def skip_if_no_cache():
        if not os.path.isdir(config.cache_dir):
            pytest.skip(f"{config.name} cache fixture not available")

    @pytest.fixture(scope="module")
    def vepyr_df(skip_if_no_cache):
        import vepyr

        return vepyr.annotate(
            str(config.input_vcf),
            str(config.cache_dir),
            everything=True,
            reference_fasta=str(config.reference_fasta),
            **config.annotate_kwargs,
        ).collect()

    @pytest.fixture(scope="module")
    def vepyr_vcf_path(skip_if_no_cache):
        import vepyr

        with tempfile.NamedTemporaryFile(suffix=".vcf", delete=False) as handle:
            vcf_path = handle.name
        vepyr.annotate(
            str(config.input_vcf),
            str(config.cache_dir),
            everything=True,
            reference_fasta=str(config.reference_fasta),
            output_vcf=vcf_path,
            show_progress=False,
            **config.annotate_kwargs,
        )
        yield Path(vcf_path)
        os.unlink(vcf_path)

    @pytest.fixture(scope="module")
    def vepyr_vcf_annotations(vepyr_vcf_path):
        return _parse_vcf_csq(vepyr_vcf_path)

    class TestGoldenVcfComparison:
        """Compare vepyr VCF output against the golden Ensembl VEP VCF."""

        def test_variant_count(self, vepyr_vcf_annotations, golden_annotations):
            assert len(vepyr_vcf_annotations) >= len(golden_annotations), (
                f"Too few: vepyr={len(vepyr_vcf_annotations)}, "
                f"golden={len(golden_annotations)}"
            )

        def test_csq_entry_count(self, vepyr_vcf_annotations, golden_annotations):
            fewer = []
            for key, golden_csqs in golden_annotations.items():
                vepyr_csqs = _lookup(key, vepyr_vcf_annotations)
                if vepyr_csqs is None:
                    fewer.append((key, len(golden_csqs), 0))
                elif len(vepyr_csqs) < len(golden_csqs):
                    fewer.append((key, len(golden_csqs), len(vepyr_csqs)))
            assert not fewer, f"Variants with fewer CSQ entries than golden: {fewer}"

        def test_csq_field_order(self, golden_field_order, vepyr_vcf_path):
            vepyr_order = _parse_vcf_csq_field_order(vepyr_vcf_path)
            if vepyr_order is None:
                pytest.skip("vepyr VCF header does not include CSQ Format line")
            assert vepyr_order == golden_field_order, (
                f"Field order mismatch:\n"
                f"  golden: {golden_field_order}\n"
                f"  vepyr:  {vepyr_order}"
            )

        def _compare_vcf_field(self, field, vepyr_vcf_annotations, golden_annotations):
            mismatches = []
            for key, golden_csqs in golden_annotations.items():
                vepyr_csqs = _lookup(key, vepyr_vcf_annotations)
                if vepyr_csqs is None:
                    continue
                golden_vals = {csq_field(csq, field) for csq in golden_csqs} - {""}
                if not golden_vals:
                    continue
                vepyr_vals = {csq_field(csq, field) for csq in vepyr_csqs} - {""}
                missing = golden_vals - vepyr_vals
                if missing:
                    mismatches.append((key, missing, vepyr_vals))
            return mismatches

    class TestGoldenDataFrameComparison:
        """Compare vepyr DataFrame output against the golden Ensembl VEP VCF."""

        @staticmethod
        def _normalize_df_value(value):
            if value is None or value == "" or value == []:
                return set()
            if isinstance(value, list):
                return {str(v) for v in value if v is not None and v != ""}
            if isinstance(value, float):
                candidates = {str(value)}
                if value == int(value):
                    candidates.add(str(int(value)))
                for precision in range(1, 7):
                    formatted = f"{value:.{precision}f}"
                    candidates.add(formatted)
                    candidates.add(formatted.rstrip("0").rstrip("."))
                return candidates
            return {str(value)}

        def _compare_df_field(self, field, vepyr_df, golden_annotations):
            assert field in vepyr_df.columns, f"Missing DataFrame column: {field}"

            mismatches = []
            for row in vepyr_df.iter_rows(named=True):
                key = (row["chrom"], row["start"], row["ref"], row["alt"])
                golden_csqs = _lookup(key, golden_annotations)
                if golden_csqs is None:
                    continue
                vepyr_strs = self._normalize_df_value(row.get(field))
                if not vepyr_strs:
                    continue
                golden_vals = set()
                for csq in golden_csqs:
                    value = csq_field(csq, field)
                    if value:
                        golden_vals.add(value)
                        if "&" in value:
                            golden_vals.update(value.split("&"))
                if not golden_vals:
                    continue
                if not vepyr_strs & golden_vals:
                    mismatches.append((key, vepyr_strs, golden_vals))
            return mismatches

        def test_variant_count(self, vepyr_df, golden_annotations):
            ratio = vepyr_df.height / len(golden_annotations)
            assert ratio >= 1.0, (
                f"Too few: vepyr={vepyr_df.height}, golden={len(golden_annotations)}"
            )

        def test_most_severe_consequence_match(self, vepyr_df, golden_annotations):
            matched = 0
            total = 0
            for row in vepyr_df.iter_rows(named=True):
                key = (row["chrom"], row["start"], row["ref"], row["alt"])
                golden_csqs = _lookup(key, golden_annotations)
                if golden_csqs is None:
                    continue
                total += 1
                golden_consequences = {
                    consequence
                    for csq in golden_csqs
                    for consequence in csq_field(csq, "Consequence").split("&")
                    if consequence
                }
                if row.get("most_severe_consequence") in golden_consequences:
                    matched += 1
            assert total > 0
            assert matched / total >= 1.0, f"Match rate {matched / total:.1%} < 100%"

        def test_impact_levels_present(self, vepyr_df):
            impacts = set()
            for value in vepyr_df["IMPACT"].drop_nulls().to_list():
                if isinstance(value, list):
                    impacts.update(v for v in value if v)
                elif value:
                    impacts.add(value)
            expected = {"HIGH", "MODERATE", "LOW", "MODIFIER"}
            assert len(impacts & expected) >= 2, (
                f"Expected >=2 IMPACT levels, got {impacts}"
            )

        def test_hgvs_annotations_present(self, vepyr_df):
            hgvsc_count = vepyr_df["HGVSc"].drop_nulls().len()
            assert hgvsc_count > 0, "No HGVSc annotations with everything=True"

        def test_has_all_csq_columns(self, vepyr_df):
            missing = [
                field for field in config.csq_fields if field not in vepyr_df.columns
            ]
            assert not missing, f"Missing CSQ columns in DataFrame: {missing}"

        def test_has_vcf_core_columns(self, vepyr_df):
            expected = ["chrom", "start", "ref", "alt", "id", "qual", "filter"]
            missing = [column for column in expected if column not in vepyr_df.columns]
            assert not missing, f"Missing VCF core columns: {missing}"

        def test_column_count(self, vepyr_df):
            assert vepyr_df.width >= config.min_df_width, (
                f"Expected >={config.min_df_width} columns, got {vepyr_df.width}"
            )

    def make_vcf_field_test(field: str):
        def test_method(self, vepyr_vcf_annotations, golden_annotations):
            mismatches = self._compare_vcf_field(
                field, vepyr_vcf_annotations, golden_annotations
            )
            assert not mismatches, f"{field} mismatches: {mismatches}"

        test_method.__name__ = f"test_{_field_test_name(field)}_match"
        return test_method

    def make_df_field_test(field: str):
        def test_method(self, vepyr_df, golden_annotations):
            mismatches = self._compare_df_field(field, vepyr_df, golden_annotations)
            assert not mismatches, f"{field} mismatches: {mismatches}"

        test_method.__name__ = f"test_df_{_field_test_name(field)}_match"
        return test_method

    for field in config.vcf_comparison_fields:
        setattr(
            TestGoldenVcfComparison,
            f"test_{_field_test_name(field)}_match",
            make_vcf_field_test(field),
        )

    for field in config.df_comparison_fields:
        setattr(
            TestGoldenDataFrameComparison,
            f"test_df_{_field_test_name(field)}_match",
            make_df_field_test(field),
        )

    namespace.update(
        {
            "golden_annotations": golden_annotations,
            "golden_field_order": golden_field_order,
            "skip_if_no_cache": skip_if_no_cache,
            "vepyr_df": vepyr_df,
            "vepyr_vcf_path": vepyr_vcf_path,
            "vepyr_vcf_annotations": vepyr_vcf_annotations,
            "TestGoldenVcfComparison": TestGoldenVcfComparison,
            "TestGoldenDataFrameComparison": TestGoldenDataFrameComparison,
        }
    )
