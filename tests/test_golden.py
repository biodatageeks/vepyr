"""Integration test: compare vepyr annotation against Ensembl VEP golden standard.

Test data:
- input.vcf.gz: 100 variants from HG002 chr1, normalized with bcftools norm -m -both
- golden.vcf: matching Ensembl VEP 115 output with --everything flag
- reference.fa: trimmed GRCh38 chr1 (first ~860kb)

The parquet cache is referenced from the default path. Set VEPYR_CACHE_DIR
env var to override. The test is skipped if the cache is not available.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"

# Trimmed cache bundled with the test data (chr1, first ~900kb)
CACHE_DIR = str(GOLDEN_DIR / "cache")

INPUT_VCF = GOLDEN_DIR / "input.vcf.gz"
GOLDEN_VCF = GOLDEN_DIR / "golden.vcf"
REFERENCE_FASTA = GOLDEN_DIR / "reference.fa"


def _parse_golden_vcf(path: Path) -> dict[tuple[str, int, str, str], list[str]]:
    """Parse golden VCF into {(chrom, pos, ref, alt): [csq1, csq2, ...]}."""
    results = {}
    with open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            fields = line.strip().split("\t")
            chrom, pos, _, ref, alt = fields[0], int(fields[1]), fields[2], fields[3], fields[4]
            info = fields[7]
            csq_entries = []
            for part in info.split(";"):
                if part.startswith("CSQ="):
                    csq_entries = part[4:].split(",")
                    break
            key = (chrom, pos, ref, alt)
            results[key] = csq_entries
    return results


def _extract_csq_field(csq: str, field_index: int) -> str:
    """Extract a field from a pipe-delimited CSQ entry by index."""
    parts = csq.split("|")
    if field_index < len(parts):
        return parts[field_index]
    return ""


# CSQ field indices (from the golden VCF header)
CSQ_CONSEQUENCE = 1
CSQ_IMPACT = 2
CSQ_SYMBOL = 3
CSQ_FEATURE = 6
CSQ_BIOTYPE = 7
CSQ_HGVSC = 10
CSQ_HGVSP = 11


def _normalize_chrom(chrom: str) -> str:
    """Strip 'chr' prefix for comparison."""
    return chrom.replace("chr", "")


@pytest.fixture(scope="module")
def golden_annotations():
    """Load golden VEP annotations."""
    return _parse_golden_vcf(GOLDEN_VCF)


@pytest.fixture(scope="module")
def vepyr_annotations():
    """Run vepyr annotation and return results as dict."""
    if not os.path.isdir(CACHE_DIR):
        pytest.skip(f"Golden test cache not available at {CACHE_DIR}. Run tests/data/golden/prepare.py to create it.")

    import vepyr

    lf = vepyr.annotate(
        str(INPUT_VCF),
        CACHE_DIR,
        everything=True,
        reference_fasta=str(REFERENCE_FASTA),
    )
    df = lf.collect()

    results = {}
    for row in df.iter_rows(named=True):
        chrom = row.get("chrom", "")
        pos = row.get("start", 0)
        ref = row.get("ref", "")
        alt = row.get("alt", "")
        csq = row.get("csq", "")
        most_severe = row.get("most_severe_consequence", "")
        key = (chrom, pos, ref, alt)
        results[key] = {
            "csq": csq.split(",") if csq else [],
            "most_severe": most_severe,
        }
    return results


class TestGoldenComparison:
    """Compare vepyr output against Ensembl VEP golden standard."""

    def test_variant_count(self, vepyr_annotations, golden_annotations):
        """vepyr should produce annotations for all input variants."""
        assert len(vepyr_annotations) > 0, "No annotations produced"
        # Allow some tolerance — multiallelic decomposition may differ
        ratio = len(vepyr_annotations) / len(golden_annotations)
        assert ratio >= 0.9, (
            f"Too few variants: vepyr={len(vepyr_annotations)}, "
            f"golden={len(golden_annotations)}"
        )

    def test_most_severe_consequence_match(self, vepyr_annotations, golden_annotations):
        """Most severe consequence should match for shared variants."""
        matched = 0
        mismatched = []

        for key, golden_csqs in golden_annotations.items():
            # Normalize chrom for matching (golden uses chr1, vepyr may use 1)
            norm_key = (_normalize_chrom(key[0]), key[1], key[2], key[3])
            vepyr = vepyr_annotations.get(key) or vepyr_annotations.get(norm_key)
            if vepyr is None:
                continue

            # Extract most severe from golden CSQ entries
            golden_consequences = set()
            for csq in golden_csqs:
                consequence = _extract_csq_field(csq, CSQ_CONSEQUENCE)
                for c in consequence.split("&"):
                    golden_consequences.add(c)

            vepyr_severe = vepyr["most_severe"]
            if vepyr_severe in golden_consequences:
                matched += 1
            else:
                mismatched.append(
                    (key, vepyr_severe, golden_consequences)
                )

        total = matched + len(mismatched)
        if total == 0:
            pytest.skip("No overlapping variants to compare")

        match_rate = matched / total
        assert match_rate >= 0.85, (
            f"Most severe consequence match rate {match_rate:.1%} < 85%. "
            f"First 5 mismatches: {mismatched[:5]}"
        )

    def test_gene_symbol_match(self, vepyr_annotations, golden_annotations):
        """Gene symbols in CSQ should match for shared variants."""
        matched = 0
        total = 0

        for key, golden_csqs in golden_annotations.items():
            norm_key = (_normalize_chrom(key[0]), key[1], key[2], key[3])
            vepyr = vepyr_annotations.get(key) or vepyr_annotations.get(norm_key)
            if vepyr is None:
                continue

            golden_symbols = {
                _extract_csq_field(csq, CSQ_SYMBOL)
                for csq in golden_csqs
                if _extract_csq_field(csq, CSQ_SYMBOL)
            }
            vepyr_symbols = {
                _extract_csq_field(csq, CSQ_SYMBOL)
                for csq in vepyr["csq"]
                if _extract_csq_field(csq, CSQ_SYMBOL)
            }

            if not golden_symbols:
                continue

            total += 1
            if golden_symbols & vepyr_symbols:  # at least one symbol overlaps
                matched += 1

        if total == 0:
            pytest.skip("No variants with gene symbols to compare")

        match_rate = matched / total
        assert match_rate >= 0.85, (
            f"Gene symbol match rate {match_rate:.1%} < 85%"
        )

    def test_csq_transcript_count(self, vepyr_annotations, golden_annotations):
        """Number of CSQ entries per variant should be similar."""
        ratios = []

        for key, golden_csqs in golden_annotations.items():
            norm_key = (_normalize_chrom(key[0]), key[1], key[2], key[3])
            vepyr = vepyr_annotations.get(key) or vepyr_annotations.get(norm_key)
            if vepyr is None or not golden_csqs:
                continue

            golden_count = len(golden_csqs)
            vepyr_count = len(vepyr["csq"])
            if golden_count > 0:
                ratios.append(vepyr_count / golden_count)

        if not ratios:
            pytest.skip("No overlapping variants to compare")

        avg_ratio = sum(ratios) / len(ratios)
        assert 0.5 <= avg_ratio <= 2.0, (
            f"CSQ count ratio {avg_ratio:.2f} out of expected range [0.5, 2.0]"
        )

    def test_impact_levels_present(self, vepyr_annotations):
        """Output should contain multiple IMPACT levels."""
        impacts = set()
        for data in vepyr_annotations.values():
            for csq in data["csq"]:
                impact = _extract_csq_field(csq, CSQ_IMPACT)
                if impact:
                    impacts.add(impact)

        expected = {"HIGH", "MODERATE", "LOW", "MODIFIER"}
        found = impacts & expected
        assert len(found) >= 2, (
            f"Expected at least 2 IMPACT levels, found: {found}"
        )

    def test_hgvs_annotations_present(self, vepyr_annotations):
        """With everything=True, HGVSc annotations should be present."""
        hgvsc_count = 0
        for data in vepyr_annotations.values():
            for csq in data["csq"]:
                hgvsc = _extract_csq_field(csq, CSQ_HGVSC)
                if hgvsc:
                    hgvsc_count += 1

        assert hgvsc_count > 0, "No HGVSc annotations found with everything=True"

    def test_no_empty_csq(self, vepyr_annotations):
        """Every annotated variant should have at least one CSQ entry."""
        empty = [
            key for key, data in vepyr_annotations.items()
            if not data["csq"] or all(c == "" for c in data["csq"])
        ]
        assert len(empty) == 0, (
            f"{len(empty)} variants with empty CSQ: {empty[:5]}"
        )
