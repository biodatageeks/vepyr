"""Region predicate pushdown: wiring, warning, and fixture parity."""

from __future__ import annotations

from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"
MERGED_GOLDEN_DIR = TESTS_DIR / "data" / "golden_merged"
CACHE_DIR = str(GOLDEN_DIR / "cache")
INPUT_VCF = str(GOLDEN_DIR / "input.vcf.gz")
PLAIN_INPUT_VCF = str(GOLDEN_DIR / "input.vcf")
REFERENCE_FASTA = str(GOLDEN_DIR / "reference.fa")


def test_vcf_contigs_reads_the_header_for_plain_and_bgzip_inputs():
    from vepyr._core import vcf_contigs

    gz = vcf_contigs(INPUT_VCF)
    assert "chr1" in gz
    assert vcf_contigs(PLAIN_INPUT_VCF) == gz


def test_vcf_contigs_missing_file_raises():
    from vepyr._core import vcf_contigs

    with pytest.raises(RuntimeError, match="Failed to open VCF"):
        vcf_contigs("/nonexistent/input.vcf")
