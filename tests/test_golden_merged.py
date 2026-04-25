"""Integration test: compare merged-cache annotation against merged Ensembl VEP output."""

from __future__ import annotations

from pathlib import Path

from tests._golden_suite import (
    DEFAULT_DF_COMPARISON_FIELDS,
    GoldenConfig,
    MERGED_CSQ_FIELDS,
    install_golden_suite,
)

TESTS_DIR = Path(__file__).parent
DEFAULT_GOLDEN_DIR = TESTS_DIR / "data" / "golden"
MERGED_GOLDEN_DIR = TESTS_DIR / "data" / "golden_merged"

install_golden_suite(
    globals(),
    GoldenConfig(
        name="merged golden",
        cache_dir=MERGED_GOLDEN_DIR / "cache",
        input_vcf=DEFAULT_GOLDEN_DIR / "input.vcf.gz",
        golden_vcf=MERGED_GOLDEN_DIR / "golden.vcf",
        reference_fasta=DEFAULT_GOLDEN_DIR / "reference.fa",
        annotate_kwargs={"merged": True},
        csq_fields=MERGED_CSQ_FIELDS,
        df_comparison_fields=[
            *DEFAULT_DF_COMPARISON_FIELDS,
            "REFSEQ_MATCH",
            "SOURCE",
            "REFSEQ_OFFSET",
            "GIVEN_REF",
            "USED_REF",
            "BAM_EDIT",
        ],
    ),
)
