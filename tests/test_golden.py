"""Integration test: compare vepyr annotation against Ensembl VEP golden output."""

from __future__ import annotations

from pathlib import Path

from tests._golden_suite import (
    DEFAULT_CSQ_FIELDS,
    DEFAULT_DF_COMPARISON_FIELDS,
    GoldenConfig,
    install_golden_suite,
)

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"

install_golden_suite(
    globals(),
    GoldenConfig(
        name="default golden",
        cache_dir=GOLDEN_DIR / "cache",
        input_vcf=GOLDEN_DIR / "input.vcf.gz",
        golden_vcf=GOLDEN_DIR / "golden.vcf",
        reference_fasta=GOLDEN_DIR / "reference.fa",
        annotate_kwargs={},
        csq_fields=DEFAULT_CSQ_FIELDS,
        df_comparison_fields=DEFAULT_DF_COMPARISON_FIELDS,
    ),
)
