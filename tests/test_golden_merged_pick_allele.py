"""Integration test: compare --pick_allele annotation against Ensembl VEP output."""

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
PICK_ALLELE_GOLDEN_DIR = TESTS_DIR / "data" / "golden_merged_pick_allele"
VEP_PICK_ORDER = "biotype,rank,mane_select,tsl,canonical,appris,ccds,length"

install_golden_suite(
    globals(),
    GoldenConfig(
        name="merged pick_allele golden",
        cache_dir=MERGED_GOLDEN_DIR / "cache",
        input_vcf=DEFAULT_GOLDEN_DIR / "input.vcf.gz",
        golden_vcf=PICK_ALLELE_GOLDEN_DIR / "golden.vcf",
        reference_fasta=DEFAULT_GOLDEN_DIR / "reference.fa",
        annotate_kwargs={
            "merged": True,
            "pick_allele": True,
            "pick_order": VEP_PICK_ORDER,
        },
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
        exact_csq_entry_count=True,
        check_most_severe_consequence=False,
    ),
)
