"""Integration test: same-length multi-base substitutions against Ensembl VEP.

The chr1 golden fixture shared by every other suite holds 91 SNVs and 9 indels,
and all nine indels have a shared prefix of exactly 1 and a shared suffix of 0.
It therefore contains no record whose annotation depends on how REF/ALT are
trimmed, which is why it stayed green while vepyr#95 emitted `Allele=TC` for
`GAC>GTC` where VEP emits `GTC`.

This fixture exists to close that blind spot permanently. `golden.vcf` is real
Ensembl VEP 115.2 output -- the same release and merged 115 cache as
``golden_merged`` -- over five purpose-built chr1 records inside the trimmed
cache window, with every REF verified against ``golden/reference.fa``:

    604358 GGT>GAT    MNV, shared prefix 1 and shared suffix 1  -> VEP: GAT
    604360 T>C        SNV control, unaffected either way        -> VEP: C
    611317 AG>AA      two-base MNV, shared prefix 1             -> VEP: AA
    818464 CCT>CTT    MNV, shared prefix 1                      -> VEP: CTT
    826577 ACTA>AA    indel with a shared suffix -- the FROZEN  -> VEP: -
                      shape vepyr#95 wrongly proposed changing

Three of the five carry DISTANCE values, which is what makes this suite
sensitive to the coordinate half of the bug and not only to `Allele`: the
golden comparison deliberately excludes `Allele` (see
``GoldenConfig.vcf_comparison_fields``), so the defect is caught through
DISTANCE, Codons, cDNA_position, CDS_position and HGVSc instead.
"""

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
MNV_GOLDEN_DIR = TESTS_DIR / "data" / "golden_mnv"

install_golden_suite(
    globals(),
    GoldenConfig(
        name="mnv golden",
        cache_dir=MERGED_GOLDEN_DIR / "cache",
        cache_source_type="merged",
        input_vcf=MNV_GOLDEN_DIR / "input.vcf.gz",
        golden_vcf=MNV_GOLDEN_DIR / "golden.vcf",
        reference_fasta=DEFAULT_GOLDEN_DIR / "reference.fa",
        annotate_kwargs={},
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
