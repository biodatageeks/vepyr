"""Projection pushdown on the HG002 chr22 fixture.

A ``LazyFrame`` query hands the engine the columns it reads, and the engine
skips the work no read column needs. Every test compares such a query with
the same query over the full ``--everything`` annotation pruned in Polars
(``projection_pushdown=False``): the values must be identical.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import vepyr

DATA = Path(__file__).parent / "data" / "hg002_chr22"
VCF = DATA / "input_chr22.vcf.gz"
RAW_VCF = DATA / "raw_chr22.vcf.gz"
CACHE = DATA / "cache"
FASTA = DATA / "chr22.fa.gz"


def _lfs_pointer(path: Path) -> bool:
    with open(path, "rb") as handle:
        return handle.read(24).startswith(b"version https://git-lfs")


_FIXTURE_FILES = [VCF, RAW_VCF, FASTA, *sorted(CACHE.glob("*/chr22.parquet"))]
_MISSING = [str(p) for p in _FIXTURE_FILES if not p.exists() or _lfs_pointer(p)]
pytestmark = pytest.mark.skipif(
    bool(_MISSING), reason=f"hg002_chr22 fixture not fetched from LFS: {_MISSING}"
)

KEY = ["chrom", "start", "ref", "alt"]
# A gene-dense 3 Mb of chr22, so the per-column cases stay fast while covering
# every feature type. As a filter it is also pushed down as an engine region.
WINDOW = (pl.col("chrom") == "chr22") & pl.col("start").is_between(
    37_000_000, 40_000_000
)

FREQUENCY_COLUMNS = [
    "AF",
    "AFR_AF",
    "AMR_AF",
    "EAS_AF",
    "EUR_AF",
    "SAS_AF",
    "gnomADe_AF",
    "gnomADe_AFR_AF",
    "gnomADe_NFE_AF",
    "gnomADg_AF",
    "gnomADg_AMI_AF",
    "gnomADg_NFE_AF",
    "MAX_AF",
    "MAX_AF_POPS",
]
CACHE_ONLY_COLUMNS = [
    "clin_sig_allele",
    "clinical_impact",
    "minor_allele",
    "minor_allele_freq",
    "clinvar_ids",
    "cosmic_ids",
    "dbsnp_ids",
]
VARIANT_LEVEL_COLUMNS = [
    "Allele",
    "VARIANT_CLASS",
    "Existing_variation",
    "AF",
    "gnomADg_AF",
    "MAX_AF",
    "CLIN_SIG",
    "SOMATIC",
    "PHENO",
    "PUBMED",
    "clinvar_ids",
    "dbsnp_ids",
]
# Named column sets, each run over the whole fixture. Several pick one flag
# group (or none) so that each narrowing the engine does is exercised alone.
QUERIES = {
    "input_only": [],
    "transcript_no_flags": ["Consequence", "SYMBOL", "IMPACT", "Feature"],
    "transcript_annotation": [
        "Consequence",
        "IMPACT",
        "SYMBOL",
        "Gene",
        "Feature_type",
        "Feature",
        "BIOTYPE",
        "EXON",
        "INTRON",
        "cDNA_position",
        "CDS_position",
        "Protein_position",
        "Amino_acids",
        "Codons",
        "DISTANCE",
        "STRAND",
        "FLAGS",
    ],
    "most_severe_only": ["most_severe_consequence"],
    "colocated": [
        "Consequence",
        "Existing_variation",
        "CLIN_SIG",
        "SOMATIC",
        "PHENO",
        "PUBMED",
    ],
    "frequencies": FREQUENCY_COLUMNS,
    "frequencies_one_group": ["Consequence", "gnomADg_AF"],
    "cache_only": CACHE_ONLY_COLUMNS,
    "variant_level_only": VARIANT_LEVEL_COLUMNS,
    "hgvs": ["HGVSc", "HGVSp"],
    "hgvs_offset": ["Consequence", "HGVS_OFFSET"],
    "sift_polyphen": ["Consequence", "SIFT", "PolyPhen"],
    "domains_without_sift": ["Consequence", "DOMAINS"],
    "mirna": ["Consequence", "miRNA"],
    "motif": [
        "Consequence",
        "MOTIF_NAME",
        "MOTIF_POS",
        "HIGH_INF_POS",
        "MOTIF_SCORE_CHANGE",
        "TRANSCRIPTION_FACTORS",
    ],
    "transcript_metadata": [
        "Feature",
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
        "SYMBOL_SOURCE",
        "HGNC_ID",
    ],
    "benchmark_everything": ["Consequence", "SYMBOL", "SIFT", "PolyPhen", "HGVSc"],
    "mixed": ["most_severe_consequence", "AF", "HGVSp", "clinvar_ids", "MANE_SELECT"],
}
# Column sets that read the raw CSQ string (in the frame with skip_csq=False).
CSQ_QUERIES = {
    "csq_only": ["CSQ"],
    "csq_with_typed": ["CSQ", "Consequence", "SIFT"],
    "csq_with_variant_level": ["CSQ", "AF", "Existing_variation"],
}


def annotate(vcf: Path = VCF, **options) -> pl.LazyFrame:
    return vepyr.annotate(
        str(vcf),
        str(CACHE),
        reference_fasta=str(FASTA),
        show_progress=False,
        **options,
    )


def full(vcf: Path = VCF, **options) -> pl.DataFrame:
    """The full ``--everything`` annotation, columns pruned only in Polars."""
    return annotate(vcf, projection_pushdown=False, **options).collect()


@pytest.fixture(scope="module")
def reference() -> pl.DataFrame:
    return full()


@pytest.fixture(scope="module")
def raw_reference() -> pl.DataFrame:
    return full(RAW_VCF)


@pytest.fixture(scope="module")
def csq_reference() -> pl.DataFrame:
    return full(skip_csq=False)


def _frame_columns() -> list[str]:
    if _MISSING:
        return []
    return annotate().collect_schema().names()


ALL_COLUMNS = _frame_columns()


@pytest.mark.parametrize("column", [c for c in ALL_COLUMNS if c not in KEY])
def test_each_column_alone_matches_the_full_run(reference, column):
    got = annotate().filter(WINDOW).select(KEY + [column]).collect()
    assert_frame_equal(got, reference.filter(WINDOW).select(KEY + [column]))


@pytest.mark.parametrize("name", list(QUERIES))
def test_named_column_sets_match_the_full_run(reference, name):
    columns = KEY + QUERIES[name]
    got = annotate().select(columns).collect()
    assert_frame_equal(got, reference.select(columns))


@pytest.mark.parametrize("name", list(CSQ_QUERIES))
def test_csq_column_sets_match_the_full_run(csq_reference, name):
    columns = KEY + CSQ_QUERIES[name]
    got = annotate(skip_csq=False).select(columns).collect()
    assert_frame_equal(got, csq_reference.select(columns))


def test_count_matches_the_full_run(reference):
    got = annotate().select(pl.len()).collect()
    assert got.item() == reference.height


@pytest.mark.parametrize(
    ("predicate", "columns"),
    [
        # A transcript column in the filter alone keeps the transcript engine.
        (pl.col("IMPACT").list.contains("HIGH"), KEY + ["AF", "Existing_variation"]),
        # A frequency column in the filter alone keeps the variation lookup.
        (pl.col("gnomADg_AF").fill_null(0) < 0.01, KEY + ["Consequence"]),
        (pl.col("SIFT").list.len() > 0, KEY + ["PolyPhen"]),
        (pl.col("Existing_variation").list.len() == 0, KEY),
    ],
    ids=["impact_filter", "frequency_filter", "sift_filter", "novel_filter"],
)
def test_filter_columns_count_as_read(reference, predicate, columns):
    got = annotate().filter(predicate).select(columns).collect()
    assert_frame_equal(got, reference.filter(predicate).select(columns))


def test_variant_level_aggregation(reference):
    def query(frame):
        return (
            frame.lazy()
            .group_by("VARIANT_CLASS")
            .agg(
                pl.len(),
                (pl.col("Existing_variation").list.len() == 0).sum().alias("novel"),
                pl.col("gnomADg_AF").mean().alias("mean_af"),
            )
            .sort("VARIANT_CLASS")
            .collect()
        )

    got = (
        annotate()
        .group_by("VARIANT_CLASS")
        .agg(
            pl.len(),
            (pl.col("Existing_variation").list.len() == 0).sum().alias("novel"),
            pl.col("gnomADg_AF").mean().alias("mean_af"),
        )
        .sort("VARIANT_CLASS")
        .collect()
    )
    assert_frame_equal(got, query(reference))


@pytest.mark.parametrize("name", ["transcript_no_flags", "variant_level_only", "hgvs"])
def test_limit_matches_the_full_run(reference, name):
    columns = KEY + QUERIES[name]
    got = annotate().select(columns).head(1000).collect()
    assert_frame_equal(got, reference.select(columns).head(1000))


@pytest.mark.parametrize(
    "name", ["transcript_no_flags", "colocated", "variant_level_only", "sift_polyphen"]
)
def test_workers_match_the_full_run(reference, name):
    columns = KEY + QUERIES[name]
    got = annotate(workers=2).select(columns).collect()
    assert_frame_equal(got, reference.select(columns))


@pytest.mark.parametrize(
    "name",
    [
        "transcript_no_flags",
        "colocated",
        "variant_level_only",
        "hgvs",
        "domains_without_sift",
    ],
)
def test_multiallelic_input_matches_the_full_run(raw_reference, name):
    columns = KEY + QUERIES[name]
    got = annotate(RAW_VCF).select(columns).collect()
    assert_frame_equal(got, raw_reference.select(columns))


def test_pushdown_off_is_the_full_run(reference):
    columns = KEY + QUERIES["transcript_no_flags"]
    got = annotate(projection_pushdown=False).select(columns).collect()
    assert_frame_equal(got, reference.select(columns))


@pytest.fixture(scope="module")
def shapes_vcf(tmp_path_factory) -> Path:
    """Window records plus the shapes the row gating treats specially: a star
    allele, a record without ALT, a REF==ALT record and a multi-allelic one."""
    header, records = [], []
    with gzip.open(VCF, "rt") as handle:
        for line in handle:
            if line.startswith("#"):
                header.append(line)
                continue
            fields = line.split("\t")
            if 37_000_000 <= int(fields[1]) <= 37_100_000:
                records.append(fields)
    assert len(records) > 40
    out = []
    for index, fields in enumerate(records[:200]):
        out.append("\t".join(fields))
        if index in (5, 15, 25, 35):
            shaped = list(fields)
            ref = shaped[3]
            shaped[4] = {5: "*", 15: ".", 25: ref, 35: "A,C" if ref != "A" else "C,G"}[
                index
            ]
            out.append("\t".join(shaped))
    path = tmp_path_factory.mktemp("shapes") / "shapes.vcf"
    path.write_text("".join(header) + "".join(out))
    return path


@pytest.mark.parametrize("allow_non_variant", [False, True])
@pytest.mark.parametrize(
    "name", ["variant_level_only", "input_only", "transcript_no_flags"]
)
def test_row_gating_shapes(shapes_vcf, allow_non_variant, name):
    columns = KEY + QUERIES[name]
    expected = full(shapes_vcf, allow_non_variant=allow_non_variant).select(columns)
    got = (
        annotate(shapes_vcf, allow_non_variant=allow_non_variant)
        .select(columns)
        .collect()
    )
    assert_frame_equal(got, expected)
    count = (
        annotate(shapes_vcf, allow_non_variant=allow_non_variant)
        .select(pl.len())
        .collect()
    )
    assert count.item() == expected.height


def _engine_options(monkeypatch, query) -> list[dict]:
    """The options each annotator of a query is created with."""
    seen = []
    real = vepyr._create_annotator

    def recording(vcf, cache_dir, options_json, *args):
        seen.append(json.loads(options_json))
        return real(vcf, cache_dir, options_json, *args)

    monkeypatch.setattr(vepyr, "_create_annotator", recording)
    query()
    return seen


def test_engine_receives_the_read_columns(monkeypatch):
    lf = annotate()
    options = _engine_options(
        monkeypatch,
        lambda: (
            lf.filter(pl.col("gnomADg_AF") < 0.5)
            .select(KEY + ["Consequence"])
            .collect()
        ),
    )
    last = options[-1]
    assert last["select_columns"] == [
        "chrom",
        "start",
        "ref",
        "alt",
        "Consequence",
        "gnomADg_AF",
    ]
    # The full option set travels; the engine narrows it.
    assert last["everything"] is True
    assert "vcf_record_layout" not in last


def test_pushdown_off_sends_no_columns(monkeypatch):
    lf = annotate(projection_pushdown=False)
    options = _engine_options(monkeypatch, lambda: lf.select(KEY).collect())
    assert "select_columns" not in options[-1]


def test_count_sends_one_input_column(monkeypatch):
    lf = annotate()
    options = _engine_options(monkeypatch, lambda: lf.select(pl.len()).collect())
    assert len(options[-1]["select_columns"]) == 1
