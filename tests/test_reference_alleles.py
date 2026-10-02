"""REF==ALT is retained without consequences, per VEP 116 VFO.pm:481."""

from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tests.cache_metadata import copy_cache_with_source_metadata
import vepyr


GOLDEN = Path(__file__).parent / "data" / "golden"
SOURCE = """##fileformat=VCFv4.2
##contig=<ID=chr1>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##INFO=<ID=FLAG,Number=0,Type=Flag,Description="Carried flag">
##INFO=<ID=CSQ,Number=.,Type=String,Description="Old consequences">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Depth">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE
chr1\t604358\tsame_snv\tG\tG\t50\tPASS\tDP=7;CSQ=stale_variant\tGT:DP\t0/0:7
chr1\t604358\tsame_mnv\tGG\tGG\t.\tPASS\tFLAG;DP=8\tGT:DP\t0/0:8
chr1\t604358\tchanged\tG\tC\t50\tPASS\tDP=9\tGT:DP\t0/1:9
chr1\t604360\tsame_t\tT\tT\t.\t.\t.\tGT:DP\t0/0:12
"""


@pytest.fixture(scope="module")
def cache_dir(tmp_path_factory):
    target = tmp_path_factory.mktemp("reference_allele_cache")
    return str(
        copy_cache_with_source_metadata(GOLDEN / "cache", target, "ensembl", "115")
    )


@pytest.fixture
def input_vcf(tmp_path):
    path = tmp_path / "reference_alleles.vcf"
    path.write_text(SOURCE)
    return path


def data_lines(text):
    return [line for line in text.splitlines() if line and not line.startswith("#")]


def assert_reference_records(output):
    records = data_lines(output.read_text())
    original = data_lines(SOURCE)
    assert [line.split("\t")[2] for line in records] == [
        "same_snv",
        "same_mnv",
        "changed",
        "same_t",
    ]
    for index in (0, 1, 3):
        assert records[index] == original[index].replace(";CSQ=stale_variant", ""), (
            "retain the input record and its INFO/FORMAT, remove stale CSQ, "
            "and generate no consequence for REF==ALT"
        )
    assert ";CSQ=" in records[2].split("\t")[7]
    assert records[2].split("\t")[:7] == original[2].split("\t")[:7]
    assert records[2].split("\t")[8:] == original[2].split("\t")[8:]


@pytest.mark.parametrize("allow_non_variant", [False, True])
def test_reference_equal_vcf_records_are_kept_without_csq(
    cache_dir, input_vcf, tmp_path, allow_non_variant
):
    output = tmp_path / "annotated.vcf"
    vepyr.annotate(
        str(input_vcf),
        cache_dir,
        reference_fasta=str(GOLDEN / "reference.fa"),
        output_vcf=str(output),
        show_progress=False,
        allow_non_variant=allow_non_variant,
    )
    assert_reference_records(output)


@pytest.mark.parametrize("include_csq", [False, True])
def test_reference_equal_lazyframe_has_null_annotations(
    cache_dir, input_vcf, include_csq
):
    annotation_columns = ["most_severe_consequence", "Consequence", "HGVSc"]
    if include_csq:
        annotation_columns.append("CSQ")
    result = (
        vepyr.annotate(
            str(input_vcf),
            cache_dir,
            reference_fasta=str(GOLDEN / "reference.fa"),
            show_progress=False,
            skip_csq=not include_csq,
        )
        .select("id", "ref", "alt", *annotation_columns)
        .collect()
    )
    rows = {row["id"]: row for row in result.to_dicts()}
    assert set(rows) == {"same_snv", "same_mnv", "changed", "same_t"}
    for name in ("same_snv", "same_mnv", "same_t"):
        assert rows[name]["ref"] == rows[name]["alt"]
        assert all(rows[name][column] is None for column in annotation_columns)
    assert rows["changed"]["Consequence"]
    assert rows["changed"]["most_severe_consequence"]


def test_reference_equal_indexed_workers_preserve_records(
    cache_dir, input_vcf, tmp_path
):
    if not (shutil.which("bgzip") and shutil.which("tabix")):
        pytest.skip("indexed multi-worker input needs bgzip and tabix")
    bgzf = tmp_path / "reference_alleles.vcf.gz"
    with bgzf.open("wb") as handle:
        subprocess.run(["bgzip", "-c", str(input_vcf)], stdout=handle, check=True)
    subprocess.run(["tabix", "-p", "vcf", str(bgzf)], check=True)
    outputs = []
    for workers in (1, 2):
        output = tmp_path / f"workers{workers}.vcf"
        vepyr.annotate(
            str(bgzf),
            cache_dir,
            reference_fasta=str(GOLDEN / "reference.fa"),
            output_vcf=str(output),
            show_progress=False,
            workers=workers,
        )
        assert_reference_records(output)
        outputs.append(data_lines(output.read_text()))
    assert outputs[0] == outputs[1]


def test_reference_equal_cli_records_are_kept_without_csq(
    cache_dir, input_vcf, tmp_path
):
    output = tmp_path / "cli.vcf"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vepyr",
            "annotate",
            "-i",
            str(input_vcf),
            "-o",
            str(output),
            "--dir_cache",
            cache_dir,
            "--fasta",
            str(GOLDEN / "reference.fa"),
            "--no_progress",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert_reference_records(output)
