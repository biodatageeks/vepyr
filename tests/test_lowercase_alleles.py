"""Annotation casing follows VEP while VCF and LazyFrame inputs stay verbatim."""

from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tests.cache_metadata import copy_cache_with_source_metadata
import vepyr


GOLDEN = Path(__file__).parent / "data" / "golden"
ALLELES = [("G", "A"), ("g", "a"), ("g", "A"), ("G", "a")]


@pytest.fixture(scope="module")
def cache_dir(tmp_path_factory):
    target = tmp_path_factory.mktemp("lowercase_cache")
    return str(
        copy_cache_with_source_metadata(GOLDEN / "cache", target, "ensembl", "115")
    )


@pytest.fixture
def input_vcf(tmp_path):
    path = tmp_path / "lowercase.vcf"
    header = (
        "##fileformat=VCFv4.2\n"
        "##contig=<ID=chr1>\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
    )
    path.write_text(
        header
        + "".join(
            f"chr1\t779047\tcase{i}\t{ref}\t{alt}\t.\t.\t.\n"
            for i, (ref, alt) in enumerate(ALLELES)
        )
    )
    return path


def assert_vcf_annotations(output):
    lines = output.read_text().splitlines()
    header = next(line for line in lines if line.startswith("##INFO=<ID=CSQ,"))
    fields = header.split("Format: ", 1)[1].split('"', 1)[0].split("|")
    records = [line.split("\t") for line in lines if not line.startswith("#")]
    assert len(records) == len(ALLELES)
    control = records[0][7]
    entries = control.removeprefix("CSQ=").split(",")
    assert entries
    for entry in entries:
        values = dict(zip(fields, entry.split("|")))
        assert values["Allele"] == "A"
        assert values["Existing_variation"] == "rs12028261"
        assert values["AF"] == "0.7728"
        assert values["MAX_AF"] == "1"
    for index, (record, alleles) in enumerate(zip(records, ALLELES)):
        assert record[:3] == ["chr1", "779047", f"case{index}"]
        assert tuple(record[3:5]) == alleles
        assert record[7] == control


@pytest.mark.parametrize("mode", ["vcf", "cli", "indexed"])
def test_lowercase_alleles_keep_known_variants_in_vcf(
    cache_dir, input_vcf, tmp_path, mode
):
    output = tmp_path / "annotated.vcf"
    if mode == "cli":
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
    else:
        if mode == "indexed":
            if not (shutil.which("bgzip") and shutil.which("tabix")):
                pytest.skip("indexed VCF input needs bgzip and tabix")
            bgzf = tmp_path / "lowercase.vcf.gz"
            with bgzf.open("wb") as handle:
                subprocess.run(
                    ["bgzip", "-c", str(input_vcf)], stdout=handle, check=True
                )
            subprocess.run(["tabix", "-p", "vcf", str(bgzf)], check=True)
            input_vcf = bgzf
        vepyr.annotate(
            str(input_vcf),
            cache_dir,
            reference_fasta=str(GOLDEN / "reference.fa"),
            output_vcf=str(output),
            show_progress=False,
            workers=2 if mode == "indexed" else 1,
        )
    assert_vcf_annotations(output)


@pytest.mark.parametrize("include_csq", [False, True])
def test_lowercase_lazyframe_preserves_input_and_annotation_projection(
    cache_dir, input_vcf, include_csq
):
    fields = ["Allele", "Existing_variation", "AF", "MAX_AF", "Consequence", "HGVSc"]
    if include_csq:
        fields.append("CSQ")
    rows = (
        vepyr.annotate(
            str(input_vcf),
            cache_dir,
            reference_fasta=str(GOLDEN / "reference.fa"),
            skip_csq=not include_csq,
            show_progress=False,
        )
        .select("id", "ref", "alt", *fields)
        .collect()
        .to_dicts()
    )
    assert len(rows) == len(ALLELES)
    assert rows[0]["Existing_variation"]
    assert rows[0]["AF"]
    for index, (row, alleles) in enumerate(zip(rows, ALLELES)):
        assert row["id"] == f"case{index}"
        assert (row["ref"], row["alt"]) == alleles
        for field in fields:
            assert row[field] == rows[0][field], (index, field)
