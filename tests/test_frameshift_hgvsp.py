"""A shifted negative-strand insertion must preserve the VEP protein frame."""

from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest

import vepyr


DATA = Path(__file__).parent / "data"
INPUT = DATA / "frameshift_hgvsp" / "input.vcf"
CACHE = DATA / "ref_genome_mismatch" / "cache"
FASTA = DATA / "ref_genome_mismatch" / "reference.fa.gz"
FEATURE = "ENST00001110241"
# Fresh Docker VEP 116.2 values for the original normalized input. All fields
# except HGVSp already matched before the fix; keep them stable as well.
EXPECTED = {
    "Consequence": "frameshift_variant&NMD_transcript_variant",
    "HGVSc": "ENST00001110241.1:c.780_783dup",
    "HGVSp": "ENSP00000780046.1:p.Leu262PhefsTer81",
    "HGVS_OFFSET": "-7",
    "CDS_position": "776-777",
    "cDNA_position": "792-793",
    "Protein_position": "259",
    "Amino_acids": "Y/YLX",
    "Codons": "tat/taTTTAt",
}


def assert_vcf_feature(output):
    lines = output.read_text().splitlines()
    header = next(line for line in lines if line.startswith("##INFO=<ID=CSQ,"))
    fields = re.search(r"Format: ([^\"]+)", header).group(1).split("|")
    records = [line.split("\t") for line in lines if not line.startswith("#")]
    assert len(records) == 1
    assert records[0][:7] == ["21", "25592985", ".", "A", "ATAAA", ".", "."]
    csq = next(info[4:] for info in records[0][7].split(";") if info.startswith("CSQ="))
    rows = [
        dict(zip(fields, entry.split("|"), strict=True)) for entry in csq.split(",")
    ]
    target = [row for row in rows if row["Feature"] == FEATURE]
    assert len(target) == 1
    assert {field: target[0][field] for field in EXPECTED} == EXPECTED


@pytest.mark.parametrize("workers,indexed", [(1, False), (1, True), (2, True)])
def test_shifted_insertion_vcf(tmp_path, workers, indexed):
    source = INPUT
    if indexed:
        bgzip, tabix = shutil.which("bgzip"), shutil.which("tabix")
        if not bgzip or not tabix:
            pytest.skip("indexed VCF regression requires bgzip and tabix")
        source = tmp_path / "input.vcf.gz"
        with source.open("wb") as out:
            subprocess.run([bgzip, "-c", str(INPUT)], stdout=out, check=True)
        subprocess.run([tabix, "-p", "vcf", str(source)], check=True)
    output = tmp_path / "annotated.vcf"
    vepyr.annotate(
        str(source),
        str(CACHE),
        reference_fasta=str(FASTA),
        output_vcf=str(output),
        workers=workers,
        show_progress=False,
    )
    assert_vcf_feature(output)


@pytest.mark.parametrize("include_csq", [False, True])
def test_shifted_insertion_lazyframe(include_csq):
    columns = ["ref", "alt", "Feature", *EXPECTED]
    if include_csq:
        columns.append("CSQ")
    rows = (
        vepyr.annotate(
            str(INPUT),
            str(CACHE),
            reference_fasta=str(FASTA),
            skip_csq=not include_csq,
            show_progress=False,
        )
        .select(columns)
        .collect()
        .to_dicts()
    )
    assert len(rows) == 1
    row = rows[0]
    assert (row["ref"], row["alt"]) == ("A", "ATAAA")
    assert row["Feature"].count(FEATURE) == 1
    index = row["Feature"].index(FEATURE)
    for field in EXPECTED:
        assert isinstance(row[field], list), f"{field} must contain per-feature values"
        assert len(row[field]) == len(row["Feature"]), (
            f"{field} must align with Feature"
        )
    assert {field: str(row[field][index]) for field in EXPECTED} == EXPECTED
    if include_csq:
        assert EXPECTED["HGVSp"] in row["CSQ"]


def test_shifted_insertion_cli(tmp_path):
    output = tmp_path / "cli.vcf"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vepyr",
            "annotate",
            "-i",
            str(INPUT),
            "-o",
            str(output),
            "--dir_cache",
            str(CACHE),
            "--fasta",
            str(FASTA),
            "--everything",
            "--no_progress",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert_vcf_feature(output)
