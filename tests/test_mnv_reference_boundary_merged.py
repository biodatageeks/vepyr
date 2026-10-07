"""Genomic HGVS reference for an MNV spanning a native cache region boundary."""

from pathlib import Path
import re
import subprocess
import sys

import pytest

import vepyr


FIXTURE = Path(__file__).parent / "data" / "mnv_reference_boundary_merged"
CASES = [
    pytest.param("original", id="DT-dff61aedd9-mnv-cache-boundary"),
    pytest.param("control", id="reference-consistent-control"),
]


def body(path):
    return b"".join(
        line
        for line in path.read_bytes().splitlines(keepends=True)
        if not line.startswith(b"#")
    )


def csq_fields(path):
    header = next(
        line
        for line in path.read_text().splitlines()
        if line.startswith("##INFO=<ID=CSQ,")
    )
    return re.search(r"Format: ([^\"]+)", header).group(1).split("|")


def records(path):
    fields = csq_fields(path)
    result = []
    for line in body(path).decode().splitlines():
        columns = line.split("\t")
        csq = next(
            value[4:] for value in columns[7].split(";") if value.startswith("CSQ=")
        )
        entries = [
            dict(zip(fields, entry.split("|"), strict=True)) for entry in csq.split(",")
        ]
        result.append((columns, csq, entries))
    return result


def assert_golden(output, name):
    expected = FIXTURE / f"{name}.golden.vcf"
    assert csq_fields(output) == csq_fields(expected)
    rows = records(output)
    assert [int(row[0][1]) for row in rows] == [1000000]
    columns, _, entries = rows[0]
    assert columns[3:5] == (["GG", "AT"] if name == "control" else ["AC", "GT"])
    assert len(entries) == 21
    assert {entry["Allele"] for entry in entries} == {
        "AT" if name == "control" else "GT"
    }
    by_feature = {entry["Feature"]: entry for entry in entries}
    utr = by_feature["ENST00000304952"]
    noncoding = by_feature["ENST00000481869"]
    for entry in (utr, noncoding):
        assert entry["GIVEN_REF"] == ("GG" if name == "control" else "AC")
        assert entry["USED_REF"] == "GG"
    assert utr["cDNA_position"] == "97-98"
    assert noncoding["cDNA_position"] == "96-97"
    assert utr["HGVSc"] == (
        "ENST00000304952.11:c.-28_-27delinsAT"
        if name == "control"
        else "ENST00000304952.11:c.-28C>A"
    )
    assert noncoding["HGVSc"] == (
        "ENST00000481869.1:n.96_97delinsAT"
        if name == "control"
        else "ENST00000481869.1:n.96C>A"
    )
    assert by_feature["ENST00000606034"]["USED_REF"] == (
        "GG" if name == "control" else "AC"
    )
    assert by_feature["ENSR1_C4ND"]["USED_REF"] == ""
    assert sum(entry["USED_REF"] == "GG" for entry in entries) == (
        20 if name == "control" else 14
    )

    assert body(output) == body(expected)


@pytest.mark.parametrize("name", CASES)
@pytest.mark.parametrize("workers,indexed", [(1, False), (1, True), (2, True)])
def test_mnv_reference_boundary_vcf(tmp_path, name, workers, indexed):
    output = tmp_path / "output.vcf"
    vepyr.annotate(
        str(FIXTURE / f"{name}.vcf{'.gz' if indexed else ''}"),
        str(FIXTURE / "cache"),
        reference_fasta=str(FIXTURE / "reference.fa.gz"),
        expected_cache_version="116",
        output_vcf=str(output),
        workers=workers,
        show_progress=False,
    )
    assert_golden(output, name)


@pytest.mark.parametrize("name", CASES)
@pytest.mark.parametrize(
    "columns",
    [
        ["HGVSc"],
        ["GIVEN_REF"],
        ["USED_REF"],
        ["GIVEN_REF", "USED_REF", "HGVSc"],
        ["CSQ", "GIVEN_REF", "USED_REF", "HGVSc"],
    ],
)
def test_mnv_reference_boundary_lazy_projections(name, columns):
    result = (
        vepyr.annotate(
            str(FIXTURE / f"{name}.vcf"),
            str(FIXTURE / "cache"),
            reference_fasta=str(FIXTURE / "reference.fa.gz"),
            expected_cache_version="116",
            skip_csq="CSQ" not in columns,
            show_progress=False,
        )
        .select(["ref", "alt", "Feature", *columns])
        .collect()
        .to_dicts()
    )
    assert len(result) == 1
    for row, (vcf, csq, entries) in zip(
        result, records(FIXTURE / f"{name}.golden.vcf"), strict=True
    ):
        assert (row["ref"], row["alt"]) == (vcf[3], vcf[4])
        assert row["Feature"] == [entry["Feature"] for entry in entries]
        for field in set(columns) - {"CSQ"}:
            assert [value or "" for value in row[field]] == [
                entry[field] for entry in entries
            ]
        if "CSQ" in columns:
            assert row["CSQ"] == csq


@pytest.mark.parametrize("name", CASES)
def test_mnv_reference_boundary_cli(tmp_path, name):
    output = tmp_path / "cli.vcf"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vepyr",
            "annotate",
            "--input_file",
            str(FIXTURE / f"{name}.vcf"),
            "--output_file",
            str(output),
            "--dir_cache",
            str(FIXTURE / "cache"),
            "--fasta",
            str(FIXTURE / "reference.fa.gz"),
            "--cache_version",
            "116",
            "--everything",
            "--no_progress",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert_golden(output, name)
