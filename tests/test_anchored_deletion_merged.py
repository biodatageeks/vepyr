"""Transcript reference selection for an anchored literal deletion."""

from pathlib import Path
import re
import subprocess
import sys

import pytest

import vepyr


FIXTURE = Path(__file__).parent / "data" / "anchored_deletion_merged"
CASES = [
    pytest.param("original", id="DT-2e50dd94b9-anchored-deletion"),
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
    assert [int(row[0][1]) for row in rows] == [25587759]
    columns, _, entries = rows[0]
    assert columns[3:5] == (["TA", "T"] if name == "control" else ["AC", "A"])
    assert len(entries) == 39
    assert {entry["Allele"] for entry in entries} == {"-"}
    assert {entry["GIVEN_REF"] for entry in entries} == {
        "A" if name == "control" else "C"
    }
    by_feature = {entry["Feature"]: entry for entry in entries}
    hgvs = {
        "ENST00000307301": "ENST00000307301.12:c.999del",
        "ENST00000985910": "ENST00000985910.1:c.999del",
        "ENST00001019018": "ENST00001019018.1:c.1012del",
        "ENST00001101474": "ENST00001101474.1:c.1012del",
        "ENST00001110241": "ENST00001110241.1:c.*1272del",
    }
    for feature, hgvsc in hgvs.items():
        assert by_feature[feature]["USED_REF"] == "A"
        assert by_feature[feature]["HGVSc"] == hgvsc
        assert by_feature[feature]["HGVS_OFFSET"] == ""
    assert (
        by_feature["ENST00000307301"]["HGVSp"] == "ENSP00000305682.7:p.Thr334ArgfsTer22"
    )
    # Seven mapped features (five Ensembl and two RefSeq) use the deleted A;
    # the remaining original annotations retain the submitted, unanchored C.
    assert sum(entry["USED_REF"] == "A" for entry in entries) == (
        39 if name == "control" else 7
    )
    assert by_feature["ENST00000352957"]["USED_REF"] == (
        "A" if name == "control" else "C"
    )

    assert body(output) == body(expected)


@pytest.mark.parametrize("name", CASES)
@pytest.mark.parametrize("workers,indexed", [(1, False), (1, True), (2, True)])
def test_anchored_deletion_vcf(tmp_path, name, workers, indexed):
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
def test_anchored_deletion_lazy_projections(name, columns):
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
def test_anchored_deletion_cli(tmp_path, name):
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
