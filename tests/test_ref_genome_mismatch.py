"""VEP 116 annotates coding alleles from CDS without requiring REF == genome.

The fixture preserves porting-tests #221's verbatim chr21 records and their
Docker VEP oracle. The genome-corrected twins are independent positive controls.
"""

from pathlib import Path
import re
import subprocess
import sys

import pytest

import vepyr


FIXTURE = Path(__file__).parent / "data" / "ref_genome_mismatch"
FIELDS = [
    "HGVSc",
    "HGVSp",
    "CDS_position",
    "Protein_position",
    "Amino_acids",
    "Codons",
    "DOMAINS",
]


def body(path):
    return b"".join(
        line
        for line in path.read_bytes().splitlines(keepends=True)
        if not line.startswith(b"#")
    )


def expected_rows(name):
    path = FIXTURE / ("golden.vcf" if name == "input" else "control.golden.vcf")
    header = next(
        line
        for line in path.read_text().splitlines()
        if line.startswith("##INFO=<ID=CSQ,")
    )
    fields = re.search(r"Format: ([^\"]+)", header).group(1).split("|")
    rows = []
    for line in body(path).decode().splitlines():
        columns = line.split("\t")
        csq = columns[7].split("CSQ=", 1)[1]
        groups = [
            dict(zip(fields, group.split("|"), strict=True)) for group in csq.split(",")
        ]
        assert len(groups) == 34
        rows.append((columns, csq, {group["Feature"]: group for group in groups}))
    assert len(rows) == 4
    return rows


@pytest.mark.parametrize("name", ["input", "control"])
@pytest.mark.parametrize("workers,indexed", [(1, False), (1, True), (2, True)])
def test_ref_genome_mismatch_vcf_matches_docker(tmp_path, name, workers, indexed):
    output = tmp_path / "annotated.vcf"
    suffix = ".vcf.gz" if indexed else ".vcf"
    vepyr.annotate(
        str(FIXTURE / f"{name}{suffix}"),
        str(FIXTURE / "cache"),
        reference_fasta=str(FIXTURE / "reference.fa.gz"),
        output_vcf=str(output),
        workers=workers,
        show_progress=False,
    )
    expected = FIXTURE / ("golden.vcf" if name == "input" else "control.golden.vcf")
    assert body(output) == body(expected)


@pytest.mark.parametrize("include_csq", [False, True])
def test_ref_genome_mismatch_lazyframe_coding_fields(include_csq):
    fields = ["Feature", *FIELDS]
    columns = ["id", "ref", "alt", *fields]
    if include_csq:
        columns.append("CSQ")
    result = (
        vepyr.annotate(
            str(FIXTURE / "input.vcf"),
            str(FIXTURE / "cache"),
            reference_fasta=str(FIXTURE / "reference.fa.gz"),
            skip_csq=not include_csq,
            show_progress=False,
        )
        .select(columns)
        .collect()
    )
    rows = result.to_dicts()
    expected = expected_rows("input")
    assert len(rows) == len(expected)
    for row, (record, csq, groups) in zip(rows, expected, strict=True):
        assert [row["id"], row["ref"], row["alt"]] == record[2:5]
        assert all(len(row[field]) == 34 for field in fields)
        assert len(set(row["Feature"])) == 34
        assert set(row["Feature"]) == set(groups)
        for i, feature in enumerate(row["Feature"]):
            assert {field: row[field][i] or "" for field in FIELDS} == {
                field: groups[feature][field] for field in FIELDS
            }
        if include_csq:
            assert row["CSQ"] == csq


def test_ref_genome_mismatch_cli_matches_docker(tmp_path):
    output = tmp_path / "cli.vcf"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vepyr",
            "annotate",
            "-i",
            str(FIXTURE / "input.vcf"),
            "-o",
            str(output),
            "--dir_cache",
            str(FIXTURE / "cache"),
            "--fasta",
            str(FIXTURE / "reference.fa.gz"),
            "--no_progress",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert body(output) == body(FIXTURE / "golden.vcf")
