"""Original miRNA REF mismatch and independent VEP 116.2 reference choices."""

from pathlib import Path
import re
import subprocess
import sys

import pytest

import vepyr


FIXTURE = Path(__file__).parent / "data" / "transcript_reference_mirna"


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


def expected_groups():
    path = FIXTURE / "golden.vcf"
    csq = body(path).decode().strip().split("\t")[7].split("CSQ=", 1)[1]
    return [
        dict(zip(csq_fields(path), group.split("|"), strict=True))
        for group in csq.split(",")
    ]


@pytest.mark.parametrize("name", ["input", "control"])
@pytest.mark.parametrize("workers,indexed", [(1, False), (1, True), (2, True)])
def test_mirna_reference_vcf_matches_vep1162(tmp_path, name, workers, indexed):
    output = tmp_path / "annotated.vcf"
    suffix = ".vcf.gz" if indexed else ".vcf"
    vepyr.annotate(
        str(FIXTURE / f"{name}{suffix}"),
        str(FIXTURE / "cache"),
        reference_fasta=str(FIXTURE / "reference.fa.gz"),
        expected_cache_version="116",
        output_vcf=str(output),
        workers=workers,
        show_progress=False,
    )
    expected = FIXTURE / ("golden.vcf" if name == "input" else "control.golden.vcf")
    assert csq_fields(output) == csq_fields(expected)
    assert body(output) == body(expected)


@pytest.mark.parametrize(
    "columns",
    [
        ["HGVSc"],
        ["USED_REF"],
        ["GIVEN_REF", "USED_REF", "HGVSc"],
        ["CSQ", "GIVEN_REF", "USED_REF", "HGVSc"],
    ],
)
def test_mirna_reference_lazy_projections(columns):
    result = (
        vepyr.annotate(
            str(FIXTURE / "input.vcf"),
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
    row = result[0]
    assert (row["ref"], row["alt"]) == ("C", "T")
    groups = expected_groups()
    assert row["Feature"] == [group["Feature"] for group in groups]
    for field in set(columns) - {"CSQ"}:
        assert [value or "" for value in row[field]] == [
            group[field] for group in groups
        ]
    if "CSQ" in columns:
        assert (
            row["CSQ"]
            == body(FIXTURE / "golden.vcf").decode().strip().split("CSQ=", 1)[1]
        )


def test_mirna_reference_cli_matches_vep1162(tmp_path):
    output = tmp_path / "cli.vcf"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vepyr",
            "annotate",
            "--input_file",
            str(FIXTURE / "input.vcf"),
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
    )
    assert result.returncode == 0, result.stderr
    assert csq_fields(output) == csq_fields(FIXTURE / "golden.vcf")
    assert body(output) == body(FIXTURE / "golden.vcf")
