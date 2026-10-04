"""Original miRNA REF mismatch and independent VEP 116.2 reference choices."""

from pathlib import Path
import json
import re
import shutil
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


def expected_groups(path):
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
        ["GIVEN_REF"],
        ["USED_REF"],
        ["GIVEN_REF", "USED_REF", "HGVSc"],
        ["CSQ", "GIVEN_REF", "USED_REF", "HGVSc"],
    ],
)
@pytest.mark.parametrize("name", ["input", "control"])
def test_mirna_reference_lazy_projections(columns, name):
    expected = FIXTURE / ("golden.vcf" if name == "input" else "control.golden.vcf")
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
    row = result[0]
    assert (row["ref"], row["alt"]) == ("C" if name == "input" else "A", "T")
    groups = expected_groups(expected)
    assert row["Feature"] == [group["Feature"] for group in groups]
    for field in set(columns) - {"CSQ"}:
        assert [value or "" for value in row[field]] == [
            group[field] for group in groups
        ]
    if "CSQ" in columns:
        assert row["CSQ"] == body(expected).decode().strip().split("CSQ=", 1)[1]


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
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert csq_fields(output) == csq_fields(FIXTURE / "golden.vcf")
    assert body(output) == body(FIXTURE / "golden.vcf")


@pytest.mark.parametrize("workers,indexed", [(1, False), (1, True), (2, True)])
def test_known_false_policy_matches_native_metadata_control(tmp_path, workers, indexed):
    """Counterfactual metadata control, not a relabelled biological cache."""
    cache = tmp_path / "cache"
    shutil.copytree(FIXTURE / "cache", cache)
    policy_path = cache / "reference_policy.json"
    policy = json.loads(policy_path.read_text())
    policy["bam_edited"] = False
    policy_path.write_text(json.dumps(policy))
    output = tmp_path / "known-false.vcf"
    vepyr.annotate(
        str(FIXTURE / ("input.vcf.gz" if indexed else "input.vcf")),
        str(cache),
        reference_fasta=str(FIXTURE / "reference.fa.gz"),
        expected_cache_version="116",
        output_vcf=str(output),
        workers=workers,
        show_progress=False,
    )
    expected = FIXTURE / "known-false.golden.vcf"
    assert csq_fields(output) == csq_fields(expected)
    assert not {"GIVEN_REF", "USED_REF", "BAM_EDIT"} & set(csq_fields(output))
    assert body(output) == body(expected)
    edited = next(
        row for row in expected_groups(expected) if row["Feature"] == "NR_001458.3"
    )
    assert edited["HGVSc"] == "NR_001458.3:n.291C>T"
