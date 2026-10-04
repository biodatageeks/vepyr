"""Synthetic negative-strand known variants in genuine merged116 context.

The cache is converted from native files during the test. These fixtures prove
engine capability, not qualification of the two blocked natural-cache ports.
"""

import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import vepyr


FIXTURE = Path(__file__).parent / "data" / "reverse_variation_strand_merged"
CASES = [
    "snv",
    "mnv",
    "deletion",
    "clinical_reverse",
    "clinical_forward",
    "clinical_missing",
]
EXPECTED = {
    "snv": (
        "rs9000000001&rs9000000004&rs9000000005&rs9000000006&rs9000000007",
        "0.2000&0.3000&0.3100&0.3200&0.4200",
        "benign",
    ),
    "mnv": ("rs9000000010", "0.1700", ""),
    "deletion": ("rs9000000011", "0.1900", ""),
    "clinical_reverse": ("rs9000000012", "", "pathogenic"),
    "clinical_forward": ("rs9000000008", "", "likely_pathogenic"),
    "clinical_missing": ("rs9000000009", "", "uncertain_significance"),
}


def body(path):
    return b"".join(
        line
        for line in path.read_bytes().splitlines(keepends=True)
        if not line.startswith(b"#")
    )


def record(path):
    header = next(
        line
        for line in path.read_text().splitlines()
        if line.startswith("##INFO=<ID=CSQ,")
    )
    fields = re.search(r"Format: ([^\"]+)", header).group(1).split("|")
    rows = body(path).decode().splitlines()
    assert len(rows) == 1
    columns = rows[0].split("\t")
    csq = next(value[4:] for value in columns[7].split(";") if value.startswith("CSQ="))
    entries = [
        dict(zip(fields, entry.split("|"), strict=True)) for entry in csq.split(",")
    ]
    return fields, columns, csq, entries


@pytest.fixture(scope="module")
def converted_cache(tmp_path_factory):
    output = tmp_path_factory.mktemp("reverse-strand-cache")
    vepyr._build_cache(
        str(FIXTURE / "native/homo_sapiens_merged/116_GRCh38"),
        str(output),
        None,
        2,
        "parquet",
        None,
        "merged",
        False,
        "116",
        ["1"],
    )
    return output


def test_native_conversion_preserves_strand_and_original_allele_labels(converted_cache):
    table = pq.read_table(converted_cache / "variation/chr1.parquet")
    assert table.schema.field("strand") == pa.field("strand", pa.int8(), nullable=True)
    rows = {row["dbsnp_ids"]: row for row in table.to_pylist()}
    assert len(rows) == 12
    for number, strand in [(1, -1), (4, 1), (5, None), (6, 0), (7, -1)]:
        assert rows[f"rs900000000{number}"]["strand"] == strand
    assert rows["rs9000000007"]["allele_string"] == "C/A/T"
    assert rows["rs9000000007"]["af_global_alleles"] == ["A", "T"]
    frequencies = rows["rs9000000007"]["af_global_freqs"]
    assert frequencies[0] == pytest.approx(0.42)
    assert frequencies[6] == pytest.approx(0.81)
    assert rows["rs9000000008"]["clin_sig_ref_allele"] == "G"
    assert rows["rs9000000009"]["clin_sig_ref_allele"] is None


@pytest.mark.parametrize("name", CASES)
def test_oracle_provenance(name):
    provenance = json.loads((FIXTURE / "provenance.json").read_text())["inputs"][name]
    assert (
        hashlib.sha256((FIXTURE / f"{name}.vcf").read_bytes()).hexdigest()
        == provenance["input_sha256"]
    )
    assert (
        hashlib.md5(body(FIXTURE / f"{name}.golden.vcf")).hexdigest()
        == provenance["expected_body_md5"]
    )
    assert provenance["vep_exit"] == 0


def assert_golden(output, name):
    expected = FIXTURE / f"{name}.golden.vcf"
    fields, columns, _, entries = record(output)
    oracle_fields, oracle_columns, _, _ = record(expected)
    assert fields == oracle_fields
    assert columns[:7] == oracle_columns[:7]
    assert len(entries) == 21
    ids, frequency, clinical = EXPECTED[name]
    assert {entry["Existing_variation"] for entry in entries} == {ids}
    assert {entry["AF"] for entry in entries} == {frequency}
    assert {entry["CLIN_SIG"] for entry in entries} == {clinical}
    assert body(output) == body(expected)


@pytest.mark.parametrize("name", CASES)
@pytest.mark.parametrize("workers,indexed", [(1, False), (1, True), (2, True)])
def test_reverse_strand_vcf(converted_cache, tmp_path, name, workers, indexed):
    output = tmp_path / "actual.vcf"
    vepyr.annotate(
        str(FIXTURE / f"{name}.vcf{'.gz' if indexed else ''}"),
        str(converted_cache),
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
        ["Existing_variation"],
        ["AF"],
        ["CLIN_SIG"],
        ["CSQ", "Existing_variation", "AF", "CLIN_SIG"],
    ],
)
def test_reverse_strand_lazy_projection(converted_cache, name, columns):
    actual = (
        vepyr.annotate(
            str(FIXTURE / f"{name}.vcf"),
            str(converted_cache),
            reference_fasta=str(FIXTURE / "reference.fa.gz"),
            expected_cache_version="116",
            skip_csq="CSQ" not in columns,
            show_progress=False,
        )
        .select(["ref", "alt", "Feature", *columns])
        .collect()
        .to_dicts()
    )
    assert len(actual) == 1
    row = actual[0]
    _, vcf, csq, entries = record(FIXTURE / f"{name}.golden.vcf")
    assert (row["ref"], row["alt"]) == (vcf[3], vcf[4])
    assert row["Feature"] == [entry["Feature"] for entry in entries]
    for field in set(columns) - {"CSQ"}:
        value = entries[0][field]
        assert {entry[field] for entry in entries} == {value}
        if field == "AF":
            # The public typed AF column is scalar Float32. Multiple co-located
            # frequencies remain available in CSQ and do not fit that scalar.
            if value and "&" not in value:
                assert row[field] == pytest.approx(float(value))
            else:
                assert row[field] is None
        else:
            assert row[field] == ([value] if value else None)
    if "CSQ" in columns:
        assert row["CSQ"] == csq


@pytest.mark.parametrize("name", CASES)
def test_reverse_strand_cli(converted_cache, tmp_path, name):
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
            str(converted_cache),
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


def test_legacy_physical_shard_without_strand_retains_forward_matching(
    converted_cache, tmp_path
):
    legacy = tmp_path / "legacy"
    shutil.copytree(converted_cache, legacy)
    path = legacy / "variation/chr1.parquet"
    table = pq.read_table(path)
    pq.write_table(table.drop(["strand"]), path, write_page_index=True)
    assert "strand" not in pq.read_schema(path).names
    rows = (
        vepyr.annotate(
            str(FIXTURE / "snv.vcf"),
            str(legacy),
            reference_fasta=str(FIXTURE / "reference.fa.gz"),
            expected_cache_version="116",
            skip_csq=True,
            show_progress=False,
        )
        .select(["Existing_variation", "AF"])
        .collect()
        .to_dicts()
    )
    assert len(rows) == 1
    assert rows[0]["Existing_variation"] == [
        "rs9000000003&rs9000000004&rs9000000005&rs9000000006"
    ]
    assert rows[0]["AF"] is None
