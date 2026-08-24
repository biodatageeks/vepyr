from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

SCRIPTS_DIR = Path(__file__).parents[1] / "e2e-testing" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import verify_parity_gate as gate  # noqa: E402


TARGET = {
    "vepyr_version": "0.2.0",
    "cache_version": "116",
    "vep_codebase_version": "116.0",
    "api_version": "116",
    "ensembl_core_revision": "c0cf13d",
    "ensembl_variation_revision": "2fb834b",
    "semantics": "V116",
}

REFERENCE = {
    "vep_version": "116.0",
    "api_version": "116",
    "cache_version": "116",
    "cache_path": "/opt/vep/.vep/homo_sapiens_merged/116_GRCh38",
    "assembly": "GRCh38",
    "ensembl_release": "116",
    "ensembl_revision": "c0cf13d",
    "ensembl_variation_release": "116",
    "ensembl_variation_revision": "2fb834b",
    "header": "##VEP=...",
}

CLEAN_BUILD = {
    "vepyr_dirty": False,
    "dependencies": {
        name: {
            "dirty": False,
            "effective_source": f"git+https://example.invalid/{name}?rev=abc",
            "manifest_path": f"/cargo/git/checkouts/{name}/Cargo.toml",
        }
        for name in gate.REQUIRED_BUILD_DEPENDENCIES
    },
}


def _expected_artifacts(report_dir: Path) -> dict[str, dict]:
    paths = {
        "input_vcf": report_dir / "benchmark-input.vcf.gz",
        "reference_fasta": report_dir / "reference.fa",
        "vep_reference_vcf": report_dir / "vep-reference.vcf.gz",
    }
    for path in paths.values():
        if not path.exists():
            path.write_bytes(path.name.encode())
    return {name: gate.vcfio.source_identity(path) for name, path in paths.items()}


def _write_report(report_dir: Path, contig: str = "chr1") -> dict:
    ledger = Path(
        gate.report.mismatch_ledger_path(str(report_dir), contig, "merged", "116")
    )
    ledger.write_bytes(b"")
    fields = sorted(gate.expected_csq_fields("merged"))
    equality = {
        field: {
            "both_empty": 2,
            "both_nonempty_equal": 8,
            "vepyr_empty_only": 0,
            "vep_empty_only": 0,
            "both_nonempty_unequal": 0,
        }
        for field in fields
    }
    value = {
        "chrom": contig,
        "profile": "merged",
        "release": "116",
        "cache_path": "/cache/116_GRCh38_merged",
        "build": CLEAN_BUILD,
        "cache_identity": {
            **TARGET,
            "cache_source_type": "merged",
            "contig": contig,
        },
        "supported_target": dict(TARGET),
        "reference_identity": dict(REFERENCE),
        "benchmark_artifacts": _expected_artifacts(report_dir),
        "input_variants": 10,
        "annotation": {"output_variants": 10},
        "comparison": {
            "variants_compared": 10,
            "variants_only_in_vepyr": 0,
            "variants_only_in_vep": 0,
            "csq_entry_count_mismatch": 0,
            "csq_entries_only_in_vepyr": 0,
            "csq_entries_only_in_vep": 0,
            "csq_order_mismatch": 0,
            "fields_only_in_vepyr": [],
            "fields_only_in_vep": [],
            "field_match_rates": {field: 100.0 for field in fields},
            "field_mismatch_counts": {},
            "field_order_mismatch_counts": {},
            "field_format_mismatch_counts": {},
            "field_equality_counts": equality,
            "equality_bucket_counts": {
                "both_empty": 2 * len(fields),
                "both_nonempty_equal": 8 * len(fields),
                "vepyr_empty_only": 0,
                "vep_empty_only": 0,
                "both_nonempty_unequal": 0,
            },
            "mismatch_ledger": {
                "path": str(ledger),
                "rows": 0,
                "sha256": hashlib.sha256(b"").hexdigest(),
            },
        },
    }
    path = Path(gate.report.report_json_path(str(report_dir), contig, "merged", "116"))
    path.write_text(json.dumps(value))
    return value


def _validate(value: dict, report_dir: Path):
    return gate.validate_reports(
        [value],
        report_dir=report_dir,
        contigs=["chr1"],
        release="116",
        profile="merged",
        suffix="merged",
        expected_package_version="0.2.0",
        build_info=CLEAN_BUILD,
        expected_artifacts=_expected_artifacts(report_dir),
    )


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (["1-3"], ["chr1", "chr2", "chr3"]),
        (["chr2-4"], ["chr2", "chr3", "chr4"]),
        (["1", "chr2"], ["chr1", "chr2"]),
    ],
)
def test_parse_contigs_expands_ranges_and_aliases(values, expected):
    assert gate.parse_contigs(values) == expected


@pytest.mark.parametrize("values", [["3-1"], ["chrA-3"], ["1", "chr1"]])
def test_parse_contigs_rejects_invalid_or_duplicate_ranges(values):
    with pytest.raises(ValueError):
        gate.parse_contigs(values)


def test_zero_parity_report_passes(tmp_path):
    value = _write_report(tmp_path)

    totals = _validate(value, tmp_path)

    assert set(totals.values()) == {0}


def test_gate_rejects_null_comparison(tmp_path):
    value = _write_report(tmp_path)
    value["comparison"] = None

    with pytest.raises(gate.GateError, match="comparison"):
        _validate(value, tmp_path)


def test_gate_rejects_wrong_release(tmp_path):
    value = _write_report(tmp_path)
    value["release"] = "115"

    with pytest.raises(gate.GateError, match="wrong release"):
        _validate(value, tmp_path)


def test_gate_rejects_nonzero_field_mismatch(tmp_path):
    value = _write_report(tmp_path)
    comparison = value["comparison"]
    comparison["field_match_rates"]["Allele"] = 90.0
    comparison["field_mismatch_counts"]["Allele"] = 1
    comparison["field_equality_counts"]["Allele"]["both_nonempty_equal"] = 7
    comparison["field_equality_counts"]["Allele"]["both_nonempty_unequal"] = 1
    comparison["equality_bucket_counts"]["both_nonempty_equal"] = 7
    comparison["equality_bucket_counts"]["both_nonempty_unequal"] = 1

    with pytest.raises(gate.GateError, match="below 100%"):
        _validate(value, tmp_path)


def test_gate_rejects_a_truncated_profile_field_set(tmp_path):
    value = _write_report(tmp_path)
    value["comparison"]["field_match_rates"].pop("Consequence")

    with pytest.raises(gate.GateError, match="profile-specific CSQ field set"):
        _validate(value, tmp_path)


def test_gate_rejects_a_truncated_equality_field_set(tmp_path):
    value = _write_report(tmp_path)
    value["comparison"]["field_equality_counts"].pop("Consequence")

    with pytest.raises(gate.GateError, match="wrong CSQ field set"):
        _validate(value, tmp_path)


def test_gate_rejects_substituted_benchmark_artifacts(tmp_path):
    value = _write_report(tmp_path)
    substitute = tmp_path / "filtered-input.vcf.gz"
    substitute.write_bytes(b"filtered")
    value["benchmark_artifacts"]["input_vcf"] = gate.vcfio.source_identity(substitute)

    with pytest.raises(gate.GateError, match="canonical full-benchmark artifacts"):
        _validate(value, tmp_path)


def test_gate_rejects_benchmark_artifact_changed_after_report(tmp_path):
    value = _write_report(tmp_path)
    input_path = Path(value["benchmark_artifacts"]["input_vcf"]["path"])
    input_path.write_bytes(b"changed after comparison")

    with pytest.raises(gate.GateError, match="canonical full-benchmark artifacts"):
        _validate(value, tmp_path)


def test_gate_rejects_equality_bucket_inconsistency(tmp_path):
    value = _write_report(tmp_path)
    value["comparison"]["equality_bucket_counts"]["both_empty"] = 3

    with pytest.raises(gate.GateError, match="aggregate both_empty"):
        _validate(value, tmp_path)


def test_gate_rejects_nonempty_zero_row_ledger(tmp_path):
    value = _write_report(tmp_path)
    ledger = Path(value["comparison"]["mismatch_ledger"]["path"])
    ledger.write_text("{}\n")

    with pytest.raises(gate.GateError, match="not empty"):
        _validate(value, tmp_path)


def test_gate_rejects_support_record_from_other_vepyr_release(tmp_path):
    value = _write_report(tmp_path)
    value["supported_target"]["vepyr_version"] = "0.1.1"

    with pytest.raises(gate.GateError, match="belongs to vepyr"):
        _validate(value, tmp_path)


def test_gate_rejects_local_path_dependency(tmp_path):
    value = _write_report(tmp_path)
    build = {
        "vepyr_dirty": False,
        "dependencies": {
            name: {
                "dirty": False,
                "effective_source": (
                    "path"
                    if name == "datafusion-bio-function-vep"
                    else f"git+https://example.invalid/{name}?rev=abc"
                ),
                "manifest_path": f"/local/{name}/Cargo.toml",
            }
            for name in gate.REQUIRED_BUILD_DEPENDENCIES
        },
    }
    value["build"] = build

    with pytest.raises(gate.GateError, match="local path"):
        gate.validate_reports(
            [value],
            report_dir=tmp_path,
            contigs=["chr1"],
            release="116",
            profile="merged",
            suffix="merged",
            expected_package_version="0.2.0",
            build_info=build,
            expected_artifacts=_expected_artifacts(tmp_path),
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("supported_target", {**TARGET, "semantics": "V115"}, "release identity"),
        (
            "reference_identity",
            {**REFERENCE, "ensembl_revision": "other"},
            "ensembl_revision",
        ),
        ("cache_path", "/cache/other", "cache root"),
    ],
)
def test_gate_rejects_cross_contig_identity_or_cache_drift(
    tmp_path, field, value, message
):
    first = _write_report(tmp_path, "chr1")
    second = _write_report(tmp_path, "chr2")
    second[field] = value
    if field == "supported_target":
        second["cache_identity"]["semantics"] = value["semantics"]

    with pytest.raises(gate.GateError, match=message):
        gate.validate_reports(
            [first, second],
            report_dir=tmp_path,
            contigs=["chr1", "chr2"],
            release="116",
            profile="merged",
            suffix="merged",
            expected_package_version="0.2.0",
            build_info=CLEAN_BUILD,
            expected_artifacts=_expected_artifacts(tmp_path),
        )


def test_exact_loader_rejects_missing_release_qualified_report(tmp_path):
    with pytest.raises(gate.GateError, match="missing release-qualified"):
        gate._load_exact_reports(tmp_path, ["chr1"], "merged", "116")


def test_gate_rejects_format_only_differences(tmp_path):
    """Absorbed into the reported match rate, still not release parity."""
    value = _write_report(tmp_path)
    comparison = value["comparison"]
    comparison["field_format_mismatch_counts"]["Allele"] = 1
    comparison["field_equality_counts"]["Allele"]["both_nonempty_equal"] = 7
    comparison["field_equality_counts"]["Allele"]["both_nonempty_unequal"] = 1
    comparison["equality_bucket_counts"]["both_nonempty_equal"] -= 1
    comparison["equality_bucket_counts"]["both_nonempty_unequal"] += 1

    with pytest.raises(gate.GateError, match="field_format_mismatch_total=1"):
        _validate(value, tmp_path)


def test_gate_rejects_a_report_without_format_only_counts(tmp_path):
    """A report predating the counter cannot be silently read as zero."""
    value = _write_report(tmp_path)
    value["comparison"].pop("field_format_mismatch_counts")

    with pytest.raises(gate.GateError, match="field_format_mismatch_counts"):
        _validate(value, tmp_path)


def test_gate_rejects_format_only_counts_for_unexpected_fields(tmp_path):
    value = _write_report(tmp_path)
    value["comparison"]["field_format_mismatch_counts"]["CADD_RAW"] = 1

    with pytest.raises(gate.GateError, match="unexpected CSQ fields"):
        _validate(value, tmp_path)


def test_gate_refuses_plugin_profiles():
    """Plugin profiles are comparison scenarios; the gate pins the core contract."""
    with pytest.raises(gate.GateError, match="plugins"):
        gate.expected_csq_fields("merged_plugins")


def test_gate_still_accepts_the_plugin_free_base_profile():
    assert "Consequence" in gate.expected_csq_fields("merged_plugins_base")
