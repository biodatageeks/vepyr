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


def _write_report(report_dir: Path, contig: str = "chr1") -> dict:
    ledger = Path(
        gate.report.mismatch_ledger_path(str(report_dir), contig, "merged", "116")
    )
    ledger.write_bytes(b"")
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
            "field_match_rates": {"Allele": 100.0},
            "field_mismatch_counts": {},
            "field_order_mismatch_counts": {},
            "field_equality_counts": {
                "Allele": {
                    "both_empty": 2,
                    "both_nonempty_equal": 8,
                    "vepyr_empty_only": 0,
                    "vep_empty_only": 0,
                    "both_nonempty_unequal": 0,
                }
            },
            "equality_bucket_counts": {
                "both_empty": 2,
                "both_nonempty_equal": 8,
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
    )


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
        )


def test_exact_loader_rejects_missing_release_qualified_report(tmp_path):
    with pytest.raises(gate.GateError, match="missing release-qualified"):
        gate._load_exact_reports(tmp_path, ["chr1"], "merged", "116")
