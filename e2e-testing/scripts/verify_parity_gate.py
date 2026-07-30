#!/usr/bin/env python3
"""Machine-enforced final parity gate for one release/profile/contig set."""

from __future__ import annotations

import argparse
import hashlib
from importlib.metadata import PackageNotFoundError, version as package_version
import json
import os
from pathlib import Path
import sys
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from comparison import profiles, report, vcfio  # noqa: E402

EQUALITY_BUCKETS = (
    "both_empty",
    "both_nonempty_equal",
    "vepyr_empty_only",
    "vep_empty_only",
    "both_nonempty_unequal",
)
ZERO_KEYS = (
    "variants_only_in_vepyr",
    "variants_only_in_vep",
    "csq_entry_count_mismatch",
    "csq_entries_only_in_vepyr",
    "csq_entries_only_in_vep",
    "csq_order_mismatch",
)
REQUIRED_BUILD_DEPENDENCIES = {
    "datafusion-bio-function-vep",
    "datafusion-bio-format-core",
    "datafusion-bio-format-ensembl-cache",
    "datafusion-bio-format-vcf",
}


class GateError(RuntimeError):
    """A report set is not a complete, exact, release-qualified result."""


def parse_contigs(values: list[str]) -> list[str]:
    if values == ["1-22"]:
        return [f"chr{number}" for number in range(1, 23)]
    contigs: list[str] = []
    for value in values:
        if "-" in value:
            start_text, end_text = value.removeprefix("chr").split("-", 1)
            if not start_text.isdigit() or not end_text.isdigit():
                raise ValueError(f"invalid contig range {value!r}")
            start = int(start_text)
            end = int(end_text)
            if start > end:
                raise ValueError(f"invalid descending contig range {value!r}")
            contigs.extend(f"chr{number}" for number in range(start, end + 1))
        else:
            contigs.append(value if value.startswith("chr") else f"chr{value}")
    if not contigs or len(contigs) != len(set(contigs)):
        raise ValueError("contigs must be non-empty and unique")
    return contigs


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True, choices=profiles.RELEASES)
    parser.add_argument(
        "--profile", default=profiles.DEFAULT_PROFILE, choices=sorted(profiles.PROFILES)
    )
    parser.add_argument(
        "--chroms",
        nargs="+",
        default=["1-22"],
        help="Contigs or numeric ranges (default: 1-22)",
    )
    parser.add_argument(
        "--report-dir",
        default=str(Path(__file__).parents[1] / "reports"),
    )
    args = parser.parse_args(argv)
    try:
        args.chroms = parse_contigs(args.chroms)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def _load_exact_reports(
    report_dir: Path, contigs: list[str], suffix: str, release: str
) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for contig in contigs:
        path = Path(report.report_json_path(report_dir, contig, suffix, release))
        if not path.is_file():
            raise GateError(f"missing release-qualified report: {path}")
        try:
            value = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise GateError(f"cannot read report {path}: {exc}") from exc
        reports.append(value)
    observed = [value.get("chrom") for value in reports]
    if observed != contigs or len(observed) != len(set(observed)):
        raise GateError(
            f"report contigs are missing, duplicated, or out of order: {observed!r}"
        )
    return reports


def _require_nonnegative_int(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise GateError(f"{label} must be a non-negative integer, got {value!r}")
    return value


def _validate_equality_counts(comparison: dict[str, Any], contig: str) -> None:
    per_field = comparison.get("field_equality_counts")
    aggregate = comparison.get("equality_bucket_counts")
    if not isinstance(per_field, dict) or not per_field:
        raise GateError(f"{contig}: missing field_equality_counts")
    if not isinstance(aggregate, dict):
        raise GateError(f"{contig}: missing equality_bucket_counts")

    calculated = {bucket: 0 for bucket in EQUALITY_BUCKETS}
    mismatch_counts = comparison.get("field_mismatch_counts", {})
    order_counts = comparison.get("field_order_mismatch_counts", {})
    for field, counts in per_field.items():
        if not isinstance(counts, dict) or set(counts) != set(EQUALITY_BUCKETS):
            raise GateError(f"{contig}/{field}: incomplete equality buckets")
        values = {
            bucket: _require_nonnegative_int(
                counts[bucket], f"{contig}/{field}/{bucket}"
            )
            for bucket in EQUALITY_BUCKETS
        }
        for bucket, count in values.items():
            calculated[bucket] += count
        strict_unequal = (
            values["vepyr_empty_only"]
            + values["vep_empty_only"]
            + values["both_nonempty_unequal"]
        )
        mismatches = _require_nonnegative_int(
            mismatch_counts.get(field, 0), f"{contig}/{field}/mismatches"
        )
        order_only = _require_nonnegative_int(
            order_counts.get(field, 0), f"{contig}/{field}/order-only"
        )
        if mismatches + order_only != strict_unequal:
            raise GateError(
                f"{contig}/{field}: equality buckets imply {strict_unequal} unequal "
                f"values but mismatch + order-only counts are {mismatches + order_only}"
            )

    for bucket in EQUALITY_BUCKETS:
        observed = _require_nonnegative_int(
            aggregate.get(bucket), f"{contig}/aggregate/{bucket}"
        )
        if observed != calculated[bucket]:
            raise GateError(
                f"{contig}: aggregate {bucket}={observed}, expected "
                f"{calculated[bucket]} from per-field counts"
            )


def _validate_empty_ledger(
    comparison: dict[str, Any],
    report_dir: Path,
    contig: str,
    suffix: str,
    release: str,
) -> None:
    ledger = comparison.get("mismatch_ledger")
    if not isinstance(ledger, dict):
        raise GateError(f"{contig}: missing mismatch_ledger")
    rows = _require_nonnegative_int(ledger.get("rows"), f"{contig}/ledger rows")
    if rows != 0:
        raise GateError(f"{contig}: mismatch ledger contains {rows} row(s)")
    expected = Path(
        report.mismatch_ledger_path(report_dir, contig, suffix, release)
    ).resolve()
    reported = ledger.get("path")
    if not isinstance(reported, str) or Path(reported).resolve() != expected:
        raise GateError(
            f"{contig}: ledger path {reported!r} does not match expected {expected}"
        )
    if not expected.is_file():
        raise GateError(f"{contig}: mismatch ledger file is missing: {expected}")
    payload = expected.read_bytes()
    if payload:
        raise GateError(f"{contig}: zero-row mismatch ledger is not empty")
    digest = hashlib.sha256(payload).hexdigest()
    if ledger.get("sha256") != digest:
        raise GateError(f"{contig}: mismatch ledger SHA-256 does not match file")


def _validate_identity(
    value: dict[str, Any],
    release: str,
    profile: str,
    expected_package_version: str,
    contig: str,
) -> None:
    target = value.get("supported_target")
    cache = value.get("cache_identity")
    reference = value.get("reference_identity")
    if not isinstance(target, dict):
        raise GateError(f"{contig}: missing native supported_target")
    if target.get("cache_version") != release:
        raise GateError(
            f"{contig}: target cache release {target.get('cache_version')!r}, "
            f"expected {release!r}"
        )
    if target.get("vepyr_version") != expected_package_version:
        raise GateError(
            f"{contig}: support record belongs to vepyr "
            f"{target.get('vepyr_version')!r}, running {expected_package_version!r}"
        )
    if not isinstance(cache, dict):
        raise GateError(f"{contig}: missing validated cache identity")
    if cache.get("contig") != contig:
        raise GateError(
            f"{contig}: cache identity was validated for {cache.get('contig')!r}"
        )
    if cache.get("cache_source_type") != profiles.PROFILES[profile].flavour:
        raise GateError(
            f"{contig}: cache source {cache.get('cache_source_type')!r} does not "
            f"match profile flavour {profiles.PROFILES[profile].flavour!r}"
        )
    for field in (
        "vepyr_version",
        "cache_version",
        "vep_codebase_version",
        "api_version",
        "ensembl_core_revision",
        "ensembl_variation_revision",
        "semantics",
    ):
        if cache.get(field) != target.get(field):
            raise GateError(
                f"{contig}: cache identity {field}={cache.get(field)!r}, "
                f"target={target.get(field)!r}"
            )
    if not isinstance(reference, dict):
        raise GateError(f"{contig}: missing reference_identity")
    try:
        vcfio.validate_vep_reference_identity(reference, target)
    except ValueError as exc:
        raise GateError(f"{contig}: {exc}") from exc


def _validate_release_dependencies(build_info: dict[str, Any]) -> None:
    if build_info.get("vepyr_dirty") is not False:
        raise GateError("vepyr worktree cleanliness is unknown or dirty")
    dependencies = build_info.get("dependencies")
    if not isinstance(dependencies, dict) or not dependencies:
        raise GateError("resolved dependency provenance is unavailable")
    missing = REQUIRED_BUILD_DEPENDENCIES - set(dependencies)
    if missing:
        raise GateError(
            "resolved dependency provenance is missing " + ", ".join(sorted(missing))
        )
    for name, dependency in dependencies.items():
        if dependency.get("dirty"):
            raise GateError(f"dependency {name} worktree is dirty")
        source = dependency.get("effective_source")
        if source in (None, "path"):
            raise GateError(f"dependency {name} resolves from a local path")


def validate_reports(
    reports: list[dict[str, Any]],
    *,
    report_dir: str | Path,
    contigs: list[str],
    release: str,
    profile: str,
    suffix: str,
    expected_package_version: str,
    build_info: dict[str, Any],
) -> dict[str, int]:
    if len(reports) != len(contigs):
        raise GateError(f"expected {len(contigs)} reports, got {len(reports)}")
    _validate_release_dependencies(build_info)

    totals = {key: 0 for key in ZERO_KEYS}
    totals["field_mismatch_total"] = 0
    totals["field_order_mismatch_total"] = 0
    totals["mismatch_ledger_rows"] = 0
    common_target: dict[str, Any] | None = None
    common_reference: dict[str, Any] | None = None
    cache_root: str | None = None
    directory = Path(report_dir).resolve()
    for expected_contig, value in zip(contigs, reports):
        if value.get("chrom") != expected_contig:
            raise GateError(
                f"expected {expected_contig}, report declares {value.get('chrom')!r}"
            )
        if value.get("release") != release or value.get("profile") != profile:
            raise GateError(
                f"{expected_contig}: wrong release/profile "
                f"{value.get('release')!r}/{value.get('profile')!r}"
            )
        if value.get("build") != build_info:
            raise GateError(
                f"{expected_contig}: report build provenance differs from the "
                "running release candidate"
            )
        comparison = value.get("comparison")
        if not isinstance(comparison, dict):
            raise GateError(f"{expected_contig}: comparison is missing or null")
        for key in ZERO_KEYS:
            count = _require_nonnegative_int(
                comparison.get(key), f"{expected_contig}/{key}"
            )
            totals[key] += count
        if comparison.get("fields_only_in_vepyr") not in ([], None):
            raise GateError(f"{expected_contig}: CSQ fields exist only in vepyr")
        if comparison.get("fields_only_in_vep") not in ([], None):
            raise GateError(f"{expected_contig}: CSQ fields exist only in VEP")
        field_mismatches = comparison.get("field_mismatch_counts")
        if not isinstance(field_mismatches, dict):
            raise GateError(f"{expected_contig}: missing field_mismatch_counts")
        totals["field_mismatch_total"] += sum(
            _require_nonnegative_int(count, f"{expected_contig}/{field}")
            for field, count in field_mismatches.items()
        )
        field_order = comparison.get("field_order_mismatch_counts")
        if not isinstance(field_order, dict):
            raise GateError(f"{expected_contig}: missing field_order_mismatch_counts")
        totals["field_order_mismatch_total"] += sum(
            _require_nonnegative_int(count, f"{expected_contig}/{field}/order-only")
            for field, count in field_order.items()
        )
        annotation = value.get("annotation")
        input_variants = _require_nonnegative_int(
            value.get("input_variants"), f"{expected_contig}/input_variants"
        )
        if not isinstance(annotation, dict):
            raise GateError(f"{expected_contig}: missing annotation result")
        output_variants = _require_nonnegative_int(
            annotation.get("output_variants"),
            f"{expected_contig}/annotation/output_variants",
        )
        variants_compared = _require_nonnegative_int(
            comparison.get("variants_compared"),
            f"{expected_contig}/variants_compared",
        )
        if input_variants != output_variants or input_variants != variants_compared:
            raise GateError(
                f"{expected_contig}: input/output/compared variants differ "
                f"({input_variants}/{output_variants}/{variants_compared})"
            )
        rates = comparison.get("field_match_rates")
        if not isinstance(rates, dict) or not rates:
            raise GateError(f"{expected_contig}: missing field_match_rates")
        if any(rate != 100.0 for rate in rates.values()):
            raise GateError(f"{expected_contig}: at least one field is below 100%")
        _validate_equality_counts(comparison, expected_contig)
        _validate_empty_ledger(comparison, directory, expected_contig, suffix, release)
        totals["mismatch_ledger_rows"] += comparison["mismatch_ledger"]["rows"]
        _validate_identity(
            value, release, profile, expected_package_version, expected_contig
        )

        target = value["supported_target"]
        reference = {
            key: item
            for key, item in value["reference_identity"].items()
            if key not in {"header", "cache_path"}
        }
        path = value.get("cache_path")
        if common_target is None:
            common_target = target
            common_reference = reference
            cache_root = path
        elif (
            target != common_target
            or reference != common_reference
            or path != cache_root
        ):
            raise GateError(
                f"{expected_contig}: release identity or cache root differs across reports"
            )

    nonzero = {key: count for key, count in totals.items() if count}
    if nonzero:
        detail = ", ".join(f"{key}={count}" for key, count in nonzero.items())
        raise GateError(f"parity gate failed: {detail}")
    return totals


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    profile = profiles.PROFILES[args.profile]
    report_dir = Path(args.report_dir).expanduser().resolve()
    try:
        reports = _load_exact_reports(
            report_dir, args.chroms, profile.suffix, args.release
        )
        try:
            running_version = package_version("vepyr")
        except PackageNotFoundError as exc:
            raise GateError("cannot resolve running vepyr package version") from exc
        totals = validate_reports(
            reports,
            report_dir=report_dir,
            contigs=args.chroms,
            release=args.release,
            profile=args.profile,
            suffix=profile.suffix,
            expected_package_version=running_version,
            build_info=report.get_build_info(),
        )
    except GateError as exc:
        print(f"PARITY GATE FAILED: {exc}", file=sys.stderr)
        return 1

    print(
        f"PARITY GATE PASSED: vepyr {running_version}, VEP/cache {args.release}, "
        f"profile {args.profile}, {len(args.chroms)} contigs"
    )
    for key, count in totals.items():
        print(f"  {key}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
