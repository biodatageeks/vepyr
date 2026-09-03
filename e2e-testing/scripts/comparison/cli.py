"""Argument parsing and orchestration for the parity comparison runner."""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime

from . import annotate, compare, profiles, report, vcfio

BACKEND = "parquet"

DESCRIPTION = """Compare vepyr annotation against an Ensembl VEP reference.

Examples:
    run_comparison.py --release 115                        # all detected contigs
    run_comparison.py --release 115 --chroms 22            # one contig
    run_comparison.py --release 116 --profile merged --chroms 1 2 22
    run_comparison.py --release 115 --bgzf --workers 4 --force
"""


def _normalise_chrom(value):
    return vcfio.canonical_contig(value)


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="run_comparison.py",
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # Without this, argparse prefix-matches the dropped --cache alias onto
        # --cache-dir and silently sets the cache path to the profile name.
        allow_abbrev=False,
    )
    p.add_argument(
        "--release",
        required=True,
        choices=profiles.RELEASES,
        help="Ensembl release; selects both the Parquet cache and the VEP reference",
    )
    p.add_argument(
        "--profile",
        choices=sorted(profiles.PROFILES),
        default=profiles.DEFAULT_PROFILE,
        help="Annotation scenario (default: %(default)s)",
    )
    p.add_argument(
        "--chroms",
        nargs="+",
        default=None,
        help="Contigs to process, e.g. '22', 'chr1 chr2', or 'all'. "
        "Default: detect from the VEP reference index",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Recreate cached input/reference contig slices; annotation outputs "
        "are always regenerated",
    )
    p.add_argument(
        "--bgzf",
        action="store_true",
        help="Write block-gzipped (.vcf.gz) annotated output and validate it",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Within-contig parallel annotation pipelines (default: %(default)s)",
    )
    p.add_argument(
        "--isolate",
        action="store_true",
        help="Run each contig in its own subprocess so a native crash loses "
        "only that contig",
    )
    p.add_argument(
        "--skip-annotate",
        action="store_true",
        help="Skip annotation and comparison; only regenerate the summary from "
        "existing JSONs",
    )
    p.add_argument(
        "--skip-compare",
        "--skip-comparison",
        dest="skip_compare",
        action="store_true",
        help="Annotate only: no comparison, and no reference slice. To "
        "compare, drop this and use --comparison-mode.",
    )
    p.add_argument(
        "--comparison-mode",
        choices=("field", "md5"),
        default=None,
        dest="comparison_mode",
        help="How to compare vepyr's output against the VEP reference "
        "(default: field). Both modes annotate and cut the reference slice if "
        "it is missing; only the check differs. 'field' compares the CSQ "
        "fields per record and writes the aggregate summary. 'md5' hashes the "
        "files and reports byte concordance per contig. Use --skip-compare to "
        "annotate with no comparison at all.",
    )
    p.add_argument(
        "--md5-mode",
        choices=("strict", "canonical", "both"),
        default="both",
        dest="md5_mode",
        help="Digest mode for --comparison-mode md5 (default: both). "
        "'strict' hashes record bytes as-is -- the parity target. 'canonical' "
        "normalises cosmetic serialization first. 'both' is the default "
        "because hashing twice is cheap and the pair is diagnostic: a strict "
        "failure with a canonical pass isolates serialization drift from a "
        "content change, which one digest alone cannot tell you.",
    )
    p.add_argument(
        "--no-normalize",
        action="store_true",
        help="Skip bcftools norm (normalization is on by default)",
    )
    p.add_argument("--vcf", default=None, help="Input VCF (default: $DATA/input/...)")
    p.add_argument(
        "--fasta", default=None, help="Reference FASTA (default: $DATA/input/...)"
    )
    p.add_argument(
        "--vep",
        default=None,
        help="VEP reference VCF (default: from profile x release)",
    )
    p.add_argument(
        "--cache-dir",
        default=None,
        help="Parquet cache (default: from profile x release)",
    )
    p.add_argument(
        "--plugin-cache",
        default=None,
        help="Plugin cache root, used only by plugin profiles "
        "(default: $DATA/cache/plugin_cache_<release>)",
    )

    args = p.parse_args(argv)
    if args.workers <= 0:
        p.error("--workers must be a positive integer")
    if args.chroms is not None:
        if len(args.chroms) == 1 and args.chroms[0].lower() == "all":
            args.chroms = None
        else:
            args.chroms = [_normalise_chrom(c) for c in args.chroms]
            if len(args.chroms) != len(set(args.chroms)):
                p.error(
                    "--chroms must not contain duplicate contigs after "
                    "chr-prefix normalization"
                )
    if args.vcf is None:
        args.vcf = profiles.default_input(profiles.DEFAULT_VCF_NAME)
    if args.fasta is None:
        args.fasta = profiles.default_input(profiles.DEFAULT_FASTA_NAME)
    return args


def results_root(e2e_dir, release):
    """Every intermediate for a release lives under one directory."""
    return os.path.join(e2e_dir, "results", release)


def select_supported_target(release, targets):
    """Select one exact native support record for a CLI cache release."""
    matches = [target for target in targets if target.get("cache_version") == release]
    if len(matches) != 1:
        supported = sorted(
            {
                target.get("cache_version")
                for target in targets
                if target.get("cache_version")
            }
        )
        raise ValueError(
            f"release {release!r} is not uniquely supported by this vepyr build; "
            f"supported cache versions: {', '.join(supported) or '(none)'}"
        )
    return dict(matches[0])


def benchmark_artifact_identities(args, resolved):
    """Bind one parity run to its original input, FASTA, and VEP artifacts."""
    paths = {
        "input_vcf": args.vcf,
        "reference_fasta": args.fasta,
        "vep_reference_vcf": resolved.vep_vcf,
    }
    missing = [name for name, path in paths.items() if path is None]
    if missing:
        raise ValueError(
            "cannot identify required benchmark artifact(s): " + ", ".join(missing)
        )
    return {name: vcfio.source_identity(path) for name, path in paths.items()}


def resolve_contigs(args, resolved, input_vcf):
    """Contigs to process: the reference index intersected with the input index.

    Reads indexes, never ##contig headers -- the headers on the real references
    list 195 contigs while only 22 carry records.
    """
    input_contigs = vcfio.detect_contigs(input_vcf)
    canonical_input = [_normalise_chrom(contig) for contig in input_contigs]

    if args.skip_compare:
        detected = canonical_input
    else:
        ref_contigs = vcfio.detect_contigs(resolved.vep_vcf)
        if not ref_contigs:
            print(
                f"  Note: {os.path.basename(resolved.vep_vcf)} has no tabix index; "
                "contig detection degraded to the input VCF",
                file=sys.stderr,
            )
            detected = canonical_input
        elif input_contigs:
            allowed = set(canonical_input)
            detected = [
                canonical
                for contig in ref_contigs
                if (canonical := _normalise_chrom(contig)) in allowed
            ]
        else:
            detected = [_normalise_chrom(contig) for contig in ref_contigs]
        detected = list(dict.fromkeys(detected))

    if args.chroms is None:
        if not detected:
            raise SystemExit(
                "Error: could not detect any contigs. Pass --chroms explicitly."
            )
        return detected

    if detected:
        missing = [c for c in args.chroms if c not in set(detected)]
        if missing:
            raise SystemExit(
                f"Error: requested contig(s) {', '.join(missing)} not present. "
                f"Available: {', '.join(detected)}"
            )
    return args.chroms


def md5_summary(report_dir, chroms, suffix, release, requested):
    """Print the per-contig md5 verdicts and return the contigs that differ.

    Read back from the per-contig report JSONs rather than kept in memory, so
    the summary is identical whether the contigs ran in-process or under
    --isolate (where each one is a separate subprocess).
    """
    modes = ("strict", "canonical") if requested == "both" else (requested,)
    rows = []
    missing = []
    for chrom in chroms:
        path = report.report_json_path(report_dir, chrom, suffix, release)
        try:
            with open(path) as f:
                payload = json.load(f).get("md5")
        except (OSError, ValueError):
            payload = None
        if not payload:
            missing.append(chrom)
            continue
        rows.append((chrom, payload))

    print(f"\n{'=' * 60}")
    print(f"  MD5 concordance vs the VEP reference ({', '.join(modes)})")
    print(f"{'=' * 60}")
    header = f"  {'CONTIG':<8} {'RECORDS':>12}  " + "  ".join(
        f"{m.upper():<9}" for m in modes
    )
    print(header)
    print("  " + "-" * (len(header) - 2))
    differing = []
    for chrom, payload in rows:
        first = payload.get(modes[0], {})
        cells = []
        for mode in modes:
            entry = payload.get(mode)
            if entry is None:
                cells.append(f"{'-':<9}")
                continue
            cells.append(f"{'ok' if entry['body_match'] else 'DIFFER':<9}")
            if not entry["body_match"]:
                differing.append(chrom)
        records = first.get("vep_records")
        shown = f"{records:,}" if isinstance(records, int) else "?"
        print(f"  {chrom:<8} {shown:>12}  " + "  ".join(cells))

    differing = sorted(set(differing))
    total = sum(
        payload.get(modes[0], {}).get("vep_records") or 0 for _, payload in rows
    )
    if missing:
        print(f"  no md5 evidence for: {', '.join(missing)}")
    if differing:
        print(f"\n  FAIL: body digest differs on {', '.join(differing)}")
    else:
        print(f"\n  PASS: {len(rows)} contig(s) concord ({total:,} records)")
    print("=" * 60)
    return differing + missing


def md5_for_contig(chrom, vep_slice, vepyr_vcf, requested):
    """Hash one contig's vepyr output against its VEP reference slice.

    Delegates to `md5_concordance` rather than re-implementing the digest, so
    the verdict here and the verdict from `md5_concordance.py --results-dir`
    cannot drift. Returns `{mode: {...}}`, printing one line per mode.
    """
    import md5_concordance

    modes = ("strict", "canonical") if requested == "both" else (requested,)
    pair = md5_concordance.Pair(label=chrom, vep=vep_slice, vepyr=vepyr_vcf)
    out = {}
    for mode in modes:
        outcome = md5_concordance.compare(pair, mode)
        out[mode] = {
            "body_match": outcome.body_match,
            "header_match": outcome.header_match,
            "count_match": outcome.count_match,
            "vep_body_md5": outcome.vep.body,
            "vepyr_body_md5": outcome.vepyr.body,
            "vep_records": outcome.vep.records,
            "vepyr_records": outcome.vepyr.records,
            "notes": list(outcome.notes),
        }
        verdict = "MATCH" if outcome.body_match else "DIFFER"
        counts = (
            f"{outcome.vep.records:,}"
            if outcome.count_match
            else f"{outcome.vep.records:,} vs {outcome.vepyr.records:,}"
        )
        print(f"  md5 {mode}: body {verdict} ({counts} records)")
        if not outcome.body_match:
            for note in outcome.notes:
                print(f"    - {note}")
    return out


def run_contig(
    chrom,
    args,
    resolved,
    input_vcf,
    results_dir,
    report_dir,
    selected_target,
    reference_identity,
    build_info,
    benchmark_artifacts,
):
    """Annotate and compare a single contig, returning its report dict."""
    work_dir = os.path.join(results_dir, f"fast_{chrom}")
    os.makedirs(work_dir, exist_ok=True)

    print(f"\n{'=' * 60}")
    print(f"  {chrom} (profile={resolved.profile}, release={resolved.release})")
    print(f"{'=' * 60}")

    chrom_vcf_gz = vcfio.slice_contig(
        input_vcf,
        chrom,
        work_dir,
        force=args.force,
    )
    n_variants = vcfio.count_data_lines(chrom_vcf_gz)
    print(f"  Input: {n_variants:,} variants for {chrom}")

    cache_identity = annotate.cache_contig_identity(
        resolved.cache_dir,
        chrom,
        selected_target["cache_version"],
    )

    ext = ".vcf.gz" if args.bgzf else ".vcf"
    output_vcf = os.path.join(
        work_dir, f"vepyr_{BACKEND}_{chrom}_{resolved.suffix}{ext}"
    )
    elapsed, n_out = annotate.annotate_contig(
        chrom_vcf_gz,
        resolved.cache_dir,
        args.fasta,
        output_vcf,
        workers=args.workers,
        annotate_kwargs=resolved.annotate_kwargs,
        force=args.force,
        bgzf=args.bgzf,
    )

    # The reference slice feeds the field comparison and the md5 check alike, so
    # it must be cut whenever either is wanted -- `--skip-compare --md5` used to
    # leave md5 with nothing to compare against.
    vep_slice = None
    if not args.skip_compare:
        vep_slice = vcfio.slice_vep(
            resolved.vep_vcf, chrom, work_dir, resolved.suffix, force=args.force
        )

    comparison = None
    if not args.skip_compare and args.comparison_mode == "field":
        comparison = compare.compare_vcfs(
            output_vcf,
            vep_slice,
            chrom,
            ignore_csq_order=resolved.ignore_csq_order,
            backend=BACKEND,
            mismatch_ledger_path=report.mismatch_ledger_path(
                report_dir,
                chrom,
                resolved.suffix,
                resolved.release,
            ),
        )

    md5_result = None
    if not args.skip_compare and args.comparison_mode == "md5":
        md5_result = md5_for_contig(chrom, vep_slice, output_vcf, args.md5_mode)

    result = {
        "chrom": chrom,
        "profile": resolved.profile,
        "release": resolved.release,
        "cache": resolved.profile,
        "cache_path": resolved.cache_dir,
        "cache_identity": cache_identity,
        "supported_target": selected_target,
        "build": build_info,
        "benchmark_artifacts": benchmark_artifacts,
        "input_variants": n_variants,
        "annotation": {
            "backend": BACKEND,
            "compression": "bgzf" if args.bgzf else "plain",
            "time_s": round(elapsed, 1) if elapsed else None,
            "output_variants": n_out,
        },
        "comparison": comparison,
    }
    if md5_result is not None:
        result["md5"] = md5_result
    if reference_identity is not None:
        result["reference_identity"] = reference_identity
    # Only for plugin profiles, so a non-plugin report keeps its exact shape.
    if resolved.plugin_cache_root is not None:
        result["plugin_cache_path"] = resolved.plugin_cache_root

    path = report.report_json_path(report_dir, chrom, resolved.suffix, resolved.release)
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"  Report: {path}")
    return result


def _run_contig_isolated(chrom, args):
    """Re-invoke this script for one contig so a SIGSEGV loses only that contig."""
    entry = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "run_comparison.py",
    )
    cmd = [
        sys.executable,
        entry,
        "--release",
        args.release,
        "--profile",
        args.profile,
        "--chroms",
        chrom,
        "--workers",
        str(args.workers),
        "--vcf",
        args.vcf,
        "--fasta",
        args.fasta,
    ]
    if args.force:
        cmd.append("--force")
    if args.bgzf:
        cmd.append("--bgzf")
    if args.skip_compare:
        cmd.append("--skip-compare")
    if args.comparison_mode:
        cmd += ["--comparison-mode", args.comparison_mode]
    if args.md5_mode:
        cmd += ["--md5-mode", args.md5_mode]
    if args.no_normalize:
        cmd.append("--no-normalize")
    if args.vep:
        cmd += ["--vep", args.vep]
    if args.cache_dir:
        cmd += ["--cache-dir", args.cache_dir]
    if args.plugin_cache:
        cmd += ["--plugin-cache", args.plugin_cache]
    return subprocess.run(cmd).returncode == 0


def main(argv=None):
    args = parse_args(argv)

    # --skip-compare means "no comparison at all"; --comparison-mode selects
    # which comparison to run. Asking for both is contradictory, so say so
    # rather than silently honouring one of them.
    if args.skip_compare and args.comparison_mode is not None:
        print(
            "Error: --skip-compare runs no comparison, so it cannot be "
            f"combined with --comparison-mode {args.comparison_mode}. "
            "Drop one.",
            file=sys.stderr,
        )
        return 2
    args.comparison_mode = args.comparison_mode or "field"

    try:
        # Per-contig plugin references resolve one file at a time, so hand the
        # sole requested contig to resolution. Without it the per-contig lookup
        # is unreachable from here and every plugin run is rejected -- including
        # the single-contig form the rejection itself recommends.
        sole_chrom = (
            args.chroms[0]
            if args.chroms is not None and len(args.chroms) == 1
            else None
        )
        resolved = profiles.resolve(
            args.profile,
            args.release,
            cache_dir=args.cache_dir,
            vep_vcf=args.vep,
            plugin_cache_root=args.plugin_cache,
            require_cache=not args.skip_annotate,
            require_reference=not args.skip_compare and not args.skip_annotate,
            chrom=sole_chrom,
        )
    except profiles.ProfileUnavailable as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    try:
        selected_target = None
        reference_identity = None
        benchmark_artifacts = None
        if not args.skip_annotate:
            selected_target = select_supported_target(
                args.release,
                annotate.supported_vep_targets(),
            )
            if not args.skip_compare:
                reference_identity = vcfio.parse_vep_header(resolved.vep_vcf)
                vcfio.validate_vep_reference_identity(
                    reference_identity,
                    selected_target,
                )
                benchmark_artifacts = benchmark_artifact_identities(args, resolved)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"Error: release identity validation failed: {exc}", file=sys.stderr)
        return 2

    if selected_target is not None:
        resolved.annotate_kwargs["expected_cache_version"] = selected_target[
            "cache_version"
        ]

    # .../e2e-testing/scripts/comparison/cli.py -> .../e2e-testing
    e2e_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    results_dir = results_root(e2e_dir, args.release)
    report_dir = os.path.join(e2e_dir, "reports")
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(report_dir, exist_ok=True)

    print("=" * 60)
    print(f"  profile:   {resolved.profile}")
    print(f"  release:   {resolved.release}")
    print(f"  cache_dir: {resolved.cache_dir}")
    if resolved.plugin_cache_root:
        print(f"  plugins:   {resolved.plugin_cache_root}")
    print(f"  vep_vcf:   {resolved.vep_vcf}")
    print(f"  results:   {results_dir}")
    print("=" * 60)

    input_vcf = args.vcf
    if not args.skip_annotate:
        shared = os.path.join(results_dir, "_shared")
        if args.no_normalize:
            input_vcf = vcfio.ensure_bgzf(args.vcf, shared)
        else:
            input_vcf = vcfio.normalize_vcf(args.vcf, shared)

    if args.skip_annotate and args.chroms is None:
        chroms = report.discover_report_contigs(
            report_dir,
            resolved.suffix,
            resolved.release,
        )
        if not chroms:
            print(
                "Error: no release-qualified reports found; pass --chroms "
                "to select legacy reports explicitly",
                file=sys.stderr,
            )
            return 1
    elif args.skip_annotate:
        chroms = args.chroms
    else:
        chroms = resolve_contigs(args, resolved, input_vcf)
    print(f"  contigs:   {', '.join(chroms)}")
    build_info = None if args.skip_annotate else report.get_build_info()

    failures = []
    if not args.skip_annotate:
        for chrom in chroms:
            try:
                report.quarantine_contig_evidence(
                    report_dir,
                    chrom,
                    resolved.suffix,
                    resolved.release,
                )
                if args.isolate:
                    if not _run_contig_isolated(chrom, args):
                        report.quarantine_contig_evidence(
                            report_dir,
                            chrom,
                            resolved.suffix,
                            resolved.release,
                        )
                        failures.append(chrom)
                else:
                    run_contig(
                        chrom,
                        args,
                        resolved,
                        input_vcf,
                        results_dir,
                        report_dir,
                        selected_target,
                        reference_identity,
                        build_info,
                        benchmark_artifacts,
                    )
            except Exception as exc:  # noqa: BLE001 - one contig must not kill the sweep
                report.quarantine_contig_evidence(
                    report_dir,
                    chrom,
                    resolved.suffix,
                    resolved.release,
                )
                print(f"  ERROR: {chrom} failed: {exc}", file=sys.stderr)
                failures.append(chrom)

    md5_failures = []
    if args.comparison_mode == "md5" and not args.skip_annotate:
        md5_failures = md5_summary(
            report_dir,
            [chrom for chrom in chroms if chrom not in failures],
            resolved.suffix,
            resolved.release,
            args.md5_mode,
        )

    if args.skip_compare:
        print("\nSkipping aggregate summary (--skip-compare)")
        return 1 if failures else 0

    if args.comparison_mode == "md5":
        # The field-level aggregate summarises per-field mismatch counts, which
        # md5 mode never produces. The md5 table above is this mode's summary.
        if failures:
            print(f"  FAILED contigs: {', '.join(failures)}")
        return 1 if failures or md5_failures else 0

    successful_chroms = [chrom for chrom in chroms if chrom not in failures]
    if not args.skip_annotate:
        missing_evidence = [
            chrom
            for chrom in successful_chroms
            if not os.path.isfile(
                report.report_json_path(
                    report_dir,
                    chrom,
                    resolved.suffix,
                    resolved.release,
                )
            )
        ]
        if missing_evidence:
            print(
                "  ERROR: fresh report evidence is missing for "
                + ", ".join(missing_evidence),
                file=sys.stderr,
            )
            failures.extend(missing_evidence)
            successful_chroms = [
                chrom for chrom in successful_chroms if chrom not in missing_evidence
            ]
    try:
        reports = report.load_reports(
            report_dir,
            successful_chroms,
            resolved.suffix,
            resolved.release,
        )
        if not reports:
            print("No reports found.", file=sys.stderr)
            return 1
        report_build_info = report.common_build_info(reports)
    except ValueError as exc:
        print(f"Error: cannot summarize reports: {exc}", file=sys.stderr)
        return 2
    if build_info is None:
        build_info = report_build_info
    elif report_build_info != build_info:
        print(
            "Error: cannot summarize reports: report build provenance differs "
            "from the running checkout",
            file=sys.stderr,
        )
        return 2

    agg = report.aggregate_mismatches(reports)
    csq_classes = report.classify_consequence_mismatches(
        agg["field_examples"].get("Consequence", [])
    )
    old_mm = report.load_old_benchmark(report_dir, BACKEND)
    md = report.generate_markdown(
        reports,
        agg,
        csq_classes,
        old_mm,
        build_info,
        release=resolved.release,
        profile=resolved.profile,
        backend=BACKEND,
    )
    span = report.contig_span([r["chrom"] for r in reports])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    summary_path = os.path.join(
        report_dir,
        f"fast_{span}_{resolved.suffix}_{resolved.release}_summary_{timestamp}.md",
    )
    with open(summary_path, "w") as f:
        f.write(md)

    n_perfect = len([f for f in agg["all_fields"] if agg["field_mm"].get(f, 0) == 0])
    print(f"\n{'=' * 60}")
    print(f"  Summary: {summary_path}")
    print(f"  Fields at 100%: {n_perfect}/{len(agg['all_fields'])}")
    print(f"  Total mismatches: {sum(agg['field_mm'].values()):,}")
    if failures:
        print(f"  FAILED contigs: {', '.join(failures)}")
    if md5_failures:
        print(f"  MD5 mismatched contigs: {', '.join(md5_failures)}")
    print("=" * 60)
    return 1 if failures or md5_failures else 0
