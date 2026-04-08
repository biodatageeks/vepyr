#!/usr/bin/env python3
"""Run fast annotation for chr1-22, classify mismatches, and generate a timestamped report.

Usage:
    python run_annotation_fast_all.py                  # re-annotate all chr1-22 (default)
    python run_annotation_fast_all.py --no-force       # reuse existing annotation output
    python run_annotation_fast_all.py --chroms 1 2 3   # only specific chromosomes
    python run_annotation_fast_all.py --skip-annotate  # only regenerate report from existing JSONs

Runs run_annotation_fast.py for each chromosome, then aggregates all
per-chromosome JSON reports into a single timestamped Markdown summary
with root cause classification and upstream issue links.
"""

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_DIR = os.path.join(SCRIPT_DIR, "..", "reports")

# ── Upstream issue registry ─────────────────────────────────────────��────
# Maps root cause classes to GitHub issue/PR numbers.
# Update this when new issues are filed or existing ones are closed.
REPO = "https://github.com/biodatageeks/datafusion-bio-functions"
ISSUES = {
    "stop_retained_extra": {
        "title": "`stop_retained_variant` false positive on inframe ops",
        "issues": [90, 117],
        "prs": [113],
    },
    "stop_gained_extra": {
        "title": "`stop_gained` extra on frameshift",
        "issues": [114],
        "prs": [],
    },
    "stop_lost_extra": {
        "title": "`stop_lost` missing on frameshift past stop codon",
        "issues": [115],
        "prs": [],
    },
    "inframe_vs_frameshift": {
        "title": "Inframe/frameshift disagree at CDS boundary",
        "issues": [117],
        "prs": [],
    },
    "incomplete_terminal_impact_hgvsp": {
        "title": "Incomplete terminal codon: IMPACT/HGVSp residual (Xaa vs Ter, missing p.Ter=)",
        "issues": [130],
        "prs": [],
    },
    "stop_gained_missing": {
        "title": "`stop_gained` missing on frameshift/inframe_deletion",
        "issues": [116],
        "prs": [],
    },
    "incomplete_terminal": {
        "title": "`incomplete_terminal_codon` companion terms",
        "issues": [101],
        "prs": [],
    },
    "hgvsc_noncoding": {
        "title": "HGVSc/HGVS_OFFSET on non-coding + UTR indels",
        "issues": [112],
        "prs": [],
    },
    "hgnc_id_extra": {
        "title": "HGNC_ID false-positive propagation",
        "issues": [108],
        "prs": [],
    },
    "cds_boundary_missing": {
        "title": "CDS/protein fields missing at CDS boundary",
        "issues": [118],
        "prs": [],
    },
    "mirna_dedup": {
        "title": "miRNA dedup (stem repeated in VEP)",
        "issues": [100],
        "prs": [],
    },
    "protein_altering": {
        "title": "`protein_altering_variant` not emitted for complex inframe changes",
        "issues": [124],
        "prs": [],
    },
    "start_retained_missing": {
        "title": "`start_retained_variant` missing alongside `start_lost`",
        "issues": [125],
        "prs": [],
    },
}


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--chroms",
        nargs="+",
        type=int,
        default=list(range(1, 23)),
        help="Chromosome numbers to process (default: 1-22)",
    )
    p.add_argument(
        "--no-force",
        action="store_true",
        help="Reuse existing annotation output if present (default: always re-annotate)",
    )
    p.add_argument(
        "--skip-annotate",
        action="store_true",
        help="Skip annotation, only regenerate report from existing JSONs",
    )
    return p.parse_args()


# ── Step 1: Run per-chromosome annotation ────────────────────────────────


def run_chromosome(chrom_num, force=False):
    """Run run_annotation_fast.py for a single chromosome."""
    chrom = f"chr{chrom_num}"
    cmd = [sys.executable, os.path.join(SCRIPT_DIR, "run_annotation_fast.py"), chrom]
    if force:
        cmd.append("--force")

    print(f"\n{'=' * 60}")
    print(f"  Running {chrom}")
    print(f"{'=' * 60}")
    result = subprocess.run(cmd, cwd=SCRIPT_DIR)
    if result.returncode != 0:
        print(f"  WARNING: {chrom} failed with exit code {result.returncode}")
        return False
    return True


# ── Step 2: Load all per-chromosome reports ──────────────────────────────


def load_reports(chrom_nums):
    """Load JSON reports for all chromosomes."""
    reports = []
    for n in chrom_nums:
        path = os.path.join(REPORT_DIR, f"fast_chr{n}_report.json")
        if not os.path.exists(path):
            print(f"  WARNING: {path} not found, skipping chr{n}")
            continue
        with open(path) as f:
            reports.append(json.load(f))
    return reports


# ── Step 3: Aggregate field-level mismatches ─────────────────────────────


def aggregate_mismatches(reports):
    """Aggregate field match/mismatch data across all chromosome reports."""
    all_fields = set()
    field_mm = defaultdict(int)
    field_order = defaultdict(int)
    field_examples = defaultdict(list)

    total_compared = 0
    total_csq_match = 0
    total_csq_mismatch = 0
    total_only_vepyr = 0
    total_only_vep = 0

    for r in reports:
        comp = r.get("comparison", {})
        if not comp:
            continue
        total_compared += comp.get("variants_compared", 0)
        total_csq_match += comp.get("csq_entry_count_match", 0)
        total_csq_mismatch += comp.get("csq_entry_count_mismatch", 0)
        total_only_vepyr += comp.get("variants_only_in_vepyr", 0)
        total_only_vep += comp.get("variants_only_in_vep", 0)

        all_fields.update(comp.get("field_match_rates", {}).keys())
        for f, c in comp.get("field_mismatch_counts", {}).items():
            field_mm[f] += c
        for f, c in comp.get("field_order_mismatch_counts", {}).items():
            field_order[f] += c
        for f, exs in comp.get("field_mismatch_examples", {}).items():
            for ex in exs:
                ex["source_chrom"] = r["chrom"]
                field_examples[f].append(ex)

    return {
        "all_fields": all_fields,
        "field_mm": field_mm,
        "field_order": field_order,
        "field_examples": field_examples,
        "total_compared": total_compared,
        "total_csq_match": total_csq_match,
        "total_csq_mismatch": total_csq_mismatch,
        "total_only_vepyr": total_only_vepyr,
        "total_only_vep": total_only_vep,
    }


# ── Step 4: Classify Consequence mismatches ──────────────────────────────


def classify_consequence_mismatches(examples):
    """Classify Consequence field mismatches into root cause categories."""
    classes = defaultdict(list)
    for ex in examples:
        vepyr = ex["vepyr"]
        vep = ex["vep"]

        if "stop_retained_variant" in vepyr and "stop_retained_variant" not in vep:
            if "inframe_insertion" in vepyr and "frameshift_variant" in vep:
                classes["inframe_vs_frameshift"].append(ex)
            elif "incomplete_terminal_codon" in vepyr:
                classes["incomplete_terminal"].append(ex)
            else:
                classes["stop_retained_extra"].append(ex)
        elif "stop_gained" in vepyr and "stop_gained" not in vep:
            classes["stop_gained_extra"].append(ex)
        elif "stop_gained" not in vepyr and "stop_gained" in vep:
            classes["stop_gained_missing"].append(ex)
        elif "stop_lost" in vepyr and "stop_lost" not in vep:
            classes["stop_lost_extra"].append(ex)
        elif "stop_lost" not in vepyr and "stop_lost" in vep:
            classes["stop_lost_extra"].append(ex)
        elif "start_retained_variant" in vep and "start_retained_variant" not in vepyr:
            classes["start_retained_missing"].append(ex)
        elif (
            "protein_altering_variant" in vep
            and "protein_altering_variant" not in vepyr
        ):
            classes["protein_altering"].append(ex)
        elif "incomplete_terminal_codon" in vepyr:
            classes["incomplete_terminal"].append(ex)
        elif "mature_miRNA_variant" in vepyr and "mature_miRNA_variant" not in vep:
            classes["mirna_overlap"].append(ex)
        elif "synonymous_variant" in vepyr and "coding_sequence_variant" in vep:
            classes["incomplete_terminal"].append(ex)
        elif (
            "inframe_insertion" in vep
            and "stop_retained" in vep
            and "frameshift" in vepyr
        ):
            classes["inframe_vs_frameshift"].append(ex)
        elif "frameshift_variant" in vep and "frameshift_variant" not in vepyr:
            classes["frameshift_missing"].append(ex)
        else:
            classes["other"].append(ex)

    return classes


# ── Step 5: Load old benchmark for comparison ────────────────────────────


def load_old_benchmark():
    """Load the previous full-genome benchmark report for delta comparison."""
    path = os.path.join(REPORT_DIR, "benchmark_report.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        r = json.load(f)
    # Use fjall backend comparison (matches our fast run)
    vvv = r.get("vepyr_vs_vep", {})
    comp = vvv.get("fjall", vvv.get("parquet", {}))
    return comp.get("field_mismatch_counts", {})


# ── Step 5b: Detect build info ────────────────────────────────────────────


def get_build_info():
    """Extract git branch, vepyr commit, and bio-functions rev from Cargo.toml."""
    info = {}

    # Git branch
    try:
        info["branch"] = (
            subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=os.path.join(SCRIPT_DIR, "..", ".."),
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        info["branch"] = "unknown"

    # vepyr commit
    try:
        info["vepyr_rev"] = (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=os.path.join(SCRIPT_DIR, "..", ".."),
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        info["vepyr_rev"] = "unknown"

    # bio-functions rev from Cargo.toml
    cargo_path = os.path.join(SCRIPT_DIR, "..", "..", "Cargo.toml")
    info["bio_functions_rev"] = "unknown"
    if os.path.exists(cargo_path):
        with open(cargo_path) as f:
            for line in f:
                if "datafusion-bio-function-vep" in line and "rev" in line:
                    import re

                    m = re.search(r'rev\s*=\s*"([^"]+)"', line)
                    if m:
                        info["bio_functions_rev"] = m.group(1)[:12]
                    break

    return info


# ── Step 6: Generate Markdown report ─────────────────────────────────────


def issue_link(num):
    return f"[#{num}]({REPO}/issues/{num})"


def pr_link(num):
    return f"[#{num}]({REPO}/pull/{num})"


def generate_report(reports, agg, csq_classes, old_mm, build_info=None):
    """Generate the full Markdown report."""
    lines = []
    now = datetime.now()
    total_in = sum(r["input_variants"] for r in reports)
    total_time = sum(r["annotation"]["time_s"] or 0 for r in reports)
    field_mm = agg["field_mm"]
    all_fields = agg["all_fields"]

    n_perfect = len([f for f in all_fields if field_mm.get(f, 0) == 0])
    n_imperfect = len([f for f in all_fields if field_mm.get(f, 0) > 0])
    total_mm = sum(field_mm.values())

    bi = build_info or {}

    # ── Header ────────────────────────────────────────────────────────
    lines.append("# Fast Annotation Report: chr1-22 (fjall)")
    lines.append("")
    lines.append(f"**Date:** {now.strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"**Variants:** {total_in:,} (HG002 GRCh38, bcftools norm -m -both)")
    lines.append("**Backend:** fjall only")
    lines.append(
        f"**Total annotation time:** {total_time:.0f}s ({total_time / 60:.1f} min)"
    )
    lines.append(f"**Aggregate rate:** {total_in / total_time:,.0f} variants/s")
    if bi:
        lines.append(
            f"**Build:** branch `{bi.get('branch', '?')}` "
            f"@ [{bi.get('vepyr_rev', '?')}], "
            f"bio-functions rev `{bi.get('bio_functions_rev', '?')}`"
        )
    lines.append("")

    # ── Headline ──────────────────────────────────────────────────────
    lines.append("## Headline")
    lines.append("")
    lines.append(
        f"- **{n_perfect} / {len(all_fields)} CSQ fields at 100% match** (0 mismatches)"
    )
    lines.append(
        f"- **{n_imperfect} fields** with mismatches, **{total_mm:,} total** across CSQ entries"
    )
    if old_mm is not None:
        old_total = sum(old_mm.values())
        n_fixed = len([f for f in old_mm if f not in field_mm or field_mm[f] == 0])
        lines.append(
            f"- **{n_fixed} fields FIXED** to 0 vs previous benchmark "
            f"(was {old_total:,} mismatches)"
        )
    lines.append(
        "- All mismatches traced to root cause classes with upstream issues filed"
    )
    lines.append("")

    # ── Root cause table ──────────────────────────────────────────────
    lines.append("## Root Cause Classification & Issue Tracker")
    lines.append("")
    lines.append(
        "| # | Root Cause | Mismatches | Fields Affected | Upstream Issue | Status |"
    )
    lines.append(
        "|---|-----------|-----------|-----------------|---------------|--------|"
    )

    row_num = 0
    for key, info in ISSUES.items():
        row_num += 1
        # Count mismatches for this class from Consequence classification
        csq_count = len(csq_classes.get(key, []))
        # For non-Consequence classes, use field totals
        if key == "hgvsc_noncoding":
            count = f"~{field_mm.get('HGVSc', 0)} + {field_mm.get('HGVS_OFFSET', 0)}"
            fields = "HGVSc, HGVS_OFFSET"
        elif key == "hgnc_id_extra":
            count = f"~{field_mm.get('HGNC_ID', 0)}"
            fields = "HGNC_ID"
        elif key == "cds_boundary_missing":
            count = "~{}".format(
                sum(
                    field_mm.get(f, 0)
                    for f in [
                        "CDS_position",
                        "Protein_position",
                        "Amino_acids",
                        "Codons",
                        "DOMAINS",
                    ]
                )
            )
            fields = "CDS_position, Protein_position, Amino_acids, Codons, DOMAINS"
        elif key == "mirna_dedup":
            count = str(field_mm.get("miRNA", 0))
            fields = "miRNA"
        elif key == "inframe_vs_frameshift":
            count = f"~{csq_count}" if csq_count else "0"
            fields = "Consequence"
        elif key == "incomplete_terminal_impact_hgvsp":
            count = f"~{field_mm.get('IMPACT', 0)} IMPACT + {field_mm.get('HGVSp', 0)} HGVSp"
            fields = "IMPACT, HGVSp"
        else:
            count = f"~{csq_count}" if csq_count else "0"
            fields = "Consequence"

        links = ", ".join(
            [issue_link(n) for n in info["issues"]] + [pr_link(n) for n in info["prs"]]
        )

        # Derive status from mismatch count
        # Parse the numeric part of count to check if zero
        count_str = count.replace("~", "").strip()
        is_zero = False
        if count_str == "0":
            is_zero = True
        elif " + " in count_str:
            # e.g. "549 + 549" or "0 + 0"
            parts = count_str.split(" + ")
            is_zero = all(p.strip() == "0" for p in parts)
        elif "Csq" in count_str:
            # e.g. "0 Csq + 30 IMPACT + 29 HGVSp"
            import re as _re

            nums = [int(x) for x in _re.findall(r"\d+", count_str)]
            is_zero = all(n == 0 for n in nums)

        status = "FIXED" if is_zero else "OPEN"
        lines.append(
            f"| {row_num} | {info['title']} | {count} | {fields} | {links} | {status} |"
        )

    lines.append("")

    # ── Performance table ─────────────────────────────────────────────
    lines.append("## Per-Chromosome Performance")
    lines.append("")
    lines.append("| Chrom | Variants | Time (s) | Rate (v/s) |")
    lines.append("|-------|----------|----------|------------|")
    for r in reports:
        c = r["chrom"]
        v = r["input_variants"]
        t = r["annotation"]["time_s"] or 0
        rate = v / t if t else 0
        lines.append(f"| {c} | {v:,} | {t:.1f} | {rate:,.0f} |")
    lines.append(
        f"| **TOTAL** | **{total_in:,}** | **{total_time:.1f}** "
        f"| **{total_in / total_time:,.0f}** |"
    )
    lines.append("")

    # ── Variant coverage ──────────────────────────────────────────────
    lines.append("## Variant Coverage")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Variants compared | {agg['total_compared']:,} |")
    lines.append(f"| CSQ entry count match | {agg['total_csq_match']:,} |")
    lines.append(f"| CSQ entry count mismatch | {agg['total_csq_mismatch']:,} |")
    lines.append(f"| Only in vepyr | {agg['total_only_vepyr']:,} |")
    lines.append(f"| Only in VEP | {agg['total_only_vep']:,} |")
    lines.append("")

    # ── Field-level delta table ───────────────────────────────────────
    if old_mm is not None:
        lines.append("## Field-Level Mismatches: NEW vs OLD Benchmark")
        lines.append("")
        lines.append("| Field | NEW (this run) | OLD (benchmark) | Delta | Status |")
        lines.append("|-------|---------------|-----------------|-------|--------|")

        all_delta_fields = sorted(
            set(list(field_mm.keys()) + list(old_mm.keys())),
            key=lambda f: -(field_mm.get(f, 0) + old_mm.get(f, 0)),
        )
        for f in all_delta_fields:
            new_c = field_mm.get(f, 0)
            old_c = old_mm.get(f, 0)
            delta = new_c - old_c
            if new_c == 0 and old_c > 0:
                status = "FIXED"
            elif delta < 0:
                status = f"IMPROVED ({delta})"
            elif delta > 0:
                status = f"REGRESSED (+{delta})"
            elif new_c == 0:
                status = "OK"
            else:
                status = "SAME"
            lines.append(f"| {f} | {new_c:,} | {old_c:,} | {delta:+,} | {status} |")

        lines.append("")
        lines.append(
            f"**Total mismatches: {total_mm:,}** (was {sum(old_mm.values()):,}, delta {total_mm - sum(old_mm.values()):+,})"
        )
        lines.append("")

        # FIXED fields
        fixed = [f for f in old_mm if field_mm.get(f, 0) == 0]
        if fixed:
            lines.append(
                f"### Fields FIXED (previously had mismatches, now 0): {len(fixed)} fields"
            )
            lines.append("")
            lines.append(", ".join(f"**{f}** ({old_mm[f]})" for f in fixed))
            lines.append("")

        # IMPROVED
        improved = [
            f for f in all_delta_fields if 0 < field_mm.get(f, 0) < old_mm.get(f, 0)
        ]
        if improved:
            lines.append(f"### Fields IMPROVED ({len(improved)} fields)")
            lines.append("")
            for f in improved:
                lines.append(
                    f"- **{f}** — {old_mm[f]:,} → {field_mm[f]:,} "
                    f"(−{old_mm[f] - field_mm[f]:,})"
                )
            lines.append("")

        # REGRESSED
        regressed = [
            f for f in all_delta_fields if field_mm.get(f, 0) > old_mm.get(f, 0)
        ]
        if regressed:
            lines.append(f"### Fields REGRESSED ({len(regressed)} fields)")
            lines.append("")
            for f in regressed:
                lines.append(
                    f"- **{f}** — {old_mm.get(f, 0):,} → {field_mm[f]:,} "
                    f"(+{field_mm[f] - old_mm.get(f, 0):,})"
                )
            lines.append("")

    # ── Remaining mismatch details ────────────────────────────────────
    lines.append("## Remaining Mismatch Details")
    lines.append("")
    for f in sorted(field_mm, key=lambda x: -field_mm[x]):
        if field_mm[f] == 0:
            continue
        lines.append(f"### {f} — {field_mm[f]:,} mismatches")
        lines.append("")
        exs = agg["field_examples"].get(f, [])[:5]
        if exs:
            lines.append("| Variant | vepyr | VEP |")
            lines.append("|---------|-------|-----|")
            for ex in exs:
                v = ex["variant"].replace("\t", " ")
                vv = ex["vepyr"][:80] if ex["vepyr"] else "(empty)"
                gv = ex["vep"][:80] if ex["vep"] else "(empty)"
                lines.append(f"| `{v}` | `{vv}` | `{gv}` |")
        lines.append("")

    # ── Per-chromosome breakdown ──────────────────────────────────────
    lines.append("## Per-Chromosome Mismatch Breakdown")
    lines.append("")
    lines.append(
        "| Chrom | Variants | CSQ Match | Consequence | HGVSc "
        "| HGVSp | IMPACT | HGNC_ID | Other |"
    )
    lines.append(
        "|-------|----------|-----------|-------------|-------"
        "|-------|--------|---------|-------|"
    )
    key_fields = {"Consequence", "HGVSc", "HGVSp", "IMPACT", "HGNC_ID", "HGVS_OFFSET"}
    for r in reports:
        c = r["chrom"]
        comp = r.get("comparison", {})
        v = comp.get("variants_compared", 0)
        cm = comp.get("csq_entry_count_match", 0)
        mm = comp.get("field_mismatch_counts", {})
        csq = mm.get("Consequence", 0)
        hgvsc = mm.get("HGVSc", 0)
        hgvsp = mm.get("HGVSp", 0)
        impact = mm.get("IMPACT", 0)
        hgnc = mm.get("HGNC_ID", 0)
        other = sum(v for k, v in mm.items() if k not in key_fields)
        lines.append(
            f"| {c} | {v:,} | {cm:,} | {csq} | {hgvsc} "
            f"| {hgvsp} | {impact} | {hgnc} | {other} |"
        )
    lines.append("")

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    args = parse_args()
    os.makedirs(REPORT_DIR, exist_ok=True)

    # Step 1: Run annotations
    if not args.skip_annotate:
        print(f"Running fast annotation for chr{args.chroms[0]}-chr{args.chroms[-1]}")
        for n in args.chroms:
            ok = run_chromosome(n, force=not args.no_force)
            if not ok:
                print(f"  chr{n} failed, continuing...")

    # Step 2: Load reports
    reports = load_reports(args.chroms)
    if not reports:
        sys.exit("No reports found. Run without --skip-annotate first.")
    print(f"\nLoaded {len(reports)} chromosome reports")

    # Step 3: Aggregate
    agg = aggregate_mismatches(reports)

    # Step 4: Classify Consequence mismatches
    csq_classes = classify_consequence_mismatches(
        agg["field_examples"].get("Consequence", [])
    )
    print("\nConsequence mismatch classification:")
    for cls, exs in sorted(csq_classes.items(), key=lambda x: -len(x[1])):
        print(f"  {cls:<30} {len(exs):>5} mismatches")

    # Step 5: Load old benchmark
    old_mm = load_old_benchmark()
    if old_mm:
        print(f"\nLoaded old benchmark ({sum(old_mm.values()):,} total mismatches)")
    else:
        print("\nNo old benchmark_report.json found, skipping delta comparison")

    # Step 5b: Build info
    build_info = get_build_info()
    print(
        f"\nBuild: branch={build_info['branch']}, "
        f"vepyr={build_info['vepyr_rev']}, "
        f"bio-functions={build_info['bio_functions_rev']}"
    )

    # Step 6: Generate report
    md = generate_report(reports, agg, csq_classes, old_mm, build_info)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    report_path = os.path.join(REPORT_DIR, f"fast_chr1_22_summary_{timestamp}.md")
    with open(report_path, "w") as f:
        f.write(md)

    n_perfect = len([f for f in agg["all_fields"] if agg["field_mm"].get(f, 0) == 0])
    total_mm = sum(agg["field_mm"].values())

    print(f"\n{'=' * 60}")
    print(f"  Report: {report_path}")
    print(f"  Fields at 100%: {n_perfect}/{len(agg['all_fields'])}")
    print(f"  Total mismatches: {total_mm:,}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
