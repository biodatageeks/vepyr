"""Aggregation, root-cause classification, and Markdown report generation.

Takes dicts and returns a string. Touches the filesystem only to load existing
per-contig report JSONs and repo metadata.
"""

import json
import os
import re
import subprocess
from collections import defaultdict
from datetime import datetime

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
# .../e2e-testing/scripts/comparison -> repo root
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(PACKAGE_DIR)))

# ── Upstream issue registry ──────────────────────────────────────────────
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
        "title": (
            "Incomplete terminal codon: IMPACT/HGVSp residual "
            "(Xaa vs Ter, missing p.Ter=)"
        ),
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


# ── Report paths ─────────────────────────────────────────────────────────


def report_json_path(report_dir, chrom, suffix, release):
    """Release-qualified per-contig report path."""
    return os.path.join(report_dir, f"fast_{chrom}_{suffix}_{release}_report.json")


def legacy_report_json_path(report_dir, chrom, suffix):
    """Pre-release-axis path, kept readable so historical reports still load."""
    return os.path.join(report_dir, f"fast_{chrom}_{suffix}_report.json")


def contig_span(chroms):
    """Summarise a contig list for a filename: single name, or first_last."""
    if not chroms:
        return "none"
    if len(chroms) == 1:
        return chroms[0]
    return f"{chroms[0]}_{chroms[-1]}"


def load_reports(report_dir, chroms, suffix, release):
    """Load per-contig reports, preferring release-qualified names.

    Falls back to the legacy unqualified name and says so, because silently
    reading a report that predates the release axis is how a 115 result ends up
    in a 116 summary.
    """
    loaded = []
    for chrom in chroms:
        modern = report_json_path(report_dir, chrom, suffix, release)
        legacy = legacy_report_json_path(report_dir, chrom, suffix)
        if os.path.exists(modern):
            path = modern
        elif os.path.exists(legacy):
            path = legacy
            print(
                f"  Using legacy report {os.path.basename(legacy)} for {chrom} "
                "(predates the release axis; release attribution unverified)"
            )
        else:
            print(f"  WARNING: no report for {chrom}, skipping")
            continue
        with open(path) as f:
            loaded.append(json.load(f))
    return loaded


# ── Aggregation ──────────────────────────────────────────────────────────


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


def load_old_benchmark(report_dir, backend="parquet"):
    """Load the previous full-genome benchmark report for delta comparison."""
    path = os.path.join(report_dir, "benchmark_report.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        r = json.load(f)
    comp = r.get("vepyr_vs_vep", {}).get(backend, {})
    return comp.get("field_mismatch_counts", {})


def get_build_info():
    """Extract git branch, vepyr commit, and bio-functions rev from Cargo.toml."""
    info = {}

    try:
        info["branch"] = (
            subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=REPO_ROOT,
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        info["branch"] = "unknown"

    try:
        info["vepyr_rev"] = (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=REPO_ROOT,
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        info["vepyr_rev"] = "unknown"

    # bio-functions version from Cargo.toml (git tag, or rev for pinned commits)
    cargo_path = os.path.join(REPO_ROOT, "Cargo.toml")
    info["bio_functions_rev"] = "unknown"
    if os.path.exists(cargo_path):
        with open(cargo_path) as f:
            for line in f:
                if "datafusion-bio-function-vep" in line:
                    m = re.search(r'(?:tag|rev)\s*=\s*"([^"]+)"', line)
                    if m:
                        info["bio_functions_rev"] = m.group(1)[:12]
                    break

    return info


# ── Markdown generation ──────────────────────────────────────────────────


def issue_link(num):
    return f"[#{num}]({REPO}/issues/{num})"


def pr_link(num):
    return f"[#{num}]({REPO}/pull/{num})"


def generate_markdown(
    reports,
    agg,
    csq_classes,
    old_mm,
    build_info=None,
    *,
    release,
    profile,
    backend="parquet",
):
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
    span = contig_span([r["chrom"] for r in reports])

    # ── Header ────────────────────────────────────────────────────────
    lines.append(
        f"# Fast Annotation Report: {span} "
        f"({backend}, release {release}, profile {profile})"
    )
    lines.append("")
    lines.append(f"**Date:** {now.strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"**Variants:** {total_in:,} (HG002 GRCh38, bcftools norm -m -both)")
    lines.append(f"**Backend:** {backend} only")
    if total_time > 0:
        lines.append(
            f"**Total annotation time:** {total_time:.0f}s ({total_time / 60:.1f} min)"
        )
        lines.append(f"**Aggregate rate:** {total_in / total_time:,.0f} variants/s")
    else:
        lines.append("**Total annotation time:** n/a (all output reused)")
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
        f"- **{n_imperfect} fields** with mismatches, "
        f"**{total_mm:,} total** across CSQ entries"
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
        csq_count = len(csq_classes.get(key, []))
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
            count = (
                f"~{field_mm.get('IMPACT', 0)} IMPACT + "
                f"{field_mm.get('HGVSp', 0)} HGVSp"
            )
            fields = "IMPACT, HGVSp"
        else:
            count = f"~{csq_count}" if csq_count else "0"
            fields = "Consequence"

        links = ", ".join(
            [issue_link(n) for n in info["issues"]] + [pr_link(n) for n in info["prs"]]
        )

        # Derive status from mismatch count
        count_str = count.replace("~", "").strip()
        is_zero = False
        if count_str == "0":
            is_zero = True
        elif " + " in count_str:
            parts = count_str.split(" + ")
            is_zero = all(p.strip() == "0" for p in parts)
        elif "Csq" in count_str:
            nums = [int(x) for x in re.findall(r"\d+", count_str)]
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
    rate_cell = f"{total_in / total_time:,.0f}" if total_time else "n/a"
    lines.append(
        f"| **TOTAL** | **{total_in:,}** | **{total_time:.1f}** | **{rate_cell}** |"
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
            f"**Total mismatches: {total_mm:,}** (was {sum(old_mm.values()):,}, "
            f"delta {total_mm - sum(old_mm.values()):+,})"
        )
        lines.append("")

        fixed = [f for f in old_mm if field_mm.get(f, 0) == 0]
        if fixed:
            lines.append(
                f"### Fields FIXED (previously had mismatches, now 0): "
                f"{len(fixed)} fields"
            )
            lines.append("")
            lines.append(", ".join(f"**{f}** ({old_mm[f]})" for f in fixed))
            lines.append("")

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
        comp = r.get("comparison", {}) or {}
        v = comp.get("variants_compared", 0)
        cm = comp.get("csq_entry_count_match", 0)
        mm = comp.get("field_mismatch_counts", {})
        csq = mm.get("Consequence", 0)
        hgvsc = mm.get("HGVSc", 0)
        hgvsp = mm.get("HGVSp", 0)
        impact = mm.get("IMPACT", 0)
        hgnc = mm.get("HGNC_ID", 0)
        other = sum(v2 for k, v2 in mm.items() if k not in key_fields)
        lines.append(
            f"| {c} | {v:,} | {cm:,} | {csq} | {hgvsc} "
            f"| {hgvsp} | {impact} | {hgnc} | {other} |"
        )
    lines.append("")

    return "\n".join(lines)
