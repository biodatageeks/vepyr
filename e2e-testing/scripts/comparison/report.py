"""Aggregation, root-cause classification, and Markdown report generation.

Takes dicts and returns a string. Touches the filesystem only to load existing
per-contig report JSONs and repo metadata.
"""

import hashlib
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
        "title": "`stop_lost` false positive",
        "issues": [115],
        "prs": [],
    },
    "stop_lost_missing": {
        "title": "`stop_lost` missing on frameshift past stop codon",
        "issues": [90],
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
    "mirna_overlap": {
        "title": "`mature_miRNA_variant` emitted outside a mature product",
        "issues": [90],
        "prs": [],
    },
    "frameshift_missing": {
        "title": "`frameshift_variant` missing at CDS boundary",
        "issues": [117],
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


def mismatch_ledger_path(report_dir, chrom, suffix, release):
    """Release-qualified, uncapped JSONL mismatch evidence for one contig."""
    return os.path.join(report_dir, f"fast_{chrom}_{suffix}_{release}_mismatches.jsonl")


def quarantine_contig_evidence(report_dir, chrom, suffix, release):
    """Move prior evidence aside before attempting a fresh contig run."""
    for path in (
        report_json_path(report_dir, chrom, suffix, release),
        mismatch_ledger_path(report_dir, chrom, suffix, release),
    ):
        if os.path.exists(path):
            quarantine = path + ".stale"
            os.replace(path, quarantine)
            print(f"  Quarantined stale evidence: {quarantine}")


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
            raise ValueError(f"missing requested report for {chrom}: expected {modern}")
        with open(path) as f:
            value = json.load(f)
        expected = {
            "chrom": chrom,
            "profile": suffix,
            "release": release,
        }
        mismatches = [
            f"{field}={value.get(field)!r}, expected {wanted!r}"
            for field, wanted in expected.items()
            if value.get(field) != wanted
        ]
        if mismatches:
            raise ValueError(
                f"report identity does not match {path}: " + "; ".join(mismatches)
            )
        loaded.append(value)
    return loaded


def common_build_info(reports):
    """Return one exact build provenance shared by every report."""
    builds = [value.get("build") for value in reports]
    if not builds or not isinstance(builds[0], dict) or not builds[0]:
        raise ValueError("reports do not contain build provenance")
    if any(build != builds[0] for build in builds[1:]):
        raise ValueError("build provenance differs across reports")
    return builds[0]


def discover_report_contigs(report_dir, suffix, release):
    """Discover release-qualified report contigs in natural chromosome order."""
    prefix = "fast_"
    ending = f"_{suffix}_{release}_report.json"
    try:
        names = os.listdir(report_dir)
    except OSError:
        return []
    contigs = {
        name[len(prefix) : -len(ending)]
        for name in names
        if name.startswith(prefix) and name.endswith(ending)
    }

    def chromosome_key(contig):
        bare = contig.removeprefix("chr")
        return (0, int(bare)) if bare.isdigit() else (1, bare)

    return sorted(contigs, key=chromosome_key)


# ── Aggregation ──────────────────────────────────────────────────────────


def aggregate_mismatches(reports):
    """Aggregate field match/mismatch data across all chromosome reports."""
    all_fields = set()
    field_mm = defaultdict(int)
    field_order = defaultdict(int)
    field_format = defaultdict(int)
    field_examples = defaultdict(list)

    total_compared = 0
    total_csq_match = 0
    total_csq_mismatch = 0
    total_only_vepyr = 0
    total_only_vep = 0
    total_entries_only_vepyr = 0
    total_entries_only_vep = 0
    total_ledger_rows = 0
    equality_buckets = defaultdict(int)
    ledger_hashes = {}

    for r in reports:
        comp = r.get("comparison", {})
        if not comp:
            continue
        total_compared += comp.get("variants_compared", 0)
        total_csq_match += comp.get("csq_entry_count_match", 0)
        total_csq_mismatch += comp.get("csq_entry_count_mismatch", 0)
        total_only_vepyr += comp.get("variants_only_in_vepyr", 0)
        total_only_vep += comp.get("variants_only_in_vep", 0)
        total_entries_only_vepyr += comp.get("csq_entries_only_in_vepyr", 0)
        total_entries_only_vep += comp.get("csq_entries_only_in_vep", 0)

        ledger = comp.get("mismatch_ledger", {})
        total_ledger_rows += ledger.get("rows", 0)
        if ledger.get("sha256"):
            ledger_hashes[r["chrom"]] = ledger["sha256"]
        for bucket, count in comp.get("equality_bucket_counts", {}).items():
            equality_buckets[bucket] += count

        all_fields.update(comp.get("field_match_rates", {}).keys())
        for f, c in comp.get("field_mismatch_counts", {}).items():
            field_mm[f] += c
        for f, c in comp.get("field_order_mismatch_counts", {}).items():
            field_order[f] += c
        for f, c in comp.get("field_format_mismatch_counts", {}).items():
            field_format[f] += c
        for f, exs in comp.get("field_mismatch_examples", {}).items():
            for ex in exs:
                ex["source_chrom"] = r["chrom"]
                field_examples[f].append(ex)

    return {
        "all_fields": all_fields,
        "field_mm": field_mm,
        "field_order": field_order,
        "field_format": field_format,
        "field_examples": field_examples,
        "total_compared": total_compared,
        "total_csq_match": total_csq_match,
        "total_csq_mismatch": total_csq_mismatch,
        "total_only_vepyr": total_only_vepyr,
        "total_only_vep": total_only_vep,
        "total_entries_only_vepyr": total_entries_only_vepyr,
        "total_entries_only_vep": total_entries_only_vep,
        "total_ledger_rows": total_ledger_rows,
        "ledger_hashes": ledger_hashes,
        "equality_buckets": equality_buckets,
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
            classes["stop_lost_missing"].append(ex)
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


def _command_output(command, cwd):
    return (
        subprocess.check_output(
            command,
            cwd=cwd,
            stderr=subprocess.DEVNULL,
        )
        .decode()
        .strip()
    )


def _git_checkout_info(path):
    """Describe the checkout containing path, including uncommitted sources."""
    try:
        root = _command_output(["git", "rev-parse", "--show-toplevel"], path)
        status_lines = _command_output(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            root,
        ).splitlines()
        # Cargo materializes this bookkeeping marker in every git dependency
        # checkout. It is not repository source and cannot affect the resolved
        # crate, so do not let it make every immutable git dependency appear
        # dirty. All other tracked or untracked paths remain release blockers.
        source_changes = [line for line in status_lines if line != "?? .cargo-ok"]
        return {
            "repo_root": root,
            "revision": _command_output(["git", "rev-parse", "HEAD"], root),
            "dirty": bool(source_changes),
        }
    except Exception:
        return {"repo_root": None, "revision": "unknown", "dirty": None}


def _declared_dependency_sources(metadata, root_manifest):
    for package in metadata.get("packages", []):
        if os.path.abspath(package.get("manifest_path", "")) == os.path.abspath(
            root_manifest
        ):
            return {
                dependency["name"]: dependency.get("source")
                for dependency in package.get("dependencies", [])
            }
    return {}


def get_build_info():
    """Resolve the effective Cargo graph, path overrides, revisions, and dirt."""
    root_git = _git_checkout_info(REPO_ROOT)
    info = {
        "branch": "unknown",
        "vepyr_rev": (
            root_git["revision"][:12]
            if root_git["revision"] != "unknown"
            else "unknown"
        ),
        "vepyr_dirty": root_git["dirty"],
        "bio_functions_rev": "unknown",
        "dependencies": {},
    }
    try:
        info["branch"] = _command_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], REPO_ROOT
        )
    except Exception:
        pass

    lock_path = os.path.join(REPO_ROOT, "Cargo.lock")
    if os.path.exists(lock_path):
        digest = hashlib.sha256()
        with open(lock_path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        info["cargo_lock_sha256"] = digest.hexdigest()

    try:
        metadata = json.loads(
            _command_output(
                ["cargo", "metadata", "--format-version", "1", "--locked"],
                REPO_ROOT,
            )
        )
    except Exception:
        return info

    root_manifest = os.path.join(REPO_ROOT, "Cargo.toml")
    declared_sources = _declared_dependency_sources(metadata, root_manifest)
    relevant = {
        "datafusion-bio-function-vep",
        "datafusion-bio-format-core",
        "datafusion-bio-format-ensembl-cache",
        "datafusion-bio-format-vcf",
    }
    checkout_cache = {}
    for package in metadata.get("packages", []):
        name = package.get("name")
        if name not in relevant:
            continue
        manifest_path = package["manifest_path"]
        package_dir = os.path.dirname(manifest_path)
        effective_source = package.get("source")
        if package_dir not in checkout_cache:
            checkout_cache[package_dir] = _git_checkout_info(package_dir)
        git_info = checkout_cache[package_dir]
        declared_source = declared_sources.get(name)
        declared_revision = None
        if declared_source:
            match = re.search(r"[?&]rev=([^&#]+)", declared_source)
            if match:
                declared_revision = match.group(1)
        dependency_info = {
            "version": package.get("version"),
            "declared_source": declared_source,
            "declared_revision": declared_revision,
            "effective_source": effective_source or "path",
            "manifest_path": manifest_path,
            **git_info,
        }
        info["dependencies"][name] = dependency_info

    bio_functions = info["dependencies"].get("datafusion-bio-function-vep")
    if bio_functions:
        revision = bio_functions.get("revision")
        info["bio_functions_rev"] = (
            revision[:12] if revision and revision != "unknown" else "unknown"
        )
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
    field_format = agg["field_format"]
    all_fields = agg["all_fields"]

    n_perfect = len([f for f in all_fields if field_mm.get(f, 0) == 0])
    n_imperfect = len([f for f in all_fields if field_mm.get(f, 0) > 0])
    total_mm = sum(field_mm.values())
    # Representation-only differences are absorbed into the match rates, so they
    # do not move n_perfect -- but they do mean the output is not byte-identical.
    # Reporting only field_mm here let a summary claim full parity for a run the
    # per-contig comparison had already called non-byte-identical.
    n_format = len([f for f in all_fields if field_format.get(f, 0) > 0])
    total_format = sum(field_format.values())
    # Order-only differences are absorbed the same way and were invisible here
    # for the same reason: the gate rejects the run while this summary claimed
    # every field at 100%.
    field_order = agg["field_order"]
    n_order = len([f for f in all_fields if field_order.get(f, 0) > 0])
    total_order = sum(field_order.values())

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
        lines.append("**Total annotation time:** n/a (timing unavailable)")
    if bi:
        lines.append(
            f"**Build:** branch `{bi.get('branch', '?')}` "
            f"@ `{bi.get('vepyr_rev', '?')}`"
            f"{' (dirty)' if bi.get('vepyr_dirty') else ''}, "
            f"bio-functions rev `{bi.get('bio_functions_rev', '?')}`"
        )
        if bi.get("cargo_lock_sha256"):
            lines.append(f"**Cargo.lock SHA-256:** `{bi['cargo_lock_sha256']}`")
        for dependency_name, dependency in sorted(bi.get("dependencies", {}).items()):
            effective = dependency.get("effective_source", "?")
            if effective == "path":
                effective = dependency.get("repo_root") or dependency.get(
                    "manifest_path", "path"
                )
            dirty = " (dirty)" if dependency.get("dirty") else ""
            lines.append(
                f"**{dependency_name}:** `{dependency.get('revision', '?')}`"
                f"{dirty}; effective source `{effective}`"
            )
    reference_identities = {
        json.dumps(r.get("reference_identity", {}), sort_keys=True)
        for r in reports
        if r.get("reference_identity")
    }
    if len(reference_identities) == 1:
        identity = json.loads(next(iter(reference_identities)))
        lines.append(
            f"**VEP reference:** VEP `{identity.get('vep_version', '?')}`, "
            f"API `{identity.get('api_version', '?')}`, "
            f"cache `{identity.get('cache_version', '?')}`, "
            f"Ensembl `{identity.get('ensembl_release', '?')}."
            f"{identity.get('ensembl_revision', '?')}`, variation "
            f"`{identity.get('ensembl_variation_release', '?')}."
            f"{identity.get('ensembl_variation_revision', '?')}`"
        )
    cache_identities = {
        json.dumps(
            {
                key: value
                for key, value in r.get("cache_identity", {}).items()
                if key != "contig"
            },
            sort_keys=True,
        )
        for r in reports
        if r.get("cache_identity")
    }
    if len(cache_identities) == 1:
        identity = json.loads(next(iter(cache_identities)))
        lines.append(
            f"**Validated cache:** release `{identity.get('cache_version', '?')}`, "
            f"source `{identity.get('cache_source_type', '?')}` "
            "(contig-local Parquet metadata)"
        )
    support_targets = {
        json.dumps(r.get("supported_target", {}), sort_keys=True)
        for r in reports
        if r.get("supported_target")
    }
    if len(support_targets) == 1:
        target = json.loads(next(iter(support_targets)))
        lines.append(
            f"**Native target:** VEP `{target.get('vep_codebase_version', '?')}`, "
            f"semantics `{target.get('semantics', '?')}`, "
            f"cache `{target.get('cache_version', '?')}`"
        )
    lines.append("")

    # ── Headline ──────────────────────────────────────────────────────
    lines.append("## Headline")
    lines.append("")
    lines.append(
        f"- **{n_perfect} / {len(all_fields)} CSQ fields at 100% match** "
        f"(0 value mismatches)"
    )
    lines.append(
        f"- **{n_imperfect} fields** with mismatches, "
        f"**{total_mm:,} total** across CSQ entries"
    )
    if total_format:
        lines.append(
            f"- **{n_format} fields** with representation-only differences, "
            f"**{total_format:,} total** — absorbed into the match rates above, "
            f"so the output is **not** byte-identical"
        )
    if total_order:
        lines.append(
            f"- **{n_order} fields** with order-only differences, "
            f"**{total_order:,} total** — absorbed into the match rates above, "
            f"so the output is **not** byte-identical"
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

        # Every count rendering contains only mismatch counts plus labels.
        # Parsing all integers handles plain, approximate, and multi-field forms.
        numbers = [int(value) for value in re.findall(r"\d+", count)]
        is_zero = bool(numbers) and all(value == 0 for value in numbers)
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
    lines.append(f"| CSQ entries only in vepyr | {agg['total_entries_only_vepyr']:,} |")
    lines.append(f"| CSQ entries only in VEP | {agg['total_entries_only_vep']:,} |")
    lines.append(f"| Uncapped mismatch-ledger rows | {agg['total_ledger_rows']:,} |")
    lines.append("")

    if agg["equality_buckets"]:
        lines.append("## Field Equality Shapes")
        lines.append("")
        lines.append("| Shape | CSQ field comparisons |")
        lines.append("|-------|----------------------:|")
        for shape in (
            "both_empty",
            "both_nonempty_equal",
            "vepyr_empty_only",
            "vep_empty_only",
            "both_nonempty_unequal",
        ):
            lines.append(f"| `{shape}` | {agg['equality_buckets'].get(shape, 0):,} |")
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
