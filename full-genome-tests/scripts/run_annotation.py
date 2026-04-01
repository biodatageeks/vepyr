#!/usr/bin/env python3
"""Full-genome annotation benchmark: preprocess, annotate, compare.

Preprocesses input VCF (bcftools norm), runs vepyr annotation with both
parquet and fjall backends, and compares results against original VEP output.
"""

import json
import os
import subprocess
import sys
import time

import polars as pl

# ── Paths ──────────────────────────────────────────────────────────────────
DATA_DIR = "/home/tgambin/workspace/data_vepyr"
CACHE_DIR = os.path.join(DATA_DIR, "115_GRCh38_vep")
REFERENCE_FASTA = os.path.join(DATA_DIR, "Homo_sapiens.GRCh38.dna.primary_assembly.fa")
VCF_INPUT = os.path.join(DATA_DIR, "HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz")

# Original VEP outputs for comparison
VEP_EVERYTHING = os.path.join(DATA_DIR, "HG002_annotated_wgs_everything.vcf")
VEP_EVERYTHING_HGVS = os.path.join(DATA_DIR, "HG002_annotated_wgs_everything_hgvs.vcf")

# Working directories
WORK_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
os.makedirs(WORK_DIR, exist_ok=True)

REPORT_DIR = os.path.join(os.path.dirname(__file__), "..", "reports")
os.makedirs(REPORT_DIR, exist_ok=True)

# ── Step 1: Normalize VCF ──────────────────────────────────────────────────
vcf_norm = os.path.join(WORK_DIR, "normalized.vcf")
vcf_gz = vcf_norm + ".gz"

if not os.path.exists(vcf_gz):
    print("=" * 60)
    print("Step 1: Normalizing VCF (bcftools norm -m -both)")
    print("=" * 60)

    result = subprocess.run(
        ["bcftools", "norm", "-m", "-both", "-o", vcf_norm, VCF_INPUT],
        capture_output=True,
        text=True,
    )
    print(result.stderr.strip())
    assert result.returncode == 0, f"bcftools norm failed: {result.stderr}"

    print("Compressing (bgzip) ...")
    subprocess.run(["bgzip", "-f", vcf_norm], check=True)
    assert os.path.exists(vcf_gz)

    print("Indexing (tabix) ...")
    subprocess.run(["tabix", "-p", "vcf", vcf_gz], check=True)

n_variants = int(
    subprocess.check_output(
        f"gunzip -c '{vcf_gz}' | grep -cv '^#'", shell=True
    ).strip()
)
print(f"Input: {n_variants:,} biallelic variants in {vcf_gz}")

# ── Step 2: Annotate with vepyr ────────────────────────────────────────────
import vepyr

backends = ["parquet", "fjall"]
timings = {}

for backend in backends:
    output_vcf = os.path.join(WORK_DIR, f"vepyr_{backend}.vcf")

    print()
    print("=" * 60)
    print(f"Step 2: Annotating with vepyr ({backend} backend)")
    print("=" * 60)

    if os.path.exists(output_vcf) and os.path.getsize(output_vcf) > 1_000_000:
        print(f"  Skipping annotation — {output_vcf} already exists")
        size_mb = os.path.getsize(output_vcf) / (1024 * 1024)
        n_out = int(
            subprocess.check_output(
                f"grep -cv '^#' '{output_vcf}'", shell=True
            ).strip()
        )
        timings[backend] = {
            "time_s": None,
            "time_min": None,
            "variants": n_out,
            "rate_per_s": None,
            "output_mb": round(size_mb),
        }
        print(f"  Existing: {n_out:,} variants, {size_mb:.0f} MB")
    else:
        t0 = time.time()
        vepyr.annotate(
            vcf_gz,
            CACHE_DIR,
            everything=True,
            reference_fasta=REFERENCE_FASTA,
            use_fjall=(backend == "fjall"),
            output_vcf=output_vcf,
        )
        elapsed = time.time() - t0

        size_mb = os.path.getsize(output_vcf) / (1024 * 1024)
        n_out = int(
            subprocess.check_output(
                f"grep -cv '^#' '{output_vcf}'", shell=True
            ).strip()
        )
        rate = n_out / elapsed if elapsed > 0 else 0

        timings[backend] = {
            "time_s": round(elapsed, 1),
            "time_min": round(elapsed / 60, 1),
            "variants": n_out,
            "rate_per_s": round(rate),
            "output_mb": round(size_mb),
        }
        print(
            f"  Done: {n_out:,} variants in {elapsed:.1f}s "
            f"({rate:,.0f} variants/s), {size_mb:.0f} MB"
        )

# ── Step 3: Compare parquet vs fjall ───────────────────────────────────────
print()
print("=" * 60)
print("Step 3: Comparing vepyr parquet vs fjall")
print("=" * 60)

parquet_vcf = os.path.join(WORK_DIR, "vepyr_parquet.vcf")
fjall_vcf = os.path.join(WORK_DIR, "vepyr_fjall.vcf")

# Line counts
for name, path in [("parquet", parquet_vcf), ("fjall", fjall_vcf)]:
    n = int(subprocess.check_output(f"grep -cv '^#' '{path}'", shell=True).strip())
    print(f"  {name}: {n:,} data lines")

# Diff data lines (sorted by position)
diff_result = subprocess.run(
    f"diff <(grep -v '^#' '{parquet_vcf}' | sort -k1,1V -k2,2n) "
    f"<(grep -v '^#' '{fjall_vcf}' | sort -k1,1V -k2,2n) | grep -c '^[<>]'",
    shell=True,
    executable="/bin/bash",
    capture_output=True,
    text=True,
)
n_diff_pf = int(diff_result.stdout.strip()) if diff_result.stdout.strip() else 0
print(f"  Differing lines (parquet vs fjall): {n_diff_pf}")

# ── Step 4 & 5: Compare BOTH vepyr backends vs original VEP ────────────────

import re


def compare_vepyr_vs_vep(vepyr_vcf_path, vep_vcf_path, backend_name):
    """Full CSQ field-by-field comparison of vepyr output vs original VEP.

    Compares ALL variants (not a sample) on all shared CSQ fields.
    Returns a dict with comparison results.
    """
    print()
    print("=" * 60)
    print(f"Comparing vepyr ({backend_name}) vs original VEP")
    print("=" * 60)

    # Line counts
    n_vepyr = int(subprocess.check_output(
        f"grep -cv '^#' '{vepyr_vcf_path}'", shell=True
    ).strip())
    n_vep = int(subprocess.check_output(
        f"grep -cv '^#' '{vep_vcf_path}'", shell=True
    ).strip())
    print(f"  vepyr ({backend_name}): {n_vepyr:,} data lines")
    print(f"  VEP (original):        {n_vep:,} data lines")

    # Get CSQ format from both
    vepyr_csq_header = subprocess.check_output(
        f"grep '^##INFO=<ID=CSQ' '{vepyr_vcf_path}'", shell=True
    ).decode().strip()
    vep_csq_header = subprocess.check_output(
        f"grep '^##INFO=<ID=CSQ' '{vep_vcf_path}'", shell=True
    ).decode().strip()

    vepyr_fields = re.search(r"Format: ([^\"]+)", vepyr_csq_header).group(1).split("|")
    vep_fields = re.search(r"Format: ([^\"]+)", vep_csq_header).group(1).split("|")

    print(f"  vepyr CSQ fields: {len(vepyr_fields)}")
    print(f"  VEP   CSQ fields: {len(vep_fields)}")
    fields_only_vepyr = sorted(set(vepyr_fields) - set(vep_fields))
    fields_only_vep = sorted(set(vep_fields) - set(vepyr_fields))
    if fields_only_vepyr:
        print(f"  Fields only in vepyr: {fields_only_vepyr}")
    if fields_only_vep:
        print(f"  Fields only in VEP:   {fields_only_vep}")

    # Extract lightweight key+CSQ files (avoid sorting full 16G VCFs)
    print(f"\n  Extracting key+CSQ from both VCFs...")
    vepyr_kc = vepyr_vcf_path + ".keyscsq.tmp"
    vep_kc = vep_vcf_path + ".keyscsq.tmp"
    # Extract: chrom\tpos\tref\talt\tCSQ_value
    csq_re = re.compile(r"CSQ=([^;\t]+)")
    for src, dst in [(vepyr_vcf_path, vepyr_kc), (vep_vcf_path, vep_kc)]:
        # Extract key+CSQ in Python (streaming, low memory)
        with open(src) as fin, open(dst + ".unsorted", "w") as fout:
            for line in fin:
                if line.startswith("#"):
                    continue
                cols = line.split("\t", 9)  # only split enough
                m = csq_re.search(cols[7])
                csq = m.group(1) if m else ""
                fout.write(f"{cols[0]}\t{cols[1]}\t{cols[3]}\t{cols[4]}\t{csq}\n")
        # Sort the lightweight file
        subprocess.run(
            f"sort -S 4G -T /tmp -k1,1V -k2,2n -k3,3 -k4,4 '{dst}.unsorted' > '{dst}'",
            shell=True, check=True,
        )
        os.remove(dst + ".unsorted")
        sz = os.path.getsize(dst) / (1024 * 1024)
        print(f"    {os.path.basename(dst)}: {sz:.0f} MB")

    # Stream merge-join on sorted lightweight files
    print(f"  Comparing vepyr ({backend_name}) against VEP on ALL variants...")
    shared_fields = [f for f in vepyr_fields if f in vep_fields]
    field_matches = {f: 0 for f in shared_fields}
    field_mismatches = {f: 0 for f in shared_fields}
    field_total = {f: 0 for f in shared_fields}
    field_mismatch_examples = {f: [] for f in shared_fields}

    n_compared = 0
    n_missing_in_vep = 0
    n_missing_in_vepyr = 0
    n_csq_entry_count_match = 0
    n_csq_entry_count_mismatch = 0

    def parse_kc_line(line):
        parts = line.rstrip("\n").split("\t", 4)
        key = (parts[0], int(parts[1]), parts[2], parts[3])
        csq = parts[4] if len(parts) > 4 else ""
        return key, csq

    with open(vepyr_kc) as fv, open(vep_kc) as fg:
        vepyr_line = fv.readline()
        vep_line = fg.readline()

        while vepyr_line and vep_line:
            vk, vepyr_csq = parse_kc_line(vepyr_line)
            gk, vep_csq = parse_kc_line(vep_line)

            if vk < gk:
                n_missing_in_vep += 1
                vepyr_line = fv.readline()
                continue
            elif vk > gk:
                n_missing_in_vepyr += 1
                vep_line = fg.readline()
                continue

            # Keys match
            n_compared += 1
            key = f"{vk[0]}\t{vk[1]}\t{vk[2]}\t{vk[3]}"

            if vepyr_csq and vep_csq:
                vepyr_entries = sorted(vepyr_csq.split(","))
                vep_entries = sorted(vep_csq.split(","))

                if len(vepyr_entries) == len(vep_entries):
                    n_csq_entry_count_match += 1
                else:
                    n_csq_entry_count_mismatch += 1

                for i in range(min(len(vepyr_entries), len(vep_entries))):
                    vepyr_vals = dict(zip(vepyr_fields, vepyr_entries[i].split("|")))
                    vep_vals = dict(zip(vep_fields, vep_entries[i].split("|")))

                    for f in shared_fields:
                        field_total[f] += 1
                        vepyr_v = vepyr_vals.get(f, "")
                        vep_v = vep_vals.get(f, "")
                        if vepyr_v == vep_v:
                            field_matches[f] += 1
                        else:
                            field_mismatches[f] += 1
                            if len(field_mismatch_examples[f]) < 5:
                                field_mismatch_examples[f].append({
                                    "variant": key,
                                    "vepyr": vepyr_v,
                                    "vep": vep_v,
                                })

            vepyr_line = fv.readline()
            vep_line = fg.readline()

        while vepyr_line:
            n_missing_in_vep += 1
            vepyr_line = fv.readline()
        while vep_line:
            n_missing_in_vepyr += 1
            vep_line = fg.readline()

    # Cleanup temp files
    os.remove(vepyr_kc)
    os.remove(vep_kc)

    print(f"\n  Results for vepyr ({backend_name}) vs VEP:")
    print(f"    Variants compared:           {n_compared:,}")
    print(f"    Variants only in vepyr:      {n_missing_in_vep:,}")
    print(f"    CSQ entry count match:       {n_csq_entry_count_match:,}")
    print(f"    CSQ entry count mismatch:    {n_csq_entry_count_mismatch:,}")

    print(f"\n  Per-field match rates (ALL {n_compared:,} variants, ALL CSQ entries):")
    print(f"  {'Field':<30} {'Match%':>8} {'Matches':>10} {'Mismatches':>10} {'Total':>10}")
    print(f"  {'-'*30} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")
    for f in shared_fields:
        total = field_total[f]
        matches = field_matches[f]
        mismatches = field_mismatches[f]
        rate = (matches / total * 100) if total > 0 else 0
        flag = "" if rate == 100 else " <--"
        print(f"  {f:<30} {rate:>7.2f}% {matches:>10,} {mismatches:>10,} {total:>10,}{flag}")

    # Show mismatch examples
    fields_with_mismatches = [f for f in shared_fields if field_mismatches[f] > 0]
    if fields_with_mismatches:
        print(f"\n  Mismatch examples:")
        for f in fields_with_mismatches:
            print(f"\n    {f} ({field_mismatches[f]:,} mismatches):")
            for ex in field_mismatch_examples[f]:
                print(f"      {ex['variant']}")
                print(f"        vepyr: {ex['vepyr']!r}")
                print(f"        VEP:   {ex['vep']!r}")
    else:
        print(f"\n  ALL {len(shared_fields)} shared CSQ fields match at 100%!")

    return {
        "backend": backend_name,
        "variants_compared": n_compared,
        "variants_only_in_vepyr": n_missing_in_vep,
        "csq_entry_count_match": n_csq_entry_count_match,
        "csq_entry_count_mismatch": n_csq_entry_count_mismatch,
        "csq_fields_only_vepyr": fields_only_vepyr,
        "csq_fields_only_vep": fields_only_vep,
        "field_match_rates": {
            f: round(field_matches[f] / field_total[f] * 100, 4)
            for f in shared_fields
            if field_total[f] > 0
        },
        "field_mismatch_counts": {
            f: field_mismatches[f]
            for f in shared_fields
            if field_mismatches[f] > 0
        },
        "field_mismatch_examples": {
            f: field_mismatch_examples[f]
            for f in shared_fields
            if field_mismatch_examples[f]
        },
    }


# Run comparison for BOTH backends
comparison_results = {}
for backend_name, vcf_path in [("parquet", parquet_vcf), ("fjall", fjall_vcf)]:
    comparison_results[backend_name] = compare_vepyr_vs_vep(
        vcf_path, VEP_EVERYTHING_HGVS, backend_name
    )

# ── Step 6: Write report ──────────────────────────────────────────────────
print()
print("=" * 60)
print("Step 6: Writing report")
print("=" * 60)

report = {
    "input": {
        "vcf": VCF_INPUT,
        "normalized_vcf": vcf_gz,
        "n_variants_input": n_variants,
        "cache": CACHE_DIR,
        "reference": REFERENCE_FASTA,
    },
    "timings": timings,
    "parquet_vs_fjall": {
        "differing_lines": n_diff_pf,
    },
    "vepyr_vs_vep": comparison_results,
}

report_path = os.path.join(REPORT_DIR, "benchmark_report.json")
with open(report_path, "w") as f:
    json.dump(report, f, indent=2)
print(f"  Report saved to {report_path}")

print()
print("=" * 60)
print("DONE")
print("=" * 60)
