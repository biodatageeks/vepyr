#!/usr/bin/env python3
"""Classify all mismatches into root-cause clusters and rewrite TSVs with cluster_id column.

Cluster definitions:
  C1 — Transcript ordering: CSQ entries sorted differently → cascading field diffs
  C2 — start_retained_variant: vepyr adds extra start_retained_variant term
  C3 — HGVSc missing: vepyr empty where VEP has HGVSc (UTR/non-coding insertions)
  C4 — HGVSp dup 3' shifting: duplication position differs (3' rule application)
  C5 — Consequence logic: frameshift/inframe/stop classification differences
  C6 — HGNC_ID extra: vepyr emits HGNC_ID where VEP doesn't (regulatory/lncRNA)
  C7 — gnomAD/AF lookup missing: single variant missing all gnomAD frequencies
  C8 — SIFT/PolyPhen missing: vepyr empty where VEP has prediction
  C9 — DISTANCE off-by-one: distance to transcript differs by 1
  C10 — HGVS_OFFSET: different offset calculation for large indels
  C11 — miRNA dedup: miRNA_stem repeated in VEP but deduplicated in vepyr
  C12 — incomplete_terminal_codon: coding_sequence_variant vs synonymous_variant
"""

import csv
import os
import sys
from collections import Counter, defaultdict

REPORT_DIR = os.path.join(os.path.dirname(__file__), "..", "reports")

# Variants with Feature ordering mismatches (transcript ordering)
ORDERING_VARIANTS = {
    ("chr10", 69343841), ("chr11", 5727045), ("chr13", 113074467),
    ("chr16", 57058065), ("chr16", 89224802), ("chr18", 36730705),
    ("chr19", 48715345), ("chr20", 37179387), ("chr2", 119437075),
    ("chr2", 230449378), ("chr3", 12606048), ("chr4", 865481),
    ("chr5", 151259799), ("chr6", 32642980), ("chr7", 891052),
}

# Variants with start_retained_variant extra term
START_RETAINED_VARIANTS = {
    ("chr11", 124214755), ("chr12", 56686880), ("chr14", 94115784),
    ("chr14", 94366696), ("chr2", 26254257),
}

# Variants with HGNC_ID extra (vepyr has, VEP doesn't) — lncRNA/regulatory
HGNC_EXTRA_VARIANTS = set()
# Will be detected dynamically

GNOMAD_VARIANT = ("chr7", 142353982)
MIRNA_VARIANT = ("chr8", 104484407)


def classify_row(row):
    """Return cluster_id for a mismatch row."""
    chrom, pos = row["chrom"], int(row["pos"])
    field = row["field"]
    vepyr_val = row["vepyr"]
    vep_val = row["vep"]
    vk = (chrom, pos)

    # C1: Transcript ordering — variant is in the ordering set
    if vk in ORDERING_VARIANTS:
        return "C1"

    # C2: start_retained_variant
    if field == "Consequence" and "start_retained_variant" in vepyr_val and "start_retained_variant" not in vep_val:
        return "C2"
    if vk in START_RETAINED_VARIANTS:
        # Impact and other fields cascading from start_retained_variant
        return "C2"

    # C7: gnomAD/AF lookup missing (single variant)
    if vk == GNOMAD_VARIANT:
        return "C7"
    if field.startswith("gnomAD") or field in ("MAX_AF", "MAX_AF_POPS"):
        if vepyr_val == "" and vep_val != "":
            return "C7"

    # C8: SIFT/PolyPhen missing
    if field in ("SIFT", "PolyPhen"):
        return "C8"

    # C11: miRNA dedup
    if field == "miRNA":
        return "C11"

    # C9: DISTANCE off-by-one
    if field == "DISTANCE":
        try:
            if vepyr_val and vep_val and abs(int(vepyr_val) - int(vep_val)) <= 1:
                return "C9"
        except ValueError:
            pass
        # DISTANCE swap (ordering cascade) — already handled by C1
        return "C1"

    # C10: HGVS_OFFSET
    if field == "HGVS_OFFSET":
        return "C10"

    # C3: HGVSc missing (vepyr empty, VEP has value) — NOT from ordering variants
    if field == "HGVSc" and vepyr_val == "" and vep_val != "":
        return "C3"
    if field == "HGVSc" and vepyr_val != "" and vep_val == "":
        # Reverse: vepyr has HGVSc that VEP doesn't — linked to ordering or HGVSc logic
        return "C3"

    # C4: HGVSp dup position shifting
    if field == "HGVSp" and "dup" in vepyr_val and "dup" in vep_val:
        return "C4"
    if field == "HGVSp" and (("dup" in vepyr_val and "ins" in vep_val) or ("ins" in vepyr_val and "dup" in vep_val)):
        return "C4"

    # C6: HGNC_ID extra
    if field == "HGNC_ID" and vepyr_val != "" and vep_val == "":
        return "C6"
    if field == "HGNC_ID" and vepyr_val == "" and vep_val != "":
        # VEP has it, vepyr doesn't — but linked to ordering, check
        return "C6"
    if field == "HGNC_ID" and vepyr_val != "" and vep_val != "":
        return "C6"

    # C12: incomplete_terminal_codon classification
    if field == "Consequence" and "incomplete_terminal_codon" in vepyr_val:
        return "C12"
    if field == "Consequence" and "incomplete_terminal_codon" in vep_val:
        return "C12"

    # C5: Other consequence logic differences
    if field == "Consequence":
        return "C5"
    if field == "IMPACT":
        return "C5"

    # C3: HGVSp missing (linked to HGVSc missing)
    if field == "HGVSp" and (vepyr_val == "" or vep_val == ""):
        return "C3"

    # Remaining fields that cascade from C5 consequence differences
    # Check if the variant also has a Consequence mismatch
    return "C5"


def process_file(input_tsv, output_tsv):
    """Read input TSV, classify each row, write output with cluster_id."""
    cluster_counts = Counter()
    cluster_variant_sets = defaultdict(set)

    rows = []
    with open(input_tsv) as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            cluster = classify_row(row)
            row["cluster_id"] = cluster
            rows.append(row)
            cluster_counts[cluster] += 1
            vk = f"{row['chrom']}:{row['pos']}:{row['ref']}:{row['alt']}"
            cluster_variant_sets[cluster].add(vk)

    # Write output
    fieldnames = ["cluster_id", "chrom", "pos", "ref", "alt", "csq_entry_idx", "field", "vepyr", "vep"]
    with open(output_tsv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return cluster_counts, {k: len(v) for k, v in cluster_variant_sets.items()}


CLUSTER_DESCRIPTIONS = {
    "C1": "Transcript ordering",
    "C2": "start_retained_variant extra",
    "C3": "HGVSc/HGVSp missing",
    "C4": "HGVSp dup 3' shifting",
    "C5": "Consequence/IMPACT logic",
    "C6": "HGNC_ID extra",
    "C7": "gnomAD/AF lookup missing",
    "C8": "SIFT/PolyPhen missing",
    "C9": "DISTANCE off-by-one",
    "C10": "HGVS_OFFSET calculation",
    "C11": "miRNA dedup",
    "C12": "incomplete_terminal_codon",
}


def main():
    for backend in ["parquet", "fjall"]:
        input_tsv = os.path.join(REPORT_DIR, f"mismatches_{backend}_vs_vep.tsv")
        output_tsv = os.path.join(REPORT_DIR, f"mismatches_{backend}_vs_vep_classified.tsv")

        print(f"\n{'='*60}")
        print(f"Classifying: {backend}")
        print(f"{'='*60}")

        counts, variant_counts = process_file(input_tsv, output_tsv)

        total_mismatches = sum(counts.values())
        total_variants = sum(variant_counts.values())

        print(f"\n{'Cluster':<8} {'Description':<35} {'Mismatches':>12} {'Variants':>10}")
        print(f"{'-'*8} {'-'*35} {'-'*12} {'-'*10}")
        for cid in sorted(counts.keys()):
            desc = CLUSTER_DESCRIPTIONS.get(cid, "Unknown")
            print(f"{cid:<8} {desc:<35} {counts[cid]:>12,} {variant_counts[cid]:>10,}")
        print(f"{'-'*8} {'-'*35} {'-'*12} {'-'*10}")
        print(f"{'TOTAL':<8} {'':<35} {total_mismatches:>12,} {''}")

        print(f"\nOutput: {output_tsv}")


if __name__ == "__main__":
    main()
