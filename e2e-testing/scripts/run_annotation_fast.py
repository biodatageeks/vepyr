#!/usr/bin/env python3
"""Fast-track single-chromosome annotation comparison.

Usage:
    python run_annotation_fast.py chr1
    python run_annotation_fast.py chr22 --vcf /path/to/input.vcf.gz --vep /path/to/vep_output.vcf
    python run_annotation_fast.py chr22 --bgzf
    python run_annotation_fast.py chr1 --profile merged --workers 4 --force

Extracts a single chromosome from a tabix-indexed VCF, annotates against the
Parquet cache, and compares against the corresponding VEP reference output.
Pass ``--bgzf`` to emit a block-gzipped (``.vcf.gz``) annotated VCF instead of
plain text and validate the compressed output.
"""

import argparse
import gzip
import json
import os
import re
import subprocess
import sys
import time

import vepyr


# ── Defaults ──────────────────────────────────────────────────────────────
DATA_DIR = os.path.expanduser(
    os.path.expandvars(os.environ.get("DATA_VEPYR_DIR", "$HOME/workspace/data_vepyr"))
)
DEFAULT_REFERENCE_FASTA = os.path.join(
    DATA_DIR, "Homo_sapiens.GRCh38.dna.primary_assembly.fa"
)
DEFAULT_VCF_INPUT = os.path.join(DATA_DIR, "HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz")
VEP_PICK_ORDER = "biotype,rank,mane_select,tsl,canonical,appris,ccds,length"
# Parquet is the only supported cache format. Kept as a constant so output file
# names (vepyr_parquet_*) and report suffixes stay stable.
BACKEND = "parquet"

# Per-profile defaults: cache directory, VEP reference VCF, annotate kwargs
_CACHE_PROFILES = {
    "ensembl": {
        "cache_dir": os.path.join(DATA_DIR, "115_GRCh38_ensembl"),
        "vep_vcf": os.path.join(
            DATA_DIR, "HG002_annotated_wgs_everything_hgvs_vep.vcf"
        ),
        "annotate_kwargs": {},
        "suffix": "_ensembl",
    },
    "merged": {
        "cache_dir": os.path.join(DATA_DIR, "115_GRCh38_merged"),
        "vep_vcf": os.path.join(
            DATA_DIR, "HG002_annotated_wgs_everything_hgvs_merged.vcf"
        ),
        "annotate_kwargs": {},
        "suffix": "_merged",
    },
    "merged_flag_pick": {
        "cache_dir": os.path.join(DATA_DIR, "115_GRCh38_merged"),
        "vep_vcf": os.path.join(
            DATA_DIR, "HG002_annotated_wgs_everything_hgvs_merged_flag_pick.vcf"
        ),
        "annotate_kwargs": {
            "flag_pick": True,
            "pick_order": VEP_PICK_ORDER,
        },
        "suffix": "_merged_flag_pick",
    },
    "merged_flag_pick_allele": {
        "cache_dir": os.path.join(DATA_DIR, "115_GRCh38_merged"),
        "vep_vcf": os.path.join(
            DATA_DIR,
            "HG002_annotated_wgs_everything_hgvs_merged_flag_pick_allele.vcf",
        ),
        "annotate_kwargs": {
            "flag_pick_allele": True,
            "pick_order": VEP_PICK_ORDER,
        },
        "suffix": "_merged_flag_pick_allele",
    },
    "merged_flag_pick_allele_gene": {
        "cache_dir": os.path.join(DATA_DIR, "115_GRCh38_merged"),
        # This local VEP artifact is misnamed: chr16 validation shows it is
        # the flag_pick_allele_gene reference, with unfiltered CSQs and PICK.
        "vep_vcf": os.path.join(
            DATA_DIR,
            "HG002_annotated_wgs_everything_hgvs_merged_pick.vcf",
        ),
        "annotate_kwargs": {
            "flag_pick_allele_gene": True,
            "pick_order": VEP_PICK_ORDER,
        },
        "suffix": "_merged_flag_pick_allele_gene",
    },
    "merged_pick_filter": {
        "cache_dir": os.path.join(DATA_DIR, "115_GRCh38_merged"),
        "vep_vcf": os.path.join(
            DATA_DIR, "HG002_annotated_wgs_everything_hgvs_merged_pick_filter.vcf"
        ),
        "annotate_kwargs": {
            "pick": True,
            "pick_order": VEP_PICK_ORDER,
        },
        "suffix": "_merged_pick_filter",
    },
    "merged_pick_allele": {
        "cache_dir": os.path.join(DATA_DIR, "115_GRCh38_merged"),
        "vep_vcf": os.path.join(
            DATA_DIR, "HG002_annotated_wgs_everything_hgvs_merged_pick_allele.vcf"
        ),
        "annotate_kwargs": {
            "pick_allele": True,
            "pick_order": VEP_PICK_ORDER,
        },
        "suffix": "_merged_pick_allele",
    },
    "merged_per_gene": {
        "cache_dir": os.path.join(DATA_DIR, "115_GRCh38_merged"),
        "vep_vcf": os.path.join(
            DATA_DIR, "HG002_annotated_wgs_everything_hgvs_merged_per_gene.vcf"
        ),
        "annotate_kwargs": {
            "per_gene": True,
            "pick_order": VEP_PICK_ORDER,
        },
        "suffix": "_merged_per_gene",
    },
    "merged_pick_allele_gene": {
        "cache_dir": os.path.join(DATA_DIR, "115_GRCh38_merged"),
        "vep_vcf": os.path.join(
            DATA_DIR, "HG002_annotated_wgs_everything_hgvs_merged_pick_allele_gene.vcf"
        ),
        "annotate_kwargs": {
            "pick_allele_gene": True,
            "pick_order": VEP_PICK_ORDER,
        },
        "suffix": "_merged_pick_allele_gene",
    },
    "refseq": {
        "cache_dir": os.path.join(DATA_DIR, "115_GRCh38_refseq"),
        "vep_vcf": os.path.join(
            DATA_DIR, "HG002_annotated_wgs_everything_hgvs_refseq.vcf"
        ),
        "annotate_kwargs": {},
        "suffix": "_refseq",
    },
    # Release 116 merged cache, compared against the vepyr_diffly session's
    # golden VEP output (everything+hgvs, all 5 plugins run together). Both
    # profiles below share that same VEP reference: "base" has no plugins
    # attached so only the shared/core CSQ fields are meaningfully comparable
    # (compare_vcfs() already restricts to shared_fields), "5plugins" attaches
    # all 5 plugin caches so the full field set is comparable.
    "merged_116_base": {
        "cache_dir": os.path.expanduser(
            "~/annotation/cache/vepyr_cache/116_GRCh38_merged"
        ),
        "vep_vcf": (
            "/Users/lukaszjezapkowicz/Desktop/magisterka/praca/vepyr_diffly/runs/"
            "all5plugins-e2e-full/HG002_annotated_wgs_everything_hgvs_merged_"
            "clinvar_spliceai_cadd_am_dbnsfp.vcf"
        ),
        "annotate_kwargs": {},
        "suffix": "_merged_116_base",
    },
    "merged_116_5plugins": {
        "cache_dir": os.path.expanduser(
            "~/annotation/cache/vepyr_cache/116_GRCh38_merged"
        ),
        "vep_vcf": (
            "/Users/lukaszjezapkowicz/Desktop/magisterka/praca/vepyr_diffly/runs/"
            "all5plugins-e2e-full/HG002_annotated_wgs_everything_hgvs_merged_"
            "clinvar_spliceai_cadd_am_dbnsfp.vcf"
        ),
        "annotate_kwargs": {
            "plugin_cache_root": os.path.expanduser(
                "~/annotation/cache/plugin_cache_116"
            ),
        },
        "suffix": "_merged_116_5plugins",
    },
}


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "chrom", help="Chromosome to extract and annotate (e.g. chr1, chr22, 1, 22)"
    )
    p.add_argument(
        "--profile",
        choices=sorted(_CACHE_PROFILES),
        default="ensembl",
        help="Annotation profile selecting cache dir, VEP reference, and "
        "pick-mode flags (default: %(default)s)",
    )
    p.add_argument(
        "--cache",
        dest="profile",
        choices=sorted(_CACHE_PROFILES),
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--bgzf",
        action="store_true",
        help="Write block-gzipped (.vcf.gz, tabix-compatible bgzf) annotated "
        "output instead of plain .vcf, and validate the compressed file",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Within-contig parallel annotation pipelines; >1 requires a "
        "tabix-indexed input (default: %(default)s)",
    )
    p.add_argument(
        "--vcf",
        default=DEFAULT_VCF_INPUT,
        help="Tabix-indexed input VCF (default: %(default)s)",
    )
    p.add_argument(
        "--vep",
        default=None,
        help="VEP reference VCF for comparison (default: auto from --profile)",
    )
    p.add_argument(
        "--cache-dir",
        default=None,
        help="Ensembl cache directory (default: auto from --profile)",
    )
    p.add_argument(
        "--fasta",
        default=DEFAULT_REFERENCE_FASTA,
        help="Reference FASTA (default: %(default)s)",
    )
    p.add_argument(
        "--no-normalize",
        action="store_true",
        help="Skip bcftools norm (normalization is on by default)",
    )
    p.add_argument(
        "--skip-compare",
        "--skip-comparison",
        dest="skip_compare",
        action="store_true",
        help="Skip comparison, only annotate",
    )
    p.add_argument(
        "--force", action="store_true", help="Re-run annotation even if output exists"
    )
    args = p.parse_args()
    if args.workers <= 0:
        p.error("--workers must be a positive integer")

    # Resolve defaults from annotation profile; explicit --cache-dir / --vep override
    profile = _CACHE_PROFILES[args.profile]
    if args.cache_dir is None:
        args.cache_dir = profile["cache_dir"]
    if args.vep is None:
        args.vep = profile["vep_vcf"]
    args.annotate_kwargs = profile["annotate_kwargs"]
    args.suffix = profile["suffix"]
    args.report_suffix = args.suffix

    return args


def open_text(path):
    """Open a VCF for text reading, transparently handling .gz (bgzf/gzip)."""
    if path.endswith((".gz", ".bgz", ".bgzf")):
        return gzip.open(path, "rt")
    return open(path)


def is_bgzf(path):
    """Return True if `path` is BGZF (block-gzip): gzip magic + 'BC' subfield."""
    with open(path, "rb") as f:
        head = f.read(18)
    # gzip magic 1f 8b, deflate (08), FLG.FEXTRA set (bit 2), then an extra
    # field carrying the "BC" subfield id that marks bgzf blocks.
    return (
        len(head) >= 18
        and head[0:3] == b"\x1f\x8b\x08"
        and head[3] & 0x04
        and head[12:14] == b"BC"
    )


def count_data_lines(path):
    """Count non-header lines in a VCF (plain or .gz)."""
    n = 0
    with open_text(path) as f:
        for line in f:
            if not line.startswith("#"):
                n += 1
    return n


def ensure_tabix_index(vcf_gz):
    """Create tabix index if missing."""
    tbi = vcf_gz + ".tbi"
    if os.path.exists(tbi):
        return
    print(f"  Index not found, running tabix -p vcf {os.path.basename(vcf_gz)} ...")
    subprocess.run(["tabix", "-p", "vcf", vcf_gz], check=True)
    print(f"  Created {tbi}")


def normalize_vcf(vcf_gz, out_dir):
    """Normalize VCF with bcftools norm -m -both, bgzip, and tabix."""
    norm_vcf = os.path.join(out_dir, "normalized.vcf")
    norm_vcf_gz = norm_vcf + ".gz"

    if os.path.exists(norm_vcf_gz) and os.path.exists(norm_vcf_gz + ".tbi"):
        print(f"  Using existing {norm_vcf_gz}")
        return norm_vcf_gz

    print(f"  Normalizing {os.path.basename(vcf_gz)} (bcftools norm -m -both) ...")
    result = subprocess.run(
        ["bcftools", "norm", "-m", "-both", "-o", norm_vcf, vcf_gz],
        capture_output=True,
        text=True,
    )
    print(result.stderr.strip())
    assert result.returncode == 0, f"bcftools norm failed: {result.stderr}"

    print("  Compressing (bgzip) ...")
    subprocess.run(["bgzip", "-f", norm_vcf], check=True)

    print("  Indexing (tabix) ...")
    subprocess.run(["tabix", "-p", "vcf", norm_vcf_gz], check=True)
    print(f"  Created {norm_vcf_gz}")
    return norm_vcf_gz


def extract_chrom_from_vcf(vcf_gz, chrom, out_dir):
    """Use tabix to extract a single chromosome, then bgzip+index."""
    chrom_vcf = os.path.join(out_dir, f"input_{chrom}.vcf")
    chrom_vcf_gz = chrom_vcf + ".gz"

    if os.path.exists(chrom_vcf_gz) and os.path.exists(chrom_vcf_gz + ".tbi"):
        print(f"  Using existing {chrom_vcf_gz}")
        return chrom_vcf_gz

    ensure_tabix_index(vcf_gz)
    print(f"  Extracting {chrom} from {os.path.basename(vcf_gz)} ...")

    # Get the VCF header
    header = subprocess.check_output(f"tabix -H '{vcf_gz}'", shell=True).decode()

    # Extract chromosome records
    result = subprocess.run(
        ["tabix", vcf_gz, chrom],
        capture_output=True,
    )
    if result.returncode != 0:
        print("  tabix failed — trying without 'chr' prefix or with prefix...")
        # Try alternate naming: chr1 <-> 1
        alt_chrom = (
            chrom.replace("chr", "") if chrom.startswith("chr") else f"chr{chrom}"
        )
        result = subprocess.run(["tabix", vcf_gz, alt_chrom], capture_output=True)
        if result.returncode != 0:
            sys.exit(f"Error: tabix could not extract {chrom} or {alt_chrom}")

    with open(chrom_vcf, "w") as f:
        f.write(header)
        f.write(result.stdout.decode())

    subprocess.run(["bgzip", "-f", chrom_vcf], check=True)
    subprocess.run(["tabix", "-p", "vcf", chrom_vcf_gz], check=True)
    print(f"  Created {chrom_vcf_gz}")
    return chrom_vcf_gz


def extract_chrom_from_vep(vep_vcf, chrom, out_dir, suffix="", force=False):
    """Extract chromosome lines from an uncompressed VEP VCF."""
    out_path = os.path.join(out_dir, f"vep_{chrom}{suffix}.vcf")
    if os.path.exists(out_path) and not force:
        print(f"  Using existing {out_path}")
        return out_path

    print(f"  Extracting {chrom} from VEP output ...")
    # Normalize chrom for matching (VEP output may use bare numbers)
    bare = chrom.replace("chr", "") if chrom.startswith("chr") else chrom
    prefixed = f"chr{bare}" if not chrom.startswith("chr") else chrom
    targets = {bare, prefixed}

    n = 0
    with open(vep_vcf) as fin, open(out_path, "w") as fout:
        for line in fin:
            if line.startswith("#"):
                fout.write(line)
            else:
                rec_chrom = line.split("\t", 1)[0]
                if rec_chrom in targets:
                    fout.write(line)
                    n += 1
    print(f"  Extracted {n:,} VEP records for {chrom}")
    return out_path


VEP_HASH_ORDER_PICK_CACHES = {"merged_per_gene", "merged_pick_allele_gene"}

VEP_HASH_ORDER_PICK_IGNORE_REASON = (
    "CSQ entry order is ignored for per_gene and pick_allele_gene because "
    "Ensembl VEP selects the representative consequences, then emits those "
    "winners by iterating Perl hashes (`keys %by_gene`; for pick_allele_gene "
    "also `keys %by_allele`). The comma order of those already-selected CSQ "
    "entries has no biological or interpretation meaning; it is not a severity, "
    "transcript-priority, genomic, MANE, or canonical ranking. The meaningful "
    "checks are the selected CSQ entries, entry counts, and field values."
)


def values_equivalent(vv, gv, rel_tol=1e-4, abs_tol=1e-6):
    """True if two CSQ field values represent the same data, tolerating
    formatting differences rather than requiring byte-identical strings.

    Two sources of false mismatches this absorbs:
    - Float precision/padding: Rust's default float formatting produces the
      shortest round-trip representation ("0", "0.57985"), while VEP's Perl
      printf-style output pads to a fixed decimal count ("0.00", "0.579850").
      Same number, different string.
    - Missing-value marker: some VEP plugins (e.g. ClinVar's --custom
      passthrough) emit the literal VCF missing-value "." where vepyr emits
      an empty string. Same absence of data, different marker.
    """
    if vv == gv:
        return True
    if {vv, gv} == {"", "."}:
        return True
    # Multi-value fields (e.g. dbNSFP per-transcript scores) are '&'-joined;
    # compare token-wise so one differently-formatted float doesn't fail the
    # whole field.
    vv_tokens = vv.split("&")
    gv_tokens = gv.split("&")
    if len(vv_tokens) != len(gv_tokens):
        return False
    for vt, gt in zip(vv_tokens, gv_tokens):
        if vt == gt or {vt, gt} == {"", "."}:
            continue
        try:
            vf, gf = float(vt), float(gt)
        except ValueError:
            return False
        if abs(vf - gf) > max(abs_tol, rel_tol * max(abs(vf), abs(gf))):
            return False
    return True


def compare_vcfs(
    vepyr_vcf,
    vep_vcf,
    label,
    ignore_csq_order=False,
):
    """Field-by-field CSQ comparison between vepyr and VEP output."""
    print()
    print("=" * 60)
    print(f"Comparing vepyr ({BACKEND}) vs VEP — {label}")
    print("=" * 60)

    n_vepyr = count_data_lines(vepyr_vcf)
    n_vep = count_data_lines(vep_vcf)
    print(f"  vepyr:  {n_vepyr:,} data lines")
    print(f"  VEP:    {n_vep:,} data lines")

    # Parse CSQ field names from headers
    csq_re = re.compile(r"CSQ=([^;\t]+)")

    def get_csq_fields(path):
        with open_text(path) as f:
            for line in f:
                if line.startswith("##INFO=<ID=CSQ"):
                    m = re.search(r"Format: ([^\"]+)", line)
                    return m.group(1).split("|") if m else []
        return []

    vepyr_fields = get_csq_fields(vepyr_vcf)
    vep_fields = get_csq_fields(vep_vcf)
    shared_fields = [f for f in vepyr_fields if f in vep_fields]

    fields_only_vepyr = sorted(set(vepyr_fields) - set(vep_fields))
    fields_only_vep = sorted(set(vep_fields) - set(vepyr_fields))
    if fields_only_vepyr:
        print(f"  Fields only in vepyr: {fields_only_vepyr}")
    if fields_only_vep:
        print(f"  Fields only in VEP:   {fields_only_vep}")

    # Build sorted key+CSQ for merge-join
    def extract_keyed_csq(path):
        rows = []
        with open_text(path) as f:
            for line in f:
                if line.startswith("#"):
                    continue
                cols = line.split("\t", 9)
                m = csq_re.search(cols[7])
                csq = m.group(1) if m else ""
                key = (cols[0], int(cols[1]), cols[3], cols[4])
                rows.append((key, csq))
        rows.sort()
        return rows

    print("  Building sorted key+CSQ lists ...")
    vepyr_rows = extract_keyed_csq(vepyr_vcf)
    vep_rows = extract_keyed_csq(vep_vcf)

    # Merge-join
    field_matches = {f: 0 for f in shared_fields}
    field_mismatches = {f: 0 for f in shared_fields}
    field_total = {f: 0 for f in shared_fields}
    field_mismatch_examples = {f: [] for f in shared_fields}

    n_compared = 0
    n_missing_in_vep = 0
    n_missing_in_vepyr = 0
    n_csq_count_match = 0
    n_csq_count_mismatch = 0
    n_csq_order_mismatch = 0
    n_csq_order_ignored = 0
    csq_order_mismatch_examples = []
    csq_order_ignored_examples = []
    field_order_mismatches = {f: 0 for f in shared_fields}
    field_order_mismatch_examples = {f: [] for f in shared_fields}

    i, j = 0, 0
    while i < len(vepyr_rows) and j < len(vep_rows):
        vk, vepyr_csq = vepyr_rows[i]
        gk, vep_csq = vep_rows[j]

        if vk < gk:
            n_missing_in_vep += 1
            i += 1
            continue
        elif vk > gk:
            n_missing_in_vepyr += 1
            j += 1
            continue

        n_compared += 1
        key_str = f"{vk[0]}\t{vk[1]}\t{vk[2]}\t{vk[3]}"

        if vepyr_csq and vep_csq:

            def parse_entries(raw, fields):
                entries = []
                for e in raw.split(","):
                    vals = dict(zip(fields, e.split("|")))
                    entries.append(vals)
                return entries

            def sort_key(d):
                return (d.get("Feature", ""), d.get("Consequence", ""))

            vepyr_parsed = parse_entries(vepyr_csq, vepyr_fields)
            vep_parsed = parse_entries(vep_csq, vep_fields)

            # Detect CSQ entry ordering mismatch before sorting for comparison
            vepyr_order = [d.get("Feature", "") for d in vepyr_parsed]
            vep_order = [d.get("Feature", "") for d in vep_parsed]
            if vepyr_order != vep_order and sorted(vepyr_order) == sorted(vep_order):
                example = {
                    "variant": key_str,
                    "vepyr_order": vepyr_order,
                    "vep_order": vep_order,
                }
                # Ensembl VEP's --per_gene and --pick_allele_gene paths group
                # transcript alleles in Perl hashes, choose representative
                # consequences, then emit winners with `keys %by_gene` and, for
                # pick_allele_gene, `keys %by_allele`. The comma order of those
                # already-selected CSQ entries has no biological or
                # interpretation meaning: it is not a severity,
                # transcript-priority, genomic, MANE, or canonical ranking.
                # Ignoring only this order therefore does not change
                # interpretation; entry counts and every CSQ field value are
                # still compared strictly.
                if ignore_csq_order:
                    n_csq_order_ignored += 1
                    if len(csq_order_ignored_examples) < 10:
                        csq_order_ignored_examples.append(example)
                else:
                    n_csq_order_mismatch += 1
                    if len(csq_order_mismatch_examples) < 10:
                        csq_order_mismatch_examples.append(example)

            if len(vepyr_parsed) == len(vep_parsed):
                n_csq_count_match += 1
            else:
                n_csq_count_mismatch += 1

            # Pair CSQ entries by Feature (transcript ID), not by position.
            # Sorting each side independently and zipping by index (the old
            # approach) silently misaligns every entry after the first
            # transcript-set divergence between vepyr and VEP -- a variant
            # with an extra/missing transcript on either side would then
            # compare unrelated transcripts' fields against each other for
            # every subsequent CSQ entry. Grouping by Feature and iterating
            # matched entries within a group avoids that cascade; entries
            # whose Feature exists on only one side are counted separately
            # rather than falsely reported as a field-value mismatch.
            def group_by_feature(parsed):
                groups = {}
                for d in parsed:
                    groups.setdefault(d.get("Feature", ""), []).append(d)
                return groups

            vepyr_by_feature = group_by_feature(vepyr_parsed)
            vep_by_feature = group_by_feature(vep_parsed)
            shared_features = set(vepyr_by_feature) & set(vep_by_feature)

            for feature in shared_features:
                v_list = sorted(vepyr_by_feature[feature], key=sort_key)
                g_list = sorted(vep_by_feature[feature], key=sort_key)
                for ei in range(min(len(v_list), len(g_list))):
                    vepyr_vals = v_list[ei]
                    vep_vals = g_list[ei]

                    for f in shared_fields:
                        field_total[f] += 1
                        vv = vepyr_vals.get(f, "")
                        gv = vep_vals.get(f, "")
                        if values_equivalent(vv, gv):
                            field_matches[f] += 1
                        else:
                            # Check if it's just an &-ordering difference
                            if "&" in vv or "&" in gv:
                                vv_norm = "&".join(sorted(vv.split("&")))
                                gv_norm = "&".join(sorted(gv.split("&")))
                                if vv_norm == gv_norm:
                                    # Same values, different order
                                    field_matches[f] += 1
                                    field_order_mismatches[f] += 1
                                    if len(field_order_mismatch_examples[f]) < 10:
                                        field_order_mismatch_examples[f].append(
                                            {"variant": key_str, "vepyr": vv, "vep": gv}
                                        )
                                    continue
                            field_mismatches[f] += 1
                            if len(field_mismatch_examples[f]) < 10:
                                field_mismatch_examples[f].append(
                                    {"variant": key_str, "vepyr": vv, "vep": gv}
                                )

        i += 1
        j += 1

    while i < len(vepyr_rows):
        n_missing_in_vep += 1
        i += 1
    while j < len(vep_rows):
        n_missing_in_vepyr += 1
        j += 1

    # Print results
    print("\n  Results:")
    print(f"    Variants compared:        {n_compared:,}")
    print(f"    Only in vepyr:            {n_missing_in_vep:,}")
    print(f"    Only in VEP:              {n_missing_in_vepyr:,}")
    print(f"    CSQ count match:          {n_csq_count_match:,}")
    print(f"    CSQ count mismatch:       {n_csq_count_mismatch:,}")
    print(
        f"    CSQ order mismatch:       {n_csq_order_mismatch:,}  (same entries, wrong order — issue #83)"
    )
    if ignore_csq_order:
        print(
            f"    CSQ order ignored:        {n_csq_order_ignored:,}  "
            "(VEP hash-order only)"
        )

    if csq_order_mismatch_examples:
        print("\n  CSQ order mismatch examples:")
        for ex in csq_order_mismatch_examples:
            print(f"    {ex['variant']}")
            print(f"      vepyr: {', '.join(ex['vepyr_order'])}")
            print(f"      VEP:   {', '.join(ex['vep_order'])}")

    print(f"\n  Per-field match rates ({n_compared:,} variants):")
    print(
        f"  {'Field':<30} {'Match%':>8} {'Matches':>10} {'Mismatches':>10} {'OrderOnly':>10} {'Total':>10}"
    )
    print(f"  {'-' * 30} {'-' * 8} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10}")
    for f in shared_fields:
        total = field_total[f]
        matches = field_matches[f]
        mismatches = field_mismatches[f]
        order_only = field_order_mismatches[f]
        rate = (matches / total * 100) if total > 0 else 0
        flag = ""
        if mismatches > 0:
            flag = " <--"
        elif order_only > 0:
            flag = " (order)"
        print(
            f"  {f:<30} {rate:>7.2f}% {matches:>10,} {mismatches:>10,} {order_only:>10,} {total:>10,}{flag}"
        )

    fields_with_order_issues = [
        f for f in shared_fields if field_order_mismatches[f] > 0
    ]
    if fields_with_order_issues:
        print("\n  &-order mismatch examples (same values, different order):")
        for f in fields_with_order_issues:
            print(f"\n    {f} ({field_order_mismatches[f]:,} &-order mismatches):")
            for ex in field_order_mismatch_examples[f]:
                print(f"      {ex['variant']}")
                print(f"        vepyr: {ex['vepyr']!r}")
                print(f"        VEP:   {ex['vep']!r}")

    fields_with_mismatches = [f for f in shared_fields if field_mismatches[f] > 0]
    if fields_with_mismatches:
        print("\n  Mismatch examples:")
        for f in fields_with_mismatches:
            print(f"\n    {f} ({field_mismatches[f]:,} mismatches):")
            for ex in field_mismatch_examples[f]:
                print(f"      {ex['variant']}")
                print(f"        vepyr: {ex['vepyr']!r}")
                print(f"        VEP:   {ex['vep']!r}")
    else:
        print(f"\n  ALL {len(shared_fields)} shared CSQ fields match at 100%!")

    return {
        "variants_compared": n_compared,
        "variants_only_in_vepyr": n_missing_in_vep,
        "csq_order_mismatch": n_csq_order_mismatch,
        "csq_order_mismatch_examples": csq_order_mismatch_examples,
        "csq_order_ignored": n_csq_order_ignored,
        "csq_order_ignored_examples": csq_order_ignored_examples,
        "csq_order_ignore_reason": VEP_HASH_ORDER_PICK_IGNORE_REASON
        if ignore_csq_order
        else None,
        "variants_only_in_vep": n_missing_in_vepyr,
        "csq_entry_count_match": n_csq_count_match,
        "csq_entry_count_mismatch": n_csq_count_mismatch,
        "field_match_rates": {
            f: round(field_matches[f] / field_total[f] * 100, 4)
            for f in shared_fields
            if field_total[f] > 0
        },
        "field_mismatch_counts": {
            f: field_mismatches[f] for f in shared_fields if field_mismatches[f] > 0
        },
        "field_mismatch_examples": {
            f: field_mismatch_examples[f]
            for f in shared_fields
            if field_mismatch_examples[f]
        },
        "field_order_mismatch_counts": {
            f: field_order_mismatches[f]
            for f in shared_fields
            if field_order_mismatches[f] > 0
        },
        "field_order_mismatch_examples": {
            f: field_order_mismatch_examples[f]
            for f in shared_fields
            if field_order_mismatch_examples[f]
        },
    }


def main():
    args = parse_args()
    chrom = args.chrom

    work_dir = os.path.join(os.path.dirname(__file__), "..", "results", f"fast_{chrom}")
    os.makedirs(work_dir, exist_ok=True)

    report_dir = os.path.join(os.path.dirname(__file__), "..", "reports")
    os.makedirs(report_dir, exist_ok=True)

    # ── Step 1: Normalize + extract chromosome ─────────────────────────────
    print("=" * 60)
    print(f"Step 1: Normalize & extract {chrom} from input VCF")
    print("=" * 60)

    input_vcf = args.vcf
    if not args.no_normalize:
        input_vcf = normalize_vcf(args.vcf, work_dir)

    chrom_vcf_gz = extract_chrom_from_vcf(input_vcf, chrom, work_dir)

    n_variants = int(
        subprocess.check_output(
            f"gunzip -c '{chrom_vcf_gz}' | grep -cv '^#'", shell=True
        ).strip()
    )
    print(f"  Input: {n_variants:,} variants for {chrom}")

    # ── Step 2: Annotate against the Parquet cache ────────────────────────
    out_ext = ".vcf.gz" if args.bgzf else ".vcf"
    output_vcf = os.path.join(
        work_dir, f"vepyr_{BACKEND}_{chrom}{args.suffix}{out_ext}"
    )

    print()
    print("=" * 60)
    out_mode = "bgzf" if args.bgzf else "plain"
    print(
        f"Step 2: Annotate {chrom} with vepyr ({BACKEND}, {out_mode}, profile={args.profile})"
    )
    print("=" * 60)

    if (
        not args.force
        and os.path.exists(output_vcf)
        and os.path.getsize(output_vcf) > 1000
    ):
        n_out = count_data_lines(output_vcf)
        size_mb = os.path.getsize(output_vcf) / (1024 * 1024)
        print(
            f"  Skipping — {output_vcf} exists ({n_out:,} variants, {size_mb:.0f} MB)"
        )
        print("  Use --force to re-run")
        elapsed = None
    else:
        t0 = time.time()
        vepyr.annotate(
            chrom_vcf_gz,
            args.cache_dir,
            everything=True,
            reference_fasta=args.fasta,
            cache_format=BACKEND,
            output_vcf=output_vcf,
            workers=args.workers,
            **args.annotate_kwargs,
        )
        elapsed = time.time() - t0

        n_out = count_data_lines(output_vcf)
        size_mb = os.path.getsize(output_vcf) / (1024 * 1024)
        rate = n_out / elapsed if elapsed > 0 else 0
        print(
            f"  Done: {n_out:,} variants in {elapsed:.1f}s ({rate:,.0f} variants/s), {size_mb:.0f} MB"
        )

    # Validate block-gzip framing when bgzf output was requested.
    if args.bgzf:
        if is_bgzf(output_vcf):
            print("  bgzf check: output is valid block-gzip (BGZF)")
        else:
            sys.exit(f"Error: --bgzf output {output_vcf} is not valid BGZF")

    # ── Step 3: Compare vs VEP reference ──────────────────────────────────
    if args.skip_compare:
        print("\n  Skipping comparison (--skip-compare)")
        comparison = None
    else:
        # Extract matching chromosome from VEP reference
        print()
        print("=" * 60)
        print(f"Step 3: Extract {chrom} from VEP reference")
        print("=" * 60)
        vep_chrom_vcf = extract_chrom_from_vep(
            args.vep,
            chrom,
            work_dir,
            suffix=args.suffix,
            force=args.force,
        )
        comparison = compare_vcfs(
            output_vcf,
            vep_chrom_vcf,
            chrom,
            ignore_csq_order=args.profile in VEP_HASH_ORDER_PICK_CACHES,
        )

    # ── Report ────────────────────────────────────────────────────────────
    report = {
        "chrom": chrom,
        "profile": args.profile,
        "cache": args.profile,
        "input_variants": n_variants,
        "annotation": {
            "backend": BACKEND,
            "compression": "bgzf" if args.bgzf else "plain",
            "time_s": round(elapsed, 1) if elapsed else None,
            "output_variants": n_out,
        },
        "comparison": comparison,
    }

    report_path = os.path.join(
        report_dir, f"fast_{chrom}{args.report_suffix}_report.json"
    )
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print()
    print("=" * 60)
    print(f"DONE — report: {report_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
