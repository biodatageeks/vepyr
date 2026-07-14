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
import json
import os
import subprocess
import sys
import time

import vepyr

# The CSQ comparator lives in the library, not in this script: the plugin-parity
# gate in vepyr-plugins runs the same comparison restricted to one plugin's
# fields. One implementation, two consumers.
from vepyr.parity import compare_vcfs, count_data_lines


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


# Profiles whose VEP reference emits already-selected CSQ entries in Perl hash
# order; see vepyr.parity.VEP_HASH_ORDER_PICK_IGNORE_REASON.
VEP_HASH_ORDER_PICK_CACHES = {"merged_per_gene", "merged_pick_allele_gene"}


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
            backend=BACKEND,
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
