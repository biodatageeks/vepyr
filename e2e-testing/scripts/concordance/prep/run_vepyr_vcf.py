#!/usr/bin/env python3
"""Run vepyr on one VCF and write annotated VCF output."""

from __future__ import annotations

import argparse
import time

import vepyr


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_vcf")
    parser.add_argument("cache_dir")
    parser.add_argument("fasta")
    parser.add_argument("output_vcf")
    parser.add_argument("--backend", choices=["fjall", "parquet"], default="fjall")
    parser.add_argument("--profile", choices=["vep", "merged", "refseq"], default="merged")
    parser.add_argument("--no-everything", action="store_false", dest="everything", default=True)
    parser.add_argument("--no-hgvs", action="store_false", dest="hgvs", default=True)
    args = parser.parse_args()

    start = time.time()
    vepyr.annotate(
        args.input_vcf,
        args.cache_dir,
        everything=args.everything,
        hgvs=args.hgvs,
        reference_fasta=args.fasta,
        use_fjall=args.backend == "fjall",
        output_vcf=args.output_vcf,
    )
    print(f"Wrote {args.output_vcf} in {time.time() - start:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
