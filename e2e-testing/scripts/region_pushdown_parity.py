#!/usr/bin/env python3
"""Region pushdown parity gate on real data.

For a contig slice of the HG002 benchmark VCF and each cache profile, the
reference is `annotate(slice).collect()` filtered in Polars; the candidate is
`annotate(slice).filter(p).collect()` with the predicate pushed down. Frames
must be identical (row order included). One region is repeated on an
unindexed copy to check the warning and equality on the sequential path.

Examples:
    region_pushdown_parity.py --release 116
    region_pushdown_parity.py --release 116 --profiles ensembl merged --chrom 22
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import warnings

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import polars as pl
from comparison import profiles, vcfio

INPUT_NAME = "HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
FASTA_NAME = "Homo_sapiens.GRCh38.dna.primary_assembly.fa"
# One profile per cache flavour; the other profiles reuse these caches with
# different pick flags and would repeat the same work.
DEFAULT_PROFILES = tuple(
    name for name, profile in profiles.PROFILES.items() if profile.flavour == name
)
# Default regions, written against the selected contig (`{c}`): a 5 Mb range,
# a 100 kb range, two disjoint ranges in one predicate, and an open-ended tail.
DEFAULT_REGION_TEMPLATES = [
    "{c}:20000000-25000000",
    "{c}:30000000-30100000",
    "{c}:17000000-17500000,{c}:40000000-40200000",
    "{c}:45000000-",
]


def parse_region_list(text):
    """'chr22:a-b,chr22:c-' -> Polars predicate as an OR of (chrom & range)."""
    predicate = None
    for item in text.split(","):
        chrom, _, span = item.partition(":")
        clause = pl.col("chrom") == chrom
        if span:
            lo, _, hi = span.partition("-")
            if lo:
                clause = clause & (pl.col("start") >= int(lo))
            if hi:
                clause = clause & (pl.col("start") <= int(hi))
        predicate = clause if predicate is None else (predicate | clause)
    return predicate


def timed(fn):
    t = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - t


def plain_copy(slice_gz, out_dir):
    plain = os.path.join(out_dir, os.path.basename(slice_gz)[: -len(".gz")])
    if not os.path.exists(plain):
        with open(plain, "wb") as f:
            subprocess.run(["bgzip", "-dc", slice_gz], stdout=f, check=True)
    for idx in (".tbi", ".csi"):
        if os.path.exists(plain + idx):
            os.remove(plain + idx)
    return plain


def profile_annotate_kwargs(profile, release):
    """The profile's own annotation options (pick flags, plugins), so a
    non-default profile is tested with the settings its name promises."""
    spec = profiles.PROFILES[profile]
    kwargs = dict(spec.annotate_kwargs)
    if spec.plugins:
        kwargs["plugin_cache_root"] = profiles.plugin_cache_dir_for(release)
        kwargs["plugins"] = list(spec.plugins)
    return kwargs


def run_profile(vepyr, profile, release, slice_gz, plain_vcf, fasta, region_texts):
    cache_dir = profiles.cache_dir_for(profile, release)
    kwargs = profile_annotate_kwargs(profile, release)
    lf = vepyr.annotate(
        slice_gz, cache_dir, everything=True, reference_fasta=fasta, **kwargs
    )
    full, full_s = timed(lf.collect)
    rows = []
    ok = True
    for text in region_texts:
        predicate = parse_region_list(text)
        reference = full.filter(predicate)
        pushed, pushed_s = timed(lambda p=predicate: lf.filter(p).collect())
        equal = pushed.equals(reference)
        ok &= equal
        rows.append(
            {
                "profile": profile,
                "region": text,
                "indexed": True,
                "rows": pushed.height,
                "reference_rows": reference.height,
                "full_s": round(full_s, 2),
                "pushed_s": round(pushed_s, 2),
                "equal": equal,
            }
        )
    # Unindexed leg: same result, plus the warning.
    predicate = parse_region_list(region_texts[0])
    reference = full.filter(predicate)
    lf_plain = vepyr.annotate(
        plain_vcf, cache_dir, everything=True, reference_fasta=fasta, **kwargs
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pushed, pushed_s = timed(lambda: lf_plain.filter(predicate).collect())
    warned = any("tabix/CSI index" in str(w.message) for w in caught)
    equal = pushed.equals(reference)
    ok &= equal and warned
    rows.append(
        {
            "profile": profile,
            "region": region_texts[0],
            "indexed": False,
            "rows": pushed.height,
            "reference_rows": reference.height,
            "full_s": round(full_s, 2),
            "pushed_s": round(pushed_s, 2),
            "equal": equal,
            "warned": warned,
        }
    )
    return ok, rows


def write_report(out_dir, rows):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "report.json"), "w") as f:
        json.dump(rows, f, indent=2)
    lines = [
        "| profile | region | indexed | rows | full s | pushed s | equal | warned |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['profile']} | {r['region']} | {r['indexed']} | {r['rows']} | "
            f"{r['full_s']} | {r['pushed_s']} | {r['equal']} | {r.get('warned', '')} |"
        )
    with open(os.path.join(out_dir, "report.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--release", required=True, choices=profiles.RELEASES)
    p.add_argument(
        "--profiles",
        nargs="+",
        default=list(DEFAULT_PROFILES),
        choices=sorted(profiles.PROFILES),
    )
    p.add_argument("--chrom", default="22")
    p.add_argument(
        "--regions",
        nargs="+",
        default=None,
        help="'chrom:lo-hi' items; comma-join several into one predicate "
        "(default: four regions on the selected --chrom)",
    )
    p.add_argument(
        "--input",
        default=None,
        help=f"Indexed benchmark VCF (default: $DATA/input/{INPUT_NAME})",
    )
    p.add_argument("--fasta", default=None)
    p.add_argument("--out-dir", default=None)
    args = p.parse_args(argv)

    import vepyr

    vcf = args.input or profiles.default_input(INPUT_NAME)
    fasta = args.fasta or profiles.default_input(FASTA_NAME)
    for path in (vcf, fasta):
        if not os.path.exists(path):
            raise SystemExit(f"missing input: {path}")
    if shutil.which("bgzip") is None or shutil.which("tabix") is None:
        raise SystemExit("bgzip and tabix are required")
    out_dir = args.out_dir or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "results",
        "region_pushdown",
        args.release,
    )
    slice_dir = os.path.join(out_dir, "input")
    contig = vcfio.canonical_contig(args.chrom)
    regions = args.regions or [t.format(c=contig) for t in DEFAULT_REGION_TEMPLATES]
    slice_gz = vcfio.slice_contig(vcf, contig, slice_dir)
    plain_vcf = plain_copy(slice_gz, slice_dir)

    all_ok = True
    rows = []
    for profile in args.profiles:
        ok, profile_rows = run_profile(
            vepyr, profile, args.release, slice_gz, plain_vcf, fasta, regions
        )
        all_ok &= ok
        rows.extend(profile_rows)
    write_report(out_dir, rows)
    print("PASS" if all_ok else "FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
