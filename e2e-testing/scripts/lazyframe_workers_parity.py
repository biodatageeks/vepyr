#!/usr/bin/env python3
"""LazyFrame workers>1 parity gate and sweep on real data.

For a contig slice of the HG002 benchmark VCF and each cache profile, the
reference is `annotate(slice, workers=1).collect()`; every candidate
`annotate(slice, workers=N).collect()` must equal it, row order included.
With the CSQ column on, the LazyFrame CSQ strings are also compared to the
INFO/CSQ field of an `output_vcf` run at the same worker count, row by row.
Wall time and peak RSS per worker count are written to the report.

Examples:
    lazyframe_workers_parity.py --release 116
    lazyframe_workers_parity.py --release 116 --chrom 1 --sweep 1 2 4 8 --profiles ensembl merged
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


from comparison import profiles, vcfio  # noqa: E402

INPUT_NAME = "HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
FASTA_NAME = "Homo_sapiens.GRCh38.dna.primary_assembly.fa"
DEFAULT_PROFILES = tuple(
    name for name, profile in profiles.PROFILES.items() if profile.flavour == name
)


def peak_rss_gb():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux kilobytes.
    return rss / 1e9 if sys.platform == "darwin" else rss / 1e6


def timed(fn):
    t = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - t


def profile_annotate_kwargs(profile, release):
    spec = profiles.PROFILES[profile]
    kwargs = dict(spec.annotate_kwargs)
    if spec.plugins:
        kwargs["plugin_cache_root"] = profiles.plugin_cache_dir_for(release)
        kwargs["plugins"] = list(spec.plugins)
    return kwargs


def vcf_csq_by_row(path):
    """INFO/CSQ per data line, in file order ('' when absent)."""
    out = []
    with vcfio.open_text(path) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            info = line.rstrip("\n").split("\t")[7]
            csq = ""
            for item in info.split(";"):
                if item.startswith("CSQ="):
                    csq = item[len("CSQ=") :]
                    break
            out.append(csq)
    return out


def run_profile(vepyr, profile, release, slice_gz, fasta, sweep, out_dir):
    cache_dir = profiles.cache_dir_for(profile, release)
    kwargs = profile_annotate_kwargs(profile, release)

    def lazy(workers, **extra):
        return vepyr.annotate(
            slice_gz,
            cache_dir,
            everything=True,
            reference_fasta=fasta,
            workers=workers,
            **kwargs,
            **extra,
        )

    rows = []
    ok = True
    reference, ref_s = timed(lambda: lazy(1).collect())
    rows.append(
        {
            "profile": profile,
            "workers": 1,
            "rows": reference.height,
            "wall_s": round(ref_s, 2),
            "peak_rss_gb": round(peak_rss_gb(), 2),
            "equal": True,
        }
    )
    for workers in sorted(set(sweep) - {1}):
        frame, wall = timed(lambda w=workers: lazy(w).collect())
        equal = frame.equals(reference)
        ok &= equal
        rows.append(
            {
                "profile": profile,
                "workers": workers,
                "rows": frame.height,
                "wall_s": round(wall, 2),
                "peak_rss_gb": round(peak_rss_gb(), 2),
                "equal": equal,
            }
        )
    # CSQ string parity against the VCF sink at the largest worker count.
    workers = max(sweep)
    with_csq = lazy(workers, skip_csq=False).collect()
    vcf_out = os.path.join(out_dir, f"{profile}_w{workers}.vcf.gz")
    vepyr.annotate(
        slice_gz,
        cache_dir,
        everything=True,
        reference_fasta=fasta,
        workers=workers,
        output_vcf=vcf_out,
        show_progress=False,
        **kwargs,
    )
    vcf_csq = vcf_csq_by_row(vcf_out)
    lf_csq = [c or "" for c in with_csq["CSQ"].to_list()]
    csq_equal = vcf_csq == lf_csq
    ok &= csq_equal
    rows.append(
        {
            "profile": profile,
            "workers": workers,
            "rows": with_csq.height,
            "wall_s": None,
            "peak_rss_gb": None,
            "equal": csq_equal,
            "check": "CSQ vs output_vcf",
        }
    )
    return ok, rows


def write_report(out_dir, rows):
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "report.json"), "w") as f:
        json.dump(rows, f, indent=2)
    lines = [
        "| profile | workers | rows | wall s | peak RSS GB | equal | check |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['profile']} | {r['workers']} | {r['rows']} | {r['wall_s']} | "
            f"{r['peak_rss_gb']} | {r['equal']} | {r.get('check', 'frame vs workers=1')} |"
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
    p.add_argument("--sweep", nargs="+", type=int, default=[1, 4])
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
        "lazyframe_workers",
        args.release,
    )
    slice_dir = os.path.join(out_dir, "input")
    contig = vcfio.canonical_contig(args.chrom)
    slice_gz = vcfio.slice_contig(vcf, contig, slice_dir)
    if 1 not in args.sweep:
        args.sweep.append(1)

    all_ok = True
    rows = []
    for profile in args.profiles:
        ok, profile_rows = run_profile(
            vepyr, profile, args.release, slice_gz, fasta, args.sweep, out_dir
        )
        all_ok &= ok
        rows.extend(profile_rows)
    write_report(out_dir, rows)
    print("PASS" if all_ok else "FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
