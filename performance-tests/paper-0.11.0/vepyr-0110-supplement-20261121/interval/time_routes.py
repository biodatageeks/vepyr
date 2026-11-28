"""Wall time of the interval-overlap routes of supplement Table 12 (one warm-up, then five runs each).

Usage: time_routes.py <venv bin dir> <label>   (run in the directory with the quick-start files)
Routes: 1 polars-bio in one process; 2 vepyr annotate (4 workers) + bcftools + bedtools; 3 bcftools + bedtools on the
annotated VCF. Prints median and range and writes <label>.json next to this script.
"""

import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

bindir, label = sys.argv[1], sys.argv[2]
here = Path(__file__).resolve().parent
env = dict(os.environ, PATH=bindir + ":" + os.environ["PATH"])
PIPE = (
    "bcftools +split-vep {vcf} -d -c gnomADg_AF:Float "
    '-i \'(IMPACT="HIGH" || IMPACT="MODERATE") && (gnomADg_AF="." || gnomADg_AF<0.01)\' '
    "-f '%CHROM\\t%POS0\\t%END\\t%SYMBOL\\t%POS:%REF:%ALT\\n' | sort -u -k1,1 -k2,2n -k4,5 "
    "| bedtools intersect -u -a - -b GRCh38_segdups.bed.gz | cut -f4 | sort | uniq -c | sort -k1,1nr -k2,2"
)
ANN = (
    "vepyr annotate -i HG002_chr22.vcf.gz -o route2.annotated.vcf.gz --dir_cache vepyr_cache/116_GRCh38_ensembl "
    "--fasta chr22.fa --fork 4 >/dev/null 2>&1 && "
)
routes = {
    "polars-bio": f"python {here / 'route1.py'}",
    "vepyr+bcftools+bedtools": ANN + PIPE.format(vcf="route2.annotated.vcf.gz"),
    "bcftools+bedtools": PIPE.format(vcf="HG002_chr22.annotated.vcf.gz"),
}
out = {}
for name, cmd in routes.items():
    walls, last = [], ""
    for i in range(6):
        t = time.perf_counter()
        r = subprocess.run(
            ["bash", "-o", "pipefail", "-c", cmd],
            env=env,
            capture_output=True,
            text=True,
        )
        w = time.perf_counter() - t
        if r.returncode != 0:
            sys.exit(f"{name} failed: {r.stderr[-500:]}")
        last = " ".join(r.stdout.split())
        if i:
            walls.append(w)
    out[name] = {
        "median": statistics.median(walls),
        "min": min(walls),
        "max": max(walls),
        "runs": walls,
        "result": last[-80:],
    }
    print(
        f"{label} {name}: median {out[name]['median']:.2f} s, range {min(walls):.2f}-{max(walls):.2f} | {last[-60:]}",
        flush=True,
    )
json.dump(out, open(here / f"{label}.json", "w"), indent=1)
