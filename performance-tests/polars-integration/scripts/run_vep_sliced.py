"""Region queries, VEP side as a user would run it: slice the window with bcftools,
annotate only the slice, then filter_vep. Timed as one workflow per (query, fork).

Complements run_e2e.py, whose VEP side annotates the whole chromosome and then filters.
"""

from __future__ import annotations

import argparse
import importlib.util
import shlex
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import repeat, write_json  # noqa: E402
from pibench.parity import vcf_keys  # noqa: E402
from pibench.paths import INPUT_VCF, PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression, panel_regions  # noqa: E402

S = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("run_vep", S / "run_vep.py")
run_vep = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_vep)

REGIONS = {
    "R1": [("chr22", 20_000_000, 25_000_000)],
    "R2": [("chr22", 30_000_000, 30_100_000)],
}


def regions_for(qid: str) -> list[tuple[str, int, int]]:
    return panel_regions() if qid == "R3" else REGIONS[qid]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--forks", nargs="+", type=int, default=[0, 1, 3, 7])
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    tmp = WORK / "runs" / "B_sliced"
    tmp.mkdir(parents=True, exist_ok=True)
    for q in (q for q in QUERIES if q.tier == "region"):
        # bcftools -t matches on POS alone, like the POS-range filter_vep expression.
        targets = ",".join(f"{c}:{s}-{e}" for c, s, e in regions_for(q.id))
        sliced = tmp / f"{q.id}.input.vcf.gz"
        for fork in a.forks:
            annotated = tmp / f"{q.id}.fork{fork}.vep.vcf"
            filtered = tmp / f"{q.id}.fork{fork}.fv.vcf"
            # vep_command reads the input path from the module; point it at the slice.
            run_vep.INPUT_VCF = sliced
            vep = shlex.join(run_vep.vep_command("core", fork, annotated))
            fv = shlex.join(
                [
                    "bash",
                    str(S / "filter_vep_once.sh"),
                    str(annotated),
                    str(filtered),
                    filter_vep_expression(q),
                ]
            )
            workflow = (
                f"bcftools view -t {targets} -Oz -o {sliced} {INPUT_VCF} && "
                f"tabix -f -p vcf {sliced} && {vep} && {fv}"
            )
            res = repeat(
                ["bash", "-c", workflow],
                run_dir / "logs" / "B_sliced" / f"{q.id}_fork{fork}",
                repeats=a.repeats,
            )
            res.update(
                query=q.id,
                tool="vep_sliced",
                fork=fork,
                processes=fork + 1,
                rows=len(vcf_keys(filtered)),
            )
            write_json(run_dir / "B_sliced" / f"{q.id}_vep_sliced_fork{fork}.json", res)
            print(
                f"{q.id} sliced fork{fork}: {res['median_wall_s']:.1f}s rows={res['rows']}",
                flush=True,
            )
        run_vep.INPUT_VCF = INPUT_VCF


if __name__ == "__main__":
    main()
