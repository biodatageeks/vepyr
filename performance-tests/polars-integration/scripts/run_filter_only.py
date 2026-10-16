"""Experiment A: filter_vep vs Polars over the same serial-VEP VCF, one process each."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import repeat, write_json  # noqa: E402
from pibench.parity import vcf_keys  # noqa: E402
from pibench.paths import PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression  # noqa: E402

S = Path(__file__).resolve().parent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    verify = json.loads((run_dir / "verify.json").read_text())
    tmp = WORK / "runs" / "A"
    tmp.mkdir(parents=True, exist_ok=True)
    for q in QUERIES:
        if q.id not in verify:
            print(f"{q.id}: excluded (not verified)")
            continue
        if not verify[q.id]["pass"]["A"]:
            print(f"{q.id}: excluded (parity)")
            continue
        vep = WORK / "vep" / ("plugins" if q.plugins else "core") / "fork0" / "rep0.vcf"
        tools = {
            "filter_vep": [
                "bash",
                str(S / "filter_vep_once.sh"),
                str(vep),
                str(tmp / f"{q.id}.fv.vcf"),
                filter_vep_expression(q),
            ],
            "polars": [
                sys.executable,
                str(S / "polars_filter_once.py"),
                "--vcf",
                str(vep),
                "--query",
                q.id,
                "--out",
                str(tmp / f"{q.id}.pl.vcf"),
            ],
        }
        for tool, cmd in tools.items():
            res = repeat(
                cmd, run_dir / "logs" / "A" / f"{q.id}_{tool}", repeats=a.repeats
            )
            out = tmp / f"{q.id}.{'fv' if tool == 'filter_vep' else 'pl'}.vcf"
            res.update(query=q.id, tool=tool, rows=len(vcf_keys(out)))
            write_json(run_dir / "A" / f"{q.id}_{tool}.json", res)
            print(
                f"{q.id} {tool}: {res['median_wall_s']:.2f}s rows={res['rows']}",
                flush=True,
            )


if __name__ == "__main__":
    main()
