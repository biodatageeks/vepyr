"""Experiment B: VEP --fork N + filter_vep vs vepyr workers=N+1 -> filter -> output.

VEP side = median VEP annotation (Task 7 JSON, same mode and fork) + median
filter_vep on that fork's output (timed here). vepyr side = one process per
(query, path, W).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import repeat, write_json  # noqa: E402
from pibench.paths import FORK_TO_WORKERS, PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression  # noqa: E402

S = Path(__file__).resolve().parent
EXT = {"collect": "rows.txt", "vcf": "vcf", "parquet": "parquet"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    verify = json.loads((run_dir / "verify.json").read_text())
    tmp = WORK / "runs" / "B"
    tmp.mkdir(parents=True, exist_ok=True)
    for q in QUERIES:
        if q.id not in verify:
            print(f"{q.id}: excluded (not verified)")
            continue
        mode = "plugins" if q.plugins else "core"
        for fork, w in FORK_TO_WORKERS.items():
            annot_path = run_dir / "vep" / f"{mode}_fork{fork}.json"
            if not annot_path.exists():
                print(f"skipped: no VEP timing for {mode} fork{fork}")
            else:
                vep = WORK / "vep" / mode / f"fork{fork}" / "rep0.vcf"
                fv = repeat(
                    [
                        "bash",
                        str(S / "filter_vep_once.sh"),
                        str(vep),
                        str(tmp / f"{q.id}.fv.fork{fork}.vcf"),
                        filter_vep_expression(q),
                    ],
                    run_dir / "logs" / "B" / f"{q.id}_filter_vep_fork{fork}",
                    repeats=a.repeats,
                )
                annot = json.loads(annot_path.read_text())
                res = {
                    "query": q.id,
                    "tool": "vep",
                    "fork": fork,
                    "processes": fork + 1,
                    "annotate_median_wall_s": annot["median_wall_s"],
                    "filter": fv,
                    "median_wall_s": annot["median_wall_s"] + fv["median_wall_s"],
                }
                write_json(run_dir / "B" / f"{q.id}_vep_fork{fork}.json", res)
                print(
                    f"{q.id} vep fork{fork}: {res['median_wall_s']:.2f}s",
                    flush=True,
                )
            for path in ("collect", "vcf", "parquet"):
                if not verify[q.id]["pass"][f"B_{path}"]:
                    continue
                out = tmp / f"{q.id}.{path}.w{w}.{EXT[path]}"
                cmd = [
                    sys.executable,
                    str(S / "vepyr_once.py"),
                    "--query",
                    q.id,
                    "--path",
                    path,
                    "--workers",
                    str(w),
                    "--out",
                    str(out),
                ]
                res = repeat(
                    cmd,
                    run_dir / "logs" / "B" / f"{q.id}_{path}_w{w}",
                    repeats=a.repeats,
                )
                res.update(query=q.id, tool="vepyr", path=path, workers=w, processes=w)
                write_json(run_dir / "B" / f"{q.id}_{path}_w{w}.json", res)
                print(f"{q.id} {path} w{w}: {res['median_wall_s']:.2f}s", flush=True)


if __name__ == "__main__":
    main()
