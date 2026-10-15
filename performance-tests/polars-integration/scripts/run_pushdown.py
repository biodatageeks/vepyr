"""Experiment C: what pushdown saves. W=1 and W=8.

Region (R1-R3): pushdown on vs off, on all three output paths.
Projection (Q*, P*): full frame vs narrow select, on collect and parquet only
(sink_vcf needs CSQ, which needs every flag).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import repeat, write_json  # noqa: E402
from pibench.paths import PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES  # noqa: E402

S = Path(__file__).resolve().parent
EXT = {"collect": "rows.txt", "vcf": "vcf", "parquet": "parquet"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--workers", nargs="+", type=int, default=[1, 8])
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    verify = json.loads((run_dir / "verify.json").read_text())
    tmp = WORK / "runs" / "C"
    tmp.mkdir(parents=True, exist_ok=True)
    for q in QUERIES:
        if q.id not in verify:
            print(f"{q.id}: excluded (not verified)")
            continue
        if not verify[q.id]["pass"]["C"]:
            print(f"{q.id}: excluded (parity)")
            continue
        if q.tier == "region":
            variants = [
                (p, v, flags)
                for p in ("collect", "vcf", "parquet")
                for v, flags in (("pushdown", []), ("no_pushdown", ["--no-pushdown"]))
            ]
        else:
            variants = [
                (p, v, flags)
                for p in ("collect", "parquet")
                for v, flags in (("full", []), ("narrow", ["--narrow"]))
            ]
        for w in a.workers:
            for path, variant, flags in variants:
                if not verify[q.id]["pass"][f"B_{path}"]:
                    continue
                out = tmp / f"{q.id}.{path}.{variant}.w{w}.{EXT[path]}"
                cmd = [
                    sys.executable,
                    str(S / "vepyr_once.py"),
                    "--query",
                    q.id,
                    "--path",
                    path,
                    "--workers",
                    str(w),
                    *flags,
                    "--out",
                    str(out),
                ]
                res = repeat(
                    cmd,
                    run_dir / "logs" / "C" / f"{q.id}_{path}_{variant}_w{w}",
                    repeats=a.repeats,
                )
                res.update(query=q.id, path=path, variant=variant, workers=w)
                write_json(run_dir / "C" / f"{q.id}_{path}_{variant}_w{w}.json", res)
                print(
                    f"{q.id} {path} {variant} w{w}: {res['median_wall_s']:.2f}s",
                    flush=True,
                )


if __name__ == "__main__":
    main()
