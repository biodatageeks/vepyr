"""Parity gate: every query must give filter_vep's records before it is timed.

Reference = filter_vep over serial VEP (fork 0). Checks per query:
  A        Polars over the same VEP VCF: record keys equal (body reported)
  B_vcf    vepyr -> pb.sink_vcf at W in {1,2,4,8}: body line-for-line equal
  B_collect / B_parquet at W=1 and W=8: record keys equal
  C        pushdown off, and narrow select, give the same keys as the reference
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import write_json  # noqa: E402
from pibench.parity import compare, parquet_keys, vcf_body, vcf_keys  # noqa: E402
from pibench.paths import PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression  # noqa: E402

S = Path(__file__).resolve().parent
PY = [sys.executable]


def sh(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--queries", nargs="*", help="default: all")
    a = ap.parse_args()
    out_dir = WORK / "verify"
    out_dir.mkdir(parents=True, exist_ok=True)
    result: dict = {}
    for q in QUERIES:
        if a.queries and q.id not in a.queries:
            continue
        mode = "plugins" if q.plugins else "core"
        vep = WORK / "vep" / mode / "fork0" / "rep0.vcf"
        ref = out_dir / f"{q.id}.filter_vep.vcf"
        sh(
            [
                "bash",
                str(S / "filter_vep_once.sh"),
                str(vep),
                str(ref),
                filter_vep_expression(q),
            ]
        )
        ref_keys, ref_body = vcf_keys(ref), vcf_body(ref)
        checks: dict = {}

        pa = out_dir / f"{q.id}.polars.vcf"
        sh(
            [
                *PY,
                str(S / "polars_filter_once.py"),
                "--vcf",
                str(vep),
                "--query",
                q.id,
                "--out",
                str(pa),
            ]
        )
        checks["A_keys"] = compare(ref_keys, vcf_keys(pa))
        checks["A_body"] = compare(ref_body, vcf_body(pa))

        for w in (1, 2, 4, 8):
            v = out_dir / f"{q.id}.vepyr.w{w}.vcf"
            sh(
                [
                    *PY,
                    str(S / "vepyr_once.py"),
                    "--query",
                    q.id,
                    "--path",
                    "vcf",
                    "--workers",
                    str(w),
                    "--out",
                    str(v),
                ]
            )
            checks[f"B_vcf_w{w}"] = compare(ref_body, vcf_body(v))
        for w in (1, 8):
            k = out_dir / f"{q.id}.collect.w{w}.parquet"
            sh(
                [
                    *PY,
                    str(S / "vepyr_once.py"),
                    "--query",
                    q.id,
                    "--path",
                    "collect",
                    "--workers",
                    str(w),
                    "--out",
                    str(out_dir / "rows.txt"),
                    "--keys-out",
                    str(k),
                ]
            )
            checks[f"B_collect_w{w}"] = compare(ref_keys, parquet_keys(k))
            p = out_dir / f"{q.id}.sink.w{w}.parquet"
            sh(
                [
                    *PY,
                    str(S / "vepyr_once.py"),
                    "--query",
                    q.id,
                    "--path",
                    "parquet",
                    "--workers",
                    str(w),
                    "--out",
                    str(p),
                ]
            )
            checks[f"B_parquet_w{w}"] = compare(ref_keys, parquet_keys(p))
        for flag, name in (
            ("--narrow", "C_narrow"),
            ("--no-pushdown", "C_no_pushdown"),
        ):
            p = out_dir / f"{q.id}.{name}.parquet"
            sh(
                [
                    *PY,
                    str(S / "vepyr_once.py"),
                    "--query",
                    q.id,
                    "--path",
                    "parquet",
                    flag,
                    "--out",
                    str(p),
                ]
            )
            checks[name] = compare(ref_keys, parquet_keys(p))

        ok = lambda *names: all(checks[n]["equal"] for n in names)  # noqa: E731
        result[q.id] = {
            "reference_rows": len(ref_keys),
            "checks": checks,
            "pass": {
                "A": ok("A_keys"),
                "B_vcf": ok(*(f"B_vcf_w{w}" for w in (1, 2, 4, 8))),
                "B_collect": ok("B_collect_w1", "B_collect_w8"),
                "B_parquet": ok("B_parquet_w1", "B_parquet_w8"),
                "C": ok("C_narrow", "C_no_pushdown"),
            },
        }
        print(q.id, len(ref_keys), result[q.id]["pass"], flush=True)
    write_json(PKG_ROOT / a.run_dir / "verify.json", result)


if __name__ == "__main__":
    main()
