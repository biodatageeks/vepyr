"""Parity gate: every query must give filter_vep's records before it is timed.

Reference = filter_vep over serial VEP (fork 0). Checks per query:
  A        Polars over the same VEP VCF: record keys equal (body reported)
  B_vcf    vepyr -> pb.sink_vcf at W in {1,2,4,8}: body line-for-line equal
  B_collect / B_parquet at W=1 and W=8: record keys equal
  C        pushdown off, and narrow select, give the same keys as the reference

verify.json is updated one query at a time (load-merge-atomic-replace), so a
crash partway through a run never loses the queries already checked, and a
`--queries Q7` rerun touches only that key.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.parity import compare, parquet_keys, vcf_body, vcf_keys  # noqa: E402
from pibench.paths import PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression  # noqa: E402

S = Path(__file__).resolve().parent
PY = [sys.executable]


def sh(cmd: list[str], log_path: Path) -> None:
    """Run `cmd`, discarding stdout and capturing stderr at `log_path`.

    Raises a RuntimeError naming the command and the stderr log on a nonzero
    exit, rather than letting a bare CalledProcessError point nowhere useful.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "wb") as err:
        proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=err)
    if proc.returncode != 0:
        raise RuntimeError(f"exit {proc.returncode}: {' '.join(cmd)} (see {log_path})")


def load_result(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def update_result(path: Path, query_id: str, entry: dict) -> dict:
    """Merge `entry` under `query_id` into the JSON object at `path`.

    Reads whatever is already there first (so earlier queries, or queries not
    named in this run, survive), then writes to a tmp file and `os.replace`s
    it into place so a crash mid-write never leaves a truncated verify.json.
    """
    result = load_result(path)
    result[query_id] = entry
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(result, indent=2, default=str) + "\n")
    os.replace(tmp, path)
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--queries", nargs="*", help="default: all")
    a = ap.parse_args()
    out_dir = WORK / "verify"
    logs = out_dir / "logs"
    out_dir.mkdir(parents=True, exist_ok=True)
    verify_json = PKG_ROOT / a.run_dir / "verify.json"
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
            ],
            logs / f"{q.id}.filter_vep.stderr.txt",
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
            ],
            logs / f"{q.id}.polars.stderr.txt",
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
                ],
                logs / f"{q.id}.B_vcf_w{w}.stderr.txt",
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
                ],
                logs / f"{q.id}.B_collect_w{w}.stderr.txt",
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
                ],
                logs / f"{q.id}.B_parquet_w{w}.stderr.txt",
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
                ],
                logs / f"{q.id}.{name}.stderr.txt",
            )
            checks[name] = compare(ref_keys, parquet_keys(p))

        ok = lambda *names: all(checks[n]["equal"] for n in names)  # noqa: E731
        entry = {
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
        update_result(verify_json, q.id, entry)
        print(q.id, len(ref_keys), entry["pass"], flush=True)


if __name__ == "__main__":
    main()
