"""Equivalence of vepyr 0.11.0 with vepyr 0.10.0 on the 18 benchmark queries (replaces verify.py, see run.sh).

Usage: equiv.py <python new> <python old> <run dir>
For each query and output path (collect, vcf, parquet), both versions run scripts/vepyr_once.py with one worker.
Compared: the kept keys of the collect path, the record lines of the VCF path (header lines excluded) and the
frame read back from the Parquet path, on the columns of the 0.10.0 frame (0.11.0 adds the helper column
_vcf_raw_info; columns present in only one version are recorded). Writes <run dir>/equiv.json and exits non-zero
on any difference. With EQUIV_REUSE=1, outputs that already exist are compared without running vepyr again.
"""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import polars as pl  # noqa: E402

from pibench.paths import PKG_ROOT, WORK  # noqa: E402
from pibench.queries import QUERIES  # noqa: E402

new, old, run_dir = sys.argv[1], sys.argv[2], PKG_ROOT / sys.argv[3]
verify = json.loads((run_dir / "verify.json").read_text())
tmp = WORK / "runs" / "equiv"
tmp.mkdir(parents=True, exist_ok=True)
once = str(PKG_ROOT / "scripts" / "vepyr_once.py")


def body_md5(path: Path) -> tuple[int, str]:
    h, n = hashlib.md5(), 0
    with open(path, "rb") as fh:
        for line in fh:
            if not line.startswith(b"#"):
                h.update(line)
                n += 1
    return n, h.hexdigest()


def run(py: str, tag: str, q: str, path: str) -> dict:
    ext = {"collect": "rows.txt", "vcf": "vcf", "parquet": "parquet"}[path]
    out = tmp / f"{q}.{path}.{tag}.{ext}"
    cmd = [py, once, "--query", q, "--path", path, "--workers", "1", "--out", str(out)]
    keys = tmp / f"{q}.keys.{tag}.parquet"
    if path == "collect":
        cmd += ["--keys-out", str(keys)]
    if (
        os.environ.get("EQUIV_REUSE")
        and out.exists()
        and (path != "collect" or keys.exists())
    ):
        return {"out": out, "keys": keys}
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    return {"out": out, "keys": keys}


res, bad = {}, []
for q in QUERIES:
    r = {"reference_rows": verify[q.id]["reference_rows"]}
    a, b = run(new, "new", q.id, "collect"), run(old, "old", q.id, "collect")
    ka, kb = pl.read_parquet(a["keys"]), pl.read_parquet(b["keys"])
    r["collect"] = {
        "rows_new": ka.height,
        "rows_old": kb.height,
        "equal": ka.equals(kb),
    }
    a, b = run(new, "new", q.id, "vcf"), run(old, "old", q.id, "vcf")
    (na, ma), (nb, mb) = body_md5(a["out"]), body_md5(b["out"])
    r["vcf"] = {"records_new": na, "records_old": nb, "equal": ma == mb}
    a, b = run(new, "new", q.id, "parquet"), run(old, "old", q.id, "parquet")
    fa, fb = pl.read_parquet(a["out"]), pl.read_parquet(b["out"])
    shared = [c for c in fb.columns if c in fa.columns]
    r["parquet"] = {
        "rows_new": fa.height,
        "rows_old": fb.height,
        "equal": len(shared) > 0 and fa.select(shared).equals(fb.select(shared)),
        "shared_columns": len(shared),
        "only_new": [c for c in fa.columns if c not in fb.columns],
        "only_old": [c for c in fb.columns if c not in fa.columns],
    }
    res[q.id] = r
    ok = all(r[p]["equal"] for p in ("collect", "vcf", "parquet"))
    if not ok:
        bad.append(q.id)
    print(q.id, "ok" if ok else "DIFFERENT", json.dumps(r), flush=True)
(run_dir / "equiv.json").write_text(json.dumps(res, indent=1))
print(
    "equivalence gate:",
    "all 18 queries equal" if not bad and len(res) == 18 else f"FAILED {bad}",
)
sys.exit(1 if bad or len(res) != 18 else 0)
