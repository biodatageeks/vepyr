"""Experiment A, Polars side only, for the 0.11.0 environment (polars-bio 0.38.0).

The VEP-annotated chr22 files of the original run no longer exist; they are taken from the stored VEP 116.0
reference outputs (see prepare step in the caller). For every query the number of kept records must equal that of
the earlier run (A/<query>_polars.json copied from ..._v0100), otherwise nothing is written. filter_vep is not timed
again: A/<query>_filter_vep.json stays as copied.
"""

import json
import sys
from pathlib import Path

PKG = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PKG))
from pibench.measure import repeat, write_json  # noqa: E402
from pibench.parity import vcf_keys  # noqa: E402
from pibench.paths import WORK  # noqa: E402
from pibench.queries import QUERIES  # noqa: E402

run_dir = Path(__file__).resolve().parent
tmp = WORK / "runs" / "A"
tmp.mkdir(parents=True, exist_ok=True)
results = {}
for q in QUERIES:
    vep = WORK / "vep" / ("plugins" if q.plugins else "core") / "fork0" / "rep0.vcf"
    out = tmp / f"{q.id}.pl.vcf"
    res = repeat(
        [
            sys.executable,
            str(PKG / "scripts" / "polars_filter_once.py"),
            "--vcf",
            str(vep),
            "--query",
            q.id,
            "--out",
            str(out),
        ],
        run_dir / "logs" / "A" / f"{q.id}_polars",
        repeats=3,
    )
    rows = len(vcf_keys(out))
    before = json.loads((run_dir / "A" / f"{q.id}_polars.json").read_text())
    if rows != before["rows"]:
        sys.exit(
            f"{q.id}: {rows} records kept, the earlier run kept {before['rows']}; nothing written"
        )
    res.update(query=q.id, tool="polars", rows=rows)
    results[q.id] = (res, before["median_wall_s"])
    print(
        f"{q.id} polars: {res['median_wall_s']:.2f}s (was {before['median_wall_s']:.2f}s) rows={rows}",
        flush=True,
    )
for qid, (res, _) in results.items():
    write_json(run_dir / "A" / f"{qid}_polars.json", res)
print("filter-only, Polars side: all 18 queries keep the same records; results written")
