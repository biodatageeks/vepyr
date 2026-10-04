"""Experiments B and C, vepyr side: annotate -> filter -> one output path, in one process."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import polars as pl  # noqa: E402
import vepyr  # noqa: E402

from pibench.paths import FASTA, INPUT_VCF, PLUGIN_CACHE_ROOT, VEPYR_CACHE  # noqa: E402
from pibench.queries import BY_ID, KEY_COLUMNS, VEPYR_PLUGINS  # noqa: E402


def build(
    query: str, path: str, workers: int, pushdown: bool, narrow: bool
) -> pl.LazyFrame:
    q = BY_ID[query]
    lf = vepyr.annotate(
        str(INPUT_VCF),
        str(VEPYR_CACHE[q.cache]),
        everything=True,
        reference_fasta=str(FASTA),
        workers=workers,
        skip_csq=(path != "vcf"),
        plugin_cache_root=str(PLUGIN_CACHE_ROOT) if q.plugins else None,
        plugins=VEPYR_PLUGINS if q.plugins else None,
        show_progress=False,
    )
    expr = q.expr() if pushdown or q.expr_no_pushdown is None else q.expr_no_pushdown()
    lf = lf.filter(expr)
    if narrow:
        lf = lf.select(list(dict.fromkeys([*KEY_COLUMNS, *q.columns])))
    return lf


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True)
    ap.add_argument("--path", required=True, choices=("collect", "vcf", "parquet"))
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--no-pushdown", action="store_true")
    ap.add_argument("--narrow", action="store_true")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument(
        "--keys-out",
        type=Path,
        help="collect path only: write the kept keys (verification runs)",
    )
    a = ap.parse_args()
    if a.narrow and a.path == "vcf":
        ap.error("--narrow does not apply to the VCF path: CSQ needs every flag")
    lf = build(a.query, a.path, a.workers, not a.no_pushdown, a.narrow)
    if a.path == "collect":
        df = lf.collect()
        a.out.write_text(f"{df.height}\n")
        if a.keys_out:
            df.select(KEY_COLUMNS).write_parquet(a.keys_out)
    elif a.path == "parquet":
        lf.sink_parquet(a.out, row_group_size=5000)
    else:
        import polars_bio as pb

        pb.sink_vcf(lf, str(a.out))


if __name__ == "__main__":
    main()
