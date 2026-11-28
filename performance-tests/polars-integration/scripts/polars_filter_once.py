"""Experiment A, Polars side: read a VEP-annotated VCF, filter with the catalogue expression, write VCF."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import polars_bio as pb  # noqa: E402

from pibench.csq import csq_fields, derive  # noqa: E402
from pibench.queries import BY_ID  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vcf", required=True, type=Path)
    ap.add_argument("--query", required=True)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args()
    q = BY_ID[a.query]
    derived = derive(csq_fields(a.vcf), q.columns)
    lf = pb.scan_vcf(str(a.vcf), preserve_record_layout=True)
    out = lf.with_columns(**derived).filter(q.expr()).drop(list(derived))
    pb.sink_vcf(out, str(a.out))


if __name__ == "__main__":
    main()
