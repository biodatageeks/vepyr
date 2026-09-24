from __future__ import annotations

import gzip
from pathlib import Path

import polars as pl


def _lines(path: Path):
    opener = gzip.open if str(path).endswith((".gz", ".bgz")) else open
    with opener(path, "rt") as fh:
        for line in fh:
            if not line.startswith("#"):
                yield line.rstrip("\n")


def vcf_body(path: Path) -> list[str]:
    return list(_lines(path))


def vcf_keys(path: Path) -> list[tuple[str, int, str, str]]:
    out = []
    for line in _lines(path):
        c, p, _, r, a = line.split("\t", 5)[:5]
        out.append((c, int(p), r, a))
    return out


def parquet_keys(path: Path) -> list[tuple]:
    df = pl.read_parquet(path, columns=["chrom", "start", "ref", "alt"])
    return [(c, int(s), r, a) for c, s, r, a in df.iter_rows()]


def compare(a: list, b: list) -> dict:
    first = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), None)
    if first is None and len(a) != len(b):
        first = min(len(a), len(b))
    diff = (
        None
        if first is None
        else {
            "index": first,
            "a": a[first] if first < len(a) else None,
            "b": b[first] if first < len(b) else None,
        }
    )
    return {"equal": first is None, "n_a": len(a), "n_b": len(b), "first_diff": diff}
