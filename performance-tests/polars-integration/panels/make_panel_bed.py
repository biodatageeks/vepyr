"""Write the chr22 loci of the panel genes from the 116 Ensembl transcript cache."""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from pibench.paths import VEPYR_CACHE  # noqa: E402


def main() -> None:
    genes = (HERE / "acmg_sf_v3.2.txt").read_text().split()
    loci = (
        pl.scan_parquet(VEPYR_CACHE["ensembl"] / "transcript/chr22.parquet")
        .filter(pl.col("gene_symbol").is_in(genes))
        .group_by("chrom", "gene_symbol")
        .agg(pl.col("start").min(), pl.col("end").max())
        .sort("start")
        .collect()
    )
    rows = [
        f"chr{c}\t{s - 1}\t{e}\t{g}"
        for c, g, s, e in loci.select(
            "chrom", "gene_symbol", "start", "end"
        ).iter_rows()
    ]
    (HERE / "acmg_sf_v3.2_chr22.bed").write_text("\n".join(rows) + "\n")
    print("\n".join(rows))


if __name__ == "__main__":
    main()
