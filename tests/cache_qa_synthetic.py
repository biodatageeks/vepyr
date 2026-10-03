"""Build tiny three-contig plugin caches for cache_qa tests."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

CONTIGS = ("chr1", "chr2", "chrX")


def _rows(bare: str, n: int, symbol: str) -> pl.DataFrame:
    starts = [100 + 10 * i for i in range(n)]
    tiers = [0 if i < 2 else 1 for i in range(n)]
    return pl.DataFrame(
        {
            "chrom": [bare] * n,
            "start": pl.Series(starts, dtype=pl.UInt32),
            "end": pl.Series(starts, dtype=pl.UInt32),
            "allele_string": ["A/G" if i % 2 == 0 else "C/T" for i in range(n)],
            "symbol": [symbol] * n,
            "score": ["0.5" if i % 3 else "" for i in range(n)],
            "raw": pl.Series([float(i) for i in range(n)], dtype=pl.Float32),
            "tier": pl.Series(tiers, dtype=pl.Int8),
        }
    ).sort(["tier", "start", "allele_string", "symbol"])


class SyntheticCache:
    def __init__(
        self,
        root: Path,
        plugin: str = "demo",
        match_columns: tuple[str, ...] = ("symbol",),
        allele_match: str = "exact",
        assume_unique: bool | None = False,
    ) -> None:
        self.root = root
        self.plugin = plugin
        self.plugin_dir = root / "plugin" / plugin
        self.match_columns = list(match_columns)
        self.allele_match = allele_match
        self.assume_unique = assume_unique
        self.rows: dict[str, pl.DataFrame] = {
            "chr1": _rows("1", 6, "GENE1"),
            "chr2": _rows("2", 4, "GENE2"),
            "chrX": _rows("X", 3, "GENEX"),
        }
        self._manifest_override: dict | None = None

    def manifest_dict(self) -> dict:
        if self._manifest_override is not None:
            return self._manifest_override
        chroms = []
        for chrom, df in self.rows.items():
            chroms.append(
                {
                    "chrom": chrom,
                    "file": f"{chrom}.parquet",
                    "rows": df.height,
                    "warm": int((df["tier"] == 0).sum()),
                    "cold": int((df["tier"] == 1).sum()),
                }
            )
        m = {
            "plugin_name": self.plugin,
            "source_manifest": f"{self.plugin}.source.toml",
            "key_columns": ["chrom", "start", "end", "allele_string"],
            "match_columns": [
                {"column": c, "template": "{SYMBOL}"} for c in self.match_columns
            ],
            "value_columns": [
                {"column": "score", "csq_field": "DEMO_SCORE", "type": "Utf8"},
                {"column": "raw", "csq_field": "DEMO_RAW", "type": "Float32"},
            ],
            "chroms": chroms,
            "sources": [],
            "cache_source_version": "v0.1.1@3e1c039405344ccb800f89f9a032c688fd048bec",
            "allele_match": self.allele_match,
            "field_order": "alphabetical",
        }
        if self.assume_unique is not None:
            m["assume_unique"] = self.assume_unique
        return m

    def set_manifest(self, manifest: dict) -> None:
        self._manifest_override = manifest

    def write(self) -> Path:
        self.plugin_dir.mkdir(parents=True, exist_ok=True)
        for chrom, df in self.rows.items():
            df.write_parquet(self.plugin_dir / f"{chrom}.parquet")
        (self.plugin_dir / "manifest.json").write_text(
            json.dumps(self.manifest_dict(), indent=2) + "\n"
        )
        return self.plugin_dir
