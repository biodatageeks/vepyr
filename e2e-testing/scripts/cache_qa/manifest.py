"""Load and validate a plugin cache manifest.json; resolve shards and policy."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl


class ManifestError(ValueError):
    """manifest.json is missing, unreadable, or lacks a required key."""


MANIFEST_TYPES: dict[str, pl.DataType] = {
    "Utf8": pl.String,
    "String": pl.String,
    "Float32": pl.Float32,
    "Float64": pl.Float64,
    "Int8": pl.Int8,
    "Int16": pl.Int16,
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "UInt8": pl.UInt8,
    "UInt16": pl.UInt16,
    "UInt32": pl.UInt32,
    "UInt64": pl.UInt64,
    "Boolean": pl.Boolean,
}

KEY_TYPES: dict[str, pl.DataType] = {
    "chrom": pl.String,
    "start": pl.UInt32,
    "end": pl.UInt32,
    "allele_string": pl.String,
}

# Used only when manifest.json predates engine #234 and has no assume_unique key.
FALLBACK_ASSUME_UNIQUE: dict[str, bool] = {
    "clinvar": False,
    "alphamissense": False,
    "dbnsfp": False,
    "spliceai": True,
    "cadd": True,
}


@dataclass(frozen=True)
class ContigEntry:
    chrom: str
    file: str
    rows: int
    warm: int
    cold: int

    @property
    def bare(self) -> str:
        """Shard `chrom` value for this contig: `chr22` -> `22`, `chrMT` -> `MT`."""
        return self.chrom.removeprefix("chr")


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    role: str  # key | match | value | tier
    dtype: pl.DataType


@dataclass(frozen=True)
class CacheManifest:
    plugin: str
    plugin_dir: Path
    cache_source_version: str | None
    allele_match: str
    assume_unique: bool | None
    key_columns: list[str]
    match_columns: list[str]
    value_columns: list[tuple[str, str]]  # (column, manifest type name)
    contigs: list[ContigEntry]

    def expected_schema(self) -> list[ColumnSpec]:
        specs = [
            ColumnSpec(c, "key", KEY_TYPES.get(c, pl.String)) for c in self.key_columns
        ]
        specs += [ColumnSpec(c, "match", pl.String) for c in self.match_columns]
        specs += [
            ColumnSpec(c, "value", MANIFEST_TYPES[t]) for c, t in self.value_columns
        ]
        specs.append(ColumnSpec("tier", "tier", pl.Int8))
        return specs

    def order_key(self) -> list[str]:
        return ["tier", "start", "allele_string", *self.match_columns]

    def probe_key(self) -> list[str]:
        return [*self.key_columns, *self.match_columns]

    def shard_path(self, entry: ContigEntry) -> Path:
        return self.plugin_dir / entry.file

    def present_shards(self) -> list[tuple[ContigEntry, Path]]:
        out = []
        for entry in self.contigs:
            path = self.shard_path(entry)
            if path.exists():
                out.append((entry, path))
        return out


def _require(raw: dict, key: str, path: Path):
    if key not in raw:
        raise ManifestError(f"{path}: missing key '{key}'")
    return raw[key]


def load_manifest(plugin_dir: Path) -> CacheManifest:
    path = Path(plugin_dir) / "manifest.json"
    if not path.exists():
        raise ManifestError(f"{path}: no such file")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ManifestError(f"{path}: invalid JSON: {e}") from e
    value_columns = []
    for v in _require(raw, "value_columns", path):
        t = v.get("type", "Utf8")
        if t not in MANIFEST_TYPES:
            raise ManifestError(
                f"{path}: value column '{v.get('column')}' has unknown type '{t}'"
            )
        value_columns.append((v["column"], t))
    contigs = [
        ContigEntry(
            c["chrom"], c["file"], int(c["rows"]), int(c["warm"]), int(c["cold"])
        )
        for c in _require(raw, "chroms", path)
    ]
    return CacheManifest(
        plugin=_require(raw, "plugin_name", path),
        plugin_dir=Path(plugin_dir),
        cache_source_version=raw.get("cache_source_version"),
        allele_match=raw.get("allele_match", "exact"),
        assume_unique=raw.get("assume_unique"),
        key_columns=list(_require(raw, "key_columns", path)),
        match_columns=[m["column"] for m in raw.get("match_columns", [])],
        value_columns=value_columns,
        contigs=contigs,
    )


def dedup_policy(m: CacheManifest) -> tuple[bool, str]:
    """(assume_unique, detail). Manifest key wins; otherwise the per-plugin fallback."""
    if m.assume_unique is not None:
        return m.assume_unique, f"manifest assume_unique={str(m.assume_unique).lower()}"
    value = FALLBACK_ASSUME_UNIQUE.get(m.plugin, False)
    return value, (
        f"manifest has no assume_unique key; fallback table says "
        f"{'assume_unique' if value else 'deduplicated'} for '{m.plugin}'"
    )
