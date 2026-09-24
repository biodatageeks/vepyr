"""Content profile of a plugin cache: contig table and per-column statistics.

One lazy scan over all present shards, collected on the streaming engine;
no shard is collected whole.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from cache_qa.manifest import CacheManifest, ColumnSpec

EXACT_DISTINCT_MAX = 10_000
TOP_VALUES_MAX_DISTINCT = 50
TOP_VALUES_N = 10


@dataclass
class ContigStats:
    chrom: str
    file: str
    rows: int
    warm: int
    cold: int
    warm_share: float
    bytes: int
    start_min: int | None
    start_max: int | None


@dataclass
class NumericStats:
    parsable_share: float
    min: float
    max: float
    mean: float
    p50: float
    p95: float


@dataclass
class ColumnStats:
    name: str
    role: str
    dtype: str
    null_share: float
    empty_share: float | None
    distinct: int | None
    approx: bool
    top_values: list[tuple[str, int]] | None
    numeric: NumericStats | None
    per_contig: dict[str, dict[str, float]] = field(default_factory=dict)


@dataclass
class Profile:
    contigs: list[ContigStats]
    columns: list[ColumnStats]
    rows: int
    warm: int
    cold: int
    bytes: int


def _is_text(spec: ColumnSpec) -> bool:
    return spec.dtype == pl.String


def _share(num: float | None, den: int) -> float:
    return float(num or 0) / den if den else 0.0


def _empty_expr(name: str) -> pl.Expr:
    return (pl.col(name) == "") | (pl.col(name) == ".")


def _contig_table(m: CacheManifest, lf: pl.LazyFrame) -> list[ContigStats]:
    per = (
        lf.group_by("chrom")
        .agg(
            pl.len().alias("rows"),
            (pl.col("tier") == 0).sum().alias("warm"),
            (pl.col("tier") == 1).sum().alias("cold"),
            pl.col("start").min().alias("start_min"),
            pl.col("start").max().alias("start_max"),
        )
        .collect(engine="streaming")
    )
    found = {r["chrom"]: r for r in per.to_dicts()}
    out = []
    for entry in m.contigs:
        path = m.shard_path(entry)
        size = path.stat().st_size if path.exists() else 0
        r = found.get(entry.bare)
        if r is None:
            out.append(
                ContigStats(entry.chrom, entry.file, 0, 0, 0, 0.0, size, None, None)
            )
            continue
        rows = int(r["rows"])
        out.append(
            ContigStats(
                entry.chrom,
                entry.file,
                rows,
                int(r["warm"]),
                int(r["cold"]),
                _share(r["warm"], rows),
                size,
                int(r["start_min"]),
                int(r["start_max"]),
            )
        )
    return out


def _numeric(lf: pl.LazyFrame, name: str, parsable: int, total: int) -> NumericStats:
    col = pl.col(name).cast(pl.Float64, strict=False)
    q = (
        lf.select(
            col.min().alias("min"),
            col.max().alias("max"),
            col.mean().alias("mean"),
            col.quantile(0.5).alias("p50"),
            col.quantile(0.95).alias("p95"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    return NumericStats(
        _share(parsable, total),
        float(q["min"]),
        float(q["max"]),
        float(q["mean"]),
        float(q["p50"]),
        float(q["p95"]),
    )


def _top_values(lf: pl.LazyFrame, name: str) -> list[tuple[str, int]]:
    top = (
        lf.group_by(name)
        .len()
        .sort(["len", name], descending=[True, False])
        .head(TOP_VALUES_N)
        .collect(engine="streaming")
    )
    return [(str(v), int(n)) for v, n in top.iter_rows()]


def _column_stats(
    m: CacheManifest, lf: pl.LazyFrame, rows_by_bare: dict[str, int], total: int
) -> list[ColumnStats]:
    specs = m.expected_schema()
    aggs: list[pl.Expr] = []
    for s in specs:
        aggs.append(pl.col(s.name).null_count().alias(f"{s.name}__null"))
        if s.role in ("match", "value") and _is_text(s):
            aggs.append(_empty_expr(s.name).sum().alias(f"{s.name}__empty"))
            parsable = pl.col(s.name).cast(pl.Float64, strict=False).is_not_null()
            aggs.append(parsable.sum().alias(f"{s.name}__parsable"))
        if s.role in ("match", "value"):
            aggs.append(pl.col(s.name).approx_n_unique().alias(f"{s.name}__approx"))
    overall = lf.select(aggs).collect(engine="streaming").row(0, named=True)
    by_contig = lf.group_by("chrom").agg(aggs).collect(engine="streaming").to_dicts()
    bare_to_chrom = {e.bare: e.chrom for e in m.contigs}

    out = []
    for s in specs:
        null_share = _share(overall[f"{s.name}__null"], total)
        per_contig: dict[str, dict[str, float]] = {}
        for r in by_contig:
            n = rows_by_bare.get(r["chrom"], 0)
            shares = {"null_share": _share(r[f"{s.name}__null"], n)}
            if f"{s.name}__empty" in r:
                shares["empty_share"] = _share(r[f"{s.name}__empty"], n)
            per_contig[bare_to_chrom.get(r["chrom"], r["chrom"])] = shares
        if s.role in ("key", "tier"):
            out.append(
                ColumnStats(
                    s.name,
                    s.role,
                    str(s.dtype),
                    null_share,
                    None,
                    None,
                    False,
                    None,
                    None,
                    per_contig,
                )
            )
            continue

        empty_share = None
        if _is_text(s):
            empty_share = _share(overall.get(f"{s.name}__empty"), total)
        approx_n = int(overall[f"{s.name}__approx"] or 0)
        if approx_n <= EXACT_DISTINCT_MAX:
            distinct = int(lf.select(pl.col(s.name).n_unique()).collect().item())
            approx = False
        else:
            distinct, approx = approx_n, True
        top = _top_values(lf, s.name) if distinct <= TOP_VALUES_MAX_DISTINCT else None

        if _is_text(s):
            parsable = int(overall.get(f"{s.name}__parsable") or 0)
        else:
            parsable = total - int(overall[f"{s.name}__null"])
        numeric = None
        if s.dtype.is_numeric() or (_is_text(s) and parsable > 0):
            numeric = _numeric(lf, s.name, parsable, total)
        out.append(
            ColumnStats(
                s.name,
                s.role,
                str(s.dtype),
                null_share,
                empty_share,
                distinct,
                approx,
                top,
                numeric,
                per_contig,
            )
        )
    return out


def profile_cache(m: CacheManifest) -> Profile:
    files = [str(p) for _, p in m.present_shards()]
    lf = pl.scan_parquet(files)
    contigs = _contig_table(m, lf)
    rows = sum(c.rows for c in contigs)
    rows_by_bare = {e.bare: c.rows for e, c in zip(m.contigs, contigs)}
    columns = _column_stats(m, lf, rows_by_bare, rows)
    return Profile(
        contigs=contigs,
        columns=columns,
        rows=rows,
        warm=sum(c.warm for c in contigs),
        cold=sum(c.cold for c in contigs),
        bytes=sum(c.bytes for c in contigs),
    )
