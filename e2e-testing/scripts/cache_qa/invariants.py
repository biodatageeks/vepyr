"""Structural invariants over a plugin cache.

Each check is a lazy per-shard Polars scan reduced to counts, so only the
columns a check needs are read. `order` needs whole-column context for
`shift`, so its projected key columns are collected per shard; that bounds
memory at one shard's key columns, never the value columns.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import polars as pl

from cache_qa.manifest import CacheManifest, ContigEntry, dedup_policy


@dataclass
class InvariantResult:
    id: str
    status: str  # pass | fail | warn
    detail: str
    per_contig: dict[str, int] | None = None


def _count_per_shard(
    m: CacheManifest, expr_builder: Callable[[ContigEntry], pl.Expr]
) -> dict[str, int]:
    """Count rows where `expr_builder(entry)` is true, per present shard."""
    counts: dict[str, int] = {}
    for entry, path in m.present_shards():
        n = (
            pl.scan_parquet(path)
            .select(
                expr_builder(entry).fill_null(False).cast(pl.UInt64).sum().alias("n")
            )
            .collect()
            .item()
        )
        counts[entry.chrom] = int(n or 0)
    return counts


def _result(
    id_: str, counts: dict[str, int], noun: str, status_on_hit: str = "fail"
) -> InvariantResult:
    bad = {k: v for k, v in counts.items() if v}
    if not bad:
        return InvariantResult(id_, "pass", f"0 {noun} in {len(counts)} shards")
    total = sum(bad.values())
    return InvariantResult(
        id_, status_on_hit, f"{total} {noun} in {len(bad)} shards", bad
    )


def check_schema(m: CacheManifest) -> InvariantResult:
    expected = [(s.name, s.dtype) for s in m.expected_schema()]
    problems = []
    n = 0
    for entry, path in m.present_shards():
        n += 1
        actual = list(pl.scan_parquet(path).collect_schema().items())
        for i, exp in enumerate(expected):
            act = actual[i] if i < len(actual) else None
            if act is None or act[0] != exp[0] or act[1] != exp[1]:
                problems.append(
                    f"{entry.chrom}: expected {exp[0]} {exp[1]}, found {act}"
                )
                break
        else:
            if len(actual) > len(expected):
                problems.append(
                    f"{entry.chrom}: extra column {actual[len(expected)][0]}"
                )
    if problems:
        return InvariantResult("schema", "fail", "; ".join(problems))
    return InvariantResult("schema", "pass", f"{n} shards match the manifest")


def check_contig(m: CacheManifest) -> InvariantResult:
    counts = _count_per_shard(m, lambda e: pl.col("chrom") != e.bare)
    return _result("contig", counts, "foreign-contig rows")


def check_tier_domain(m: CacheManifest) -> InvariantResult:
    counts = _count_per_shard(
        m, lambda e: pl.col("tier").is_null() | ~pl.col("tier").is_in([0, 1])
    )
    return _result("tier_domain", counts, "rows with tier outside {0,1}")


def check_positions(m: CacheManifest) -> InvariantResult:
    start = pl.col("start").cast(pl.Int64)
    end = pl.col("end").cast(pl.Int64)
    counts = _count_per_shard(m, lambda e: (start < 1) | (end < start - 1))
    return _result("positions", counts, "rows with start < 1 or end < start - 1")


def _allele_parts() -> tuple[pl.Expr, pl.Expr]:
    parts = pl.col("allele_string").str.split_exact("/", 1)
    return parts.struct.field("field_0"), parts.struct.field("field_1")


def _allele_malformed(allele_match: str) -> pl.Expr:
    """Hard failures: no `/`, empty REF, or a shared leading base when minimised."""
    ref, alt = _allele_parts()
    bad = ref.is_null() | alt.is_null() | (ref == "")
    if allele_match == "minimised":
        shared = (
            (ref.str.slice(0, 1) == alt.str.slice(0, 1)) & (ref != "-") & (alt != "-")
        )
        bad = bad | shared.fill_null(False)
    return bad


def _allele_empty_alt() -> pl.Expr:
    """`REF/` rows: a source record whose ALT was `.`; unmatchable, not corrupt."""
    ref, alt = _allele_parts()
    return ref.is_not_null() & (ref != "") & (alt == "")


def check_allele_form(m: CacheManifest) -> InvariantResult:
    hard = _count_per_shard(m, lambda e: _allele_malformed(m.allele_match))
    r = _result("allele_form", hard, "malformed allele strings")
    if r.status == "fail":
        return r
    empty = _count_per_shard(m, lambda e: _allele_empty_alt())
    warn = _result(
        "allele_form", empty, "rows with an empty ALT (source ALT '.')", "warn"
    )
    if warn.status == "warn":
        return warn
    return r


def descending_steps(cols: list[str]) -> pl.Expr:
    """True where row i is lexicographically smaller than row i-1 on `cols`."""
    step = pl.lit(False)
    equal_prefix = pl.lit(True)
    for c in cols:
        cur, prev = pl.col(c), pl.col(c).shift(1)
        step = step | (equal_prefix & (cur < prev))
        equal_prefix = equal_prefix & (cur == prev)
    return step.fill_null(False)


def check_order(m: CacheManifest) -> InvariantResult:
    key = m.order_key()
    counts = _count_per_shard(m, lambda e: descending_steps(key))
    return _result("order", counts, "descending steps")


def check_duplicates(m: CacheManifest) -> InvariantResult:
    assume_unique, policy = dedup_policy(m)
    key = m.probe_key()
    counts: dict[str, int] = {}
    for entry, path in m.present_shards():
        n = (
            pl.scan_parquet(path)
            .group_by(key)
            .len()
            .filter(pl.col("len") > 1)
            .select((pl.col("len") - 1).sum().alias("n"))
            .collect(engine="streaming")
            .item()
        )
        counts[entry.chrom] = int(n or 0)
    r = _result(
        "duplicates",
        counts,
        "duplicate probe keys",
        "warn" if assume_unique else "fail",
    )
    r.detail = f"{r.detail} ({policy})"
    return r


def check_manifest_counts(m: CacheManifest) -> InvariantResult:
    problems = []
    checked = 0
    for entry, path in m.present_shards():
        checked += 1
        found = (
            pl.scan_parquet(path)
            .select(
                pl.len().alias("rows"),
                (pl.col("tier") == 0).sum().alias("warm"),
                (pl.col("tier") == 1).sum().alias("cold"),
            )
            .collect()
            .row(0, named=True)
        )
        for field in ("rows", "warm", "cold"):
            expected = getattr(entry, field)
            if int(found[field]) != expected:
                problems.append(
                    f"{entry.chrom} {field}: manifest {expected}, shard {int(found[field])}"
                )
    if problems:
        return InvariantResult("manifest_counts", "fail", "; ".join(problems))
    return InvariantResult(
        "manifest_counts", "pass", f"rows/warm/cold match in {checked} shards"
    )


def check_manifest_files(m: CacheManifest) -> InvariantResult:
    missing = [e.file for e in m.contigs if e.rows > 0 and not m.shard_path(e).exists()]
    listed = {e.file for e in m.contigs}
    unlisted = sorted(
        p.name for p in m.plugin_dir.glob("chr*.parquet") if p.name not in listed
    )
    problems = []
    if missing:
        problems.append("missing: " + ", ".join(missing))
    if unlisted:
        problems.append("unlisted: " + ", ".join(unlisted))
    if problems:
        return InvariantResult("manifest_files", "fail", "; ".join(problems))
    return InvariantResult(
        "manifest_files", "pass", f"{len(listed)} manifest contigs, no stray shards"
    )


def run_all(m: CacheManifest) -> list[InvariantResult]:
    return [
        check_schema(m),
        check_contig(m),
        check_order(m),
        check_tier_domain(m),
        check_manifest_counts(m),
        check_manifest_files(m),
        check_positions(m),
        check_allele_form(m),
        check_duplicates(m),
    ]
