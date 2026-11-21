#!/usr/bin/env python3
"""Cache parity: is a rebuilt vepyr cache the published one?

`compare-core` and `compare-plugin` compare two cache directories that already
exist. `run` is one /parity-caches job: it rebuilds a core cache or a plugin
cache with the public API from inputs staged on the runner, takes the published
cache at the revision pinned in `vepyr._published`, and compares the two.

Compared for every entity and contig shard: the row count, the schema (column
names, order, types, nullability and key-value metadata) and the content of
every column, row by row. Plugin caches also compare the warm/cold tier counts
(the shard's `tier` column and `manifest.json`).

Exit codes: 0 identical, 1 parity mismatch (the summary has a `FAIL:` line),
3 the runner is not ready (staged data missing or stale, scratch too small),
anything else is an error.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import threading
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

BATCH_ROWS = 65_536
# Above this a shard is never loaded whole: a difference in row order alone
# cannot be told from a difference in content and is reported as a mismatch.
SORT_LIMIT_ROWS = 2_000_000
# Above this a row-count mismatch is reported without example rows.
EXPLAIN_LIMIT_ROWS = 1_000_000
MAX_EXAMPLES = 3
MAX_SUMMARY_PROBLEMS = 40
TIER_COLUMN = "tier"
TIER_NAMES = {0: "warm", 1: "cold"}
CORE_JSON_SIDECARS = ("reference_policy.json",)
CORE_BYTE_SIDECARS = ("chr_synonyms.txt",)
CHROM_MANIFEST = "chrom_manifest.json"
# manifest.json keys that describe the cache content. `sources` (paths, inode,
# mtime), `source_manifest` and `cache_source_version` describe the build.
PLUGIN_MANIFEST_FIELDS = (
    "plugin_name",
    "key_columns",
    "match_columns",
    "value_columns",
    "allele_match",
    "field_order",
    "assume_unique",
    "lookup",
)
# A manifest written before the key existed means its default.
PLUGIN_MANIFEST_DEFAULTS = {"lookup": "point"}
BUILT = "the rebuilt cache"
PUBLISHED = "the published cache"
# Shards are compared in parallel as streams; the two steps that load a whole
# shard (only after a difference was found) run one at a time.
_WHOLE_SHARD = threading.Lock()


class RunnerNotReady(Exception):
    """Staged data is missing or stale, or the scratch disk is too small."""


@dataclass
class ShardResult:
    entity: str
    shard: str
    rows_built: int | None = None
    rows_published: int | None = None
    tiers_built: dict[str, int] | None = None
    tiers_published: dict[str, int] | None = None
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


@dataclass
class Report:
    kind: str  # "core" or "plugin"
    label: str
    shards: list[ShardResult] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)  # cache level
    notes: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems and all(s.ok for s in self.shards)

    def entities(self) -> list[str]:
        return list(dict.fromkeys(s.entity for s in self.shards))

    def verdict(self) -> str:
        total = len(self.shards)
        bad = [s for s in self.shards if not s.ok]
        rows = sum(s.rows_published or 0 for s in self.shards)
        checked = "rows, schema, tiers" if self.kind == "plugin" else "rows, schema"
        if self.ok:
            reordered = sum(1 for s in self.shards if s.notes)
            extra = f"; {reordered} with notes" if reordered else ""
            return (
                f"PASS: {total} of {total} shards identical in {checked} and "
                f"every column ({rows:,} rows{extra})"
            )
        if bad:
            first = bad[0]
            names = ", ".join(_shard_name(s, self.kind) for s in bad[:5])
            more = ", ..." if len(bad) > 5 else ""
            return (
                f"FAIL: {len(bad)} of {total} shards differ ({names}{more}); "
                f"{_shard_name(first, self.kind)}: {first.problems[0]}"
            )
        return f"FAIL: {self.problems[0]}"

    def text(self) -> str:
        lines = [f"cache parity: {self.label}"]
        lines += [f"  note: {n}" for n in self.notes]
        if self.kind == "plugin":
            for s in self.shards:
                tiers = s.tiers_published or {}
                tier_text = "".join(f"  {k} {v:,}" for k, v in tiers.items())
                state = "identical" if s.ok else "DIFFERS"
                note = f" ({'; '.join(s.notes)})" if s.notes else ""
                lines.append(
                    f"  {s.shard:<8} {_rows(s.rows_published):>14} rows{tier_text}"
                    f"  {state}{note}"
                )
        else:
            for entity in self.entities():
                group = [s for s in self.shards if s.entity == entity]
                bad = sum(1 for s in group if not s.ok)
                rows = sum(s.rows_published or 0 for s in group)
                noted = sum(1 for s in group if s.notes)
                state = "identical" if not bad else f"{bad} shard(s) DIFFER"
                note = f" ({noted} with notes)" if noted else ""
                lines.append(
                    f"  {entity:<18} {len(group):>5} shards {rows:>16,} rows  "
                    f"{state}{note}"
                )
        problems = list(self.problems)
        for s in self.shards:
            problems += [f"{_shard_name(s, self.kind)}: {p}" for p in s.problems]
        for p in problems[:MAX_SUMMARY_PROBLEMS]:
            lines.append(f"  - {p}")
        if len(problems) > MAX_SUMMARY_PROBLEMS:
            lines.append(
                f"  - ... {len(problems) - MAX_SUMMARY_PROBLEMS} more in the report"
            )
        lines.append(self.verdict())
        return "\n".join(lines) + "\n"

    def to_json(self) -> dict:
        return {
            "kind": self.kind,
            "label": self.label,
            "ok": self.ok,
            "verdict": self.verdict(),
            "problems": self.problems,
            "notes": self.notes,
            "shards": [asdict(s) for s in self.shards],
        }


def _rows(n: int | None) -> str:
    return "?" if n is None else f"{n:,}"


def _shard_name(shard: ShardResult, kind: str) -> str:
    return shard.shard if kind == "plugin" else f"{shard.entity}/{shard.shard}"


def _show(value) -> str:
    text = repr(value.as_py() if isinstance(value, pa.Scalar) else value)
    return text if len(text) <= 80 else text[:77] + "..."


# --------------------------------------------------------------------------
# One Parquet shard
# --------------------------------------------------------------------------


def _key_value_metadata(schema: pa.Schema) -> dict[str, str]:
    return {
        k.decode("utf-8", "replace"): v.decode("utf-8", "replace")
        for k, v in (schema.metadata or {}).items()
        if k != b"ARROW:schema"
    }


def schema_problems(built: pa.Schema, published: pa.Schema) -> list[str]:
    """Every way the rebuilt shard's schema differs from the published one."""
    problems = []
    b = {f.name: f for f in built}
    p = {f.name: f for f in published}
    missing = [n for n in published.names if n not in b]
    extra = [n for n in built.names if n not in p]
    if missing:
        problems.append(f"schema: column(s) missing from {BUILT}: {', '.join(missing)}")
    if extra:
        problems.append(f"schema: column(s) not in {PUBLISHED}: {', '.join(extra)}")
    for name in published.names:
        if name not in b:
            continue
        if b[name].type != p[name].type:
            problems.append(
                f"schema: column {name} is {b[name].type} in {BUILT}, "
                f"{p[name].type} in {PUBLISHED}"
            )
        elif b[name].nullable != p[name].nullable:
            problems.append(
                f"schema: column {name} is nullable={b[name].nullable} in {BUILT}, "
                f"nullable={p[name].nullable} in {PUBLISHED}"
            )
    if not missing and not extra and built.names != published.names:
        problems.append(
            f"schema: column order is {', '.join(built.names)} in {BUILT}, "
            f"{', '.join(published.names)} in {PUBLISHED}"
        )
    mb, mp = _key_value_metadata(built), _key_value_metadata(published)
    for key in sorted(set(mb) | set(mp)):
        if mb.get(key) != mp.get(key):
            problems.append(
                f"schema: metadata {key} is {_show(mb.get(key))} in {BUILT}, "
                f"{_show(mp.get(key))} in {PUBLISHED}"
            )
    return problems


_FLOAT_BITS = {pa.float16(): pa.int16(), pa.float32(): pa.int32()}
_FLOAT_BITS[pa.float64()] = pa.int64()


def _normalise(array: pa.Array) -> pa.Array:
    """An array whose equality is the equality of the stored values.

    Floats become their bit patterns, so NaN equals NaN and 0.0 differs from
    -0.0. Dictionary, large and view encodings become the plain type, so only
    the schema check reports an encoding change.
    """
    t = array.type
    try:
        if pa.types.is_dictionary(t):
            array, t = array.cast(t.value_type), t.value_type
        if t in _FLOAT_BITS:
            return array.view(_FLOAT_BITS[t])
        if pa.types.is_large_string(t) or t == pa.string_view():
            return array.cast(pa.string())
        if pa.types.is_large_binary(t) or t == pa.binary_view():
            return array.cast(pa.binary())
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        pass
    return array


def _diff_mask(a: pa.Array, b: pa.Array) -> pa.Array:
    """True where two normalised arrays of one type hold different values."""
    try:
        one_null = pc.xor(pc.is_null(a), pc.is_null(b))
        return pc.or_(one_null, pc.fill_null(pc.not_equal(a, b), False))
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        # Nested types have no element-wise comparison kernel.
        return pa.array(
            [x != y for x, y in zip(a.to_pylist(), b.to_pylist())], type=pa.bool_()
        )


def _batches(file: pq.ParquetFile, columns: list[str], batch_rows: int):
    for batch in file.iter_batches(
        batch_size=batch_rows, columns=columns, use_threads=False
    ):
        if batch.num_rows:
            yield batch


def _aligned(built, published, columns, batch_rows):
    """Equal-length slices of both files, in row order, in bounded memory."""
    ib, ip = (
        _batches(built, columns, batch_rows),
        _batches(published, columns, batch_rows),
    )
    a = b = None
    while True:
        if a is None or a.num_rows == 0:
            a = next(ib, None)
        if b is None or b.num_rows == 0:
            b = next(ip, None)
        if a is None or b is None:
            return
        n = min(a.num_rows, b.num_rows)
        yield a.slice(0, n), b.slice(0, n)
        a, b = a.slice(n), b.slice(n)


def _row_label(batch: pa.RecordBatch, index: int, columns: list[str]) -> str:
    return ", ".join(f"{n}={_show(batch.column(n)[index])}" for n in columns[:4])


def _positional_diff(built, published, columns, batch_rows):
    """Rows that differ per column when both files are read in row order."""
    counts: Counter = Counter()
    examples: list[str] = []
    offset = 0
    for a, b in _aligned(built, published, columns, batch_rows):
        for name in columns:
            ca, cb = _normalise(a.column(name)), _normalise(b.column(name))
            if ca.type != cb.type or ca.equals(cb):
                continue  # a type change is the schema check's finding
            mask = _diff_mask(ca, cb)
            differing = pc.sum(mask).as_py() or 0
            if not differing:
                continue
            counts[name] += differing
            if len(examples) < MAX_EXAMPLES:
                i = pc.index(mask, True).as_py()
                examples.append(
                    f"row {offset + i:,} ({_row_label(b, i, columns)}): {name} is "
                    f"{_show(a.column(name)[i])} in {BUILT}, "
                    f"{_show(b.column(name)[i])} in {PUBLISHED}"
                )
        offset += a.num_rows
    return counts, examples


def _normalised_table(file: pq.ParquetFile, columns: list[str]) -> pa.Table:
    table = file.read(columns=columns, use_threads=False)
    arrays = []
    for name in columns:
        chunks = [_normalise(c) for c in table.column(name).chunks]
        arrays.append(
            pa.chunked_array(chunks, type=chunks[0].type)
            if chunks
            else table.column(name)
        )
    return pa.table(arrays, names=columns)


def _same_rows(built, published, columns) -> bool:
    """True when both files hold the same rows, whatever their order."""
    a, b = _normalised_table(built, columns), _normalised_table(published, columns)
    if a.schema.types != b.schema.types:
        return False
    keys = [
        (name, "ascending")
        for name, t in zip(columns, a.schema.types)
        if not pa.types.is_nested(t)
    ]
    if not keys:
        return False
    a = a.take(pc.sort_indices(a, sort_keys=keys))
    b = b.take(pc.sort_indices(b, sort_keys=keys))
    return a.equals(b)


def _row_multiset(file: pq.ParquetFile, columns: list[str]) -> Counter:
    table = file.read(columns=columns, use_threads=False)
    cols = []
    for name in columns:
        column = table.column(name)
        values = column.to_pylist()
        if pa.types.is_nested(column.type) or pa.types.is_floating(column.type):
            values = [repr(v) for v in values]  # hashable; NaN equals NaN
        cols.append(values)
    return Counter(zip(*cols)) if cols else Counter()


def _row_set_examples(built, published, columns) -> list[str]:
    """Example rows present on one side only, for a shard small enough."""
    rows = max(built.metadata.num_rows, published.metadata.num_rows)
    if rows > EXPLAIN_LIMIT_ROWS or not columns:
        return []
    cb, cp = _row_multiset(built, columns), _row_multiset(published, columns)
    out = []
    for only, where in ((cp - cb, PUBLISHED), (cb - cp, BUILT)):
        if not only:
            continue
        row = next(iter(only))
        shown = ", ".join(f"{n}={_show(v)}" for n, v in zip(columns[:8], row))
        out.append(f"{sum(only.values()):,} row(s) only in {where}, e.g. {shown}")
    return out


def _tier_counts(file: pq.ParquetFile, batch_rows: int) -> dict[str, int] | None:
    if TIER_COLUMN not in file.schema_arrow.names:
        return None
    counts: Counter = Counter()
    for batch in _batches(file, [TIER_COLUMN], batch_rows * 16):
        for item in pc.value_counts(batch.column(TIER_COLUMN)).to_pylist():
            counts[item["values"]] += item["counts"]
    named = {TIER_NAMES.get(k, f"tier {k}"): v for k, v in counts.items()}
    return {name: named.get(name, 0) for name in (*TIER_NAMES.values(), *named)}


def compare_parquet(
    built: Path,
    published: Path,
    *,
    entity: str,
    shard: str,
    strict_order: bool = False,
    sort_limit_rows: int = SORT_LIMIT_ROWS,
    batch_rows: int = BATCH_ROWS,
) -> ShardResult:
    """Compare one rebuilt shard with the published one."""
    result = ShardResult(entity, shard)
    files = []
    for path, where in ((built, BUILT), (published, PUBLISHED)):
        try:
            files.append(pq.ParquetFile(path))
        except Exception as exc:  # noqa: BLE001 - any unreadable file is a finding
            result.problems.append(f"shard of {where} is unreadable: {exc}")
    if len(files) != 2:
        return result
    fb, fp = files
    result.rows_built, result.rows_published = (
        fb.metadata.num_rows,
        fp.metadata.num_rows,
    )
    result.problems += schema_problems(fb.schema_arrow, fp.schema_arrow)
    result.tiers_built = _tier_counts(fb, batch_rows)
    result.tiers_published = _tier_counts(fp, batch_rows)
    tier_problem = (
        f"tier counts: {_tiers(result.tiers_built)} in {BUILT}, "
        f"{_tiers(result.tiers_published)} in {PUBLISHED}"
    )
    columns = [n for n in fp.schema_arrow.names if n in fb.schema_arrow.names]
    if result.rows_built != result.rows_published:
        delta = result.rows_built - result.rows_published
        result.problems.append(
            f"rows: {result.rows_built:,} in {BUILT}, {result.rows_published:,} "
            f"in {PUBLISHED} ({delta:+,})"
        )
        if result.tiers_built != result.tiers_published:
            result.problems.append(tier_problem)
        with _WHOLE_SHARD:
            result.problems += _row_set_examples(fb, fp, columns)
        return result
    if result.tiers_built != result.tiers_published:
        result.problems.append(tier_problem)
    if not columns:
        return result
    counts, examples = _positional_diff(fb, fp, columns, batch_rows)
    if not counts:
        return result
    small = result.rows_built <= sort_limit_rows
    with _WHOLE_SHARD:
        reordered = small and _same_rows(fb, fp, columns)
    if reordered:
        target = result.problems if strict_order else result.notes
        target.append("same rows in a different order")
        return result
    detail = ", ".join(f"{name} ({n:,} rows)" for name, n in counts.most_common())
    result.problems.append(f"content: values differ in {detail}")
    result.problems += examples
    if not small:
        result.notes.append(
            f"more than {sort_limit_rows:,} rows: a difference in row order alone "
            "is not told apart from a difference in content"
        )
    return result


def _tiers(tiers: dict[str, int] | None) -> str:
    if tiers is None:
        return "no tier column"
    return ", ".join(f"{name} {n:,}" for name, n in tiers.items()) or "no rows"


# --------------------------------------------------------------------------
# A whole cache
# --------------------------------------------------------------------------


def _load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _shards(directory: Path) -> dict[str, Path]:
    if not directory.is_dir():
        return {}
    return {p.stem: p for p in sorted(directory.glob("*.parquet")) if p.is_file()}


def _natural(name: str) -> tuple:
    stem = name[3:] if name.startswith("chr") else name
    return (0, int(stem), name) if stem.isdigit() else (1, 0, name)


def _compare_shards(entity, built, published, jobs, options) -> list[ShardResult]:
    sb, sp = _shards(built), _shards(published)
    names = sorted(set(sb) | set(sp), key=_natural)

    def one(name: str) -> ShardResult:
        if name not in sb:
            result = ShardResult(entity, name, rows_published=_footer_rows(sp[name]))
            result.problems.append(f"shard missing from {BUILT}")
            return result
        if name not in sp:
            result = ShardResult(entity, name, rows_built=_footer_rows(sb[name]))
            result.problems.append(f"shard is not in {PUBLISHED}")
            return result
        return compare_parquet(sb[name], sp[name], entity=entity, shard=name, **options)

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        return list(pool.map(one, names))


def _footer_rows(path: Path) -> int | None:
    try:
        return pq.ParquetFile(path).metadata.num_rows
    except Exception:  # noqa: BLE001
        return None


def _json_sidecar_problem(name: str, built: Path, published: Path) -> str | None:
    jb, jp = _load_json(built), _load_json(published)
    if jb is None or jp is None:
        gone = [w for j, w in ((jb, BUILT), (jp, PUBLISHED)) if j is None]
        return f"{name} is missing or unreadable in {' and '.join(gone)}"
    if jb != jp:
        return f"{name} differs: {_show(jb)} in {BUILT}, {_show(jp)} in {PUBLISHED}"
    return None


def _chrom_manifest_problems(entity: str, built: Path, published: Path) -> list[str]:
    name = f"{entity}/{CHROM_MANIFEST}"
    jb, jp = _load_json(built / CHROM_MANIFEST), _load_json(published / CHROM_MANIFEST)
    if jb is None and jp is None:
        return []
    if not isinstance(jb, list) or not isinstance(jp, list):
        gone = [w for j, w in ((jb, BUILT), (jp, PUBLISHED)) if not isinstance(j, list)]
        return [f"{name} is missing or unreadable in {' and '.join(gone)}"]

    def by_chrom(entries):
        return {str(e.get("chrom")): e for e in entries if isinstance(e, dict)}

    eb, ep = by_chrom(jb), by_chrom(jp)
    differing = [
        c for c in sorted(set(eb) | set(ep), key=_natural) if eb.get(c) != ep.get(c)
    ]
    if not differing:
        return []
    first = differing[0]
    return [
        f"{name} differs for {len(differing)} contig(s), e.g. {first}: "
        f"{_show(eb.get(first))} in {BUILT}, {_show(ep.get(first))} in {PUBLISHED}"
    ]


def compare_core_cache(
    built: Path, published: Path, *, label: str = "", jobs: int = 4, **options
) -> Report:
    """Compare a rebuilt core cache with the published one, entity by entity."""
    report = Report("core", label or f"{built} vs {published}")

    def entity_dirs(root: Path) -> set[str]:
        if not root.is_dir():
            return set()
        return {
            d.name
            for d in root.iterdir()
            if d.is_dir() and not d.name.startswith(".") and _shards(d)
        }

    entities = sorted(entity_dirs(built) | entity_dirs(published))
    if not entities:
        report.problems.append("no entity with Parquet shards in either cache")
    for entity in entities:
        report.problems += _chrom_manifest_problems(
            entity, built / entity, published / entity
        )
        report.shards += _compare_shards(
            entity, built / entity, published / entity, jobs, options
        )
    for name in CORE_JSON_SIDECARS:
        problem = _json_sidecar_problem(name, built / name, published / name)
        if problem:
            report.problems.append(problem)
    for name in CORE_BYTE_SIDECARS:
        fb, fp = built / name, published / name
        if not fb.is_file() or not fp.is_file():
            if fb.is_file() != fp.is_file():
                gone = BUILT if not fb.is_file() else PUBLISHED
                report.problems.append(f"{name} is missing from {gone}")
        elif fb.read_bytes() != fp.read_bytes():
            report.problems.append(f"{name} differs between the two caches")
    return report


def _plugin_manifest_problems(built: dict, published: dict) -> list[str]:
    problems = []
    for key in PLUGIN_MANIFEST_FIELDS:
        default = PLUGIN_MANIFEST_DEFAULTS.get(key)
        vb, vp = built.get(key, default), published.get(key, default)
        if vb != vp:
            problems.append(
                f"manifest.json: {key} is {_show(vb)} in {BUILT}, {_show(vp)} "
                f"in {PUBLISHED}"
            )

    def by_chrom(manifest):
        return {
            str(c.get("chrom")): c
            for c in manifest.get("chroms") or []
            if isinstance(c, dict)
        }

    cb, cp = by_chrom(built), by_chrom(published)
    for chrom in sorted(set(cb) | set(cp), key=_natural):
        eb, ep = cb.get(chrom), cp.get(chrom)
        if eb is None or ep is None:
            gone = BUILT if eb is None else PUBLISHED
            problems.append(f"manifest.json: {chrom} is not listed in {gone}")
        elif eb != ep:
            problems.append(
                f"manifest.json: {chrom} is {_counts(eb)} in {BUILT}, "
                f"{_counts(ep)} in {PUBLISHED}"
            )
    return problems


def _counts(entry: dict) -> str:
    return ", ".join(f"{k} {entry.get(k)}" for k in ("file", "rows", "warm", "cold"))


def _own_manifest_problem(shard: ShardResult, manifest: dict, where: str, built: bool):
    """A shard that disagrees with the manifest.json shipped beside it."""
    rows = shard.rows_built if built else shard.rows_published
    tiers = shard.tiers_built if built else shard.tiers_published
    entry = next(
        (
            c
            for c in manifest.get("chroms") or []
            if isinstance(c, dict) and c.get("file") == f"{shard.shard}.parquet"
        ),
        None,
    )
    if rows is None:
        return None
    if entry is None:
        return f"shard is not listed in the manifest.json of {where}"
    held = {"rows": rows, **(tiers or {})}
    listed = {k: entry.get(k) for k in held if k in ("rows", "warm", "cold")}
    held = {k: held[k] for k in listed}
    if held != listed:
        return (
            f"manifest.json of {where} lists {_show(listed)} but the shard holds "
            f"{_show(held)}"
        )
    return None


def compare_plugin_cache(
    built: Path, published: Path, *, label: str = "", jobs: int = 4, **options
) -> Report:
    """Compare a rebuilt plugin cache (`.../plugin/<name>`) with the published one."""
    report = Report("plugin", label or f"{built} vs {published}")
    mb, mp = (
        _load_json(built / "manifest.json"),
        _load_json(published / "manifest.json"),
    )
    for manifest, where in ((mb, BUILT), (mp, PUBLISHED)):
        if not isinstance(manifest, dict):
            report.problems.append(f"manifest.json is missing or unreadable in {where}")
    if isinstance(mb, dict) and isinstance(mp, dict):
        report.problems += _plugin_manifest_problems(mb, mp)
    report.shards = _compare_shards(published.name, built, published, jobs, options)
    if not report.shards:
        report.problems.append("no Parquet shard in either cache")
    for shard in report.shards:
        for manifest, where, is_built in ((mb, BUILT, True), (mp, PUBLISHED, False)):
            if isinstance(manifest, dict):
                problem = _own_manifest_problem(shard, manifest, where, is_built)
                if problem:
                    shard.problems.append(problem)
    return report


# --------------------------------------------------------------------------
# One /parity-caches job
# --------------------------------------------------------------------------

PLUGIN_PREFIX = "plugin_"
RESTAGE_WGS = "re-run ci/wgs-runner/stage-data.sh"
RESTAGE = "re-run ci/wgs-runner/stage-cache-inputs.sh"


def _log(message: str) -> None:
    print(message, flush=True)


def _require_revision(cache: Path, repo_id: str, revision: str, restage: str) -> None:
    """The staged cache must carry the JSON identity marker of the pinned revision."""
    want = {"repo_id": repo_id, "revision": revision}
    got = _load_json(cache / ".revision")
    if got != want:
        raise RunnerNotReady(
            f"staged cache {cache} is not {repo_id}@{revision} "
            f"(.revision says {_show(got)}); {restage}"
        )


def _pin(api, config: dict, name: str) -> str:
    key = (config["release"], config["assembly"], name)
    pin = api._published.PINS.get(key)
    if pin is None:
        raise ValueError(f"{name} has no pinned revision in vepyr._published.PINS")
    return pin


def _require_scratch(work: Path, need_gb) -> None:
    if not need_gb:
        return
    free_gb = shutil.disk_usage(work).free / 2**30
    if free_gb < need_gb:
        raise RunnerNotReady(
            f"{work} has {free_gb:.0f} GiB free; this target needs {need_gb} GiB"
        )


def _staged_core(api, config: dict, data: Path, cache_type: str) -> Path:
    release, assembly = config["release"], config["assembly"]
    cache = data / "cache" / f"{release}_{assembly}_{cache_type}"
    _require_revision(
        cache,
        api._published.cache_repo_id(release, assembly, cache_type),
        _pin(api, config, cache_type),
        RESTAGE_WGS,
    )
    return cache


def run_core(api, config: dict, cache_type: str, data: Path, work: Path, jobs: int):
    release, assembly = config["release"], config["assembly"]
    entry = (config.get("core") or {}).get(cache_type) or {}
    if not entry.get("raw"):
        raise ValueError(f"no raw cache path for {cache_type} in the parity manifest")
    published = _staged_core(api, config, data, cache_type)
    raw = data / entry["raw"]
    if not (raw / "info.txt").is_file():
        raise RunnerNotReady(
            f"raw Ensembl VEP cache missing: {raw}/info.txt; {RESTAGE}"
        )
    _require_scratch(work, entry.get("scratch_gb"))
    pin = _pin(api, config, cache_type)
    _log(f"building the {cache_type} cache from {raw} with vepyr {api.__version__}")
    out = work / "built"
    api.build_cache(
        release,
        str(out),
        cache_type=cache_type,
        assembly=assembly,
        local_cache=str(raw),
        show_progress=False,
    )
    built = out / f"{release}_{assembly}_{cache_type}"
    repo_id = api._published.cache_repo_id(release, assembly, cache_type)
    _log(f"comparing {built} with {published}")
    return compare_core_cache(
        built,
        published,
        label=f"{cache_type} rebuilt with vepyr {api.__version__} vs {repo_id}@{pin[:12]}",
        jobs=jobs,
    )


def plugin_build_plan(plugin: str, published_manifest: dict, entry: dict, data: Path):
    """How to rebuild a plugin cache the way the published one was built.

    The sources, the contigs and the manifest revision come from the published
    `manifest.json`; the parity manifest may name another `manifest_ref`.
    Returns `(manifest_ref, source_path, verify_source, chroms)`.
    """
    sources = published_manifest.get("sources") or []
    if not sources:
        raise ValueError(f"the published {plugin} manifest.json records no sources")
    paths: dict[str | None, str] = {}
    for source in sources:
        files = [source.get("file"), (source.get("index") or {}).get("file")]
        if not files[0]:
            raise ValueError(f"a published {plugin} source records no file name")
        for name in filter(None, files):
            path = data / "plugin-source" / plugin / name
            if not path.is_file():
                raise RunnerNotReady(
                    f"staged plugin source missing: {path} "
                    f"(url {source.get('url')}, md5 {source.get('md5')}); {RESTAGE}"
                )
        paths[source.get("part")] = str(data / "plugin-source" / plugin / files[0])
    if len(paths) != len(sources) or (len(sources) > 1 and None in paths):
        raise ValueError(f"the published {plugin} sources do not name distinct parts")
    source_path = paths[None] if list(paths) == [None] else paths
    # The published build verified these bytes: equal digests mean the upstream
    # file was used as is and the build can insist on it. Otherwise the input
    # is a derived artifact (a BGZF re-compression) no upstream digest matches.
    exact = all(s.get("md5") == s.get("verified_md5") for s in sources if s.get("md5"))
    recorded = str(published_manifest.get("cache_source_version") or "")
    ref = entry.get("manifest_ref") or recorded.rpartition("@")[2]
    if not ref:
        raise ValueError(
            f"the published {plugin} manifest.json has no cache_source_version and "
            "the parity manifest names no manifest_ref"
        )
    # A build without `chroms` writes a shard for every contig of the tiering
    # cache (patches and scaffolds included); the published cache holds these.
    chroms = [
        c["chrom"]
        for c in published_manifest.get("chroms") or []
        if isinstance(c, dict) and c.get("chrom")
    ]
    if not chroms:
        raise ValueError(f"the published {plugin} manifest.json lists no contigs")
    return ref, source_path, "strict" if exact else "warn", chroms


def _source_notes(built_manifest: dict | None, published_manifest: dict) -> list[str]:
    """Source digests of this build that differ from the published build's."""
    notes = []
    built = {s.get("part"): s for s in (built_manifest or {}).get("sources") or []}
    for source in published_manifest.get("sources") or []:
        mine = built.get(source.get("part")) or {}
        found, wanted = mine.get("verified_md5"), source.get("verified_md5")
        if found and wanted and found != wanted:
            notes.append(
                f"source {source.get('file')} has md5 {found}; the published cache "
                f"was built from md5 {wanted}"
            )
    return notes


def run_plugin(api, config: dict, plugin: str, data: Path, work: Path, jobs: int):
    release, assembly = config["release"], config["assembly"]
    entry = (config.get("plugins") or {}).get(plugin) or {}
    pin = _pin(api, config, PLUGIN_PREFIX + plugin)
    repo_id = api._published.plugin_repo_id(release, assembly, plugin)
    _require_scratch(work, entry.get("scratch_gb"))
    staged = data / "plugin-cache" / f"{release}_{assembly}" / "plugin" / plugin
    want = {"repo_id": repo_id, "revision": pin}
    if _load_json(staged / ".revision") == want:
        published = staged
        _log(f"published {plugin} cache: staged copy {staged}")
    else:
        # Not staged, or staged at another revision (a PR that moves the pin):
        # the pinned revision is small enough to fetch for every default plugin.
        _log(f"published {plugin} cache: downloading {repo_id}@{pin}")
        published = Path(
            api.download_plugin_cache(
                plugin,
                release,
                str(work / "published"),
                assembly=assembly,
                show_progress=False,
            )
        )
    published_manifest = _load_json(published / "manifest.json")
    if not isinstance(published_manifest, dict):
        raise ValueError(f"{published}/manifest.json is missing or unreadable")
    tiering = _staged_core(
        api, config, data, config.get("plugin_tiering_cache", "merged")
    )
    ref, source_path, verify, chroms = plugin_build_plan(
        plugin, published_manifest, entry, data
    )
    _log(
        f"building the {plugin} plugin cache with vepyr {api.__version__}: "
        f"manifest {ref}, {len(chroms)} contigs, verify_source={verify}, "
        f"tiering from {tiering}"
    )
    out = work / "built"
    api.build_plugin_cache(
        plugin,
        ref,
        source_path=source_path,
        cache_dir=str(tiering),
        plugin_cache_root=str(out),
        chroms=chroms,
        verify_source=verify,
    )
    built = out / "plugin" / plugin
    _log(f"comparing {built} with {published}")
    report = compare_plugin_cache(
        built,
        published,
        label=(
            f"plugin {plugin} rebuilt with vepyr {api.__version__} (manifest "
            f"{ref[:12]}) vs {repo_id}@{pin[:12]}"
        ),
        jobs=jobs,
    )
    report.notes += _source_notes(
        _load_json(built / "manifest.json"), published_manifest
    )
    return report


def run_target(
    target: str, *, data: Path, work: Path, config: dict, api=None, jobs: int = 4
) -> Report:
    """Rebuild one target and compare it with the published cache."""
    if api is None:
        import vepyr as api  # the wheel under test
    work.mkdir(parents=True, exist_ok=True)
    if target.startswith(PLUGIN_PREFIX):
        return run_plugin(api, config, target[len(PLUGIN_PREFIX) :], data, work, jobs)
    return run_core(api, config, target, data, work, jobs)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _write(path: Path | None, text: str) -> None:
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p):
        p.add_argument("--summary", type=Path, help="write the text summary here")
        p.add_argument("--report", type=Path, help="write the JSON report here")
        p.add_argument("--jobs", type=int, default=4, help="shards compared at once")

    for name in ("compare-core", "compare-plugin"):
        p = sub.add_parser(name)
        p.add_argument("built", type=Path)
        p.add_argument("published", type=Path)
        p.add_argument(
            "--strict-order",
            action="store_true",
            help="fail when the rows are the same but their order differs",
        )
        common(p)
    p = sub.add_parser("run")
    p.add_argument("--target", required=True, help="a cache type or plugin_<name>")
    p.add_argument("--data", type=Path, required=True, help="staged data root")
    p.add_argument("--work", type=Path, required=True, help="scratch directory")
    p.add_argument("--manifest", type=Path, required=True)
    common(p)
    args = parser.parse_args(argv)

    try:
        if args.command == "run":
            config = json.loads(args.manifest.read_text(encoding="utf-8"))
            report = run_target(
                args.target,
                data=args.data,
                work=args.work,
                config=config,
                jobs=args.jobs,
            )
        else:
            compare = (
                compare_core_cache
                if args.command == "compare-core"
                else compare_plugin_cache
            )
            report = compare(
                args.built,
                args.published,
                jobs=args.jobs,
                strict_order=args.strict_order,
            )
    except RunnerNotReady as exc:
        text = f"ERROR: {exc}\n"
        _write(args.summary, text)
        print(text, end="", flush=True)
        return 3
    except Exception as exc:  # noqa: BLE001 - reported as an error, never a verdict
        traceback.print_exc()
        first = (str(exc).splitlines() or [""])[0]
        text = f"ERROR: {type(exc).__name__}: {first}\n"
        _write(args.summary, text)
        print(text, end="", flush=True)
        return 4
    _write(args.summary, report.text())
    _write(args.report, json.dumps(report.to_json(), indent=2) + "\n")
    print(report.text(), end="", flush=True)
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
