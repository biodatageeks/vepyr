"""Assemble qa_profile.json from invariant results and the content profile."""

from __future__ import annotations

import json
from dataclasses import asdict
from importlib import metadata
from pathlib import Path

import polars as pl

from cache_qa.invariants import InvariantResult
from cache_qa.manifest import CacheManifest
from cache_qa.profile import Profile

SCHEMA_VERSION = 1


def overall_status(results: list[InvariantResult]) -> str:
    statuses = {r.status for r in results}
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    return "pass"


def tool_versions() -> dict[str, str | int]:
    try:
        vepyr_version = metadata.version("vepyr")
    except metadata.PackageNotFoundError:
        vepyr_version = "unknown"
    return {
        "vepyr": vepyr_version,
        "polars": pl.__version__,
        "schema_version": SCHEMA_VERSION,
    }


def _invariant_dict(r: InvariantResult) -> dict:
    d = {"id": r.id, "status": r.status, "detail": r.detail}
    if r.per_contig:
        d["per_contig"] = dict(r.per_contig)
    return d


def _column_dict(c) -> dict:
    d = asdict(c)
    if d["top_values"] is not None:
        d["top_values"] = [[v, n] for v, n in d["top_values"]]
    return d


def build_report(
    m: CacheManifest,
    results: list[InvariantResult],
    profile: Profile,
    generated_at: str,
    tool: dict[str, str | int],
) -> dict:
    return {
        "plugin": m.plugin,
        "cache_source_version": m.cache_source_version,
        "generated_at": generated_at,
        "tool": dict(tool),
        "status": overall_status(results),
        "invariants": [_invariant_dict(r) for r in results],
        "summary": {
            "rows": profile.rows,
            "warm": profile.warm,
            "cold": profile.cold,
            "bytes": profile.bytes,
            "contigs": len(profile.contigs),
        },
        "contigs": [asdict(c) for c in profile.contigs],
        "columns": [_column_dict(c) for c in profile.columns],
    }


def write_report(report: dict, path: Path) -> None:
    Path(path).write_text(json.dumps(report, indent=2) + "\n")
