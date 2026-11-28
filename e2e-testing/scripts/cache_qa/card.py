"""Render the `## Quality profile` card section and splice it into a README."""

from __future__ import annotations

START = "<!-- qa-profile:start -->"
END = "<!-- qa-profile:end -->"
_STATUS_ICON = {"pass": "✅", "warn": "⚠️", "fail": "❌"}


def format_int(n: int) -> str:
    return f"{int(n):,}"


def format_bytes(n: int) -> str:
    n = int(n)
    if n < 1_000:
        return f"{n} B"
    for unit, div in (("KB", 1e3), ("MB", 1e6), ("GB", 1e9), ("TB", 1e12)):
        if n < div * 1000 or unit == "TB":
            v = n / div
            return f"{v:.1f} {unit}" if v < 10 else f"{v:.0f} {unit}"
    raise AssertionError("unreachable")


def format_pct(x: float) -> str:
    return f"{100 * float(x):.1f}%"


def format_count_short(n: int) -> str:
    n = int(n)
    if n >= 1_000_000:
        v = n / 1e6
        return f"{v:.1f}M" if v < 10 else f"{v:.0f}M"
    if n >= 1_000:
        v = n / 1e3
        return f"{v:.1f}K" if v < 10 else f"{v:.0f}K"
    return str(n)


def _numeric_cell(numeric: dict | None) -> str:
    if not numeric:
        return "—"
    return " / ".join(f"{numeric[k]:.3f}" for k in ("min", "p50", "p95", "max"))


def _top_cell(top: list | None) -> str:
    if not top:
        return "—"
    return ", ".join(f"{v} ({format_count_short(n)})" for v, n in top)


def _distinct_cell(col: dict) -> str:
    if col["distinct"] is None:
        return "—"
    if col["approx"]:
        return f"~{format_count_short(col['distinct'])}"
    return format_int(col["distinct"])


def render_section(report: dict) -> str:
    tool = report["tool"]
    lines = [
        "## Quality profile",
        "",
        f"Generated {report['generated_at'][:10]} by `profile_plugin_cache.py` "
        f"(vepyr {tool['vepyr']}, Polars {tool['polars']}) from the shards in this "
        "commit; machine-readable copy in [`qa_profile.json`](qa_profile.json).",
        "",
        "### Invariants",
        "",
        "| check | status | detail |",
        "|---|---|---|",
    ]
    for inv in report["invariants"]:
        icon = _STATUS_ICON[inv["status"]]
        lines.append(f"| {inv['id']} | {icon} {inv['status']} | {inv['detail']} |")
    lines += [
        "",
        "### Contigs",
        "",
        "| contig | rows | warm | cold | warm % | size |",
        "|---|--:|--:|--:|--:|--:|",
    ]
    for c in report["contigs"]:
        lines.append(
            f"| {c['chrom']} | {format_int(c['rows'])} | {format_int(c['warm'])} "
            f"| {format_int(c['cold'])} | {format_pct(c['warm_share'])} "
            f"| {format_bytes(c['bytes'])} |"
        )
    s = report["summary"]
    warm_share = s["warm"] / s["rows"] if s["rows"] else 0.0
    lines.append(
        f"| **total** | **{format_int(s['rows'])}** | **{format_int(s['warm'])}** "
        f"| **{format_int(s['cold'])}** | **{format_pct(warm_share)}** "
        f"| **{format_bytes(s['bytes'])}** |"
    )
    lines += [
        "",
        "### Columns",
        "",
        "| column | role | type | null % | empty % | distinct "
        "| numeric (min / p50 / p95 / max) | top values |",
        "|---|---|---|--:|--:|--:|---|---|",
    ]
    for col in report["columns"]:
        if col["role"] not in ("match", "value"):
            continue
        empty = "—" if col["empty_share"] is None else f"{100 * col['empty_share']:.2f}"
        lines.append(
            f"| {col['name']} | {col['role']} | {col['dtype']} "
            f"| {100 * col['null_share']:.2f} | {empty} | {_distinct_cell(col)} "
            f"| {_numeric_cell(col['numeric'])} | {_top_cell(col['top_values'])} |"
        )
    return "\n".join(lines) + "\n"


def splice(readme: str, section: str) -> str:
    """Replace the marked block, else insert it before `## Usage`, else append."""
    block = f"{START}\n{section.rstrip()}\n{END}\n"
    if START in readme and END in readme:
        head = readme[: readme.index(START)]
        tail = readme[readme.index(END) + len(END) :].lstrip("\n")
        return head + block + ("\n" + tail if tail else "")
    marker = "\n## Usage"
    if marker in readme:
        i = readme.index(marker) + 1
        return readme[:i] + block + "\n" + readme[i:]
    return readme.rstrip("\n") + "\n\n" + block
