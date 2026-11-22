"""Per-chromosome lines and whole-genome totals of the tiering comparison.

    python aggregate_wgs.py WGS_ROOT [--pages PAGES_ROOT] [--chroms chr1 ... chr22]

WGS_ROOT/<chrom>/summary.json are written by run_tiering_gain.py, PAGES_ROOT/<chrom>/pages.json by
pages_summary.py. Chromosomes without a summary yet are skipped, so this can run after every
chromosome. Totals add the per-chromosome medians of each layout; ratio = sorted / tiered,
reduction = 1 - tiered / sorted. Writes WGS_ROOT/wgs_summary.md and WGS_ROOT/wgs_summary.json.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

CHROMS = [f"chr{i}" for i in range(1, 23)]
METRICS = ("wall_s", "cpu_s", "annotation_s", "max_rss_kib")
PAGE_METRICS = (
    "cand_pages",
    "hit_pages",
    "p3_req_ranges",
    "p2_bytes",
    "p3_bytes",
    "p2_ops",
    "p3_ops",
)


def chromosome_rows(root: Path, chroms: list[str]) -> list[dict]:
    rows = []
    for chrom in chroms:
        path = Path(root) / chrom / "summary.json"
        if not path.exists():
            continue
        summary = json.loads(path.read_text())
        row: dict = {"chrom": chrom, "invalid_runs": summary.get("invalid_runs", 0)}
        for e in summary["layouts"]:
            key = (e["mode"], e["workers"])
            cell = row.setdefault(key, {"tiered": {}, "sorted": {}, "original": {}})
            cell[e["layout"]] = {m: e[m]["median"] for m in METRICS if m in e}
        for key, cell in row.items():
            if isinstance(key, tuple) and cell["tiered"] and cell["sorted"]:
                cell["ratio"] = {
                    m: cell["sorted"][m] / cell["tiered"][m]
                    for m in cell["tiered"]
                    if m in cell["sorted"] and cell["tiered"][m]
                }
            if isinstance(key, tuple) and cell["tiered"] and cell["original"]:
                cell["ratio_original"] = {
                    m: cell["original"][m] / cell["tiered"][m]
                    for m in cell["tiered"]
                    if m in cell["original"] and cell["tiered"][m]
                }
        rows.append(row)
    return rows


def totals(rows: list[dict]) -> dict:
    out: dict = {
        "chromosomes": len(rows),
        "invalid_runs": sum(r["invalid_runs"] for r in rows),
    }
    keys = sorted({k for r in rows for k in r if isinstance(k, tuple)})
    for key in keys:
        have = [
            r[key] for r in rows if key in r and r[key]["tiered"] and r[key]["sorted"]
        ]
        cell: dict = {
            "chromosomes": len(have),
            "tiered": {},
            "sorted": {},
            "original": {},
            "ratio": {},
            "reduction": {},
            "ratio_original": {},
        }
        for m in METRICS:
            if m == "max_rss_kib":
                continue  # peak memory does not add up across chromosomes
            t = sum(c["tiered"].get(m, 0.0) for c in have)
            s = sum(c["sorted"].get(m, 0.0) for c in have)
            cell["tiered"][m], cell["sorted"][m] = t, s
            if t and s:
                cell["ratio"][m], cell["reduction"][m] = s / t, 1 - t / s
            if have and all(c["original"] for c in have):
                o = sum(c["original"].get(m, 0.0) for c in have)
                cell["original"][m] = o
                if t and o:
                    cell["ratio_original"][m] = o / t
        out[key] = cell
    return out


def pages_totals(root: Path, chroms: list[str]) -> dict:
    out: dict = {"chromosomes": 0}
    for chrom in chroms:
        path = Path(root) / chrom / "pages.json"
        if not path.exists():
            continue
        out["chromosomes"] += 1
        for buf in json.loads(path.read_text())["buffers"].values():
            for m in PAGE_METRICS:
                if m in buf:
                    cell = out.setdefault(m, {"tiered": 0, "sorted": 0})
                    cell["tiered"] += buf[m]["tiered"]
                    cell["sorted"] += buf[m]["sorted"]
    for m in PAGE_METRICS:
        if m in out and out[m]["sorted"]:
            out[m]["reduction"] = 1 - out[m]["tiered"] / out[m]["sorted"]
    return out


def _fmt(row_cell: dict | None, layout: str, m: str) -> str:
    if not row_cell or m not in row_cell.get(layout, {}):
        return "-"
    return f"{row_cell[layout][m]:.2f}"


def render(rows: list[dict], tot: dict, pages: dict | None) -> str:
    keys = [("warm", 1), ("warm", 8), ("cold", 1), ("cold", 8)]
    head = (
        "| Chromosome | "
        + " | ".join(
            f"{m} {w}w wall tiered/sorted (ratio), orig/tiered" for m, w in keys
        )
        + " | invalid |"
    )
    lines = [head, "|" + "---|" * (len(keys) + 2)]

    def cells(c: dict | None, metric: str) -> str:
        if not c or "ratio" not in c or metric not in c["ratio"]:
            return "-"
        text = f"{c['tiered'][metric]:.1f} / {c['sorted'][metric]:.1f} ({c['ratio'][metric]:.2f})"
        if metric in c.get("ratio_original", {}):
            text += f", orig {c['ratio_original'][metric]:.2f}"
        return text

    for r in rows:
        lines.append(
            f"| {r['chrom']} | "
            + " | ".join(cells(r.get(k), "wall_s") for k in keys)
            + f" | {r['invalid_runs']} |"
        )
    lines.append(
        f"| **total ({tot['chromosomes']})** | "
        + " | ".join(cells(tot.get(k), "wall_s") for k in keys)
        + f" | {tot['invalid_runs']} |"
    )
    lines += ["", "CPU (user + system), sum of per-chromosome medians:", ""]
    for k in keys:
        c = tot.get(k)
        if c and "cpu_s" in c["ratio"]:
            lines.append(
                f"- {k[0]}, {k[1]} workers: tiered {c['tiered']['cpu_s']:.1f} s, sorted {c['sorted']['cpu_s']:.1f} s, "
                f"ratio {c['ratio']['cpu_s']:.3f}, reduction {c['reduction']['cpu_s']:.1%}; wall reduction "
                f"{c['reduction']['wall_s']:.1%}"
            )
    lines += [
        "",
        "original / tiered (the shipped cache against the rewritten current layout):",
        "",
    ]
    for k in keys:
        c = tot.get(k)
        if c and c.get("ratio_original"):
            lines.append(
                f"- {k[0]}, {k[1]} workers: "
                + ", ".join(f"{m} {v:.3f}" for m, v in c["ratio_original"].items())
            )
    if pages:
        lines += [
            "",
            f"Pages and bytes per lookup, summed over all buffers of {pages['chromosomes']} chromosomes:",
            "",
        ]
        for m in PAGE_METRICS:
            if m in pages:
                p = pages[m]
                lines.append(
                    f"- {m}: tiered {p['tiered']:,}, sorted {p['sorted']:,}, reduction {p.get('reduction', 0):.1%}"
                )
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("root", type=Path)
    ap.add_argument("--pages", type=Path)
    ap.add_argument("--chroms", nargs="+", default=CHROMS)
    args = ap.parse_args()
    rows = chromosome_rows(args.root, args.chroms)
    tot = totals(rows)
    pages = pages_totals(args.pages, args.chroms) if args.pages else None
    text = render(rows, tot, pages)
    (args.root / "wgs_summary.md").write_text(text)
    serial = {
        "chromosomes": [{"chrom": r["chrom"], "invalid_runs": r["invalid_runs"],
                         **{f"{k[0]}_{k[1]}w": v for k, v in r.items() if isinstance(k, tuple)}} for r in rows],
        "totals": {(f"{k[0]}_{k[1]}w" if isinstance(k, tuple) else k): v for k, v in tot.items()},
        "pages": pages,
    }  # fmt: skip
    (args.root / "wgs_summary.json").write_text(json.dumps(serial, indent=2) + "\n")
    print(text)


if __name__ == "__main__":
    main()
