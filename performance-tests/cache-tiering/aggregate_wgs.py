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


LAYOUTS = ("tiered", "sorted", "original")


def page_rows(root: Path, chroms: list[str], layouts=LAYOUTS) -> dict[str, dict]:
    """Per chromosome and layout, page counts summed over all buffers of the page tool's output
    (<root>/<chrom>/<layout>_ext.txt): pages matched, pages decoded to find the rows (position
    column) and to read them (all columns), and both together."""
    import pages_summary

    out: dict[str, dict] = {}
    for chrom in chroms:
        per: dict[str, dict] = {}
        for layout in layouts:
            path = Path(root) / chrom / f"{layout}_ext.txt"
            if not path.exists():
                continue
            bufs = pages_summary.parse(path).values()
            find = sum(b["cand_pages"] for b in bufs)
            read = sum(b["p3_req_ranges"] for b in bufs)
            per[layout] = {
                "hit_pages": sum(b["hit_pages"] for b in bufs),
                "find_decoded": find,
                "read_decoded": read,
                "decoded": find + read,
            }
        if per:
            out[chrom] = per
    return out


def page_totals_by_layout(rows: dict[str, dict]) -> dict[str, dict]:
    tot: dict[str, dict] = {}
    for per in rows.values():
        for layout, vals in per.items():
            cell = tot.setdefault(layout, {})
            for k, v in vals.items():
                cell[k] = cell.get(k, 0) + v
    return tot


def render_pages(rows: dict[str, dict], tot: dict[str, dict]) -> str:
    layouts = [lay for lay in LAYOUTS if any(lay in per for per in rows.values())]
    names = " / ".join(layouts)

    def trio(per: dict, key: str) -> str:
        return " / ".join(f"{per[lay][key]}" if lay in per else "-" for lay in layouts)

    def red(per: dict, key: str, other: str) -> str:
        if other in per and "tiered" in per and per[other][key]:
            return f"{1 - per['tiered'][key] / per[other][key]:.1%}"
        return "-"

    lines = [
        f"| Chromosome | pages matched ({names}) | pages decoded, find + read ({names}) | "
        "decoded: tiered vs sorted | decoded: tiered vs original |",
        "|---|---|---|---|---|",
    ]
    for chrom, per in list(rows.items()) + [("total", tot)]:
        label = f"**{chrom}**" if chrom == "total" else chrom
        lines.append(
            f"| {label} | {trio(per, 'hit_pages')} | {trio(per, 'decoded')} | "
            f"{red(per, 'decoded', 'sorted')} | {red(per, 'decoded', 'original')} |"
        )
    lines += [
        "",
        "Pages decoded split into find (position column) and read (all columns of the matched rows):",
        "",
    ]
    for lay in layouts:
        if lay in tot:
            lines.append(
                f"- {lay}: find {tot[lay]['find_decoded']:,}, read {tot[lay]['read_decoded']:,}"
            )
    return "\n".join(lines) + "\n"


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
    ap.add_argument(
        "--page-dir",
        type=Path,
        help="per-chromosome raw page-tool outputs, all layouts",
    )
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
    if args.page_dir:
        prows = page_rows(args.page_dir, args.chroms)
        ptext = render_pages(prows, page_totals_by_layout(prows))
        (args.root / "wgs_pages.md").write_text(ptext)
        print(ptext)


if __name__ == "__main__":
    main()
