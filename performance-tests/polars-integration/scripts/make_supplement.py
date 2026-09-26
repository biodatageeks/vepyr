"""Raw results -> LaTeX tables and PDF figures for papers/vepyr/supplementary/."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from statistics import median

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402

from pibench.paths import FORK_TO_WORKERS, PKG_ROOT  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression  # noqa: E402

_QORDER = {q.id: i for i, q in enumerate(QUERIES)}
_TIER = {q.id: q.tier for q in QUERIES}
_PATHS = ("collect", "vcf", "parquet")


def _qkey(key: tuple) -> tuple:
    """Sort (query, ...) keys in catalogue order, unknown queries last."""
    return (_QORDER.get(key[0], len(_QORDER)), key[0], *key[1:])


_ESC = {
    "\\": r"\textbackslash{}",
    "&": r"\&",
    "%": r"\%",
    "$": r"\$",
    "#": r"\#",
    "_": r"\_",
    "{": r"\{",
    "}": r"\}",
    "~": r"\textasciitilde{}",
    "^": r"\textasciicircum{}",
    "<": r"$<$",
    ">": r"$>$",
}


_OPS = {">=": r"$\geq$", "<=": r"$\leq$"}


def latex_escape(s: str) -> str:
    out, i = [], 0
    while i < len(s):
        op = _OPS.get(s[i : i + 2])
        if op:
            out.append(op)
            i += 2
        else:
            out.append(_ESC.get(s[i], s[i]))
            i += 1
    return "".join(out)


def fmt_s(x: float) -> str:
    return f"{x:.2f}" if x < 10 else f"{x:.1f}" if x < 1000 else f"{x:.0f}"


def _gib(b: float) -> str:
    return f"{b / 2**30:.2f}"


def _tabular(
    cols: str, header: list[str], rows: list[list[str]], long: bool = False
) -> str:
    body = "\n".join(" & ".join(r) + r" \\" for r in rows)
    if long:
        head = " & ".join(header) + " \\\\\n\\midrule\n"
        return (
            f"\\begin{{longtable}}{{{cols}}}\n\\toprule\n"
            + head
            + "\\endfirsthead\n\\toprule\n"
            + head
            + "\\endhead\n"
            + body
            + "\n\\bottomrule\n\\end{longtable}\n"
        )
    return (
        f"\\begin{{tabular}}{{{cols}}}\n\\toprule\n"
        + " & ".join(header)
        + " \\\\\n\\midrule\n"
        + body
        + "\n\\bottomrule\n\\end{tabular}\n"
    )


def equivalence_table(queries, long: bool = False) -> str:
    rows = [
        [
            q.id,
            latex_escape(q.tier),
            r"\texttt{" + latex_escape(filter_vep_expression(q)) + "}",
            latex_escape(q.note),
        ]
        for q in queries
    ]
    return _tabular(
        "llp{0.55\\linewidth}p{0.2\\linewidth}",
        ["Id", "Tier", r"\texttt{filter\_vep}", "Note"],
        rows,
        long=long,
    )


def filter_only_table(rows: list[dict]) -> str:
    by = defaultdict(dict)
    for r in rows:
        qid, tool = r.get("query"), r.get("tool")
        if qid is None or tool is None:
            continue
        by[qid][tool] = r
    out = []
    for qid, t in sorted(by.items(), key=lambda kv: _qkey((kv[0],))):
        fv, pl_ = t.get("filter_vep"), t.get("polars")
        if not (fv and pl_):
            continue
        out.append(
            [
                qid,
                str(fv["rows"]),
                fmt_s(fv["median_wall_s"]),
                fmt_s(pl_["median_wall_s"]),
                f"{fv['median_wall_s'] / pl_['median_wall_s']:.1f}",
                _gib(pl_["median_rss_bytes"]),
            ]
        )
    return _tabular(
        "lrrrrr",
        [
            "Query",
            "Rows",
            r"\texttt{filter\_vep} (s)",
            "Polars (s)",
            "Speed-up",
            "Polars RSS (GiB)",
        ],
        out,
    )


def e2e_table(rows: list[dict], procs: int | None = None) -> str:
    vep = {}
    vy = {}
    for r in rows:
        tool = r.get("tool")
        if tool == "vep" and "query" in r and "processes" in r:
            vep[(r["query"], r["processes"])] = r
        elif tool == "vepyr" and "query" in r and "path" in r and "processes" in r:
            vy[(r["query"], r["path"], r["processes"])] = r
    out = []
    for q in QUERIES:
        for fork, w in FORK_TO_WORKERS.items():
            if procs is not None and fork + 1 != procs:
                continue
            v = vep.get((q.id, fork + 1))
            if not v:
                continue
            cells = [q.id, f"{fork + 1} / {w}", fmt_s(v["median_wall_s"])]
            for path in ("collect", "vcf", "parquet"):
                r = vy.get((q.id, path, w))
                cells.append(
                    f"{fmt_s(r['median_wall_s'])} ({v['median_wall_s'] / r['median_wall_s']:.0f}$\\times$)"
                    if r
                    else "--"
                )
            out.append(cells)
    return _tabular(
        "llrrrr",
        [
            "Query",
            "Processes VEP / vepyr",
            "VEP + \\texttt{filter\\_vep} (s)",
            "collect (s)",
            "sink\\_vcf (s)",
            "sink\\_parquet (s)",
        ],
        out,
    )


def pushdown_table(rows: list[dict], workers: int | None = None) -> str:
    by = defaultdict(dict)
    for r in rows:
        qid, path, w, variant = (
            r.get("query"),
            r.get("path"),
            r.get("workers"),
            r.get("variant"),
        )
        if qid is None or path is None or w is None or variant is None:
            continue
        if workers is not None and w != workers:
            continue
        by[(qid, path, w)][variant] = r
    out = []
    for (qid, path, w), v in sorted(by.items(), key=lambda kv: _qkey(kv[0])):
        on = v.get("pushdown") or v.get("narrow")
        off = v.get("no_pushdown") or v.get("full")
        if on and off:
            kind = "region" if "pushdown" in v else "projection"
            out.append(
                [
                    qid,
                    kind,
                    latex_escape(path),
                    str(w),
                    fmt_s(off["median_wall_s"]),
                    fmt_s(on["median_wall_s"]),
                    f"{off['median_wall_s'] / on['median_wall_s']:.1f}",
                ]
            )
    return _tabular(
        "lllrrrr",
        [
            "Query",
            "Pushdown",
            "Output",
            "Workers",
            "Without (s)",
            "With (s)",
            "Speed-up",
        ],
        out,
    )


# ---- flat per-config rows (the CSVs; the tables below are built on them) --


def filter_only_rows(rows: list[dict]) -> list[dict]:
    by = defaultdict(dict)
    for r in rows:
        if r.get("query") is not None and r.get("tool") is not None:
            by[r["query"]][r["tool"]] = r
    out = []
    for qid, t in sorted(by.items(), key=lambda kv: _qkey((kv[0],))):
        fv, pl_ = t.get("filter_vep"), t.get("polars")
        if not (fv and pl_):
            continue
        out.append(
            {
                "query": qid,
                "group": _TIER.get(qid, ""),
                "filter_vep_rows": fv.get("rows"),
                "polars_rows": pl_.get("rows"),
                "filter_vep_wall_s": fv["median_wall_s"],
                "polars_wall_s": pl_["median_wall_s"],
                "filter_vep_rss_bytes": fv.get("median_rss_bytes"),
                "polars_rss_bytes": pl_.get("median_rss_bytes"),
                "speedup": fv["median_wall_s"] / pl_["median_wall_s"],
            }
        )
    return out


def e2e_rows(rows: list[dict]) -> list[dict]:
    """One row per (query, processes): VEP annotate + filter_vep vs vepyr paths.

    The speed-up is taken against the slowest vepyr path, so it is a floor.
    """
    vep, vy = {}, {}
    for r in rows:
        if r.get("tool") == "vep" and "query" in r and "processes" in r:
            vep[(r["query"], r["processes"])] = r
        elif r.get("tool") == "vepyr" and {"query", "path", "processes"} <= r.keys():
            vy[(r["query"], r["processes"], r["path"])] = r
    keys = sorted({k[:2] for k in vy} | set(vep), key=_qkey)
    out = []
    for qid, p in keys:
        v = vep.get((qid, p))
        row = {
            "query": qid,
            "group": _TIER.get(qid, ""),
            "processes": p,
            "vep_fork": v.get("fork") if v else None,
            "vep_annotate_s": v.get("annotate_median_wall_s") if v else None,
            "vep_filter_s": (v.get("filter") or {}).get("median_wall_s") if v else None,
            "vep_total_s": v["median_wall_s"] if v else None,
            "vepyr_workers": None,
        }
        walls = []
        for path in _PATHS:
            r = vy.get((qid, p, path))
            row[f"vepyr_{path}_s"] = r["median_wall_s"] if r else None
            row[f"vepyr_{path}_rss_bytes"] = r.get("median_rss_bytes") if r else None
            if r and row["vepyr_workers"] is None:
                row["vepyr_workers"] = r.get("workers")
            if r:
                walls.append(r["median_wall_s"])
        row["vepyr_slowest_s"] = max(walls) if walls else None
        row["speedup_vs_slowest"] = (
            row["vep_total_s"] / row["vepyr_slowest_s"] if v and walls else None
        )
        out.append(row)
    return out


def e2e_sliced_rows(sliced: list[dict], e2e: list[dict]) -> list[dict]:
    """Tabix-sliced VEP (slice, annotate, filter_vep) vs the same vepyr runs."""
    by = {(r["query"], r["processes"]): r for r in e2e}
    out = []
    for s in sorted(sliced, key=lambda r: _qkey((r["query"], r["processes"]))):
        e = by.get((s["query"], s["processes"]), {})
        slowest = e.get("vepyr_slowest_s")
        out.append(
            {
                "query": s["query"],
                "processes": s["processes"],
                "vep_fork": s.get("fork"),
                "vep_sliced_rows": s.get("rows"),
                "vep_sliced_total_s": s["median_wall_s"],
                "vep_whole_chrom_total_s": e.get("vep_total_s"),
                **{f"vepyr_{p}_s": e.get(f"vepyr_{p}_s") for p in _PATHS},
                "vepyr_slowest_s": slowest,
                "speedup_vs_slowest": s["median_wall_s"] / slowest if slowest else None,
            }
        )
    return out


def pushdown_rows(rows: list[dict]) -> list[dict]:
    by = defaultdict(dict)
    for r in rows:
        k = (r.get("query"), r.get("path"), r.get("workers"))
        if None in k or r.get("variant") is None:
            continue
        by[k][r["variant"]] = r
    out = []
    for (qid, path, w), v in sorted(by.items(), key=lambda kv: _qkey(kv[0])):
        on = v.get("pushdown") or v.get("narrow")
        off = v.get("no_pushdown") or v.get("full")
        if not (on and off):
            continue
        out.append(
            {
                "query": qid,
                "kind": "region" if "pushdown" in v else "projection",
                "path": path,
                "workers": w,
                "without_s": off["median_wall_s"],
                "with_s": on["median_wall_s"],
                "without_rss_bytes": off.get("median_rss_bytes"),
                "with_rss_bytes": on.get("median_rss_bytes"),
                "speedup": off["median_wall_s"] / on["median_wall_s"],
            }
        )
    return out


def vep_matrix_rows(rows: list[dict]) -> list[dict]:
    return [
        {
            "mode": r["mode"],
            "fork": r["fork"],
            "processes": r["processes"],
            "median_wall_s": r["median_wall_s"],
            "min_wall_s": r.get("min_wall_s"),
            "max_wall_s": r.get("max_wall_s"),
            "median_rss_bytes": r.get("median_rss_bytes"),
        }
        for r in sorted(rows, key=lambda r: (r["mode"], r["fork"]))
        if "mode" in r and "fork" in r
    ]


def plugin_fix_rows(old: list[dict], new: list[dict]) -> list[dict]:
    """Experiment C before vs after the plugin fix, where both builds ran it."""

    def key(r):
        return (r.get("query"), r.get("path"), r.get("variant"), r.get("workers"))

    fixed = {key(r): r for r in new}
    out = []
    for r in sorted(old, key=lambda r: _qkey(key(r))):
        n = fixed.get(key(r))
        if n is None or None in key(r):
            continue
        out.append(
            {
                "query": r["query"],
                "path": r["path"],
                "variant": r["variant"],
                "workers": r["workers"],
                "pre_fix_s": r["median_wall_s"],
                "fixed_s": n["median_wall_s"],
                "speedup": r["median_wall_s"] / n["median_wall_s"],
            }
        )
    return out


def write_csv(rows: list[dict], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0]) if rows else []
    with dest.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    k: f"{v:.4f}" if isinstance(v, float) else ("" if v is None else v)
                    for k, v in r.items()
                }
            )


# ---- extra tables ------------------------------------------------------------


def sliced_table(rows: list[dict]) -> str:
    out = [
        [
            r["query"],
            f"{r['processes']}",
            fmt_s(r["vep_sliced_total_s"]),
            fmt_s(r["vep_whole_chrom_total_s"])
            if r["vep_whole_chrom_total_s"]
            else "--",
            fmt_s(r["vepyr_slowest_s"]) if r["vepyr_slowest_s"] else "--",
            f"{r['speedup_vs_slowest']:.1f}" if r["speedup_vs_slowest"] else "--",
        ]
        for r in rows
    ]
    return _tabular(
        "lrrrrr",
        [
            "Query",
            "Processes",
            "VEP sliced (s)",
            "VEP whole chr22 (s)",
            "vepyr slowest (s)",
            "Speed-up vs sliced",
        ],
        out,
    )


def summary_table(rows: list[dict], sliced: list[dict] | None = None) -> str:
    """Median over each query group of VEP total / slowest vepyr path.

    `sliced` (e2e_sliced_rows()) adds a region row against tabix-sliced VEP.
    """
    procs = sorted({r["processes"] for r in rows if r["speedup_vs_slowest"]})
    groups = []
    for q in QUERIES:
        if q.tier not in groups:
            groups.append(q.tier)
    out = []
    for g in groups:
        cells = [latex_escape(g), str(sum(1 for q in QUERIES if q.tier == g))]
        for p in procs:
            s = [
                r["speedup_vs_slowest"]
                for r in rows
                if r["group"] == g and r["processes"] == p and r["speedup_vs_slowest"]
            ]
            cells.append(f"{median(s):.0f}$\\times$" if s else "--")
        out.append(cells)
        if g == "region" and sliced:
            cells = ["region, sliced VEP", str(len({r["query"] for r in sliced}))]
            for p in procs:
                v = [
                    r["speedup_vs_slowest"]
                    for r in sliced
                    if r["processes"] == p and r["speedup_vs_slowest"]
                ]
                cells.append(f"{median(v):.0f}$\\times$" if v else "--")
            out.append(cells)
    return _tabular(
        "lr" + "r" * len(procs),
        ["Group", "Queries", *[f"{p} proc." for p in procs]],
        out,
    )


def vep_matrix_table(rows: list[dict]) -> str:
    out = [
        [
            latex_escape(r["mode"]),
            str(r["fork"]),
            str(r["processes"]),
            fmt_s(r["median_wall_s"]),
            fmt_s(r["min_wall_s"]),
            fmt_s(r["max_wall_s"]),
        ]
        for r in rows
    ]
    return _tabular(
        "lrrrrr",
        ["Mode", r"\texttt{--fork}", "Processes", "Median (s)", "Min (s)", "Max (s)"],
        out,
    )


def plugin_fix_table(rows: list[dict]) -> str:
    out = [
        [
            r["query"],
            latex_escape(r["path"]),
            latex_escape(r["variant"]),
            str(r["workers"]),
            fmt_s(r["pre_fix_s"]),
            fmt_s(r["fixed_s"]),
            f"{r['speedup']:.1f}",
        ]
        for r in rows
        if r["query"].startswith("P")
    ]
    return _tabular(
        "lllrrrr",
        [
            "Query",
            "Output",
            "Variant",
            "Workers",
            "Pre-fix (s)",
            "Fixed (s)",
            "Speed-up",
        ],
        out,
    )


def load(run_dir: Path, sub: str) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted((run_dir / sub).glob("*.json"))]


# ---- figures -------------------------------------------------------------------


def _fig_e2e(rows: list[dict], dest: Path) -> None:
    """VEP + filter_vep vs vepyr wall time against processes, core | plugins.

    `rows` are e2e_rows(); each point is the median over the panel's queries.
    """
    panels = (
        ("Core (Ensembl cache)", lambda q: not q.startswith("P")),
        ("Plugins (merged cache + 5 plugins)", lambda q: q.startswith("P")),
    )
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.8), sharey=True)
    for ax, (title, sel) in zip(axes, panels):
        for col, style, label in (
            ("vep_total_s", "k-o", "VEP + filter_vep"),
            ("vepyr_collect_s", "C0-s", "vepyr collect"),
            ("vepyr_vcf_s", "C1-^", "vepyr sink_vcf"),
            ("vepyr_parquet_s", "C2-d", "vepyr sink_parquet"),
        ):
            pts = defaultdict(list)
            for r in rows:
                if sel(r["query"]) and r.get(col):
                    pts[r["processes"]].append(r[col])
            xs = sorted(pts)
            if xs:
                ax.plot(xs, [median(pts[x]) for x in xs], style, label=label)
        ax.set(xscale="log", yscale="log", title=title, xlabel="processes")
        ax.set_xticks([1, 2, 4, 8], labels=["1", "2", "4", "8"])
        ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
        ax.grid(True, which="both", alpha=0.3)
    axes[0].set_ylabel("median wall time over queries (s)")
    axes[1].legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(dest)
    plt.close(fig)


def _fig_filter_only(rows: list[dict], dest: Path) -> None:
    """filter_only_rows(): wall time per query, both tools, log y."""
    fig, ax = plt.subplots(figsize=(9, 3.5))
    xs = range(len(rows))
    ax.bar(
        [x - 0.2 for x in xs],
        [r["filter_vep_wall_s"] for r in rows],
        0.4,
        label="filter_vep",
        color="0.4",
    )
    ax.bar(
        [x + 0.2 for x in xs],
        [r["polars_wall_s"] for r in rows],
        0.4,
        label="Polars",
        color="C0",
    )
    ax.set_xticks(list(xs), labels=[r["query"] for r in rows])
    ax.set(yscale="log", ylabel="median wall time (s)")
    ax.legend()
    ax.grid(True, axis="y", which="both", alpha=0.3)
    fig.tight_layout()
    fig.savefig(dest)
    plt.close(fig)


def write_all_csvs(run_dir: Path) -> dict[str, list[dict]]:
    """Flatten a run dir into <run_dir>/csv/*.csv and return the rows."""
    e2e = e2e_rows(load(run_dir, "B"))
    tables = {
        "filter_only": filter_only_rows(load(run_dir, "A")),
        "e2e": e2e,
        "e2e_sliced": e2e_sliced_rows(load(run_dir, "B_sliced"), e2e),
        "pushdown": pushdown_rows(load(run_dir, "C")),
        "vep_matrix": vep_matrix_rows(load(run_dir, "vep")),
        "plugin_fix": plugin_fix_rows(
            load(run_dir, "C_old_build_pre_fix"), load(run_dir, "C")
        ),
    }
    for name, rows in tables.items():
        write_csv(rows, run_dir / "csv" / f"{name}.csv")
    return tables


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument(
        "--dest", required=True, type=Path, help="papers/vepyr/supplementary"
    )
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    t = write_all_csvs(run_dir)
    tab, fig = a.dest / "tables", a.dest / "figures"
    tab.mkdir(parents=True, exist_ok=True)
    fig.mkdir(parents=True, exist_ok=True)
    (tab / "equivalence.tex").write_text(equivalence_table(QUERIES, long=True))
    (tab / "filter_only.tex").write_text(filter_only_table(load(run_dir, "A")))
    c = load(run_dir, "C")
    for w in sorted({r["workers"] for r in c if "workers" in r}):
        (tab / f"pushdown_w{w}.tex").write_text(pushdown_table(c, workers=w))
    b = load(run_dir, "B")
    for p in sorted({r["processes"] for r in t["e2e"]}):
        (tab / f"e2e_p{p}.tex").write_text(e2e_table(b, procs=p))
    (tab / "e2e_sliced.tex").write_text(sliced_table(t["e2e_sliced"]))
    (tab / "e2e_summary.tex").write_text(summary_table(t["e2e"], t["e2e_sliced"]))
    (tab / "vep_matrix.tex").write_text(vep_matrix_table(t["vep_matrix"]))
    (tab / "plugin_fix.tex").write_text(plugin_fix_table(t["plugin_fix"]))
    _fig_e2e(t["e2e"], fig / "e2e_scaling.pdf")
    _fig_filter_only(t["filter_only"], fig / "filter_only.pdf")


if __name__ == "__main__":
    main()
