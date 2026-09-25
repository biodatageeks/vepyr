"""Raw results -> LaTeX tables and PDF figures for papers/vepyr/supplementary/."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from pibench.paths import FORK_TO_WORKERS, PKG_ROOT  # noqa: E402
from pibench.queries import QUERIES, filter_vep_expression  # noqa: E402

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


def latex_escape(s: str) -> str:
    return "".join(_ESC.get(c, c) for c in s)


def fmt_s(x: float) -> str:
    return f"{x:.2f}" if x < 10 else f"{x:.1f}" if x < 1000 else f"{x:.0f}"


def _gib(b: float) -> str:
    return f"{b / 2**30:.2f}"


def _tabular(cols: str, header: list[str], rows: list[list[str]]) -> str:
    body = "\n".join(" & ".join(r) + r" \\" for r in rows)
    return (
        f"\\begin{{tabular}}{{{cols}}}\n\\toprule\n"
        + " & ".join(header)
        + " \\\\\n\\midrule\n"
        + body
        + "\n\\bottomrule\n\\end{tabular}\n"
    )


def equivalence_table(queries) -> str:
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
    )


def filter_only_table(rows: list[dict]) -> str:
    by = defaultdict(dict)
    for r in rows:
        qid, tool = r.get("query"), r.get("tool")
        if qid is None or tool is None:
            continue
        by[qid][tool] = r
    out = []
    for qid, t in by.items():
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


def e2e_table(rows: list[dict]) -> str:
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


def pushdown_table(rows: list[dict]) -> str:
    by = defaultdict(dict)
    for r in rows:
        qid, path, workers, variant = (
            r.get("query"),
            r.get("path"),
            r.get("workers"),
            r.get("variant"),
        )
        if qid is None or path is None or workers is None or variant is None:
            continue
        by[(qid, path, workers)][variant] = r
    out = []
    for (qid, path, w), v in sorted(by.items()):
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


def load(run_dir: Path, sub: str) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted((run_dir / sub).glob("*.json"))]


def _fig_e2e(rows: list[dict], dest: Path) -> None:
    fig, ax = plt.subplots(figsize=(6, 4))
    for tool, path, style in (
        ("vep", None, "k-o"),
        ("vepyr", "collect", "C0-s"),
        ("vepyr", "vcf", "C1-^"),
        ("vepyr", "parquet", "C2-d"),
    ):
        pts = defaultdict(list)
        for r in rows:
            if r.get("tool") == tool and (path is None or r.get("path") == path):
                if "processes" in r and "median_wall_s" in r:
                    pts[r["processes"]].append(r["median_wall_s"])
        xs = sorted(pts)
        if not xs:
            continue
        ax.plot(
            xs,
            [sorted(pts[x])[len(pts[x]) // 2] for x in xs],
            style,
            label="VEP + filter_vep" if tool == "vep" else f"vepyr {path}",
        )
    ax.set(
        xscale="log",
        yscale="log",
        xlabel="processes",
        ylabel="median wall time, median over queries (s)",
    )
    ax.legend()
    fig.tight_layout()
    fig.savefig(dest)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument(
        "--dest", required=True, type=Path, help="papers/vepyr/supplementary"
    )
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    (a.dest / "tables").mkdir(parents=True, exist_ok=True)
    (a.dest / "figures").mkdir(parents=True, exist_ok=True)
    (a.dest / "tables/equivalence.tex").write_text(equivalence_table(QUERIES))
    (a.dest / "tables/filter_only.tex").write_text(
        filter_only_table(load(run_dir, "A"))
    )
    (a.dest / "tables/pushdown.tex").write_text(pushdown_table(load(run_dir, "C")))
    e2e = load(run_dir, "B")
    (a.dest / "tables/e2e.tex").write_text(e2e_table(e2e))
    _fig_e2e(e2e, a.dest / "figures/e2e_scaling.pdf")


if __name__ == "__main__":
    main()
