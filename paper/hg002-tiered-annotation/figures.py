#!/usr/bin/env python3
"""Supplementary figures for the HG002 three-tier annotation analysis.

Reads the tables written by ``analysis.py analyze`` from results/ and writes
vector PDFs (and PNG previews) to figures/. Monochrome, to match Figure 1.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import polars as pl  # noqa: E402


HERE = Path(__file__).resolve().parent
RESULTS = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "results"
FIGURES = HERE / "figures"

INK = "#111111"
MID = "#6b6b6b"
LIGHT = "#bdbdbd"
GRID = "#e6e6e6"
TIER_FILL = {"input": "#e6e6e6", "tier 1": "#bdbdbd", "tier 2": "#6b6b6b", "tier 3": "#111111"}

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans"],
    "font.size": 8,
    "axes.edgecolor": MID,
    "axes.linewidth": 0.6,
    "axes.labelcolor": INK,
    "xtick.color": MID,
    "ytick.color": MID,
    "xtick.major.width": 0.6,
    "ytick.major.width": 0.6,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "pdf.fonttype": 42,
    "savefig.dpi": 300,
})


def save(fig: plt.Figure, name: str) -> None:
    FIGURES.mkdir(exist_ok=True)
    fig.savefig(FIGURES / f"{name}.pdf", bbox_inches="tight")
    fig.savefig(FIGURES / f"{name}.png", bbox_inches="tight", dpi=200)
    plt.close(fig)


def funnel() -> None:
    df = pl.read_csv(RESULTS / "funnel.tsv", separator="\t")
    fig, ax = plt.subplots(figsize=(6.4, 2.2))
    y = list(range(df.height))[::-1]
    labels = []
    for yi, (step, tier, n) in zip(y, df.iter_rows()):
        ax.barh(yi, n, height=0.62, color=TIER_FILL[tier], edgecolor="none")
        ax.text(n * 1.25, yi, f"{n:,}", va="center", ha="left", fontsize=7.5, color=INK, fontweight="bold")
        labels.append(f"{step}  [{tier}]")
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7.5)
    ax.tick_params(axis="y", length=0)
    ax.set_xscale("log")
    ax.set_xlim(1, df["variants"].max() * 20)
    ax.spines["left"].set_visible(False)
    ax.xaxis.grid(True, color=GRID, linewidth=0.5)
    ax.set_axisbelow(True)
    ax.set_xlabel("variants (log scale)")
    save(fig, "figS_funnel")


LOEUF_SERIES = [
    ("Rare synonymous (reference)", LIGHT, "-", "o"),
    ("All pLoF, MANE Select", INK, "-", "o"),
]


def loeuf() -> None:
    dec = pl.read_csv(RESULTS / "loeuf_deciles.tsv", separator="\t")
    summ = pl.read_csv(RESULTS / "loeuf_summary.tsv", separator="\t")
    fig, (a, b) = plt.subplots(1, 2, figsize=(7.0, 2.6), gridspec_kw={"width_ratios": [1.15, 1]})

    for name, color, ls, marker in LOEUF_SERIES:
        d = dec.filter(pl.col("category") == name).sort("decile")
        n = d["total"][0]
        a.plot(d["decile"] + 1, d["fraction"] * 100, color=color, linestyle=ls, linewidth=1.6,
               marker=marker, markersize=3.5, label=f"{name} (n={n:,})")
    a.axhline(10, color=GRID, linewidth=0.8, zorder=0)
    a.set_xticks(range(1, 11))
    a.set_xlabel("gnomAD v4.1 LOEUF decile (1 = most constrained)")
    a.set_ylabel("% of variant-transcript pairs")
    a.yaxis.grid(True, color=GRID, linewidth=0.5)
    a.set_axisbelow(True)
    a.legend(frameon=False, fontsize=7, loc="lower left", bbox_to_anchor=(0, 1.0), ncol=1)

    order = summ["category"].to_list()
    ref = summ.filter(pl.col("category").str.starts_with("Rare synonymous"))["frac_constrained"][0] * 100
    for i, row in enumerate(summ.iter_rows(named=True)):
        yi = len(order) - 1 - i
        if row["n"] == 0:
            continue
        x = row["frac_constrained"] * 100
        b.plot([row["ci_low"] * 100, row["ci_high"] * 100], [yi, yi], color=MID, linewidth=1.2)
        b.plot(x, yi, "o", color=INK if "pLoF" in row["category"] else MID, markersize=5,
               markeredgecolor="white", markeredgewidth=1)
        p = row["fisher_p"]
        tag = "" if row["category"].startswith("Rare synonymous") else (f"p={p:.1g}" if p < 0.05 else "n.s.")
        b.text(101, yi, f"n={row['n']:,}  {tag}", va="center", ha="left", fontsize=6.5, color=MID,
               clip_on=False)
    b.axvline(ref, color=LIGHT, linewidth=1, zorder=0)
    b.set_yticks(range(len(order)))
    b.set_yticklabels([o.replace(">=", "\u2265") for o in order[::-1]], fontsize=7)
    b.set_xlim(0, 100)
    b.set_xlabel("% in the two most constrained deciles (95% CI)")
    b.xaxis.grid(True, color=GRID, linewidth=0.5)
    b.set_axisbelow(True)
    fig.tight_layout(w_pad=2.5)
    save(fig, "figS_loeuf")


def callability() -> None:
    cal = pl.read_csv(RESULTS / "callability.tsv", separator="\t", infer_schema_length=50000)
    clin = cal.filter(pl.col("ClinGen").is_not_null())
    fig, (a, b) = plt.subplots(1, 2, figsize=(7.0, 3.0), gridspec_kw={"width_ratios": [1, 1.25]})

    bins = [("\u2265 99%", 0.99, 1.01), ("90\u201399%", 0.9, 0.99), ("50\u201390%", 0.5, 0.9), ("< 50%", -0.01, 0.5)]
    for j, (df, color, label) in enumerate(((cal, LIGHT, "all MANE Select genes"),
                                            (clin, INK, "ClinGen genes (Moderate+)"))):
        n = df.height
        pct = [100 * df.filter((pl.col("callable_frac") >= lo) & (pl.col("callable_frac") < hi)).height / n
               for _, lo, hi in bins]
        y = [len(bins) - 1 - i + (0.18 if j == 0 else -0.18) for i in range(len(bins))]
        a.barh(y, pct, height=0.34, color=color, label=f"{label} (n={n:,})")
        for yi, v in zip(y, pct):
            a.text(v + 1.5, yi, f"{v:.1f}%", va="center", fontsize=6.5, color=INK)
    a.set_yticks(range(len(bins)))
    a.set_yticklabels([b_[0] for b_ in bins][::-1])
    a.set_ylabel("MANE CDS in HG002 benchmark")
    a.set_xlabel("% of genes")
    a.set_xlim(0, 105)
    a.xaxis.grid(True, color=GRID, linewidth=0.5)
    a.set_axisbelow(True)
    a.legend(frameon=False, fontsize=6.5, loc="center right")

    low = clin.sort("callable_frac").head(20)
    y = list(range(low.height))[::-1]
    for yi, row in zip(y, low.iter_rows(named=True)):
        b.plot([0, 100], [yi, yi], color=GRID, linewidth=0.5, zorder=0)
        b.plot(row["difficult_frac"] * 100, yi, "o", markersize=5, markerfacecolor="white",
               markeredgecolor=MID, markeredgewidth=1.1)
        b.plot(row["callable_frac"] * 100, yi, "o", markersize=4.5, color=INK)
        if row["falsedup_frac"] and row["falsedup_frac"] > 0:
            b.text(103, yi, "false dup.", va="center", fontsize=6, color=MID, clip_on=False)
    b.set_yticks(y)
    b.set_yticklabels([f"{r['SYMBOL']} ({r['MOI']})" for r in low.iter_rows(named=True)], fontsize=6.5,
                      style="italic")
    b.set_xlim(-3, 103)
    b.set_xlabel("% of MANE CDS")
    b.plot([], [], "o", color=INK, markersize=4.5, label="in HG002 benchmark")
    b.plot([], [], "o", markerfacecolor="white", markeredgecolor=MID, label="in GIAB difficult regions")
    b.legend(frameon=False, fontsize=6.5, loc="upper center", bbox_to_anchor=(0.5, 1.12), ncol=2)
    b.spines["left"].set_visible(False)
    b.tick_params(axis="y", length=0)
    fig.tight_layout(w_pad=2.0)
    save(fig, "figS_callability")


if __name__ == "__main__":
    funnel()
    loeuf()
    callability()
    print(f"figures written to {FIGURES}")
