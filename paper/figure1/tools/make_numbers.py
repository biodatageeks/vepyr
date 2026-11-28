#!/usr/bin/env python3
"""Generate the paper's values and Figure 1 from the files in data/.

    python3 tools/make_numbers.py          # write the generated files
    python3 tools/make_numbers.py --check  # exit 1 if anything is out of date or inconsistent

The files in data/ are the only place where a measured value is entered. This script writes
  generated/data.tex         every value, already formatted, used as \\val{key} in the main text,
                             the supplement and their tables (one file for both documents; the
                             supplement reads it as ../generated/data.tex);
  figures/figure1-tikz.tex   Figure 1, drawn by tools/figure1/ (panel A from panel_a.py, B from
  figures/figure1-table.tex  data/figure1/, E from data/wgs_scaling.csv);
  generated/figure1-panel-a-size.txt  the size of the panel A table, measured with pdflatex
                             when the table changes.
latexmkrc runs it before every compile, locally and on Overleaf.

Every run also reads the project's .tex files and reports
  - a \\val{key} that this script does not define (a typo, or a value not yet in a CSV);
  - a number typed by hand (a time such as 3:15, a decimal, or a speed-up such as 97$\\times$)
    inside a tabular that otherwise takes its values from \\val, i.e. a cell that was missed.
With --check these reports or an out-of-date generated file (e.g. data.tex or Figure 1 not
regenerated after a change in data/) make the script exit with status 1.

The script and tools/figure1/ use the standard library only. They run locally (any python3),
on the Overleaf compile server (through latexmkrc; files written there are used for that PDF
only) and in the Overleaf Python Script Runner (Overleaf Labs: open this file and press Run;
files written there are saved to the project and reach git with the next sync).
"""

from __future__ import annotations

import csv
import json
import re
import statistics
from decimal import ROUND_HALF_UP, Decimal
import sys
from pathlib import Path


def find_root() -> Path:
    """The project folder: the one holding data/wgs_scaling.csv.

    Locally it is the parent of tools/. The Overleaf Python Script Runner may run the script
    without __file__, or from another folder, so the current folder and its parents are tried too.
    """
    candidates = []
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parents[1])
    cwd = Path.cwd().resolve()
    candidates += [cwd, *cwd.parents]
    for c in candidates:
        if (c / "data/wgs_scaling.csv").is_file():
            return c
    raise SystemExit(
        f"Cannot find data/wgs_scaling.csv from {cwd} (contents: "
        f"{sorted(x.name for x in cwd.iterdir())}). Run the script from the project."
    )


ROOT = find_root()
WGS = ROOT / "data/wgs_scaling.csv"
ENGINE_RUNS = ROOT / "data/raw/engines_two_modes_20261120_runs.csv"
CLEAN_HOST = ROOT / "data/raw/vepyr_clean_host_20261123_engines.csv"
MEASUREMENTS = ROOT / "data/measurements.csv"
TRIO = ROOT / "data/figure1/hg002-tiered-annotation"
PUSHDOWN = ROOT / "data/raw/polars_chr22_v0110_pushdown.csv"
CONCORDANCE = {
    cache: (
        ROOT / f"data/raw/engine_concordance_{cache}_totals.csv",
        ROOT / f"data/raw/engine_concordance_{cache}_mismatches.json",
    )
    for cache in ("merged", "ensembl")
}
FIELDS_FILLED = ROOT / "data/raw/fields_filled_hg002"
SUPP_RUNS = {
    "interval": ROOT / "data/raw/interval_routes_chr22_v0110.json",
    "writer": ROOT / "data/raw/writer_memory_chr1_v0110.json",
    "workers22": ROOT / "data/raw/workers_chr22_v0110.json",
    "workers1": ROOT / "data/raw/workers_chr1_v0110.json",
}
POLARS = {
    name: ROOT / f"data/raw/polars_chr22_v0110_{name}.csv"
    for name in ("vep_matrix", "e2e", "e2e_sliced", "filter_only")
}
DATA_TEX = ROOT / "generated/data.tex"
SKIP_DIRS = {"out", "generated", ".git", ".idea", ".superpowers", "node_modules"}


# ---------------------------------------------------------------- formatting


def fixed(x: Decimal, places: int) -> str:
    """x rounded half up (not to even) to `places` decimals, with thousands separators."""
    return f"{x.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP):,}"


def whole(x: float) -> int:
    """x rounded half up to an integer (every value of the paper uses this rule)."""
    return int(Decimal(repr(x)).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def gib(value: float) -> str:
    return fixed(Decimal(repr(value)), 1)


def p_value(p: Decimal) -> str:
    """A p-value for math mode: three decimals, or m\\times10^{e} with one decimal below 0.001."""
    if p >= Decimal("0.001"):
        return fixed(p, 3)
    e = p.adjusted()
    m = p.scaleb(-e).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    if m >= 10:
        m, e = (m / 10).quantize(Decimal("0.1")), e + 1
    return f"{m}\\times10^{{{e}}}"


# ---------------------------------------------------------------- vepyr on macOS (data/raw/vepyr_clean_host_20261123_engines.csv)


def clean_host_medians(path: Path = CLEAN_HOST) -> dict[str, dict[str, Decimal]]:
    """vepyr 0.11.0, HG002, merged cache, whole VCF, macOS: per worker count the median over the
    measured runs (phase r1-r3, valid) of wall time, CPU time (user + system) and peak RSS (GiB).
    These are the vepyr values of Table 18, Figure 1E, the main text, the HG002 merged-cache
    whole-VCF row of Table 36, Figure 1A and the abstract."""
    runs: dict[str, list[dict]] = {}
    for r in csv.DictReader(path.open(newline="")):
        if r["phase"] != "warmup" and r["valid"] == "1" and r["engine"] == "vepyr":
            if r["md5_ok"] not in ("1", "-"):
                raise SystemExit(
                    f"{path.name}: run {r['phase']} w{r['workers']} has a wrong checksum"
                )
            runs.setdefault(r["workers"], []).append(r)
    out = {}
    for w, rs in runs.items():
        out[w] = {
            "wall": statistics.median(Decimal(r["wall_s"]) for r in rs),
            "cpu": statistics.median(
                Decimal(r["user_s"]) + Decimal(r["sys_s"]) for r in rs
            ),
            "rss": statistics.median(Decimal(r["rss_gib"]) for r in rs),
            "n": Decimal(len(rs)),
        }
    if sorted(out, key=int) != ["1", "2", "4", "8"]:
        raise SystemExit(f"{path.name}: needs runs with 1, 2, 4 and 8 workers")
    return out


# ---------------------------------------------------------------- whole genome (data/wgs_scaling.csv)


def read_wgs(path: Path = WGS) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    clean = clean_host_medians()
    for row in csv.DictReader(path.open(newline="")):
        if row["vepyr_source"].startswith(
            "data/raw/vepyr_clean_host_20261123_engines.csv"
        ):
            m = clean[row["processes"]]
            row["vepyr_seconds"], row["vepyr_peak_gib"], row["vepyr_runs"] = (
                str(m["wall"]),
                str(m["rss"]),
                str(m["n"]),
            )
        row["processes"] = int(row["processes"])
        row["vep_seconds"] = float(row["vep_seconds"])
        row["vepyr_seconds"] = float(row["vepyr_seconds"])
        row["vepyr_peak_gib"] = float(row["vepyr_peak_gib"])
        out.setdefault(row["platform"], []).append(row)
    for rows in out.values():
        rows.sort(key=lambda r: r["processes"])
        if [r["processes"] for r in rows] != [1, 2, 4, 8]:
            raise SystemExit(f"{path}: every platform needs 1, 2, 4 and 8 processes")
    return out


def speedup(row: dict) -> int:
    return whole(row["vep_seconds"] / row["vepyr_seconds"])


def wgs_values(data: dict[str, list[dict]]) -> dict[str, str]:
    """wgs.<platform>.<processes>.vep_s (whole seconds, 31,541), .vepyr_s (seconds, 1 decimal),
    .speedup (VEP / vepyr, whole number), .peak (GiB), and wgs.<platform>.peaks (the four peaks)."""
    vals: dict[str, str] = {}
    for plat, rows in data.items():
        for r in rows:
            k = f"wgs.{plat}.{r['processes']}"
            vals[f"{k}.vepyr_s"] = fixed(Decimal(repr(r["vepyr_seconds"])), 1)
            vals[f"{k}.speedup"] = str(speedup(r))
            vals[f"{k}.vep_s"] = f"{whole(r['vep_seconds']):,}"
            vals[f"{k}.peak"] = gib(r["vepyr_peak_gib"])
        peaks = [gib(r["vepyr_peak_gib"]) for r in rows]
        vals[f"wgs.{plat}.peaks"] = ", ".join(peaks[:-1]) + " and " + peaks[-1]
    return vals


# ---------------------------------------------------------------- engines (data/raw/engines_two_modes_20261120_runs.csv)

ENGINE_KEY = {"vepyr": "vepyr", "vep-rs": "veprs", "fastVEP": "fastvep"}
MODE_KEY = {"split": "perchrom", "whole": "whole"}
CONTIGS = {
    "HG002": [str(c) for c in range(1, 23)],
    "ClinVar": [str(c) for c in range(1, 23)] + ["X", "Y", "MT"],
}


def engine_values(path: Path = ENGINE_RUNS) -> dict[str, str]:
    """Supplementary Table 36 from the raw runs (macOS, Apple M3 Max, 2026-11-20/21; the measured
    rows of the run log of ~/research/benchmarks/engines-two-modes-20261120).

    engines.<hg002|clinvar>.<merged|ensembl>.<perchrom|whole>.<vepyr|veprs|fastvep>.<field>:
    wall1, wall2, wall4, wall8, cpu1, cpu8 (s; per chromosome: sums over the chromosomes),
    scaling8 (CPU time at 1 worker / wall time at 8: how well an engine uses its workers, not a
    comparison with VEP), peak8 (largest peak RSS at 8 workers, GiB),
    and for Figure 1A peak8_r1 (GiB, 1 decimal). Sums are exact
    (Decimal) and every value is rounded half up, so a printed value never depends on the order
    of a floating-point sum."""
    runs: dict[tuple, dict] = {}
    for r in csv.DictReader(path.open(newline="")):
        if r["phase"] == "measured":
            runs[
                (
                    r["dataset"],
                    r["cache"],
                    r["mode"],
                    r["engine"],
                    r["contig"],
                    r["workers"],
                )
            ] = r
    vals: dict[str, str] = {}
    for ds, contigs in CONTIGS.items():
        for cache in ("merged", "ensembl"):
            for mode in ("split", "whole"):
                for engine, ekey in ENGINE_KEY.items():

                    def block(w: str) -> list[dict]:
                        want = contigs if mode == "split" else ["all"]
                        found = [
                            runs.get((ds, cache, mode, engine, c, w)) for c in want
                        ]
                        if None in found:
                            raise SystemExit(
                                f"{path.name}: {ds} {cache} {mode} {engine} {w} workers is incomplete"
                            )
                        return found

                    if (ds, cache, mode, engine) == (
                        "HG002",
                        "merged",
                        "whole",
                        "vepyr",
                    ):
                        # median of three runs (data/raw/vepyr_clean_host_20261123_engines.csv)
                        clean = clean_host_medians()
                        wall = {w: clean[w]["wall"] for w in "1248"}
                        cpu = {w: clean[w]["cpu"] for w in "18"}
                        peak = clean["8"]["rss"]
                    else:
                        wall = {
                            w: sum(Decimal(r["wall_s"]) for r in block(w))
                            for w in "1248"
                        }
                        cpu = {
                            w: sum(
                                Decimal(r["user_s"]) + Decimal(r["sys_s"])
                                for r in block(w)
                            )
                            for w in "18"
                        }
                        peak = max(Decimal(r["rss_gib"]) for r in block("8"))
                    k = f"engines.{ds.lower()}.{cache}.{MODE_KEY[mode]}.{ekey}"
                    for w in "1248":
                        vals[f"{k}.wall{w}"] = fixed(wall[w], 1)
                    for w in "18":
                        vals[f"{k}.cpu{w}"] = fixed(cpu[w], 1)
                    vals[f"{k}.scaling8"] = fixed(cpu["1"] / wall["8"], 2)
                    vals[f"{k}.peak8"] = fixed(peak, 2)
                    vals[f"{k}.peak8_r1"] = fixed(peak, 1)
    return vals


# ---------------------------------------------------------------- trio analysis (data/figure1/hg002-tiered-annotation)


def trio_values(path: Path = TRIO) -> dict[str, str]:
    """trio.*: the GIAB trio analysis of Figure 1B and Supplementary Section 5.7 (annotation of
    HG002 with plugins, then queries with polars-bio over 12 files read from URLs). Times in
    seconds, 1 decimal; LOEUF shares in %, 1 decimal; trio.loeuf.plof_p for math mode."""
    annotate = json.loads((path / "annotate.json").read_text())
    summary = json.loads((path / "summary.json").read_text())
    sources = json.loads((path / "sources.json").read_text())
    plp = list(
        csv.DictReader((path / "clinvar_plp.tsv").open(newline=""), delimiter="\t")
    )
    if len(plp) != 1:
        raise SystemExit(
            f"{path.name}/clinvar_plp.tsv: the text describes one pathogenic variant, found {len(plp)}"
        )
    loeuf = {
        r["category"]: r
        for r in csv.DictReader(
            (path / "loeuf_summary.tsv").open(newline=""), delimiter="\t"
        )
    }
    plof, syn = loeuf["All pLoF, MANE Select"], loeuf["Rare synonymous (reference)"]
    lof, inheritance = summary["lof"], summary["inheritance_counts"]
    asj = max(
        Decimal(plp[0][c]) for c in ("gnomADe_ASJ_AF", "gnomADg_ASJ_AF") if plp[0][c]
    )
    ann, queries = (
        Decimal(repr(annotate["seconds"])),
        Decimal(repr(summary["seconds"]["total"])),
    )
    # session.json: both stages run in one Python process (analysis.py all); its total is the
    # measured wall time of the whole session. The text names the machine, so it must match.
    session = json.loads((path / "session.json").read_text())
    host = session["host"]
    if (host.get("model"), host.get("chip"), host.get("memory")) != (
        "MacBook Air",
        "Apple M2",
        "24 GB",
    ):
        raise SystemExit(
            f"{path.name}/session.json: the text names a MacBook Air (Apple M2, 24 GB), the run was on {host}"
        )
    if (
        session["versions"]["vepyr"] != annotate["vepyr"]
        or summary["versions"]["vepyr"] != annotate["vepyr"]
    ):
        raise SystemExit(
            f"{path.name}: annotate.json, summary.json and session.json name different vepyr versions"
        )
    return {
        "trio.vepyr_version": annotate["vepyr"],
        "trio.files_s": fixed(sum(Decimal(repr(s["seconds"])) for s in sources), 1),
        "trio.files_rows_million": fixed(
            Decimal(sum(s["rows"] for s in sources)) / 10**6, 1
        ),
        "trio.plof": f"{lof['plof_variants']:,}",
        "trio.hc": f"{lof['hc_variants']:,}",
        "trio.hc_genes": f"{lof['hc_genes']:,}",
        "trio.hc_rare": f"{lof['hc_rare']:,}",
        "trio.hc_rare_resolved": str(
            sum(n for k, n in inheritance.items() if k != "unresolved")
        ),
        "trio.clingen_genes": f"{summary['clingen_genes']:,}",
        "trio.plp_gene": plp[0]["SYMBOL"],
        "trio.plp_af_pct": fixed(Decimal(plp[0]["af"]) * 100, 3),
        "trio.plp_asj_pct": fixed(asj * 100, 2),
        "trio.loeuf.plof_pct": fixed(Decimal(plof["frac_constrained"]) * 100, 1),
        "trio.loeuf.syn_pct": fixed(Decimal(syn["frac_constrained"]) * 100, 1),
        "trio.loeuf.plof_p": p_value(Decimal(plof["fisher_p"])),
        "trio.annotate_s": fixed(ann, 1),
        "trio.queries_s": fixed(queries, 1),
        "trio.total_s": fixed(Decimal(repr(session["seconds"]["session_total"])), 1),
        "trio.url_reads_s": fixed(Decimal(repr(summary["seconds"]["tier3_reads"])), 1),
        "trio.files": str(len(sources)),
        "trio.workers": str(annotate["workers"]),
        "trio.clingen_genes_lt90": f"{summary['clingen_genes_lt90']:,}",
        "trio.plp_hgvsp": plp[0]["HGVSp"].split(":", 1)[1],
    }


# ---------------------------------------------------------------- pushdown (data/raw/polars_chr22_v0110_pushdown.csv)


def pushdown_values(path: Path = PUSHDOWN) -> dict[str, str]:
    """Supplementary Tables 28 and 29 (vepyr 0.11.0, HG002 chr22, merged cache, macOS; median of
    three runs per cell in the raw file): pushdown.<query>.<output>.w<workers>.{without,with}
    (s, 2 decimals) and .speedup (without / with, 1 decimal); and the ranges quoted in the main
    text, pushdown.w<workers>.<region|projection>.{min,max} of the speed-ups."""
    vals: dict[str, str] = {}
    ranges: dict[tuple, list[Decimal]] = {}
    for r in csv.DictReader(path.open(newline="")):
        k = f"pushdown.{r['query']}.{r['path']}.w{r['workers']}"
        without, with_ = Decimal(r["without_s"]), Decimal(r["with_s"])
        vals[f"{k}.without"] = fixed(without, 2)
        vals[f"{k}.with"] = fixed(with_, 2)
        vals[f"{k}.speedup"] = fixed(without / with_, 1)
        ranges.setdefault((r["workers"], r["kind"]), []).append(without / with_)
    for (w, kind), xs in ranges.items():
        vals[f"pushdown.w{w}.{kind}.min"] = fixed(min(xs), 1)
        vals[f"pushdown.w{w}.{kind}.max"] = fixed(max(xs), 1)
    return vals


# ---------------------------------------------------------------- Polars integration, chr22 (data/raw/polars_chr22_v0110_*.csv)


def secs(x: Decimal) -> str:
    """Times as in the Polars tables: 2 decimals below 10 s, 1 below 1,000 s, else whole seconds."""
    return fixed(x, 2 if x < 10 else 1 if x < 1000 else 0)


def rows_of(path: Path) -> list[dict]:
    return list(csv.DictReader(path.open(newline="")))


def polars_values() -> dict[str, str]:
    """Supplementary Tables 20-27 (vepyr 0.11.0, HG002 chr22, merged cache, macOS; medians of
    three runs in the raw files) and the end-to-end ranges quoted in the main text.

    vepmatrix.<mode>.p<processes>.{median,min,max}   VEP alone (Table 20)
    e2e.<query>.p<processes>.{vep,collect,vcf,parquet} and .{collect,vcf,parquet}_x
                                                     VEP + filter_vep vs vepyr (Tables 23-26)
    e2esum.<group>.p<processes>, e2esum.<group>.queries   medians per query group (Table 21)
    e2esum.sliced.p<processes>, e2esum.sliced.queries     region queries vs sliced VEP
    sliced.<query>.p<processes>.{sliced,whole,slowest,speedup}   (Table 22)
    filteronly.<query>.{rows,fv,pl,speedup,rss}            filtering only (Table 27)
    e2e.text.nonregion.p<processes>.{min,max}, e2e.text.sliced.{min,max}, e2e.queries,
    e2e.text.sliced.individual.{min,max}, e2e.nonregion_queries   main text
    Ratios are computed from the unrounded medians; every value is rounded half up."""
    D = Decimal
    vals: dict[str, str] = {}
    for r in rows_of(POLARS["vep_matrix"]):
        k = f"vepmatrix.{r['mode']}.p{r['processes']}"
        for f in ("median", "min", "max"):
            vals[f"{k}.{f}"] = secs(D(r[f"{f}_wall_s"]))
    e2e = rows_of(POLARS["e2e"])
    group_speedups: dict[tuple, list[Decimal]] = {}
    groups: dict[str, set] = {}
    for r in e2e:
        k = f"e2e.{r['query']}.p{r['processes']}"
        vep = D(r["vep_total_s"])
        vals[f"{k}.vep"] = secs(vep)
        walls = []
        for path in ("collect", "vcf", "parquet"):
            t = D(r[f"vepyr_{path}_s"])
            walls.append(t)
            vals[f"{k}.{path}"] = secs(t)
            vals[f"{k}.{path}_x"] = fixed(vep / t, 0)
        group_speedups.setdefault((r["group"], r["processes"]), []).append(
            vep / max(walls)
        )
        groups.setdefault(r["group"], set()).add(r["query"])
    for (g, n), xs in group_speedups.items():
        vals[f"e2esum.{g}.p{n}"] = fixed(statistics.median(xs), 0)
    for g, qs in groups.items():
        vals[f"e2esum.{g}.queries"] = str(len(qs))
    sliced = rows_of(POLARS["e2e_sliced"])
    sliced_by_n: dict[str, list[Decimal]] = {}
    for r in sliced:
        k = f"sliced.{r['query']}.p{r['processes']}"
        slowest = D(r["vepyr_slowest_s"])
        vals[f"{k}.sliced"] = secs(D(r["vep_sliced_total_s"]))
        vals[f"{k}.whole"] = secs(D(r["vep_whole_chrom_total_s"]))
        vals[f"{k}.slowest"] = secs(slowest)
        speedup = D(r["vep_sliced_total_s"]) / slowest
        vals[f"{k}.speedup"] = fixed(speedup, 1)
        sliced_by_n.setdefault(r["processes"], []).append(speedup)
    sliced_medians = {n: statistics.median(xs) for n, xs in sliced_by_n.items()}
    for n, m in sliced_medians.items():
        vals[f"e2esum.sliced.p{n}"] = fixed(m, 1)
    vals["e2esum.sliced.queries"] = str(len({r["query"] for r in sliced}))
    for r in rows_of(POLARS["filter_only"]):
        k = f"filteronly.{r['query']}"
        fv, pl = D(r["filter_vep_wall_s"]), D(r["polars_wall_s"])
        vals[f"{k}.rows"] = f"{int(r['filter_vep_rows']):,}"
        vals[f"{k}.fv"] = secs(fv)
        vals[f"{k}.pl"] = secs(pl)
        vals[f"{k}.speedup"] = fixed(fv / pl, 1)
        vals[f"{k}.rss"] = fixed(D(r["polars_rss_bytes"]) / 2**30, 2)
    # main text: medians of the groups without region restrictions, and the sliced row
    for n in ("1", "8"):
        ms = [
            statistics.median(xs)
            for (g, m), xs in group_speedups.items()
            if m == n and g != "region"
        ]
        vals[f"e2e.text.nonregion.p{n}.min"] = fixed(min(ms), 0)
        vals[f"e2e.text.nonregion.p{n}.max"] = fixed(max(ms), 0)
    vals["e2e.text.sliced.min"] = fixed(min(sliced_medians.values()), 1)
    vals["e2e.text.sliced.max"] = fixed(max(sliced_medians.values()), 1)
    sliced_speedups = [speedup for group in sliced_by_n.values() for speedup in group]
    vals["e2e.text.sliced.individual.min"] = fixed(min(sliced_speedups), 1)
    vals["e2e.text.sliced.individual.max"] = fixed(max(sliced_speedups), 1)
    vals["e2e.queries"] = str(len({r["query"] for r in e2e}))
    vals["e2e.nonregion_queries"] = str(
        sum(len(q) for g, q in groups.items() if g != "region")
    )
    return vals


# ---------------------------------------------------------------- concordance (data/raw/engine_concordance_*)

CONC_ENGINES = {
    "vepyr-main": "vepyr",
    "vep-rs-0.5.0": "veprs",
    "fastVEP-main": "fastvep",
}


def concordance_values() -> dict[str, str]:
    """Supplementary Tables 33 and 34 and the concordance percentages of the main text and
    Figure 1A (HG002 and ClinVar, compared with VEP 116.0 by the engine-comparison tool; vepyr
    main 524c6b1 and fastVEP main aa5e5ee, whose output equals that of vepyr 0.11.0 and fastVEP 0.6.1).

    conc.<hg002|clinvar>.<merged|ensembl>.<vepyr|veprs|fastvep>.<field>: expected, returned, md5
    (row MD5 matches), allmatch (records whose CSQ entries agree in every shared field), na (records
    without CSQ, ALT=*), withcsq, mean (mean matching fields, 2 decimals), compared (fields compared),
    missing, extra (CSQ entries), unequal (records with a different number of entries), pct1, pct0
    (allmatch in % of withcsq, 1 and 0 decimals). Table 35: mism.<data set>.<cache>.<engine>.r<rank>.
    {field,rows}, the shared fields with the most records with an unequal value, and .n."""
    vals: dict[str, str] = {}
    for cache, (totals, mismatches) in CONCORDANCE.items():
        unequal = {}
        for g in json.loads(mismatches.read_text())["groups"]:
            unequal[(g["dataset"], g["engine"])] = g["counts"][
                "rows_annotation_count_mismatch"
            ]
        for r in csv.DictReader(totals.open(newline="")):
            if r["Engine"] not in CONC_ENGINES:
                continue
            k = f"conc.{r['Dataset'].lower()}.{cache}.{CONC_ENGINES[r['Engine']]}"
            n = {
                f: int(r[c])
                for f, c in (
                    ("expected", "Expected rows"),
                    ("returned", "Returned rows"),
                    ("md5", "Row MD5 matches"),
                    ("na", "Rows without golden CSQ"),
                    ("allmatch", "All shared match"),
                    ("missing", "Missing annotations"),
                    ("extra", "Extra annotations"),
                    ("compared", "Compared fields"),
                )
            }
            n["withcsq"] = n["expected"] - n["na"]
            n["unequal"] = unequal[(r["Dataset"], r["Engine"])]
            for f, v in n.items():
                vals[f"{k}.{f}"] = f"{v:,}"
            vals[f"{k}.mean"] = fixed(Decimal(r["Mean matching fields (paired)"]), 2)
            pct = Decimal(n["allmatch"]) / n["withcsq"] * 100
            vals[f"{k}.pct1"] = fixed(pct, 1)
            vals[f"{k}.pct0"] = fixed(pct, 0)
        # Table 35: shared fields ranked by records with an unequal value
        for g in json.loads(mismatches.read_text())["groups"]:
            if g["engine"] not in CONC_ENGINES:
                continue
            k = f"mism.{g['dataset'].lower()}.{cache}.{CONC_ENGINES[g['engine']]}"
            ranked = [f for f in g["fields"] if f["Rows with mismatched field"] > 0]
            vals[f"{k}.n"] = str(len(ranked))
            for f in ranked[:5]:
                vals[f"{k}.r{f['Rank']}.field"] = f["Field"].replace("_", "\\_")
                vals[f"{k}.r{f['Rank']}.rows"] = f"{f['Rows with mismatched field']:,}"
    return vals


# ---------------------------------------------------------------- CSQ fields filled (data/raw/fields_filled_hg002)


def fields_values(path: Path = FIELDS_FILLED) -> dict[str, str]:
    """Supplementary Table 37 and the "Fields filled" row of Figure 1A: per-chromosome scans of the
    whole-genome HG002 outputs (fields declared in the header, fields with a value in at least one
    record). vepyr uses the VEP scan: its record lines are identical to VEP's.

    fields.<ensembl|merged>.vep.{declared,filled}; fields.<cache>.<vepyr|veprs|fastvep>.{filled,
    notdeclared,empty}: filled counts VEP's fields that have a value; notdeclared and empty list
    VEP's fields that the engine does not declare, or declares and never fills, as \\texttt{}
    names (fields that VEP itself never fills carry footnote a), or "none"."""

    def scan(name: str) -> tuple[list[str], set[str]]:
        chrom = [
            json.loads(f.read_text()) for f in sorted((path / name).glob("*.json"))
        ]
        if len(chrom) != 22 or any(c["fields"] != chrom[0]["fields"] for c in chrom):
            raise SystemExit(
                f"{path.name}/{name}: needs 22 chromosomes with the same header"
            )
        return chrom[0]["fields"], set().union(*(set(c["filled"]) for c in chrom))

    def names(fs: list[str], vep_empty: set[str]) -> str:
        if not fs:
            return "none"
        return ", ".join(
            "\\texttt{"
            + f.replace("_", "\\_")
            + "}"
            + ("\\textsuperscript{a}" if f in vep_empty else "")
            for f in fs
        )

    def prose(fs: list[str]) -> str:
        """'\\texttt{A}, \\texttt{B} and \\texttt{C}', for running text (no footnote marks)."""
        t = ["\\texttt{" + f.replace("_", "\\_") + "}" for f in fs]
        return t[0] if len(t) == 1 else ", ".join(t[:-1]) + " and " + t[-1]

    vals: dict[str, str] = {}
    for cache in ("ensembl", "merged"):
        vep_fields, vep_filled = scan(f"vep-{cache}-HG002")
        vep_empty = {f for f in vep_fields if f not in vep_filled}
        vals[f"fields.{cache}.vep.declared"] = str(len(vep_fields))
        vals[f"fields.{cache}.vep.filled"] = str(len(vep_filled & set(vep_fields)))
        if vep_empty:
            vals[f"fields.{cache}.vep.empty_text"] = prose(
                [f for f in vep_fields if f in vep_empty]
            )
        engines = {
            "vepyr": (vep_fields, vep_filled),
            "veprs": scan(f"vep-rs-{cache}-HG002"),
            "fastvep": scan(f"fastVEP-{cache}-HG002"),
        }
        for eng, (fields, filled) in engines.items():
            k = f"fields.{cache}.{eng}"
            vals[f"{k}.filled"] = str(len(filled & set(vep_fields)))
            vals[f"{k}.notdeclared"] = names(
                [f for f in vep_fields if f not in fields], vep_empty
            )
            vals[f"{k}.empty"] = names(
                [f for f in vep_fields if f in fields and f not in filled], vep_empty
            )
            missing = [f for f in vep_fields if f not in fields]
            if missing:
                vals[f"{k}.notdeclared_text"] = prose(missing)
    return vals


# ---------------------------------------------------------------- single values (data/measurements.csv)


def measurement_values(path: Path = MEASUREMENTS) -> dict[str, str]:
    """<key> as entered, and <key>.r0, .r1, .r2 rounded to 0, 1 or 2 decimals."""
    vals: dict[str, str] = {}
    for r in csv.DictReader(path.open(newline="")):
        vals[r["key"]] = r["value"]
        for d in (0, 1, 2):
            vals[f"{r['key']}.r{d}"] = fixed(Decimal(r["value"]), d)
    return vals


# ---------------------------------------------------------------- output


def data_tex(vals: dict[str, str], sources: list[Path]) -> str:
    names = ", ".join(str(p.relative_to(ROOT)) for p in sources)
    lines = [
        f"% Generated by tools/make_numbers.py from {names}. Do not edit; change the CSV and rerun.\n",
        "% Use a value as \\val{key}; an unknown key prints ??key?? in bold and a warning in the log.\n",
        "\\providecommand{\\val}[1]{\\ifcsname vpval@#1\\endcsname\\csname vpval@#1\\endcsname"
        "\\else\\textbf{??#1??}\\PackageWarning{data}{No value for #1}\\fi}\n",
    ]
    for key in sorted(vals):
        lines.append(
            f"\\expandafter\\def\\csname vpval@{key}\\endcsname{{{vals[key]}}}\n"
        )
    return "".join(lines)


def figure1(data: str) -> tuple[dict[Path, str], list[str]]:
    """Figure 1 (figures/figure1-tikz.tex, figures/figure1-table.tex) and the measured size of
    its panel A table (generated/figure1-panel-a-size.txt), drawn by tools/figure1/."""
    sys.dont_write_bytecode = (
        True  # no __pycache__ in the project (the Script Runner would save it)
    )
    sys.path.insert(0, str(ROOT / "tools/figure1"))
    import build_tikz  # noqa: PLC0415 (tools/figure1 is not a package)
    import panel_a  # noqa: PLC0415

    panel_a.DATA_TEX = data  # the table uses \\val; it is measured with these values
    import build_performance  # noqa: PLC0415

    build_performance.WGS_ROWS = read_wgs()  # panel E: the same values as Table 18
    files = {ROOT / rel: text for rel, text in build_tikz.render().items()}
    return files, list(panel_a.warnings)


def supplement_run_values() -> dict[str, str]:
    """Interval-overlap routes (chr22), writer memory (chr1) and the worker sweep (Table 19;
    chr22 and chr1, three caches), vepyr 0.11.0 on macOS.
    interval.<polarsbio|vepyr|bcftools>.{runs,median,min,max}; writer.<collect|parquet|vcf>.rss
    (GiB, median of three runs); workers.<chr22|chr1>.<ensembl|merged|refseq>.w<n> (s) and
    workers.<chr>.rows. Interval times: 2 decimals below 10 s, 1 below 1,000 s; worker times: 2 decimals."""
    vals: dict[str, str] = {}
    route = {
        "polars-bio": "polarsbio",
        "vepyr+bcftools+bedtools": "vepyr",
        "bcftools+bedtools": "bcftools",
    }
    for name, r in json.loads(SUPP_RUNS["interval"].read_text()).items():
        if name in route:
            k = f"interval.{route[name]}"
            vals[f"{k}.runs"] = str(len(r["runs"]))
            for f in ("median", "min", "max"):
                vals[f"{k}.{f}"] = secs(Decimal(repr(r[f])))
    for mode, r in json.loads(SUPP_RUNS["writer"].read_text()).items():
        vals[f"writer.{mode}.rss"] = fixed(Decimal(repr(r["median_rss_gib"])), 2)
    for chrom, key in (("chr22", "workers22"), ("chr1", "workers1")):
        for r in json.loads(SUPP_RUNS[key].read_text()):
            if r.get("wall_s") is None:
                continue
            vals[f"workers.{chrom}.{r['profile']}.w{r['workers']}"] = fixed(
                Decimal(repr(r["wall_s"])), 2
            )
            vals[f"workers.{chrom}.rows"] = f"{r['rows']:,}"
    return vals


def tiering_values() -> dict[str, str]:
    """tiering.*: effect of the warm/cold layout (Figure 1C3, C4 and the main text), from
    tools/figure1/tiering_data.py: reductions (%) of CPU and wall time with 1 and 8 workers,
    of matched and read pages, the increase of searched pages, and chr22's matched pages (C3)."""
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(ROOT / "tools/figure1"))
    import tiering_data  # noqa: PLC0415

    t = tiering_data.load()
    D = lambda x: Decimal(repr(x))  # noqa: E731
    less = lambda pair: (1 - D(pair[0]) / D(pair[1])) * 100  # noqa: E731
    vals = {}
    for metric, name in (("cpu_s", "cpu"), ("wall_s", "wall")):
        for w in (1, 8):
            vals[f"tiering.{name}.w{w}.less_pct"] = fixed(
                less(t["time"][(metric, w)]), 0
            )
    vals["tiering.cpu.less_min"] = min(
        vals["tiering.cpu.w1.less_pct"], vals["tiering.cpu.w8.less_pct"], key=int
    )
    vals["tiering.cpu.less_max"] = max(
        vals["tiering.cpu.w1.less_pct"], vals["tiering.cpu.w8.less_pct"], key=int
    )
    vals["tiering.matched.less_pct"] = fixed(less(t["pages"]["matched"]), 0)
    vals["tiering.read.less_pct"] = fixed(less(t["pages"]["read"]), 0)
    vals["tiering.searched.more_pct"] = fixed(-less(t["pages"]["searched"]), 0)
    s22, t22 = t["chr22_matched"]
    vals["tiering.chr22.less_pct"] = fixed((1 - D(t22) / D(s22)) * 100, 0)
    return vals


def parity_values(
    path: Path = ROOT / "data/raw/wgs_parity_v0110.csv",
) -> dict[str, str]:
    """Supplementary Table 32: strict MD5 comparison with VEP 116.0 on HG002, one row per report
    (extracted from vepyr performance-tests/paper-0.11.0/vepyr-0110-supplement-20261121/parity).
    parity.<set>.{records,matching,chromosomes,version}; for the transcript-selection set (pick)
    parity.pick.{profiles,records_each,matching_each}."""
    rows = list(csv.DictReader(path.open(newline="")))
    vals: dict[str, str] = {}
    for set_ in sorted({r["set"] for r in rows}):
        rs = [r for r in rows if r["set"] == set_]
        k = f"parity.{set_}"
        versions = {r["vepyr_version"] for r in rs}
        if len(versions) != 1:
            raise SystemExit(f"{path.name}: {set_} mixes vepyr versions {versions}")
        vals[f"{k}.version"] = versions.pop()
        if set_ == "pick":
            profiles = sorted({r["profile"] for r in rs})
            per = {pr: [r for r in rs if r["profile"] == pr] for pr in profiles}
            recs = {sum(int(r["vepyr_records"]) for r in v) for v in per.values()}
            if len(recs) != 1:
                raise SystemExit(f"{path.name}: pick profiles differ in records")
            vals[f"{k}.profiles"] = str(len(profiles))
            vals[f"{k}.records_each"] = f"{recs.pop():,}"
            n = {len(v) for v in per.values()}.pop()
            m = {sum(r["body_match"] == "True" for r in v) for v in per.values()}
            vals[f"{k}.matching_each"] = f"{min(m)}/{n}"
        else:
            vals[f"{k}.records"] = f"{sum(int(r['vepyr_records']) for r in rs):,}"
            vals[f"{k}.matching"] = (
                f"{sum(r['body_match'] == 'True' for r in rs)}/{len(rs)}"
            )
    return vals


def input_values() -> dict[str, str]:
    """input.<hg002|clinvar>.{records,records_million}: the record counts of the normalised inputs.
    Several raw files report them (engine runs, concordance totals, parity reports, trio summary);
    they must all agree, so that the paper has one count per input."""
    counts: dict[str, dict[str, int]] = {"HG002": {}, "ClinVar": {}}
    for r in csv.DictReader(ENGINE_RUNS.open(newline="")):
        if (
            r["phase"] == "measured"
            and r["mode"] == "whole"
            and r["workers"] == "8"
            and r["records"].isdigit()
        ):
            counts[r["dataset"]][f"engine run {r['engine']} {r['cache']}"] = int(
                r["records"]
            )
    for cache, (totals, _) in CONCORDANCE.items():
        for r in csv.DictReader(totals.open(newline="")):
            counts[r["Dataset"]][f"concordance {cache} {r['Engine']}"] = int(
                r["Expected rows"]
            )
    parity = list(
        csv.DictReader((ROOT / "data/raw/wgs_parity_v0110.csv").open(newline=""))
    )
    for set_ in {r["set"] for r in parity if r["set"] != "pick"}:
        counts["HG002"][f"parity {set_}"] = sum(
            int(r["vep_records"]) for r in parity if r["set"] == set_
        )
    counts["HG002"]["trio summary"] = json.loads((TRIO / "summary.json").read_text())[
        "variants"
    ]
    vals = {}
    for ds, c in counts.items():
        if len(set(c.values())) != 1:
            raise SystemExit(f"{ds} record counts disagree between raw files: {c}")
        n = next(iter(c.values()))
        vals[f"input.{ds.lower()}.records"] = f"{n:,}"
        vals[f"input.{ds.lower()}.records_million"] = fixed(Decimal(n) / 1000000, 1)
    return vals


def tool_values() -> dict[str, str]:
    """tool.<vep|fastvep|veprs|vepyr>.{release,commit}: the releases compared in Figure 1A and
    Supplementary Section 10, from the column heads (HEAD in tools/figure1/panel_a.py)."""
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(ROOT / "tools/figure1"))
    import panel_a  # noqa: PLC0415

    key = {"VEP": "vep", "fastVEP": "fastvep", "vep-rs": "veprs", "vepyr": "vepyr"}
    vals = {}
    for tool, (_, release, commit) in panel_a.HEAD.items():
        vals[f"tool.{key[tool]}.release"] = release
        vals[f"tool.{key[tool]}.commit"] = commit
    return vals


def build() -> tuple[dict[str, str], dict[Path, str], list[str]]:
    wgs = read_wgs()
    vals = wgs_values(wgs)
    for more in (
        engine_values(),
        measurement_values(),
        trio_values(),
        pushdown_values(),
        polars_values(),
        concordance_values(),
        fields_values(),
        tool_values(),
        supplement_run_values(),
        tiering_values(),
        parity_values(),
        input_values(),
    ):
        clash = vals.keys() & more.keys()
        if clash:
            raise SystemExit(f"keys defined twice: {sorted(clash)[:5]}")
        vals.update(more)
    data = data_tex(
        vals,
        [
            WGS,
            CLEAN_HOST,
            ENGINE_RUNS,
            MEASUREMENTS,
            TRIO,
            PUSHDOWN,
            *POLARS.values(),
            *(f for pair in CONCORDANCE.values() for f in pair),
            FIELDS_FILLED,
            *SUPP_RUNS.values(),
        ],
    )
    files = {DATA_TEX: data}
    problems = []
    try:
        fig_files, fig_warnings = figure1(data)
        files.update(fig_files)
        problems += fig_warnings
    except Exception as e:  # keep data.tex usable when only the figure fails
        problems.append(f"Figure 1 was not regenerated: {type(e).__name__}: {e}")
    return vals, files, problems


# ---------------------------------------------------------------- checks


def tex_files() -> list[Path]:
    return sorted(
        p
        for p in ROOT.rglob("*.tex")
        if not SKIP_DIRS.intersection(p.relative_to(ROOT).parts)
    )


VAL = re.compile(r"\\val\{([^}]*)\}")
TABULAR = re.compile(r"\\begin\{tabular\}.*?\\end\{tabular\}", re.S)
HAND_NUMBER = re.compile(r"\b\d+:\d{2}\b|\b\d+\.\d+\b|\b\d+\s*\$\\times\$")
# Not results: version numbers (0.11.0, and a release followed by its commit, as in the column
# heads "116.0\enspace\texttt{57ea5c5}"), lengths (3pt, 0.8mm) and \fontsize arguments.
NOT_RESULTS = re.compile(
    r"\bv?\d+(?:\.\d+){2,}\b"
    r"|VEP~\d+(?:\.\d+)?"  # a VEP release, e.g. VEP~116.0
    r"|\d+(?:\.\d+)?\\(?:textwidth|linewidth|columnwidth)"
    r"|\b\d+(?:\.\d+)?\\enspace\\texttt"
    r"|\b\d+(?:\.\d+)?\s*(?:pt|mm|cm|em|ex|bp|in)\b"
    r"|\\fontsize\{[^}]*\}\{[^}]*\}"
)


def strip_comments(text: str) -> str:
    """Blank out LaTeX comments, keeping line numbers."""
    return re.sub(r"(?<!\\)%.*", "", text)


def line_of(text: str, pos: int) -> int:
    return text.count("\n", 0, pos) + 1


def check_tex(vals: dict[str, str]) -> list[str]:
    problems = []
    for path in tex_files():
        rel = path.relative_to(ROOT)
        text = strip_comments(path.read_text(errors="replace"))
        for m in VAL.finditer(text):
            if m.group(1) not in vals:
                problems.append(
                    f"{rel}:{line_of(text, m.start())}: \\val{{{m.group(1)}}} has no value"
                )
        for t in TABULAR.finditer(text):
            if "\\val{" not in t.group(0):
                continue
            # blank the \val calls (same length) so positions still map to lines
            body = VAL.sub(lambda m: " " * len(m.group(0)), t.group(0))
            body = NOT_RESULTS.sub(lambda m: " " * len(m.group(0)), body)
            for h in HAND_NUMBER.finditer(body):
                line = line_of(text, t.start() + h.start())
                problems.append(
                    f"{rel}:{line}: '{h.group(0)}' typed by hand in a table that uses \\val"
                )
    return problems


def main(argv: list[str]) -> int:
    check = "--check" in argv
    vals, files, stale = build()
    print(f"project folder: {ROOT}")
    for path, text in files.items():
        rel = path.relative_to(ROOT)
        if check:
            if not path.exists() or path.read_text() != text:
                stale.append(f"{rel} is out of date; run tools/make_numbers.py")
        elif path.exists() and path.read_text() == text:
            print(f"unchanged {rel}")
        else:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
            except OSError as e:
                raise SystemExit(f"could not write {rel}: {e}")
            print(f"wrote {rel}")
    stale += check_tex(vals)
    # stdout, not stderr, so the messages show in the Overleaf output panel in order
    for line in stale:
        print("WARNING: " + line)
    if not stale:
        done = "are up to date" if check else "were regenerated"
        print(
            f"{len(vals)} values; every \\val in the .tex files is defined; data.tex and Figure 1 {done}"
        )
    return 1 if check and stale else 0


if __name__ == "__main__":
    # Exit only on failure: a SystemExit(0) can show as an error in the Overleaf Script Runner.
    if main(sys.argv[1:]):
        sys.exit(1)
