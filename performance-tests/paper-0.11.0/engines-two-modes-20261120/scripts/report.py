"""Summarise runs.csv of the two-mode engine comparison.

Usage: report.py <runs.csv> [--tex] [--contigs]
Per panel (data set, cache), mode (split: one process per chromosome, totals over chromosomes; whole: one
process for the whole VCF) and engine: wall time at 1, 2, 4 and 8 workers, CPU time at 1 and 8, speed-up
(CPU time at 1 worker / wall time at 8 workers) and peak RSS at 8 workers (largest process). Output checks:
identical at 1 and 8 workers; split mode against the checksums stored by the earlier engine comparison;
whole mode against split mode per contig. The sentinel log (sentinel.csv beside runs.csv) is summarised last.
"""

import collections
import csv
import json
import statistics
import sys
from pathlib import Path

M = Path(
    "/Users/mwiewior/research/benchmarks/merged-engine-comparison-20261107/all-chromosomes-w8"
)
W = Path(
    "/Users/mwiewior/research/benchmarks/ensembl-engine-comparison-20261108/hg002-w8"
)
PANELS = [
    ("HG002", "merged"),
    ("HG002", "ensembl"),
    ("ClinVar", "merged"),
    ("ClinVar", "ensembl"),
]
TITLES = {
    ("HG002", "merged"): "HG002, merged cache",
    ("HG002", "ensembl"): "HG002, Ensembl cache",
    ("ClinVar", "merged"): "ClinVar, merged cache",
    ("ClinVar", "ensembl"): "ClinVar, Ensembl cache",
}
ENGINES = [
    ("vepyr", "vepyr 0.11.0", "vepyr-main"),
    ("vep-rs", "vep-rs 0.5.0", "vep-rs-0.5.0"),
    ("fastVEP", "fastVEP 0.6.1", "fastVEP-main"),
]
EXPECTED = {
    "HG002": [str(c) for c in range(1, 23)],
    "ClinVar": [str(c) for c in range(1, 23)] + ["X", "Y", "MT"],
}
RECORDS = {"HG002": 4096123, "ClinVar": 4593372}
WORKERS = ("1", "2", "4", "8")

path = Path(sys.argv[1])
rows = [r for r in csv.DictReader(open(path)) if r["phase"] == "measured"]
by = {
    (r["dataset"], r["cache"], r["mode"], r["engine"], r["contig"], r["workers"]): r
    for r in rows
}


def stored_md5(ds, cache, contig, label):
    p = (
        (M if cache == "merged" else W)
        / ds
        / f"chr{contig}"
        / "runs"
        / "measured-1"
        / ds
        / f"{label}-w8"
        / "result.json"
    )
    return json.load(open(p))["body_md5"] if p.exists() else None


def whole_contigs(ds, cache, engine, w):
    p = path.parent / "whole-md5" / f"{cache}-{ds}-{engine}-w{w}.json"
    if not p.exists():
        return None
    d = json.load(open(p))["contigs"]
    return {k[3:] if k.startswith("chr") else k: v for k, v in d.items()}


tex = collections.defaultdict(list)
for ds, cache in PANELS:
    print(f"\n=== {TITLES[(ds, cache)]}")
    print(
        f"{'':30s} {'wall 1':>8s} {'wall 2':>8s} {'wall 4':>8s} {'wall 8':>8s} {'cpu 1':>8s} {'cpu 8':>8s} {'speedup':>7s} {'rss 8':>6s}"
    )
    notes = []
    for mode in ("split", "whole"):
        for engine, label, stored_label in ENGINES:
            contigs = EXPECTED[ds] if mode == "split" else ["all"]
            have = {
                w: [
                    by[(ds, cache, mode, engine, c, w)]
                    for c in contigs
                    if (ds, cache, mode, engine, c, w) in by
                ]
                for w in WORKERS
            }
            done = {w: len(have[w]) for w in WORKERS}
            name = f"{label}, {mode}"
            if not any(done.values()):
                continue
            if any(done[w] != len(contigs) for w in WORKERS):
                print(
                    f"{name:30s} incomplete: "
                    + ", ".join(f"w{w} {done[w]}/{len(contigs)}" for w in WORKERS)
                )
                continue
            wall = {w: sum(float(r["wall_s"]) for r in have[w]) for w in WORKERS}
            cpu = {
                w: sum(float(r["user_s"]) + float(r["sys_s"]) for r in have[w])
                for w in WORKERS
            }
            rss = max(float(r["rss_gib"]) for r in have["8"])
            swap = max(int(r["swap_mb"]) for w in WORKERS for r in have[w])
            print(
                f"{name:30s} "
                + " ".join(f"{wall[w]:8.1f}" for w in WORKERS)
                + f" {cpu['1']:8.1f} {cpu['8']:8.1f} {cpu['1'] / wall['8']:7.2f} {rss:6.2f}"
            )
            tex[(ds, cache)].append(
                (
                    mode,
                    f"{label} & "
                    + " & ".join(f"{wall[w]:,.1f}" for w in WORKERS)
                    + f" & {cpu['1']:,.1f} & {cpu['8']:,.1f} & {cpu['1'] / wall['8']:.2f} & {rss:.2f} \\\\",
                )
            )
            recs = sum(int(r["records"]) for r in have["8"])
            if recs != RECORDS[ds]:
                notes.append(
                    f"{name}: {recs:,} records at 8 workers, expected {RECORDS[ds]:,}"
                )
            if swap:
                notes.append(f"{name}: swap in use during the runs, up to {swap} MB")
            if mode == "split":
                diff = [
                    c
                    for c in contigs
                    if by[(ds, cache, mode, engine, c, "8")]["md5"]
                    != by[(ds, cache, mode, engine, c, "1")]["md5"]
                ]
                st = {c: stored_md5(ds, cache, c, stored_label) for c in contigs}
                known = [c for c in contigs if st[c]]
                off = [
                    c
                    for c in known
                    if st[c] != by[(ds, cache, mode, engine, c, "8")]["md5"]
                ]
                notes.append(
                    f"{name}: 8 against 1 workers differs on {len(diff)} of {len(contigs)} chromosomes {diff[:6]}; "
                    f"against the stored {stored_label} checksum differs on {len(off)} of {len(known)} {off[:6]}"
                )
            else:
                w8, w1 = (
                    whole_contigs(ds, cache, engine, "8"),
                    whole_contigs(ds, cache, engine, "1"),
                )
                if w8 and w1:
                    diff = [c for c in w8 if w8[c] != w1.get(c)]
                    split = {
                        c: by.get((ds, cache, "split", engine, c, "8"))
                        for c in EXPECTED[ds]
                    }
                    cmp = [c for c in EXPECTED[ds] if split[c]]
                    off = [
                        c for c in cmp if w8.get(c, {}).get("md5") != split[c]["md5"]
                    ]
                    notes.append(
                        f"{name}: 8 against 1 workers differs on {len(diff)} of {len(w8)} contigs {diff[:6]}; "
                        f"against split mode differs on {len(off)} of {len(cmp)} contigs {off[:6]}; contigs {len(w8)}"
                    )
    for n in notes:
        print("  " + n)
    if "--contigs" in sys.argv:
        for engine, label, _ in ENGINES:
            print(f"  {label}, split, wall at 8 / 1 workers per chromosome:")
            for c in EXPECTED[ds]:
                a, b = (
                    by.get((ds, cache, "split", engine, c, "8")),
                    by.get((ds, cache, "split", engine, c, "1")),
                )
                if a and b:
                    print(
                        f"    {c:>2s} {float(a['wall_s']):7.2f} {float(b['wall_s']):8.2f}  rss {a['rss_gib']}"
                    )

sent = path.parent / "sentinel.csv"
if sent.exists():
    s = list(csv.DictReader(open(sent)))
    if s:
        v = [float(r["wall_s"]) for r in s]
        print(
            f"\nsentinel (fastVEP 0.6.1, HG002 chr1, merged cache, 8 threads): {len(v)} readings, "
            f"min {min(v):.2f} median {statistics.median(v):.2f} max {max(v):.2f} s"
        )
        for r in s:
            flag = "  <-- slow" if float(r["wall_s"]) > 1.08 * min(v) else ""
            print(
                f"  {r['time']} {r['wall_s']:>6s} s  mem free {r['mem_free_pct']}%  swap {r['swap_mb']} MB  bg {r['bg_cpu_pct']}%  {r['power']}  before {r['before']}{flag}"
            )
fails = path.parent / "failures.txt"
if fails.exists():
    print("\nfailures:\n" + fails.read_text())
if "--tex" in sys.argv:
    for key in PANELS:
        if tex[key]:
            print(f"\n% {TITLES[key]}")
            for mode, line in tex[key]:
                print(f"% {mode}\n{line}")
