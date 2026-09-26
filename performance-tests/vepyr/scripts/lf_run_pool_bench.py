#!/usr/bin/env python3
"""LazyFrame run-pool scaling bench: raw engine stream, LF collect, output_vcf.

Each run is its own subprocess, so its peak RSS is its own (os.wait4). The first
run of every configuration is a discarded warm-up and the median of the rest is
reported. Modes:

  raw  iterate the engine stream the LazyFrame reads, with no Python work
  lf   annotate(...).collect()
  vcf  annotate(..., output_vcf=...)

``raw`` rebuilds the annotator from the arguments ``annotate()`` passed to its
schema probe, which for a plain collect are the arguments the LazyFrame source
uses. It reaches into ``vepyr._create_annotator`` for that, so it is a
benchmarking hook, not an API.

Example (chr22, merged, core and five plugins):

    lf_run_pool_bench.py --out-dir /tmp/lfb \\
        --input chr22=$DATA_VEPYR_DIR/polars_integration/chr22/input/HG002_chr22.vcf.gz \\
        --cache-dir $DATA_VEPYR_DIR/cache/116_GRCh38_merged \\
        --fasta $DATA_VEPYR_DIR/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \\
        --plugin-cache-root $DATA_VEPYR_DIR/plugin_cache_116 \\
        --workers 1 4 8 --modes raw lf vcf --plugin-sets none all
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

PLUGINS = ["spliceai", "cadd", "alphamissense", "dbnsfp", "clinvar"]


def child(
    mode: str, workers: int, src: str, plugins: str, args: argparse.Namespace
) -> None:
    import vepyr

    names = PLUGINS if plugins == "all" else []
    kw = dict(
        everything=True,
        reference_fasta=args.fasta,
        workers=workers,
        show_progress=False,
        plugin_cache_root=args.plugin_cache_root if names else None,
        plugins=names or None,
    )
    stats = {"wait": 0.0, "between": 0.0, "batches": 0, "rows": 0}

    class Timed:
        def __init__(self, it):
            self.it, self.last, self.schema = it, None, it.schema

        def __iter__(self):
            return self

        def __next__(self):
            t0 = time.perf_counter()
            if self.last is not None:
                stats["between"] += t0 - self.last
            try:
                batch = next(self.it)
            finally:
                self.last = time.perf_counter()
                stats["wait"] += self.last - t0
            stats["batches"] += 1
            stats["rows"] += batch.num_rows
            return batch

    create = vepyr._create_annotator
    calls: list[tuple] = []

    def wrapped(*a):
        calls.append(a)
        return Timed(create(*a))

    vepyr._create_annotator = wrapped
    out_vcf = os.path.join(args.out_dir, f"scratch_{os.getpid()}.vcf")
    t0 = time.perf_counter()
    try:
        if mode == "vcf":
            vepyr.annotate(src, args.cache_dir, output_vcf=out_vcf, **kw)
            wall = time.perf_counter() - t0
            # Counted after the clock stops: output_vcf returns the path, not
            # a row count, and the rows are what prove the run did the work.
            stats["rows"] = count_records(out_vcf)
        else:
            lf = vepyr.annotate(src, args.cache_dir, **kw)
            stats.update(wait=0.0, between=0.0, batches=0, rows=0)
            t0 = time.perf_counter()
            if mode == "raw":
                for _ in Timed(create(*calls[0])):
                    pass
                wall = time.perf_counter() - t0
            else:
                height = lf.collect().height
                wall = time.perf_counter() - t0
                stats["rows"] = height
    finally:
        if os.path.exists(out_vcf):
            os.remove(out_vcf)
    print(
        json.dumps(
            {
                "wall_s": wall,
                "engine_wait_s": stats["wait"],
                "consumer_s": stats["between"],
                "batches": stats["batches"],
                "rows": stats["rows"],
            }
        )
    )


def count_records(path: str) -> int:
    """Data lines (non-header) of a plain or gzip/BGZF-compressed VCF."""
    with open(path, "rb") as probe:
        compressed = probe.read(2) == b"\x1f\x8b"
    opener = gzip.open if compressed else open
    with opener(path, "rt") as f:
        return sum(1 for line in f if not line.startswith("#"))


def check_rows(label: str, runs: list[dict], expected: int) -> None:
    """Refuse a configuration whose runs did not all process every record.

    A run that drops records finishes sooner, so an empty or truncated input,
    or an engine regression that loses rows, would otherwise read as a speed-up.
    """
    if expected <= 0:
        raise SystemExit(f"{label}: the input has no records to annotate")
    bad = [r["rows"] for r in runs if r["rows"] != expected]
    if bad:
        raise SystemExit(f"{label}: runs produced {bad} rows, expected {expected}")


def wait_for_quiet(max_load: float, timeout_s: float = 600.0) -> float:
    deadline = time.monotonic() + timeout_s
    while os.getloadavg()[0] > max_load and time.monotonic() < deadline:
        time.sleep(10)
    return os.getloadavg()[0]


def run_once(cmd: list[str], env: dict[str, str]) -> dict:
    import tempfile

    with tempfile.TemporaryFile("w+") as out, tempfile.TemporaryFile("w+") as err:
        proc = subprocess.Popen(cmd, env=env, stdout=out, stderr=err, text=True)
        _, status, usage = os.wait4(proc.pid, 0)
        proc.returncode = os.waitstatus_to_exitcode(status)
        out.seek(0)
        err.seek(0)
        stdout, stderr = out.read(), err.read()
    if proc.returncode != 0:
        raise SystemExit(
            f"run failed ({proc.returncode}): {' '.join(cmd)}\n{stderr[-4000:]}"
        )
    # macOS reports bytes, Linux kilobytes.
    rss_bytes = usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024
    return {
        **json.loads(stdout.strip().splitlines()[-1]),
        "maxrss_gib": rss_bytes / 2**30,
    }


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--out-dir", required=True)
    p.add_argument("--input", action="append", required=True, help="NAME=PATH")
    p.add_argument("--cache-dir", required=True)
    p.add_argument("--fasta", required=True)
    p.add_argument("--plugin-cache-root", required=True)
    p.add_argument("--workers", nargs="+", type=int, default=[1, 4, 8])
    p.add_argument(
        "--modes", nargs="+", choices=["raw", "lf", "vcf"], default=["raw", "lf", "vcf"]
    )
    p.add_argument(
        "--plugin-sets", nargs="+", choices=["none", "all"], default=["none", "all"]
    )
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--max-load", type=float, default=4.0)
    p.add_argument("--env", action="append", default=[], help="KEY=VAL for every run")
    p.add_argument("--child", nargs=4, metavar=("MODE", "WORKERS", "SRC", "PLUGINS"))
    args = p.parse_args()
    if args.child:
        mode, w, src, plugins = args.child
        child(mode, int(w), src, plugins, args)
        return

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, **dict(kv.split("=", 1) for kv in args.env)}
    base = [
        sys.executable,
        os.path.abspath(__file__),
        "--out-dir",
        str(out_dir),
        "--input",
        "x=x",
        "--cache-dir",
        args.cache_dir,
        "--fasta",
        args.fasta,
        "--plugin-cache-root",
        args.plugin_cache_root,
    ]
    rows = []
    for spec in args.input:
        name, src = spec.split("=", 1)
        expected = count_records(src)
        for plugins in args.plugin_sets:
            for mode in args.modes:
                for w in args.workers:
                    runs = []
                    for i in range(1 + args.repeats):
                        load = wait_for_quiet(args.max_load)
                        r = run_once(
                            base + ["--child", mode, str(w), src, plugins], env
                        )
                        r["load_before"] = round(load, 2)
                        runs.append(r)
                    check_rows(f"{name} {plugins} {mode} w{w}", runs, expected)
                    kept = runs[1:]
                    row = {
                        "input": name,
                        "plugins": plugins,
                        "mode": mode,
                        "workers": w,
                        "env": args.env,
                        "median_wall_s": round(
                            statistics.median(r["wall_s"] for r in kept), 3
                        ),
                        "min_wall_s": round(min(r["wall_s"] for r in kept), 3),
                        "max_wall_s": round(max(r["wall_s"] for r in kept), 3),
                        "median_rss_gib": round(
                            statistics.median(r["maxrss_gib"] for r in kept), 2
                        ),
                        "median_engine_wait_s": round(
                            statistics.median(r["engine_wait_s"] for r in kept), 3
                        ),
                        "median_consumer_s": round(
                            statistics.median(r["consumer_s"] for r in kept), 3
                        ),
                        "batches": kept[-1]["batches"],
                        "runs": runs,
                    }
                    rows.append(row)
                    print(
                        json.dumps({k: v for k, v in row.items() if k != "runs"}),
                        flush=True,
                    )
    (out_dir / "results.json").write_text(json.dumps(rows, indent=2))
    (out_dir / "summary.md").write_text(render_summary(rows))
    print(render_summary(rows))


def render_summary(rows: list[dict]) -> str:
    lines = [
        "| input | plugins | mode | workers | median wall s | min | max | peak RSS GiB | engine wait s | consumer s |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['input']} | {r['plugins']} | {r['mode']} | {r['workers']} | {r['median_wall_s']} | "
            f"{r['min_wall_s']} | {r['max_wall_s']} | {r['median_rss_gib']} | "
            f"{r['median_engine_wait_s']} | {r['median_consumer_s']} |"
        )
    lines.append("")
    for name, plugins in sorted({(r["input"], r["plugins"]) for r in rows}):
        top = max(
            r["workers"] for r in rows if (r["input"], r["plugins"]) == (name, plugins)
        )
        pick = {
            r["mode"]: r["median_wall_s"]
            for r in rows
            if (r["input"], r["plugins"], r["workers"]) == (name, plugins, top)
        }
        if "lf" in pick and "vcf" in pick:
            lines.append(
                f"- {name} plugins={plugins} w{top}: lf/vcf = {pick['lf'] / pick['vcf']:.2f}"
            )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
