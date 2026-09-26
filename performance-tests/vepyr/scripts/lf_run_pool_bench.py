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
    setup = 0.0
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
            # The stream wall starts here; annotate()'s schema probe and
            # setup are reported apart, so the wall stays comparable across
            # runs while the lf/vcf gate can still add them back.
            setup = time.perf_counter() - t0
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
                "setup_s": setup,
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


def child_env(inherited: dict[str, str], overrides: list[str]) -> dict[str, str]:
    """The environment every run gets: the caller's, less any inherited `VEP_*`
    engine knob, plus the `--env` overrides.

    A tuning or tracing variable exported in the calling shell would otherwise
    reach every run without being recorded, so a baseline and a final could
    measure different hidden settings instead of the defaults.
    """
    env = {k: v for k, v in inherited.items() if not k.startswith("VEP_")}
    env.update(kv.split("=", 1) for kv in overrides)
    return env


def median_end_to_end(kept: list[dict]) -> float:
    """Median of each run's own setup + stream time.

    Summing the two medians is not the median of the sums when they vary
    across repeats, so the pairing is kept per run.
    """
    return statistics.median(r["setup_s"] + r["wall_s"] for r in kept)


def wait_for_quiet(max_load: float, timeout_s: float = 600.0) -> float:
    """Wait for the 1-minute load to drop to `max_load`; refuse to measure if it never does.

    The host is shared, and a timing taken under load has to be rerun, not
    recorded, so a timeout aborts the bench rather than measuring anyway.
    """
    deadline = time.monotonic() + timeout_s
    while (load := os.getloadavg()[0]) > max_load:
        if time.monotonic() >= deadline:
            raise SystemExit(
                f"load {load:.2f} stayed above --max-load {max_load} for "
                f"{timeout_s:.0f} s; not measuring on a busy host"
            )
        time.sleep(10)
    return load


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
    p.add_argument(
        "--gate",
        metavar="RESULTS_JSON",
        help="check a finished run's results.json against the scaling bars and exit 1 on a miss",
    )
    p.add_argument(
        "--gate-plugins",
        nargs="+",
        default=["none"],
        help="plugin sets (every input) or INPUT:PLUGINS pairs whose lf/vcf ratio is gated (default: none)",
    )
    p.add_argument(
        "--gate-expect",
        action="append",
        default=[],
        metavar="INPUT:PLUGINS:MODES:WORKERS",
        help="a configuration matrix the results must contain, e.g. chr1:none,all:raw,lf,vcf:4,8",
    )
    p.add_argument(
        "--gate-baseline",
        metavar="BASELINE_JSON",
        help="baseline results.json; a configuration more than 10%% slower than it is a miss",
    )
    gating = "--gate" in sys.argv
    p.add_argument("--out-dir", required=not gating)
    p.add_argument("--input", action="append", required=not gating, help="NAME=PATH")
    p.add_argument("--cache-dir", required=not gating)
    p.add_argument("--fasta", required=not gating)
    p.add_argument("--plugin-cache-root", required=not gating)
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
    if args.gate:
        rows = json.loads(Path(args.gate).read_text())
        failures = gate(rows, set(args.gate_plugins), parse_expect(args.gate_expect))
        if args.gate_baseline:
            base = json.loads(Path(args.gate_baseline).read_text())
            failures += baseline_regressions(base, rows, parse_expect(args.gate_expect))
        for line in failures:
            print(f"GATE MISS: {line}")
        print("VERDICT: " + ("FAIL" if failures else "PASS"))
        raise SystemExit(1 if failures else 0)
    if args.child:
        mode, w, src, plugins = args.child
        child(mode, int(w), src, plugins, args)
        return

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    env = child_env(dict(os.environ), args.env)
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
                        "median_setup_s": round(
                            statistics.median(r["setup_s"] for r in kept), 3
                        ),
                        "median_end_to_end_s": round(median_end_to_end(kept), 3),
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


RATIO_BAR = 1.20


def parse_expect(specs: list[str]) -> set[tuple[str, str, str, int]]:
    """`INPUT:PLUGINS:MODES:WORKERS` items, comma lists allowed in the last
    three fields, as the set of (input, plugins, mode, workers) a run must hold."""
    expected = set()
    for spec in specs:
        name, plugin_sets, modes, workers = spec.split(":")
        for plugins in plugin_sets.split(","):
            for mode in modes.split(","):
                for w in workers.split(","):
                    expected.add((name, plugins, mode, int(w)))
    return expected


def gate(
    rows: list[dict],
    ratio_plugins: set[str],
    expected: set[tuple[str, str, str, int]] | None = None,
) -> list[str]:
    """The misses against the scaling bars; empty when every bar holds.

    - expected: every (input, plugins, mode, workers) in `expected` must be
      present, so a run that left out an input or a plugin set cannot pass;
    - completeness: results must be non-empty, every gated plugin set must be
      present, and every input and plugin set needs raw at two worker counts;
    - raw stream: the largest worker count must be faster than the next one down;
    - LazyFrame: for each `ratio_plugins` entry, a plugin set (every input) or
      an `INPUT:PLUGINS` pair (that input only), `lf end-to-end/vcf` at
      the largest worker count must be at most RATIO_BAR.
    """
    if not rows:
        return ["results are empty: nothing was measured"]
    present = {(r["input"], r["plugins"], r["mode"], r["workers"]) for r in rows}
    missing = sorted((expected or set()) - present)
    if missing:
        return [
            f"{i} {p} {m} w{w}: expected configuration is missing"
            for i, p, m, w in missing
        ]
    failures = [
        f"ratio scope {scope} is gated but was not measured"
        for scope in sorted(ratio_plugins)
        if scope
        not in {r["plugins"] for r in rows}
        | {f"{r['input']}:{r['plugins']}" for r in rows}
    ]
    for name, plugins in sorted({(r["input"], r["plugins"]) for r in rows}):
        group = [r for r in rows if (r["input"], r["plugins"]) == (name, plugins)]
        raw = sorted(
            (r for r in group if r["mode"] == "raw"), key=lambda r: r["workers"]
        )
        if len(raw) < 2:
            failures.append(
                f"{name} {plugins} raw: measured at {len(raw)} worker count(s), "
                "two are needed to check scaling"
            )
        elif raw[-1]["median_wall_s"] >= raw[-2]["median_wall_s"]:
            failures.append(
                f"{name} {plugins} raw: w{raw[-1]['workers']} {raw[-1]['median_wall_s']} s"
                f" is not faster than w{raw[-2]['workers']} {raw[-2]['median_wall_s']} s"
            )
        if plugins not in ratio_plugins and f"{name}:{plugins}" not in ratio_plugins:
            continue
        top = max(r["workers"] for r in group)
        pick = {r["mode"]: r for r in group if r["workers"] == top}
        if "lf" not in pick or "vcf" not in pick:
            failures.append(f"{name} {plugins}: no lf and vcf pair at w{top} to gate")
            continue
        lf = pick["lf"]
        # Rows from before setup was reported carry only the stream wall.
        ratio = (
            lf.get("median_end_to_end_s", lf["median_wall_s"])
            / pick["vcf"]["median_wall_s"]
        )
        if ratio > RATIO_BAR:
            failures.append(
                f"{name} {plugins} w{top}: lf end-to-end/vcf = {ratio:.2f} > {RATIO_BAR:.2f}"
            )
    return failures


REGRESSION_BAR = 1.10


def _compared_time(row: dict) -> float:
    """What a baseline comparison measures: for lf, the end-to-end time a user
    sees (setup included); for raw and vcf, the median wall. Rows recorded
    before setup was reported fall back to the wall."""
    if row["mode"] == "lf":
        return row.get("median_end_to_end_s", row["median_wall_s"])
    return row["median_wall_s"]


def baseline_regressions(
    base: list[dict],
    final: list[dict],
    expected: set[tuple[str, str, str, int]] | None = None,
) -> list[str]:
    """Configurations whose final time (`_compared_time`) is more than
    REGRESSION_BAR times the baseline's. An empty baseline, or one missing any `expected`
    configuration, is itself a miss: a comparison that was never made must not
    read as a pass. Other configurations measured on one side only are skipped.
    """
    if not base:
        return ["baseline results are empty: nothing to compare against"]
    have = {(r["input"], r["plugins"], r["mode"], r["workers"]) for r in base}
    found = [
        f"{i} {p} {m} w{w}: missing from the baseline, so not compared"
        for i, p, m, w in sorted((expected or set()) - have)
    ]
    before = {(r["input"], r["plugins"], r["mode"], r["workers"]): r for r in base}
    for r in final:
        key = (r["input"], r["plugins"], r["mode"], r["workers"])
        if key not in before:
            continue
        was, now = _compared_time(before[key]), _compared_time(r)
        if now > was * REGRESSION_BAR:
            found.append(
                f"{key[0]} {key[1]} {key[2]} w{key[3]}: {now} s against {was} s at baseline"
                f" (+{(now / was - 1) * 100:.0f}% > {(REGRESSION_BAR - 1) * 100:.0f}%)"
            )
    return found


def render_summary(rows: list[dict]) -> str:
    """The results table, then one lf/vcf ratio per input and plugin set at the
    largest worker count. `median wall s` for raw and lf starts after
    annotate() returns; `setup s` is that call, which output_vcf's wall already
    includes, so the end-to-end ratio is the one to gate on."""
    lines = [
        "| input | plugins | mode | workers | median wall s | min | max | setup s | peak RSS GiB | engine wait s | consumer s |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['input']} | {r['plugins']} | {r['mode']} | {r['workers']} | {r['median_wall_s']} | "
            f"{r['min_wall_s']} | {r['max_wall_s']} | {r.get('median_setup_s', 0.0)} | "
            f"{r['median_rss_gib']} | {r['median_engine_wait_s']} | {r['median_consumer_s']} |"
        )
    lines.append("")
    for name, plugins in sorted({(r["input"], r["plugins"]) for r in rows}):
        top = max(
            r["workers"] for r in rows if (r["input"], r["plugins"]) == (name, plugins)
        )
        pick = {
            r["mode"]: r
            for r in rows
            if (r["input"], r["plugins"], r["workers"]) == (name, plugins, top)
        }
        if "lf" in pick and "vcf" in pick:
            lf, vcf = pick["lf"], pick["vcf"]["median_wall_s"]
            stream = lf["median_wall_s"]
            whole = lf.get("median_end_to_end_s", stream)
            lines.append(
                f"- {name} plugins={plugins} w{top}: lf end-to-end/vcf = {whole / vcf:.2f}"
                f" (stream only {stream / vcf:.2f})"
            )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
