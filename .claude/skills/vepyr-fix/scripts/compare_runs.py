#!/usr/bin/env python3
"""Compare two benchmark archives on all three gated metrics and return a verdict.

Phase durations, total wall and peak RSS are one command because they are one
gate: a human reading three tables and deciding is exactly how a regression gets
waved through. This exits nonzero if any bar is exceeded, so the workflow stops
on its own.

An exact diff of two trace files is useless: every `*_ms` value moves a little
between runs, so byte equality rejects changes far below any sensible threshold.
And the traces live in one file per worker count, so they have to be compared
per worker rather than concatenated -- a regression at 8 workers and a matching
speed-up at 1 would otherwise cancel out.

Reads `*_workers<N>.stderr.txt` from each archive directory, sums each
(worker, stage, event, metric) duration, and fails when any of them regresses
by more than the allowed percentage.

    compare_runs.py BASE_ARCHIVE FINAL_ARCHIVE

Exit 0 when every phase is within tolerance, 1 on a regression, 2 when there is
nothing to compare -- an empty trace must never read as "no regression".
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

TRACE = re.compile(r"\[VEP_PIPELINE_TRACE\]\s+(.*)")
FIELD = re.compile(r"(\w+)=(\S+)")
WORKERS = re.compile(r"_workers(\d+)\.stderr\.txt$")


def read_archive(root: Path) -> dict[tuple[str, str, str, str], float]:
    """Sum every `*_ms` duration, keyed by worker count, stage, event and metric."""
    totals: defaultdict[tuple[str, str, str, str], float] = defaultdict(float)
    files = sorted(root.glob("*_workers*.stderr.txt"))
    if not files:
        sys.exit(f"no *_workers*.stderr.txt under {root} -- nothing to compare")

    for path in files:
        match = WORKERS.search(path.name)
        if not match:
            continue
        worker = match.group(1)
        for line in path.read_text(errors="replace").splitlines():
            body = TRACE.search(line)
            if not body:
                continue
            fields = dict(FIELD.findall(body.group(1)))
            stage, event = fields.get("stage", "?"), fields.get("event", "?")
            for key, value in fields.items():
                # t_ms is the wall offset since process start, not a duration.
                if not key.endswith("_ms") or key == "t_ms":
                    continue
                try:
                    totals[(worker, stage, event, key)] += float(value)
                except ValueError:
                    continue
    return dict(totals)


def read_summary(root: Path) -> dict[str, dict[str, float]]:
    """Per worker count, the numeric wall and peak RSS the runner recorded.

    `annotation_seconds` comes from the run's metrics file and is empty when that
    file is missing. An empty value means the run did not report, not that it was
    instant, so it is an error rather than a zero.
    """
    path = root / "summary.tsv"
    if not path.exists():
        sys.exit(f"no summary.tsv under {root} -- nothing to compare")

    import csv

    out: dict[str, dict[str, float]] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            worker = row["workers"]
            try:
                out[worker] = {
                    "wall_s": float(row["annotation_seconds"]),
                    "rss_kb": float(row["max_rss_kb"]),
                }
            except (KeyError, ValueError):
                sys.exit(
                    f"{path}: worker {worker} has no numeric annotation_seconds/"
                    f"max_rss_kb -- the run did not report, fix the run"
                )
    return out


def compare_summary(base: Path, final: Path, wall_pct: float, rss_pct: float) -> int:
    """Print the wall/RSS table and count how many bars were exceeded."""
    before, after = read_summary(base), read_summary(final)
    workers = sorted(before.keys() | after.keys(), key=int)
    failures = 0

    print(f"\n{'workers':>7} {'metric':>8} {'base':>14} {'final':>14} {'delta':>8}")
    for worker in workers:
        if worker not in before or worker not in after:
            side = "final" if worker not in after else "baseline"
            print(f"worker {worker} is absent from the {side} run", file=sys.stderr)
            failures += 1
            continue
        for metric, bar, unit in (("wall_s", wall_pct, "s"), ("rss_kb", rss_pct, "KB")):
            b, a = before[worker][metric], after[worker][metric]
            pct = ((a - b) / b * 100) if b else float("inf")
            over = pct > bar
            failures += over
            flag = f"  <-- OVER {bar:g}%" if over else ""
            print(f"{worker:>7} {metric:>8} {b:>14.1f} {a:>14.1f} {pct:>+7.1f}%{flag}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("base", type=Path)
    parser.add_argument("final", type=Path)
    parser.add_argument("--max-regression-pct", type=float, default=5.0)
    parser.add_argument("--max-wall-pct", type=float, default=10.0)
    parser.add_argument("--max-rss-pct", type=float, default=10.0)
    # Sub-millisecond phases swing wildly in relative terms and mean nothing.
    parser.add_argument("--floor-ms", type=float, default=50.0)
    args = parser.parse_args()

    base, final = read_archive(args.base), read_archive(args.final)
    if not base or not final:
        sys.exit("one side produced no phase durations -- refusing to report a pass")

    rows, regressions, missing = [], [], []
    for key in sorted(base.keys() | final.keys()):
        before, after = base.get(key), final.get(key)
        if before is None or after is None:
            missing.append((key, before, after))
            continue
        if before < args.floor_ms and after < args.floor_ms:
            continue
        pct = ((after - before) / before * 100) if before else float("inf")
        rows.append((key, before, after, pct))
        if pct > args.max_regression_pct:
            regressions.append((key, before, after, pct))

    width = max((len("/".join(k)) for k, *_ in rows), default=20)
    print(
        f"{'worker/stage/event/metric':<{width}}  {'base_ms':>12} {'final_ms':>12} {'delta':>8}"
    )
    for key, before, after, pct in rows:
        flag = "  <-- REGRESSION" if pct > args.max_regression_pct else ""
        print(
            f"{'/'.join(key):<{width}}  {before:>12.1f} {after:>12.1f} {pct:>+7.1f}%{flag}"
        )

    for key, before, after in missing:
        side = "final" if after is None else "baseline"
        print(f"\nphase {'/'.join(key)} is absent from the {side} run", file=sys.stderr)

    print(
        f"\n{len(rows)} phases compared, {len(regressions)} over "
        f"{args.max_regression_pct}%, {len(missing)} present on only one side"
    )

    summary_failures = compare_summary(
        args.base, args.final, args.max_wall_pct, args.max_rss_pct
    )

    # A phase that exists on one side only is a shape change, not noise: the
    # pipeline did something different, which is exactly what the gate is for.
    failed = len(regressions) + len(missing) + summary_failures
    print(f"\nVERDICT: {'FAIL' if failed else 'PASS'} ({failed} bar(s) exceeded)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
