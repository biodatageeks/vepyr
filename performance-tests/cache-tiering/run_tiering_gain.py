"""Time vepyr on two cache layouts (tiered, sorted) and check that their outputs agree.

Each run calls performance-tests/vepyr/scripts/run_vepyr_worker_scaling.py for one worker count
with one layout's cache, reads its GNU-time-style record and metrics, checksums the output VCF
per contig (contig_md5.py) and deletes it. Per worker count: one warm-up per layout, then the
repeats in ABBA order. A run counts only if it exited 0 and its per-contig MD5 equals that of
the first valid run with the same worker count (either layout).

    python run_tiering_gain.py --cache-root DIR --input-vcf HG002_chr22.vcf.gz \
        --expected-records 50861 --workdir OUT --workers 1 8 --repeats 5 [--purge-run]

Writes OUT/runs.csv (every run), OUT/summary.json and OUT/summary.md.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCALING = HERE.parent / "vepyr" / "scripts" / "run_vepyr_worker_scaling.py"
CONTIG_MD5 = HERE / "contig_md5.py"
FIELDS = [
    "layout",
    "workers",
    "label",
    "wall_s",
    "user_s",
    "sys_s",
    "cpu_s",
    "max_rss_kib",
    "annotation_s",
    "exit_status",
    "md5_ok",
    "valid",
]
METRICS = ("wall_s", "cpu_s", "user_s", "sys_s", "max_rss_kib", "annotation_s")


def schedule(
    layouts: list[str], workers: list[int], repeats: int
) -> list[tuple[str, int, str]]:
    a, b = layouts
    out: list[tuple[str, int, str]] = []
    for w in workers:
        out += [(a, w, "warmup"), (b, w, "warmup")]
        for r in range(1, repeats + 1):
            order = (a, b) if r % 2 else (b, a)
            out += [(layout, w, f"r{r}") for layout in order]
    return out


def check_md5(reference: dict[int, dict], workers: int, md5: dict) -> bool:
    if workers not in reference:
        reference[workers] = md5
        return True
    return reference[workers] == md5


def parse_time_file(path: Path) -> dict:
    text = path.read_text()

    def field(label: str) -> str:
        m = re.search(rf"{re.escape(label)}: (.+)", text)
        if not m:
            raise ValueError(f"{label} missing in {path}")
        return m.group(1).strip()

    wall = 0.0
    for part in field("Elapsed (wall clock) time (h:mm:ss or m:ss)").split(":"):
        wall = wall * 60 + float(part)
    return {
        "user_s": float(field("User time (seconds)")),
        "sys_s": float(field("System time (seconds)")),
        "wall_s": round(wall, 3),
        "max_rss_kib": int(field("Maximum resident set size (kbytes)")),
        "exit_status": int(field("Exit status")),
    }


def valid(row: dict) -> bool:
    return (
        row["exit_status"] == 0 and bool(row["md5_ok"]) and row["label"].startswith("r")
    )


def summarize(rows: list[dict]) -> dict:
    groups: dict[tuple[str, int], list[dict]] = {}
    for r in rows:
        r = {**r, "cpu_s": r.get("cpu_s", r["user_s"] + r["sys_s"])}
        if valid(r):
            groups.setdefault((r["layout"], r["workers"]), []).append(r)
    layouts = []
    for (layout, workers), rs in sorted(
        groups.items(), key=lambda kv: (kv[0][1], kv[0][0])
    ):
        entry = {"layout": layout, "workers": workers, "n": len(rs)}
        for m in METRICS:
            vals = [float(r[m]) for r in rs if r.get(m) is not None]
            if vals:
                entry[m] = {
                    "median": statistics.median(vals),
                    "min": min(vals),
                    "max": max(vals),
                }
        layouts.append(entry)
    by = {(e["layout"], e["workers"]): e for e in layouts}
    ratios = {}
    for workers in sorted({w for _, w in by}):
        t, s = by.get(("tiered", workers)), by.get(("sorted", workers))
        if t and s:
            ratios[workers] = {
                m: s[m]["median"] / t[m]["median"]
                for m in METRICS
                if m in t and m in s and t[m]["median"]
            }
    cold = [
        {
            "layout": r["layout"],
            "workers": r["workers"],
            **{m: r.get(m) for m in METRICS},
        }
        for r in (
            {**r, "cpu_s": r.get("cpu_s", r["user_s"] + r["sys_s"])} for r in rows
        )
        if r["label"] == "purged" and r["exit_status"] == 0 and r["md5_ok"]
    ]
    invalid = sum(
        1
        for r in rows
        if r["label"] != "warmup"
        and not (
            valid(r)
            or (r["label"] == "purged" and r["exit_status"] == 0 and r["md5_ok"])
        )
    )
    return {
        "layouts": layouts,
        "ratios": ratios,
        "cold_start": cold,
        "invalid_runs": invalid,
    }


def free_disk_gib(path: Path) -> float:
    return shutil.disk_usage(path).free / 2**30


def execute(
    plan: list[tuple[str, int, str]],
    run_one: Callable[[str, int, str], dict],
    workdir: Path,
    min_disk_gib: float = 40.0,
    csv_path: Path | None = None,
    reference: dict[int, dict] | None = None,
) -> list[dict]:
    reference = {} if reference is None else reference
    rows: list[dict] = []
    for layout, workers, label in plan:
        free = free_disk_gib(workdir)
        if free < min_disk_gib:
            raise SystemExit(
                f"only {free:.1f} GiB free disk on {workdir}; need {min_disk_gib} GiB"
            )
        row = run_one(layout, workers, label)
        md5 = row.pop("contig_md5", None)
        row["md5_ok"] = (
            row["exit_status"] == 0
            and md5 is not None
            and check_md5(reference, workers, md5)
        )
        row["cpu_s"] = round(row["user_s"] + row["sys_s"], 3)
        row["valid"] = valid(row)
        rows.append(row)
        if csv_path is not None:
            write_csv(rows, csv_path)
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def free_memory_gib() -> float:
    if sys.platform != "darwin":
        with open("/proc/meminfo") as f:
            m = dict(line.split(":", 1) for line in f)
        return int(m["MemAvailable"].split()[0]) / 2**20
    out = subprocess.run(["vm_stat"], capture_output=True, text=True, check=True).stdout
    page = int(re.search(r"page size of (\d+)", out).group(1))
    free = int(re.search(r"Pages free:\s+(\d+)", out).group(1))
    return free * page / 2**30


def ensure_free_memory(limit_gib: float, release_gib: int = 27) -> None:
    """Push the file cache out by touching release_gib of memory once (benchmark protocol)."""
    if free_memory_gib() >= limit_gib:
        return
    code = "n=int(__import__('sys').argv[1])<<30\nb=bytearray(n)\nfor i in range(0,n,16384): b[i]=1"
    subprocess.run([sys.executable, "-I", "-c", code, str(release_gib)], check=True)


def make_run_one(args: argparse.Namespace) -> Callable[[str, int, str], dict]:
    def run_one(layout: str, workers: int, label: str) -> dict:
        ensure_free_memory(args.min_free_memory_gib)
        run_dir = args.workdir / "runs" / f"{layout}-w{workers}-{label}"
        archive = run_dir / "archive"
        archive.mkdir(parents=True, exist_ok=True)
        cache_dir = args.cache_root / layout / args.cache_name
        cmd = [
            str(args.vepyr_python), "-P", str(SCALING),
            "--input-vcf", str(args.input_vcf), "--cache-dir", str(cache_dir),
            "--cache-type", "merged", "--reference-fasta", str(args.reference_fasta),
            "--ssd-output-dir", str(run_dir), "--archive-dir", str(archive),
            "--expected-records", str(args.expected_records), "--compression", "plain",
            "--minimum-free-gib", str(args.min_disk_gib), "--force", "--workers", str(workers),
        ]  # fmt: skip
        env = {**os.environ, "VEPYR_EXPECTED_VERSION": args.vepyr_version}
        print(f"[{layout} w{workers} {label}]", flush=True)
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
        (run_dir / "runner.log").write_text(proc.stdout + proc.stderr)
        time_file = archive / f"merged_workers{workers}.time.txt"
        if proc.returncode != 0 or not time_file.exists():
            return {"layout": layout, "workers": workers, "label": label, "wall_s": None,
                    "user_s": 0.0, "sys_s": 0.0, "max_rss_kib": None, "annotation_s": None,
                    "exit_status": proc.returncode or 1}  # fmt: skip
        row = {
            "layout": layout,
            "workers": workers,
            "label": label,
            **parse_time_file(time_file),
        }
        metrics = json.loads(
            (archive / f"merged_workers{workers}.metrics.json").read_text()
        )
        row["annotation_s"] = round(metrics["annotation_seconds"], 3)
        vcf = next(archive.glob("*.vcf"), None)
        if vcf is not None and row["exit_status"] == 0:
            md5_json = run_dir / "contig_md5.json"
            subprocess.run([sys.executable, "-I", str(CONTIG_MD5), str(vcf), str(md5_json)],
                           check=True, capture_output=True)  # fmt: skip
            row["contig_md5"] = {
                c: v["md5"]
                for c, v in json.loads(md5_json.read_text())["contigs"].items()
            }
            vcf.unlink()
        return row

    return run_one


def purge_os_cache() -> bool:
    if sys.platform == "darwin":
        return (
            subprocess.run(["sudo", "-n", "purge"], capture_output=True).returncode == 0
        )
    return False


def write_summary(summary: dict, out: Path) -> None:
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = [
        "| Layout | Workers | n | Wall (s) | CPU (s) | User (s) | System (s) | Peak RSS (GiB) |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for e in summary["layouts"]:

        def cell(m: str, scale: float = 1.0) -> str:
            v = e.get(m)
            if not v:
                return "-"
            return f"{v['median'] / scale:.2f} ({v['min'] / scale:.2f}-{v['max'] / scale:.2f})"

        lines.append(
            f"| {e['layout']} | {e['workers']} | {e['n']} | {cell('wall_s')} | {cell('cpu_s')} | "
            f"{cell('user_s')} | {cell('sys_s')} | {cell('max_rss_kib', 2**20)} |"
        )
    lines += ["", "Ratio sorted / tiered of the medians (> 1: tiering helps):", ""]
    for w, r in summary["ratios"].items():
        lines.append(
            f"- {w} workers: " + ", ".join(f"{m} {v:.3f}" for m, v in r.items())
        )
    for c in summary.get("cold_start", []):
        lines.append(f"- cold start (OS file cache purged), {c['layout']}, {c['workers']} workers: "
                     f"wall {c['wall_s']} s, CPU {c['cpu_s']} s")  # fmt: skip
    lines.append(
        f"\nInvalid runs (failed or output differs): {summary['invalid_runs']}"
    )
    (out / "summary.md").write_text("\n".join(lines) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--cache-root", required=True, type=Path)
    p.add_argument("--cache-name", default="116_GRCh38_merged")
    p.add_argument("--input-vcf", required=True, type=Path)
    p.add_argument("--expected-records", required=True, type=int)
    p.add_argument("--reference-fasta", type=Path,
                   default=Path.home() / "workspace/data_vepyr/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa")  # fmt: skip
    p.add_argument("--workdir", required=True, type=Path)
    p.add_argument("--workers", nargs="+", type=int, default=[1, 8])
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument(
        "--vepyr-python",
        type=Path,
        default=Path.home() / "workspace/venvs/vepyr-0.11.0/bin/python",
    )
    p.add_argument("--vepyr-version", default="0.11.0")
    p.add_argument("--min-free-memory-gib", type=float, default=20.0)
    p.add_argument("--min-disk-gib", type=float, default=40.0)
    p.add_argument("--purge-run", action="store_true")
    args = p.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    env = {"python": platform.python_version(), "platform": platform.platform(),
           "vepyr_python": str(args.vepyr_python), "args": {k: str(v) for k, v in vars(args).items()}}  # fmt: skip
    (args.workdir / "env.json").write_text(json.dumps(env, indent=2) + "\n")
    plan = schedule(["tiered", "sorted"], args.workers, args.repeats)
    run_one = make_run_one(args)
    reference: dict[int, dict] = {}
    rows = execute(
        plan,
        run_one,
        args.workdir,
        args.min_disk_gib,
        args.workdir / "runs.csv",
        reference,
    )
    if args.purge_run:
        for layout in ("tiered", "sorted"):
            if not purge_os_cache():
                print(
                    "purge unavailable (sudo -n purge failed); cold-start run skipped"
                )
                break
            rows += execute([(layout, args.workers[0], "purged")], run_one, args.workdir,
                            args.min_disk_gib, reference=reference)  # fmt: skip
        write_csv(rows, args.workdir / "runs.csv")
    write_summary(summarize(rows), args.workdir)


if __name__ == "__main__":
    main()
