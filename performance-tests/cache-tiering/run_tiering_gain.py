"""Time vepyr on two cache layouts (tiered, sorted) and check that their outputs agree.

Each run calls performance-tests/vepyr/scripts/run_vepyr_worker_scaling.py for one worker count
with one layout's cache, reads its GNU-time-style record and metrics, checksums the output VCF
per contig (contig_md5.py) and deletes it. Per worker count: one warm-up per layout, then the
repeats in ABBA order. A run counts only if it exited 0 and its per-contig MD5 equals that of
the first valid run with the same worker count (either layout).

    python run_tiering_gain.py --cache-root DIR --input-vcf HG002_chr22.vcf.gz \
        --expected-records 50861 --workdir OUT --workers 1 8 --repeats 5 [--cold 3]

With --cold N, N more repeats per layout and worker count run after `--purge-cmd` (default
`sudo purge`; run the script in your own terminal so sudo can ask once). Before each cold run the
share of the variation shard still in the OS page cache is measured (mincore) and recorded; a cold
run with more than 5% of the shard cached does not count. With --repeats 0 only cold runs are made.

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
    "resident_pct",
    "released",
    "valid",
]
METRICS = ("wall_s", "cpu_s", "user_s", "sys_s", "max_rss_kib", "annotation_s")


def schedule(
    layouts: list[str],
    workers: list[int],
    repeats: int,
    prefix: str = "r",
    warmup: bool = True,
) -> list[tuple[str, int, str]]:
    """Two layouts: ABBA order. More layouts: the order rotates by one each repeat."""
    out: list[tuple[str, int, str]] = []
    k = len(layouts)
    for w in workers:
        if warmup:
            out += [(layout, w, "warmup") for layout in layouts]
        for r in range(1, repeats + 1):
            if k == 2:
                order = layouts if r % 2 else layouts[::-1]
            else:
                shift = (r - 1) % k
                order = layouts[shift:] + layouts[:shift]
            out += [(layout, w, f"{prefix}{r}") for layout in order]
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


MAX_COLD_RESIDENT_PCT = 5.0


def mode(label: str) -> str | None:
    """'warm' for r1..rN, 'cold' for c1..cN (OS file cache purged before the run)."""
    if re.fullmatch(r"r\d+", label):
        return "warm"
    if re.fullmatch(r"c\d+", label):
        return "cold"
    return None


def valid(row: dict) -> bool:
    m = mode(row["label"])
    if m is None or row["exit_status"] != 0 or not row["md5_ok"]:
        return False
    if m == "cold":
        pct = row.get("resident_pct")
        return pct is not None and float(pct) <= MAX_COLD_RESIDENT_PCT
    return True


def summarize(rows: list[dict]) -> dict:
    groups: dict[tuple[str, str, int], list[dict]] = {}
    for r in rows:
        r = {**r, "cpu_s": r.get("cpu_s", r["user_s"] + r["sys_s"])}
        if valid(r):
            groups.setdefault((mode(r["label"]), r["layout"], r["workers"]), []).append(
                r
            )
    layouts = []
    for (m_, layout, workers), rs in sorted(
        groups.items(), key=lambda kv: (kv[0][0] != "warm", kv[0][2], kv[0][1])
    ):
        entry = {"mode": m_, "layout": layout, "workers": workers, "n": len(rs)}
        for m in METRICS:
            vals = [float(r[m]) for r in rs if r.get(m) is not None]
            if vals:
                entry[m] = {
                    "median": statistics.median(vals),
                    "min": min(vals),
                    "max": max(vals),
                }
        layouts.append(entry)
    by = {(e["mode"], e["layout"], e["workers"]): e for e in layouts}
    ratios: dict[str, dict[int, dict]] = {}
    for m_, _, workers in sorted(by):
        t, s = by.get((m_, "tiered", workers)), by.get((m_, "sorted", workers))
        if t and s:
            ratios.setdefault(m_, {})[workers] = {
                m: s[m]["median"] / t[m]["median"]
                for m in METRICS
                if m in t and m in s and t[m]["median"]
            }
    vs_tiered: dict[str, dict[int, dict]] = {}
    for m_, layout, workers in sorted(by):
        t, o = by.get((m_, "tiered", workers)), by[(m_, layout, workers)]
        if layout != "tiered" and t:
            vs_tiered.setdefault(m_, {}).setdefault(workers, {})[layout] = {
                m: o[m]["median"] / t[m]["median"]
                for m in METRICS
                if m in t and m in o and t[m]["median"]
            }
    invalid = sum(1 for r in rows if mode(r["label"]) is not None and not valid(r))
    return {
        "layouts": layouts,
        "ratios": ratios,
        "ratios_vs_tiered": vs_tiered,
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


def ensure_free_memory(limit_gib: float, release_gib: int = 27) -> bool:
    """Push the file cache out by touching release_gib of memory once (benchmark protocol).

    Returns whether a release was needed; stops the runner if memory is still low after it.
    """
    if free_memory_gib() >= limit_gib:
        return False
    code = "n=int(__import__('sys').argv[1])<<30\nb=bytearray(n)\nfor i in range(0,n,16384): b[i]=1"
    subprocess.run([sys.executable, "-I", "-c", code, str(release_gib)], check=True)
    free = free_memory_gib()
    if free < limit_gib:
        raise SystemExit(
            f"only {free:.1f} GiB free memory after a release; need {limit_gib} GiB"
        )
    return True


def resident_pct(path: Path) -> float:
    """Share of the file's pages in the OS page cache (mincore on a read-only mapping)."""
    import ctypes
    import ctypes.util
    import mmap

    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    libc.mmap.restype = ctypes.c_void_p
    libc.mmap.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_long,
    ]
    libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_char_p]
    size = Path(path).stat().st_size
    if size == 0:
        return 0.0
    fd = os.open(path, os.O_RDONLY)
    try:
        addr = libc.mmap(None, size, mmap.PROT_READ, mmap.MAP_SHARED, fd, 0)
        if addr in (None, ctypes.c_void_p(-1).value):
            raise OSError(ctypes.get_errno(), "mmap failed")
        try:
            pages = (size + mmap.PAGESIZE - 1) // mmap.PAGESIZE
            vec = ctypes.create_string_buffer(pages)
            if libc.mincore(addr, size, vec) != 0:
                raise OSError(ctypes.get_errno(), "mincore failed")
            resident = sum(1 for b in vec.raw if b & 1)
        finally:
            libc.munmap(addr, size)
    finally:
        os.close(fd)
    return 100.0 * resident / pages


def make_run_one(args: argparse.Namespace) -> Callable[[str, int, str], dict]:
    def run_one(layout: str, workers: int, label: str) -> dict:
        shard = (
            args.cache_root
            / layout
            / args.cache_name
            / "variation"
            / f"{args.contig}.parquet"
        )
        released = False
        if mode(label) == "cold":
            subprocess.run(args.purge_cmd.split(), check=True)
        else:
            released = ensure_free_memory(args.min_free_memory_gib)
        resident = round(resident_pct(shard), 2)
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
                    "exit_status": proc.returncode or 1, "resident_pct": resident,
                    "released": released}  # fmt: skip
        row = {
            "layout": layout,
            "workers": workers,
            "label": label,
            **parse_time_file(time_file),
            "resident_pct": resident,
            "released": released,
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


def write_summary(summary: dict, out: Path) -> None:
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = [
        "| Mode | Layout | Workers | n | Wall (s) | CPU (s) | User (s) | System (s) | Peak RSS (GiB) |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for e in summary["layouts"]:

        def cell(m: str, scale: float = 1.0) -> str:
            v = e.get(m)
            if not v:
                return "-"
            return f"{v['median'] / scale:.2f} ({v['min'] / scale:.2f}-{v['max'] / scale:.2f})"

        lines.append(
            f"| {e['mode']} | {e['layout']} | {e['workers']} | {e['n']} | {cell('wall_s')} | "
            f"{cell('cpu_s')} | {cell('user_s')} | {cell('sys_s')} | {cell('max_rss_kib', 2**20)} |"
        )
    lines += ["", "Ratio sorted / tiered of the medians (> 1: tiering helps):", ""]
    for m_, by_workers in summary["ratios"].items():
        for w, r in by_workers.items():
            lines.append(
                f"- {m_}, {w} workers: "
                + ", ".join(f"{m} {v:.3f}" for m, v in r.items())
            )
    for m_, by_workers in summary.get("ratios_vs_tiered", {}).items():
        for w, per in by_workers.items():
            for layout, r in per.items():
                if layout != "sorted":
                    lines.append(
                        f"- {m_}, {w} workers, {layout} / tiered: "
                        + ", ".join(f"{m} {v:.3f}" for m, v in r.items())
                    )
    lines.append(
        f"\nInvalid runs (failed, output differs, or cold run with > 5% of the shard cached): {summary['invalid_runs']}"
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
    p.add_argument("--contig", default="chr22")
    p.add_argument("--layouts", nargs="+", default=["tiered", "sorted"])
    p.add_argument(
        "--cold", type=int, default=0, help="cold repeats per layout and workers"
    )
    p.add_argument("--purge-cmd", default="sudo purge")
    args = p.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    env = {"python": platform.python_version(), "platform": platform.platform(),
           "vepyr_python": str(args.vepyr_python), "args": {k: str(v) for k, v in vars(args).items()}}  # fmt: skip
    (args.workdir / "env.json").write_text(json.dumps(env, indent=2) + "\n")
    plan = schedule(args.layouts, args.workers, args.repeats) if args.repeats else []
    if args.cold:
        plan += schedule(
            args.layouts, args.workers, args.cold, prefix="c", warmup=False
        )
    run_one = make_run_one(args)
    rows = execute(
        plan, run_one, args.workdir, args.min_disk_gib, args.workdir / "runs.csv"
    )
    write_summary(summarize(rows), args.workdir)


if __name__ == "__main__":
    main()
