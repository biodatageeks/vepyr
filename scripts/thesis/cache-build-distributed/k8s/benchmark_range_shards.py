#!/usr/bin/env python3
"""Benchmark variation-only genomic sharding with Docker workers.

Each worker gets one physical BGZF range. Multiple ranges can be assigned to
the fixed worker pool, modelling a queue without adding intra-worker
partitioning. Extraction time is recorded separately from build time.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


DATA = Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38")
ROOT = Path.cwd() / "runs/point5_k8s/range_shard_bench"
IMAGE = "vepyr-cache-worker:chr1-3"
LENGTHS = {"1": 248_956_422, "2": 242_193_529, "3": 198_295_559, "22": 50_818_468}


def run(cmd: list[str], **kwargs):
    return subprocess.run(cmd, check=True, text=True, **kwargs)


def prepare(chrom: str, n: int) -> tuple[Path, float, list[dict]]:
    out = ROOT / "prepared" / f"chr{chrom}_n{n}"
    if out.exists() and (out / "ranges.json").exists():
        existing = json.loads((out / "ranges.json").read_text())
        if all(Path(e["file"]).exists() for e in existing):
            return out, 0.0, existing
        shutil.rmtree(out)
    cache = out / "homo_sapiens_merged" / "116_GRCh38"
    (cache / chrom).mkdir(parents=True, exist_ok=True)
    shutil.copy2(DATA / "info.txt", cache / "info.txt")
    length = LENGTHS[chrom]
    ranges = []
    started = time.time()
    for i in range(n):
        start = i * length // n + 1
        end = (i + 1) * length // n
        target = cache / chrom / f"all_vars_{i:02d}.gz"
        cmd = ["bash", "-lc", f"tabix {DATA / chrom / 'all_vars.gz'} {chrom}:{start}-{end} | bgzip -c > {target}"]
        run(cmd)
        # Build API expects all_vars.gz in source chromosome directory. Keep
        # one shard per root; the caller rewrites this link before each run.
        ranges.append({"index": i, "chrom": chrom, "start": start, "end": end, "file": str(target)})
    (out / "ranges.json").write_text(json.dumps(ranges, indent=2))
    return out, time.time() - started, ranges


def run_worker(prepared: Path, range_entry: dict, output: Path, tag: str) -> tuple[float, str]:
    source_dir = prepared / "homo_sapiens_merged" / "116_GRCh38" / range_entry["chrom"]
    source = Path(range_entry["file"])
    link = source_dir / "all_vars.gz"
    if link.exists() or link.is_symlink():
        link.unlink()
    link.symlink_to(source.name)
    output.mkdir(parents=True, exist_ok=True)
    log = output / f"{tag}.log"
    cmd = [
        "docker", "run", "--rm", "--cpus=2", "--memory=6g", "--memory-swap=6g",
        "-e", "CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38",
        "-e", "OUTPUT_DIR=/output", "-e", f"CHROM={range_entry['chrom']}",
        "-v", f"{prepared}:/raw_cache", "-v", f"{Path.cwd() / 'runs/point5_k8s/range_worker.py'}:/app/range_worker.py:ro",
        "-v", f"{output}:/output", IMAGE, "python", "/app/range_worker.py",
    ]
    (output / f"{tag}.command").write_text(" ".join(cmd) + "\n")
    started = time.time()
    with log.open("w") as fh:
        proc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, text=True)
    elapsed = time.time() - started
    if proc.returncode:
        raise RuntimeError(f"worker failed: {tag}; see {log}")
    return elapsed, log.read_text()


def benchmark(chrom: str, n: int, workers: int) -> dict:
    prepared, extract_s, ranges = prepare(chrom, n)
    run_dir = ROOT / "results" / f"chr{chrom}_n{n}_w{workers}"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)
    started = time.time()
    # Fixed worker pool. One worker handles one range at a time; next range
    # starts only after it finishes, modelling a queue.
    pending = list(ranges)
    active: list[tuple[subprocess.Popen, dict, Path, object, float]] = []
    # Keep implementation simple and deterministic: launch up to `workers`,
    # then launch next range as each process exits.
    while pending or active:
        while pending and len(active) < workers:
            entry = pending.pop(0)
            out = run_dir / f"range_{entry['index']:02d}"
            out.mkdir()
            # Isolate each worker's source tree. A shared all_vars.gz symlink
            # races when workers start together and invalidates row counts.
            cache_mount = out / "cache"
            source_dir = cache_mount / "homo_sapiens_merged" / "116_GRCh38" / entry["chrom"]
            source_dir.mkdir(parents=True)
            shutil.copy2(DATA / "info.txt", cache_mount / "homo_sapiens_merged" / "116_GRCh38" / "info.txt")
            os.link(Path(entry["file"]).resolve(), source_dir / "all_vars.gz")
            log = (out / "worker.log").open("w")
            cmd = ["docker", "run", "--rm", "--cpus=2", "--memory=6g", "--memory-swap=6g",
                   "-e", "CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38",
                   "-e", "OUTPUT_DIR=/output", "-e", f"CHROM={chrom}",
                   "-v", f"{cache_mount}:/raw_cache", "-v", f"{Path.cwd() / 'runs/point5_k8s/range_worker.py'}:/app/range_worker.py:ro",
                   "-v", f"{out}:/output", "--entrypoint", "python", IMAGE, "/app/range_worker.py"]
            (out / "command.txt").write_text(" ".join(cmd) + "\n")
            p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, text=True)
            active.append((p, entry, out, log, time.time()))
        time.sleep(1)
        still = []
        for p, entry, out, log, t0 in active:
            if p.poll() is None:
                still.append((p, entry, out, log, t0))
            else:
                log.close()
                if p.returncode:
                    raise RuntimeError(f"worker failed for {entry}: {out / 'worker.log'}")
                (out / "elapsed.txt").write_text(f"{time.time()-t0:.6f}\n")
        active = still
    wall = time.time() - started
    result = {"chrom": chrom, "ranges": n, "workers": workers, "extract_seconds": extract_s,
              "build_wall_seconds": wall, "total_with_extract_seconds": wall + extract_s,
              "rows": None, "range_files": ranges}
    (run_dir / "summary.json").write_text(json.dumps(result, indent=2))
    return result


def main():
    chrom = sys.argv[1]
    n = int(sys.argv[2])
    workers = int(sys.argv[3])
    print(json.dumps(benchmark(chrom, n, workers), indent=2), flush=True)


if __name__ == "__main__":
    main()
