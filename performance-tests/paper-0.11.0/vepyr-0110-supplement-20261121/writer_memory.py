"""Writer memory (supplement Section 8.4): one warm-up and three runs per output path, median peak RSS and wall.

Usage: writer_memory.py <python of the venv> <input.vcf.gz> <cache_dir> <fasta> <work_dir> <out.json>
"""

import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

py, vcf, cache, fasta, work, out = sys.argv[1:7]
here = Path(__file__).resolve().parent
Path(work).mkdir(parents=True, exist_ok=True)
res = {}
for path, ext in (("collect", "rows.txt"), ("parquet", "parquet"), ("vcf", "vcf")):
    target = str(Path(work) / f"chr1.{path}.{ext}")
    runs = []
    for i in range(4):  # run 0 is the warm-up
        if os.path.exists(target):
            os.remove(target)
        t0 = time.perf_counter()
        proc = subprocess.Popen(
            [py, str(here / "writer_once.py"), path, vcf, cache, fasta, target],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        _, status, ru = os.wait4(proc.pid, 0)
        wall = time.perf_counter() - t0
        if os.waitstatus_to_exitcode(status) != 0:
            sys.exit(f"{path} run {i} failed: {proc.stderr.read().decode()[-2000:]}")
        runs.append(
            {
                "wall_s": wall,
                "rss_bytes": ru.ru_maxrss,
                "cpu_s": ru.ru_utime + ru.ru_stime,
            }
        )
    m = runs[1:]
    res[path] = {
        "warmup": runs[0],
        "runs": m,
        "median_wall_s": statistics.median(r["wall_s"] for r in m),
        "median_rss_gib": statistics.median(r["rss_bytes"] for r in m) / 2**30,
    }
    print(
        path,
        f"wall {res[path]['median_wall_s']:.2f} s",
        f"rss {res[path]['median_rss_gib']:.2f} GiB",
        flush=True,
    )
    if os.path.exists(target):
        os.remove(target)
json.dump(res, open(out, "w"), indent=1)
