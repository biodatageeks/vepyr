"""One subprocess per run, measured with wait4 so RSS belongs to that run only.

Peak RSS is the child's own. For `docker run` that is the Docker client, not
the container, so VEP memory is not reported.
"""

from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pibench.paths import MAX_LOAD, REPO_ROOT, VEP_IMAGE

RSS_UNIT = (
    1 if sys.platform == "darwin" else 1024
)  # ru_maxrss: bytes on macOS, KiB on Linux


def wait_for_quiet(
    max_load: float, timeout_s: float = 900, poll_s: float = 15
) -> float:
    deadline = time.monotonic() + timeout_s
    while True:
        load = os.getloadavg()[0]
        if load < max_load:
            return load
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"load average {load:.2f} stayed >= {max_load} for {timeout_s:.0f}s"
            )
        time.sleep(poll_s)


def run_once(
    cmd: list[str], log_prefix: Path, env: dict | None = None, load: float | None = None
) -> dict:
    log_prefix.parent.mkdir(parents=True, exist_ok=True)
    with (
        open(f"{log_prefix}.stdout.txt", "wb") as out,
        open(f"{log_prefix}.stderr.txt", "wb") as err,
    ):
        t0 = time.perf_counter()
        proc = subprocess.Popen(cmd, stdout=out, stderr=err, env=env)
        _, status, ru = os.wait4(proc.pid, 0)
        wall = time.perf_counter() - t0
    proc.returncode = os.waitstatus_to_exitcode(status)
    return {
        "cmd": cmd,
        "exit": proc.returncode,
        "wall_s": wall,
        "user_s": ru.ru_utime,
        "sys_s": ru.ru_stime,
        "max_rss_bytes": ru.ru_maxrss * RSS_UNIT,
        "load_1m_before": load,
    }


def summarize(runs: list[dict]) -> dict:
    walls = [r["wall_s"] for r in runs]
    return {
        "median_wall_s": statistics.median(walls),
        "min_wall_s": min(walls),
        "max_wall_s": max(walls),
        "median_rss_bytes": statistics.median(r["max_rss_bytes"] for r in runs),
    }


def repeat(
    cmd: list[str],
    log_dir: Path,
    *,
    warmups: int = 1,
    repeats: int = 3,
    max_load: float = MAX_LOAD,
    env: dict | None = None,
) -> dict:
    out: dict = {"warmups": [], "runs": []}
    for kind, n in (("warmups", warmups), ("runs", repeats)):
        for i in range(n):
            load = wait_for_quiet(max_load)
            r = run_once(cmd, log_dir / f"{kind[:-1]}{i}", env=env, load=load)
            if r["exit"] != 0:
                raise RuntimeError(f"exit {r['exit']}: {' '.join(cmd)} (see {log_dir})")
            out[kind].append(r)
    out.update(summarize(out["runs"]))
    return out


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args], capture_output=True, text=True
    ).stdout.strip()


def environment() -> dict:
    import importlib.metadata as md

    def version(pkg: str) -> str | None:
        try:
            return md.version(pkg)
        except md.PackageNotFoundError:
            return None

    cpu = subprocess.run(
        ["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True
    ).stdout.strip()
    engine = next(
        (
            line
            for line in (REPO_ROOT / "Cargo.toml").read_text().splitlines()
            if line.startswith("datafusion-bio-function-vep")
        ),
        "",
    )
    image = subprocess.run(
        ["docker", "image", "inspect", VEP_IMAGE, "--format", "{{.Id}}"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {
        "date_utc": datetime.now(timezone.utc).isoformat(),
        "host": platform.node(),
        "os": platform.platform(),
        "cpu": cpu,
        "ncpu": os.cpu_count(),
        "vepyr_commit": _git("rev-parse", "HEAD"),
        "vepyr_dirty": bool(_git("status", "--porcelain", "--untracked-files=no")),
        "engine_pin": engine,
        "python": sys.version.split()[0],
        "polars": version("polars"),
        "polars_bio": version("polars-bio"),
        "vepyr": version("vepyr"),
        "vep_image": VEP_IMAGE,
        "vep_image_id": image,
    }


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str) + "\n")
