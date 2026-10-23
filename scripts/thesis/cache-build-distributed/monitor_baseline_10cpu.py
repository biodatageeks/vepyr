#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path

ROOT = Path("runs/full_cache_20")
HB = ROOT / "baseline_10cpu.heartbeat.json"
LOG = ROOT / "baseline_10cpu_monitor.log"

def snap():
    now = time.time()
    hb = json.loads(HB.read_text()) if HB.exists() else {}
    age = now - hb.get("ts", 0)
    ps = subprocess.run(["docker", "ps", "--format", "{{.Names}} {{.Status}}"], text=True, capture_output=True).stdout.strip()
    stats = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}} {{.MemUsage}}"], text=True, capture_output=True).stdout.strip()
    disk = subprocess.run(["df", "-h", "."], text=True, capture_output=True).stdout.splitlines()[-1]
    line = json.dumps({"ts": now, "heartbeat_age_s": round(age, 1), "docker": ps, "stats": stats, "disk": disk}, ensure_ascii=False)
    with LOG.open("a") as f:
        f.write(line + "\n")
    print(line, flush=True)

while True:
    snap()
    time.sleep(1800)
