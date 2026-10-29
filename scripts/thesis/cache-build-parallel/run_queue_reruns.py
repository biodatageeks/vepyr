#!/usr/bin/env python3
"""Re-run chosen (repeat, processes) cells of the queue-scaling series (thesis E1, 2026-10-30).

On 2026-10-29 the laptop was unplugged at 18:47 CET during the series, so p=1 r1 (partly),
p=8 r1, p=10 r1, p=10 r2 and p=2 r2 ran on battery (p=2 r2 from 25% down to 1%, after
which the machine hibernated until AC was attached at 22:12). These cells are measured
again on AC power after the series, with the same queue script, interpreter and record
format as run_queue_scaling.py (snapshot of pmset therm/batt and load before and after,
per-task summary kept, outputs deleted, 20 s pause). A cell is not started unless pmset
reports AC power.

Usage: run_queue_reruns.py OUT_DIR REPEAT:PROCESSES [...]
  e.g. run_queue_reruns.py runs/queue_scaling_20261029_ac_reruns 1:1 1:8 1:10 2:10 2:2
Run with the .venv-lpt interpreter.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

QUEUE = Path('/private/tmp/claude-501/-Users-lukaszjezapkowicz/3fc203b0-e9c0-4fa4-b8fa-1b56289ba13b/scratchpad'
             '/vepyr-scaling/scripts/thesis/cache-build-parallel/parallel_entity_queue.py')
QUEUE = Path(os.environ.get('QUEUE', QUEUE))
PY = sys.executable


def sh(cmd):
    return subprocess.run(cmd, capture_output=True, text=True).stdout.strip()


def snapshot(out):
    return {'utc': datetime.now(timezone.utc).isoformat(), 'therm': sh(['pmset', '-g', 'therm']),
            'batt': sh(['pmset', '-g', 'batt']), 'load': os.getloadavg(), 'free_gb': shutil.disk_usage(out).free / 1e9}


def main():
    out = Path(sys.argv[1]).resolve()
    cells = [tuple(map(int, c.split(':'))) for c in sys.argv[2:]]
    out.mkdir(parents=True, exist_ok=True)
    current = out / 'runs/parallel_full_cache/current'
    (out / 'plan.json').write_text(json.dumps({'cells': cells, 'python': PY, 'queue': str(QUEUE), 'groups': 20,
                                               'reason': 'battery-powered cells of 2026-10-29 re-run on AC'}, indent=2) + '\n')
    jsonl = out / 'runs.jsonl'
    done = {(r['repeat'], r['processes']) for r in map(json.loads, jsonl.read_text().splitlines())
            if r.get('exit_code') == 0} if jsonl.exists() else set()
    for rep, p in cells:
        if (rep, p) in done:
            continue
        while "drawing from 'AC Power'" not in sh(['pmset', '-g', 'batt']):
            print('waiting for AC power', flush=True)
            time.sleep(60)
        shutil.rmtree(current, ignore_errors=True)
        before = snapshot(out)
        t0 = time.time()
        with (out / f'p{p:02d}_r{rep}.log').open('w') as fh:
            proc = subprocess.run([PY, '-u', str(QUEUE)], cwd=out, stdout=fh, stderr=subprocess.STDOUT,
                                  env={**os.environ, 'PARALLEL_GROUPS': '20', 'PARALLEL_PROCESSES': str(p)})
        item = {'repeat': rep, 'processes': p, 'exit_code': proc.returncode, 'seconds_wall_process': time.time() - t0,
                'before': before, 'after': snapshot(out), 'rerun_on_ac': True}
        summary = current / 'summary.json'
        ok = proc.returncode == 0 and summary.exists()
        if summary.exists():
            s = json.loads(summary.read_text())
            item['seconds_wall'] = s['seconds_wall']
            item['sum_task_seconds'] = sum(t['seconds'] for t in s['results'])
            item['max_task_seconds'] = max(t['seconds'] for t in s['results'])
            item['files'] = sum(len(st[1]) for t in s['results'] for st in t['stats'])
            (out / f'p{p:02d}_r{rep}.summary.json').write_text(json.dumps(s, indent=1) + '\n')
        with jsonl.open('a') as f:
            f.write(json.dumps(item) + '\n')
        print(json.dumps({k: v for k, v in item.items() if k not in ('before', 'after')}), flush=True)
        shutil.rmtree(current, ignore_errors=True)
        if not ok:
            return 1
        time.sleep(20)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
