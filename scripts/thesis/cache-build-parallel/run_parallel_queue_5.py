#!/usr/bin/env python3
import json, os, shutil, subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNROOT = ROOT / 'runs' / 'parallel_full_cache'
CURRENT = RUNROOT / 'current'
OUT = RUNROOT / 'queue_20_10_repeats'
OUT.mkdir(parents=True, exist_ok=True)
py = '/Users/lukaszjezapkowicz/workspace/data_vepyr/.venv-lpt/bin/python3'
runner = ROOT / 'runs' / 'parallel_entity_queue.py'

rows=[]
for rep in range(1, 6):
    shutil.rmtree(CURRENT, ignore_errors=True)
    t0=time.time()
    log=OUT / f'repeat_{rep}.log'
    with log.open('w') as fh:
        proc=subprocess.run([py, '-u', str(runner)], cwd=ROOT, env={**os.environ, 'PARALLEL_GROUPS':'20', 'PARALLEL_PROCESSES':'10'}, stdout=fh, stderr=subprocess.STDOUT)
    wall=time.time()-t0
    summary=CURRENT/'summary.json'
    item={'repeat':rep,'exit_code':proc.returncode,'seconds_wall_process':wall}
    if summary.exists(): item.update(json.loads(summary.read_text()))
    (OUT/f'repeat_{rep}.json').write_text(json.dumps(item,indent=2)+'\n')
    rows.append(item)
    print(json.dumps(item), flush=True)
    if proc.returncode != 0 or not summary.exists():
        break
mean=sum(x['seconds_wall'] for x in rows if 'seconds_wall' in x)/max(1,sum('seconds_wall' in x for x in rows))
final={'repeats':rows,'n':len(rows),'mean_seconds':mean}
(OUT/'summary.json').write_text(json.dumps(final,indent=2)+'\n')
print(json.dumps(final), flush=True)
