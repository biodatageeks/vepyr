#!/usr/bin/env python3
import json, os, shutil, subprocess, time
from pathlib import Path

ROOT=Path('/Users/lukaszjezapkowicz/Desktop/magisterka/praca/magisterska_jezapkowicz').resolve()
RUNS=ROOT/'runs'
IMAGE='vepyr-cache-worker:chr1-3'
BASELINE=4197.655177116394
MIN_FREE=35.0
CURRENT=RUNS/'persistent_dist_queue/run_1792251749'
LOG=RUNS/'night_distributed_search.log'

# One-host distributed candidates. Memory caps keep total host use below 32 GiB.
CANDIDATES=[
    ('persistent_5pods_2cpu', 'runs/persistent_dist_queue.py', {'DIST_GROUPS':'20','DIST_PODS':'5','DIST_CPU':'2','DIST_MEMORY':'10g'}),
    ('persistent_10pods_1cpu_40groups', 'runs/persistent_dist_queue.py', {'DIST_GROUPS':'40','DIST_PODS':'10','DIST_CPU':'1','DIST_MEMORY':'6g'}),
    ('static_30groups_10active', 'runs/distributed_queue_sweep.py', {'DIST_GROUPS':'30','DIST_MAX_ACTIVE':'10','DIST_CPU':'1','DIST_MEMORY':'6g','DIST_ENTITY_TASKS':'0'}),
]

def log(msg):
    with LOG.open('a') as f: f.write(time.strftime('%Y-%m-%dT%H:%M:%S%z ')+msg+'\n')

def free_gib(): return shutil.disk_usage(ROOT).free/1024**3

def containers():
    try:
        out=subprocess.check_output(['docker','ps','-q','--filter',f'ancestor={IMAGE}'],text=True)
        return [x for x in out.splitlines() if x]
    except Exception: return []

def kill_all():
    ids=containers()
    if ids: subprocess.run(['docker','kill',*ids],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

def run_one(name, script, env, repeat=False):
    if free_gib()<MIN_FREE: log(f'STOP disk_guard before {name} free={free_gib():.1f}'); return None
    e=os.environ.copy(); e.update(env); e['PYTHONUNBUFFERED']='1'
    out=RUNS/'night_candidates'; out.mkdir(exist_ok=True)
    logfile=out/f'{name}_{int(time.time())}.log'
    cmd=['python3','-u',script]
    log(f'START {name} cmd={cmd} env={env}')
    with logfile.open('w') as f:
        try:
            r=subprocess.run(cmd,cwd=ROOT,env=e,stdout=f,stderr=subprocess.STDOUT,timeout=3*3600)
            log(f'END {name} rc={r.returncode} free={free_gib():.1f} log={logfile}')
        except subprocess.TimeoutExpired:
            log(f'TIMEOUT {name}; killing containers')
            kill_all(); return None
    # Locate newest summary under known run roots.
    summaries=[]
    for base in [RUNS/'persistent_dist_queue', RUNS/'distributed_queue_sweep']:
        if base.exists(): summaries += list(base.glob('run_*/summary.json'))
    if not summaries: return None
    try:
        return json.loads(max(summaries,key=lambda x:x.stat().st_mtime).read_text())
    except Exception: return None

def cleanup_outputs():
    # Keep JSON/log/command evidence, remove generated Parquet payloads.
    for base in [RUNS/'persistent_dist_queue', RUNS/'distributed_queue_sweep']:
        if not base.exists(): continue
        for run in base.glob('run_*'):
            for d in [run/'output', run/'prepared_cache']:
                if d.exists(): shutil.rmtree(d,ignore_errors=True)

def main():
    log('CONTROLLER_START')
    while containers():
        if free_gib()<MIN_FREE:
            log(f'DISK_GUARD current free={free_gib():.1f}; killing'); kill_all(); break
        time.sleep(60)
    # Give current runner time to write summary after containers exit.
    time.sleep(10)
    cleanup_outputs()
    for name,script,env in CANDIDATES:
        result=run_one(name,script,env)
        cleanup_outputs()
        wall=None
        if isinstance(result,dict): wall=result.get('wall_seconds')
        log(f'RESULT {name} wall={wall} result={result}')
        if isinstance(wall,(int,float)) and wall < BASELINE*0.90:
            log(f'PROMISING {name} wall={wall}; five repeats start')
            for i in range(5):
                rr=run_one(f'{name}_repeat{i+1}',script,env,True)
                cleanup_outputs(); log(f'REPEAT_RESULT {name} i={i+1} result={rr}')
    log('CONTROLLER_END')

if __name__=='__main__': main()
