#!/usr/bin/env python3
import json, os, shutil, subprocess, time
from pathlib import Path
ROOT=Path('/Users/lukaszjezapkowicz/Desktop/magisterka/praca/magisterska_jezapkowicz').resolve()
IMAGE='vepyr-cache-worker:chr1-3'; MIN_FREE=35.0
DIST=4932.31; BASE=10984.15
TOTAL_CPU=9; TOTAL_RAM_GIB=24
CONFIGS=[
 ('persistent_3p_3cpu_8g_g20','runs/persistent_dist_queue.py','persistent',{'DIST_GROUPS':'20','DIST_PODS':'3','DIST_CPU':'3','DIST_MEMORY':'8g'}),
 ('persistent_4p_225cpu_6g_g20','runs/persistent_dist_queue.py','persistent',{'DIST_GROUPS':'20','DIST_PODS':'4','DIST_CPU':'2.25','DIST_MEMORY':'6g'}),
 ('persistent_6p_15cpu_4g_g20','runs/persistent_dist_queue.py','persistent',{'DIST_GROUPS':'20','DIST_PODS':'6','DIST_CPU':'1.5','DIST_MEMORY':'4g'}),
]
OUT=ROOT/'runs/isolated_distributed_fixed_resources_5x'; OUT.mkdir(exist_ok=True)

def free(): return shutil.disk_usage(ROOT).free/1024**3
def kill_image():
 ids=subprocess.check_output(['docker','ps','-q','--filter',f'ancestor={IMAGE}'],text=True).split()
 for i in ids: subprocess.run(['docker','kill',i],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
def cleanup():
 for base in [ROOT/'runs/persistent_dist_queue',ROOT/'runs/distributed_queue_sweep']:
  if not base.exists(): continue
  for r in base.glob('run_*'):
   d=r/'output'
   if d.exists(): shutil.rmtree(d,ignore_errors=True)

def new_summary(kind, t0):
 base=ROOT/'runs'/('persistent_dist_queue' if kind=='persistent' else 'distributed_queue_sweep')
 pats=('run_*/summary.json' if kind=='persistent' else 'groups_*/summary.json')
 xs=[x for x in base.glob(pats) if x.stat().st_mtime>=t0-1]
 if not xs: return None
 x=max(xs,key=lambda z:z.stat().st_mtime)
 try: return json.loads(x.read_text())
 except Exception: return None

def valid(kind,s):
 if not s: return False
 if kind=='persistent': return s.get('done')==s.get('tasks') and all(x==0 for x in s.get('returncodes',[]))
 return s.get('all_success') is True and all(x.get('returncode')==0 for x in s.get('results',[]))

report=[]
cleanup(); kill_image()
for name,script,kind,env in CONFIGS:
 for rep in range(1,6):
  if free()<MIN_FREE: raise SystemExit(f'disk guard before {name} rep{rep}: {free():.1f} GiB')
  kill_image(); t0=time.time(); log=OUT/f'{name}_rep{rep}.log'
  e=os.environ.copy(); e.update(env); e['PYTHONUNBUFFERED']='1'
  cmd=['python3','-u',script]
  print(f'START {name} rep={rep} cmd={cmd} env={env}',flush=True)
  with log.open('w') as f:
   try: proc=subprocess.run(cmd,cwd=ROOT,env=e,stdout=f,stderr=subprocess.STDOUT,timeout=3*3600)
   except subprocess.TimeoutExpired: kill_image(); proc=None
  s=new_summary(kind,t0); wall=s.get('wall_seconds') if s else None
  ok=valid(kind,s)
  row={'config':name,'repeat':rep,'returncode':None if proc is None else proc.returncode,'valid':ok,'wall_seconds':wall,'summary':s,'log':str(log)}
  report.append(row); (OUT/'progress.json').write_text(json.dumps(report,indent=2))
  print(f'END {name} rep={rep} valid={ok} wall={wall} free={free():.1f}',flush=True)
  cleanup(); kill_image()
summary={}
for name,_,_,_ in CONFIGS:
 xs=[r['wall_seconds'] for r in report if r['config']==name and r['valid'] and r['wall_seconds'] is not None]
 summary[name]={'valid_runs':len(xs),'times':xs,'mean_seconds':sum(xs)/len(xs) if xs else None,'speedup_vs_distributed':DIST/(sum(xs)/len(xs)) if xs else None,'speedup_vs_baseline9':BASE/(sum(xs)/len(xs)) if xs else None}
(OUT/'summary.json').write_text(json.dumps({'runs':report,'summary':summary},indent=2))
print(json.dumps(summary,indent=2),flush=True)
