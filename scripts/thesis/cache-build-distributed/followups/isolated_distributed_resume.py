import json, os, shutil, subprocess, time
from pathlib import Path
ROOT=Path('/Users/lukaszjezapkowicz/Desktop/magisterka/praca/magisterska_jezapkowicz').resolve(); IMAGE='vepyr-cache-worker:chr1-3'; MIN_FREE=35
OUT=ROOT/'runs/isolated_distributed_fixed_resources_5x'; OUT.mkdir(exist_ok=True)
CONFIGS=[('persistent_3p_3cpu_8g_g20',{'DIST_GROUPS':'20','DIST_PODS':'3','DIST_CPU':'3','DIST_MEMORY':'8g'},4),('persistent_4p_225cpu_6g_g20',{'DIST_GROUPS':'20','DIST_PODS':'4','DIST_CPU':'2.25','DIST_MEMORY':'6g'},1),('persistent_6p_15cpu_4g_g20',{'DIST_GROUPS':'20','DIST_PODS':'6','DIST_CPU':'1.5','DIST_MEMORY':'4g'},1)]
old=[]
try: old=json.loads((OUT/'progress.json').read_text())
except: pass
# Keep only verified prior records.
report=[x for x in old if x.get('config')=='persistent_3p_3cpu_8g_g20' and x.get('repeat') in (1,2,3) and x.get('valid') and x.get('summary',{}).get('done')==x.get('summary',{}).get('tasks') and all(c==0 for c in x.get('summary',{}).get('returncodes',[]))]
def free(): return shutil.disk_usage(ROOT).free/1024**3
def kill():
 for x in subprocess.check_output(['docker','ps','-q','--filter',f'ancestor={IMAGE}'],text=True).split(): subprocess.run(['docker','kill',x],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
def clean():
 for base in [ROOT/'runs/persistent_dist_queue']:
  for r in base.glob('run_*'):
   if (r/'output').exists(): shutil.rmtree(r/'output',ignore_errors=True)
def summaries(): return set((ROOT/'runs/persistent_dist_queue').glob('run_*/summary.json'))
def valid(s): return bool(s and s.get('done')==s.get('tasks') and all(c==0 for c in s.get('returncodes',[])))
clean(); kill()
for name,env,start in CONFIGS:
 for rep in range(start,6):
  if free()<MIN_FREE: raise SystemExit(f'disk guard {free():.1f} GiB')
  kill(); before=summaries(); t0=time.time(); log=OUT/f'{name}_rep{rep}.log'; e=os.environ.copy(); e.update(env); e['PYTHONUNBUFFERED']='1'
  print(f'START {name} rep={rep}',flush=True)
  with log.open('w') as f:
   try: proc=subprocess.run(['python3','-u','runs/persistent_dist_queue.py'],cwd=ROOT,env=e,stdout=f,stderr=subprocess.STDOUT,timeout=4*3600)
   except subprocess.TimeoutExpired: kill(); proc=None
  after=[x for x in summaries()-before if x.stat().st_mtime>=t0-1]; s=json.loads(max(after,key=lambda x:x.stat().st_mtime).read_text()) if after else None; ok=valid(s); row={'config':name,'repeat':rep,'returncode':None if proc is None else proc.returncode,'valid':ok,'wall_seconds':s.get('wall_seconds') if s else None,'summary':s,'log':str(log)}; report.append(row); (OUT/'progress.json').write_text(json.dumps(report,indent=2)); print(f'END {name} rep={rep} valid={ok} wall={row["wall_seconds"]} free={free():.1f}',flush=True); clean(); kill()
summary={}
for name,_,_ in CONFIGS:
 xs=[r['wall_seconds'] for r in report if r['config']==name and r['valid'] and r['wall_seconds']]
 summary[name]={'valid_runs':len(xs),'times':xs,'mean_seconds':sum(xs)/len(xs) if xs else None,'speedup_vs_distributed':4932.31/(sum(xs)/len(xs)) if xs else None,'speedup_vs_baseline9':10984.15/(sum(xs)/len(xs)) if xs else None}
(OUT/'summary.json').write_text(json.dumps({'runs':report,'summary':summary},indent=2)); print(json.dumps(summary,indent=2),flush=True)
