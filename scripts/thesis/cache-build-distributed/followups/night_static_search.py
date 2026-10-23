import json,os,shutil,subprocess,time
from pathlib import Path
ROOT=Path('/Users/lukaszjezapkowicz/Desktop/magisterka/praca/magisterska_jezapkowicz').resolve(); IMAGE='vepyr-cache-worker:chr1-3'; MIN_FREE=35; TARGET=5492
OUT=ROOT/'runs/night_static_search'; OUT.mkdir(exist_ok=True)
C=[('g20_a9_m2.6',{'DIST_GROUPS':'20','DIST_MAX_ACTIVE':'9','DIST_CPU':'1','DIST_MEMORY':'2.6g','DIST_ENTITY_TASKS':'0'}),('g30_a9_m2.6',{'DIST_GROUPS':'30','DIST_MAX_ACTIVE':'9','DIST_CPU':'1','DIST_MEMORY':'2.6g','DIST_ENTITY_TASKS':'0'}),('g40_a9_m2.6',{'DIST_GROUPS':'40','DIST_MAX_ACTIVE':'9','DIST_CPU':'1','DIST_MEMORY':'2.6g','DIST_ENTITY_TASKS':'0'}),('g20_a6_m4',{'DIST_GROUPS':'20','DIST_MAX_ACTIVE':'6','DIST_CPU':'1.5','DIST_MEMORY':'4g','DIST_ENTITY_TASKS':'0'}),('g20_a4_m6',{'DIST_GROUPS':'20','DIST_MAX_ACTIVE':'4','DIST_CPU':'2.25','DIST_MEMORY':'6g','DIST_ENTITY_TASKS':'0'})]
def free(): return shutil.disk_usage(ROOT).free/1024**3
def ids(): return subprocess.check_output(['docker','ps','-q','--filter',f'ancestor={IMAGE}'],text=True).split()
def kill():
 for x in ids(): subprocess.run(['docker','kill',x],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
def wait_idle():
 while ids():
  if free()<MIN_FREE: kill(); raise SystemExit('disk guard')
  time.sleep(600)
def run(name,e,rep=0):
 wait_idle(); before=set((ROOT/'runs/distributed_queue_sweep').glob('groups_*/summary.json')); t=time.time(); log=OUT/f'{name}_rep{rep}.log'; env=os.environ.copy(); env.update(e); env['PYTHONUNBUFFERED']='1'; print('START',name,rep,flush=True)
 with log.open('w') as f:
  try: p=subprocess.run(['python3','-u','runs/distributed_queue_sweep.py'],cwd=ROOT,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=4*3600)
  except subprocess.TimeoutExpired: kill(); p=None
 after=[x for x in (ROOT/'runs/distributed_queue_sweep').glob('groups_*/summary.json') if x not in before and x.stat().st_mtime>=t-1]; s=json.loads(max(after,key=lambda x:x.stat().st_mtime).read_text()) if after else None; ok=bool(s and s.get('all_success') and all(x.get('returncode')==0 for x in s.get('results',[]))); wall=s.get('wall_seconds') if s else None; row={'name':name,'rep':rep,'ok':ok,'wall':wall,'summary':s,'log':str(log)}; print('END',row,flush=True); return row
rows=[]
wait_idle()
for name,e in C:
 r=run(name,e); rows.append(r)
 if r['ok'] and r['wall'] and r['wall']<TARGET:
  for i in range(1,6): rows.append(run(name,e,i))
 (OUT/'results.json').write_text(json.dumps(rows,indent=2))
print('DONE',json.dumps(rows,indent=2),flush=True)
