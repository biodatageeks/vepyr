#!/usr/bin/env python3
"""Full-cache build with a dynamic queue of (contig-group, entity) tasks."""
from __future__ import annotations
import concurrent.futures, json, os, shutil, time
from pathlib import Path
import vepyr

DATA=Path('/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38')
RUN=Path('runs/parallel_full_cache/current')
ENTITIES=['variation','transcript','exon','regulatory','motif']
GROUPS=int(os.environ.get('PARALLEL_GROUPS','20')); PROCESSES=int(os.environ.get('PARALLEL_PROCESSES','10'))
RAM_LIMIT=28.0

def groups(contigs,n):
 b=[(0,[]) for _ in range(n)]
 for size,c in sorted((( (DATA/c/'all_vars.gz').stat().st_size,c) for c in contigs),reverse=True):
  i=min(range(n),key=lambda j:b[j][0]); b[i]=(b[i][0]+size,b[i][1]+[c])
 return [(i,sorted(g),size) for i,(size,g) in enumerate(b) if g]

def write(p,v):
 p.write_text(json.dumps(v,indent=2,default=str)+'\n')

def task(tid,gid,group,gbytes,entity):
 out=RUN/f'task_{tid:03d}'; out.mkdir(parents=True,exist_ok=True)
 state={'task':tid,'group':gid,'entity':entity,'contigs':len(group),'bytes':gbytes,'started':time.time(),'status':'running'}
 write(out/'state.json',state); os.environ['RAYON_NUM_THREADS']='1'; os.environ['MALLOC_ARENA_MAX']='1'
 t0=time.time(); stats=vepyr._core.build_cache_entity(str(DATA),str(out/'cache'),entity,partitions=1,cache_source_type='merged',overwrite=True,expected_cache_version='116',chroms=group)
 state.update(status='done',finished=time.time(),seconds=time.time()-t0,stats=stats); write(out/'state.json',state)
 return state

def rss():
 try:
  import psutil; me=psutil.Process(); return sum(p.memory_info().rss for p in [me]+me.children(recursive=True))/1024**3
 except Exception:return 0.0

def main():
 shutil.rmtree(RUN,ignore_errors=True); RUN.mkdir(parents=True)
 contigs=sorted(d.name for d in DATA.iterdir() if d.is_dir() and (d/'all_vars.gz').is_file())
 gs=groups(contigs,GROUPS); tasks=[]
 tid=0
 for gid,g,gb in gs:
  for e in ENTITIES:
   tasks.append((tid,gid,g,gb,e)); tid += 1
 total=sum(gb for _,_,_,gb,_ in tasks); write(RUN/'manifest.json',{'contigs':len(contigs),'groups':len(gs),'tasks':len(tasks),'processes':PROCESSES,'entities':ENTITIES})
 start=time.time(); done=set(); futures={}
 with concurrent.futures.ProcessPoolExecutor(max_workers=PROCESSES) as pool:
  for tid,gid,g,gb,e in tasks:futures[pool.submit(task,tid,gid,g,gb,e)]=tid
  while len(done)<len(futures):
   time.sleep(10); completed=0; active=[]
   for fut,tid in futures.items():
    p=RUN/f'task_{tid:03d}'/'state.json'
    if p.exists():
     s=json.load(open(p))
     if s.get('status')=='done': completed+=s['bytes']; done.add(tid)
    if tid not in done: active.append(tid)
   elapsed=time.time()-start; frac=completed/total if total else 1
   write(RUN/'progress.json',{'elapsed_seconds':elapsed,'completed_tasks':len(done),'total_tasks':len(tasks),'active_tasks':active,'completed_bytes':completed,'total_bytes':total,'percent':100*frac,'rss_gib':rss(),'ram_limit_gib':RAM_LIMIT})
  results=[f.result() for f in futures]
  elapsed=time.time()-start; write(RUN/'summary.json',{'seconds_wall':elapsed,'groups':len(gs),'tasks':len(tasks),'processes':PROCESSES,'results':results})
  print(json.dumps({'seconds_wall':elapsed,'groups':len(gs),'tasks':len(tasks),'processes':PROCESSES}))

if __name__=='__main__':main()
