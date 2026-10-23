#!/usr/bin/env python3
from __future__ import annotations
import json, os, shutil, subprocess, time
from pathlib import Path

DATA=Path('/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38')
IMAGE='vepyr-cache-worker:chr1-3'; ROOT=Path('runs/persistent_dist_queue')
ENTITIES=['variation','transcript','exon','regulatory','motif']
GROUPS=int(os.environ.get('DIST_GROUPS','20')); PODS=int(os.environ.get('DIST_PODS','5'))
CPU=os.environ.get('DIST_CPU','2'); MEMORY=os.environ.get('DIST_MEMORY','10g'); MIN_FREE=35.0

def free(): return shutil.disk_usage(Path.cwd()).free/1024**3
def contigs(): return sorted(d.name for d in DATA.iterdir() if (d/'all_vars.gz').is_file())
def make_groups(cs):
    bins=[(0,[]) for _ in range(GROUPS)]; fixed=32*1024*1024
    for c in sorted(cs,key=lambda x:(DATA/x/'all_vars.gz').stat().st_size,reverse=True):
        s=(DATA/c/'all_vars.gz').stat().st_size+fixed; i=min(range(GROUPS),key=lambda j:bins[j][0])
        bins[i]=(bins[i][0]+s,bins[i][1]+[c])
    return [sorted(g) for _,g in bins]
def main():
    run=ROOT/f'run_{int(time.time())}'; (run/'tasks/claims').mkdir(parents=True); (run/'tasks/results').mkdir()
    gs=make_groups(contigs()); tasks=[]; tid=0
    # Schedule dominant entity first so all pods begin useful variation work.
    for e in ENTITIES:
        for gi,g in enumerate(gs):
            tasks.append({'id':tid,'group':gi,'entity':e,'chroms':g}); tid+=1
    (run/'tasks/tasks.json').write_text(json.dumps(tasks)); (run/'manifest.json').write_text(json.dumps({'groups':len(gs),'tasks':len(tasks),'pods':PODS,'cpu':CPU,'memory':MEMORY},indent=2))
    run=run.resolve(); out=run/'output'; out.mkdir(); agents=run/'persistent_dist_agent.py'; agents.write_text(Path('runs/persistent_dist_agent.py').read_text())
    cmds=[]; start=time.time()
    for i in range(PODS):
        pod_out=run/'output'/f'pod_{i}'; pod_out.mkdir(parents=True)
        cmd=['docker','run','--rm','--cpus',CPU,'--memory',MEMORY,'--memory-swap',MEMORY,
             '-e','MALLOC_ARENA_MAX=1','-e','RAYON_NUM_THREADS=1','-v',f'{DATA}:/raw_cache/homo_sapiens_merged/116_GRCh38:ro',
             '-v',f'{run}/tasks:/tasks','-v',str(agents)+':/app/agent.py:ro','-v',f'{pod_out}:/output',
             '--entrypoint','python',IMAGE,'/app/agent.py']
        (run/f'pod_{i}.command').write_text(' '.join(cmd)+'\n'); log=(run/f'pod_{i}.log').open('w'); log.write('COMMAND: '+' '.join(cmd)+'\n'); log.flush()
        cmds.append((i,None,subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT),log))
    while True:
        states=[]
        for i,vol,p,log in cmds:
            states.append(p.poll())
        done=sum(1 for p in (run/'tasks/results').glob('*.json'))
        (run/'progress.json').write_text(json.dumps({'done':done,'total':len(tasks),'elapsed':time.time()-start,'free_gib':free()},indent=2))
        if free()<MIN_FREE: raise RuntimeError('disk guard')
        if all(x is not None for x in states): break
        time.sleep(10)
    codes=[]
    for i,vol,p,log in cmds:
        codes.append(p.wait()); log.close()
    summary={'wall_seconds':time.time()-start,'tasks':len(tasks),'pods':PODS,'returncodes':codes,'done':len(list((run/'tasks/results').glob('*.json'))),'run':str(run)}
    (run/'summary.json').write_text(json.dumps(summary,indent=2)+'\n'); print(json.dumps(summary),flush=True)
if __name__=='__main__': main()
