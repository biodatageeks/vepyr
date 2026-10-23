#!/usr/bin/env python3
import json, re, shutil, subprocess, time
from pathlib import Path

ROOT=Path.cwd()/"runs/point5_k8s/point4_fragmented_variation"; DATA=Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38"); IMAGE="vepyr-cache-worker:chr1-3"
LENGTH=50_818_468

def prepare(n=3):
    prep=ROOT/'prepared'
    if prep.exists(): shutil.rmtree(prep)
    for i in range(n):
        start=i*LENGTH//n
        end=(i+1)*LENGTH//n
        base=prep/f'worker_{i}'/'homo_sapiens_merged'/'116_GRCh38'
        (base/'22').mkdir(parents=True)
        shutil.copy2(DATA/'info.txt',base/'info.txt')
        target=base/'22'/'all_vars.gz'
        cmd=f"tabix {DATA/'22'/'all_vars.gz'} 22:{start+1}-{end} | bgzip -c > {target}"
        subprocess.run(['bash','-lc',cmd],check=True)
    return prep

def main():
    ROOT.mkdir(parents=True,exist_ok=True); prep=prepare(); out=ROOT/'out'; shutil.rmtree(out,ignore_errors=True); out.mkdir()
    orch=ROOT/'orchestrator.py'
    orch.write_text('''import os, subprocess, time, json\nprocs=[]\nt0=time.time()\nfor i in range(3):\n    env=os.environ.copy(); env.update({"CACHE_ROOT":f"/raw_cache/worker_{i}/homo_sapiens_merged/116_GRCh38", "OUTPUT_DIR":f"/output/worker_{i}", "CHROM":"22", "ENTITY":"variation"})\n    procs.append(subprocess.Popen(["python","/app/range_worker.py"], env=env, stdout=open(f"/output/worker_{i}.log","w"), stderr=subprocess.STDOUT))\nrc=[p.wait() for p in procs]\nprint(json.dumps({"seconds":time.time()-t0,"returncodes":rc}), flush=True)\nraise SystemExit(max(rc))\n''')
    cmd=['docker','run','--rm','--cpus=6','--memory=8g','--memory-swap=8g','-v',f'{prep}:/raw_cache','-v',f'{out}:/output','-v',f'{Path.cwd()/"runs/point5_k8s/range_worker.py"}:/app/range_worker.py:ro','-v',f'{orch}:/app/orchestrator.py:ro','--entrypoint','python',IMAGE,'/app/orchestrator.py']
    (ROOT/'command.txt').write_text(' '.join(cmd)+'\n'); t=time.time()
    proc=subprocess.run(cmd,capture_output=True,text=True); (ROOT/'container.log').write_text(proc.stdout+proc.stderr)
    rows=[]
    for i in range(3):
        log=(out/f'worker_{i}.log').read_text()
        m=re.search(r'"seconds":\s*([0-9.]+)',log)
        rows.append({'worker':i,'seconds':float(m.group(1)) if m else None,'log':str(out/f'worker_{i}.log')})
    summary={'ranges':3,'wall_seconds':time.time()-t,'returncode':proc.returncode,'workers':rows}
    (ROOT/'summary.json').write_text(json.dumps(summary,indent=2)); print(json.dumps(summary,indent=2))
    shutil.rmtree(prep)
    if proc.returncode: raise SystemExit(proc.returncode)
if __name__=='__main__': main()
