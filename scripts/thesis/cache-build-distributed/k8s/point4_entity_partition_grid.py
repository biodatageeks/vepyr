#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path

ROOT=Path.cwd()/"runs/point5_k8s/point4_entity_partition_grid"; IMAGE="vepyr-cache-worker:chr1-3"
def main():
 ROOT.mkdir(parents=True,exist_ok=True); rows=[]
 for p in (1,2,3,4):
  for rep in range(1,4):
   vol=f"vepyr-point4-c2-p{p}-r{rep}"; subprocess.run(["docker","volume","create",vol],check=True,stdout=subprocess.DEVNULL)
   env=["WORKER_COUNT=1","WORKER_INDEX=0","CHROMS=22",f"VEPYR_PARTITIONS={p}",f"VEPYR_VAR_PARTITIONS={p}","VEPYR_ENTITY_CONCURRENCY=2","CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38","OUTPUT_DIR=/output"]
   cmd=["docker","run","--rm","--cpus=6","--memory=24g","--memory-swap=24g"]
   for e in env: cmd += ["-e",e]
   cmd += ["-v",f"{vol}:/output","-v",f"{Path.cwd() / 'runs/point5_k8s/worker.py'}:/app/worker.py:ro",IMAGE]
   pre=ROOT/f"p{p}_r{rep}"; pre.with_suffix('.command').write_text(' '.join(cmd)+'\n'); t=time.time()
   with pre.with_suffix('.log').open('w') as fh: proc=subprocess.run(cmd,stdout=fh,stderr=subprocess.STDOUT)
   row={"entity_concurrency":2,"partitions":p,"repeat":rep,"seconds_wall":time.time()-t,"returncode":proc.returncode}; rows.append(row); (ROOT/'summary.json').write_text(json.dumps(rows,indent=2)); print(json.dumps(row),flush=True)
   subprocess.run(["docker","volume","rm",vol],check=True,stdout=subprocess.DEVNULL)
   if proc.returncode: raise SystemExit(proc.returncode)
if __name__=='__main__': main()
