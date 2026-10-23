#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path
ROOT=Path.cwd()/"runs/point5_k8s/point4_c3_repeats"; IMAGE="vepyr-cache-worker:chr1-3"
def main():
 ROOT.mkdir(parents=True,exist_ok=True); rows=[]
 for rep in range(1,4):
  vol=f"vepyr-point4-c3-r{rep}"; subprocess.run(["docker","volume","create",vol],check=True,stdout=subprocess.DEVNULL)
  env=["WORKER_COUNT=1","WORKER_INDEX=0","CHROMS=22","VEPYR_PARTITIONS=1","VEPYR_VAR_PARTITIONS=1","VEPYR_ENTITY_CONCURRENCY=3","CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38","OUTPUT_DIR=/output"]
  cmd=["docker","run","--rm","--cpus=6","--memory=24g","--memory-swap=24g"]
  for e in env: cmd += ["-e",e]
  cmd += ["-v",f"{vol}:/output","-v",f"{Path.cwd() / 'runs/point5_k8s/worker.py'}:/app/worker.py:ro",IMAGE]
  pre=ROOT/f"r{rep}"; pre.with_suffix('.command').write_text(' '.join(cmd)+'\n'); t=time.time()
  with pre.with_suffix('.log').open('w') as fh: proc=subprocess.run(cmd,stdout=fh,stderr=subprocess.STDOUT)
  row={"entity_concurrency":3,"partitions":1,"repeat":rep,"seconds_wall":time.time()-t,"returncode":proc.returncode}; rows.append(row); (ROOT/'summary.json').write_text(json.dumps(rows,indent=2)); print(json.dumps(row),flush=True)
  subprocess.run(["docker","volume","rm",vol],check=True,stdout=subprocess.DEVNULL)
  if proc.returncode: raise SystemExit(proc.returncode)
if __name__=='__main__': main()
