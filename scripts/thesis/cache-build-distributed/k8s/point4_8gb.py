#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path
ROOT=Path.cwd()/"runs/point5_k8s/point4_8gb"; IMAGE="vepyr-cache-worker:chr1-3"
def main():
 ROOT.mkdir(parents=True,exist_ok=True); vol="vepyr-point4-8gb"; subprocess.run(["docker","volume","create",vol],check=True,stdout=subprocess.DEVNULL)
 env=["WORKER_COUNT=1","WORKER_INDEX=0","CHROMS=22","VEPYR_PARTITIONS=1","VEPYR_VAR_PARTITIONS=1","VEPYR_ENTITY_CONCURRENCY=3","CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38","OUTPUT_DIR=/output"]
 cmd=["docker","run","--rm","--cpus=6","--memory=8g","--memory-swap=8g"]
 for e in env: cmd += ["-e",e]
 cmd += ["-v",f"{vol}:/output","-v",f"{Path.cwd() / 'runs/point5_k8s/worker.py'}:/app/worker.py:ro",IMAGE]
 (ROOT/'command.txt').write_text(' '.join(cmd)+'\n'); t=time.time()
 with (ROOT/'run.log').open('w') as fh: proc=subprocess.run(cmd,stdout=fh,stderr=subprocess.STDOUT)
 row={"memory_limit":"8g","entity_concurrency":3,"partitions":1,"seconds_wall":time.time()-t,"returncode":proc.returncode}; (ROOT/'summary.json').write_text(json.dumps(row,indent=2)); print(json.dumps(row))
 subprocess.run(["docker","volume","rm",vol],check=True,stdout=subprocess.DEVNULL)
 if proc.returncode: raise SystemExit(proc.returncode)
if __name__=='__main__': main()
