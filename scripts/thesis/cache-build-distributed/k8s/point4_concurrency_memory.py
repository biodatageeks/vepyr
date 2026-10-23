#!/usr/bin/env python3
import json, re, subprocess, threading, time
from pathlib import Path

ROOT=Path.cwd()/"runs/point5_k8s/point4_concurrency_memory"; IMAGE="vepyr-cache-worker:chr1-3"
def one(c, rows):
 vol=f"vepyr-point4-mem-c{c}"; subprocess.run(["docker","volume","create",vol],check=True,stdout=subprocess.DEVNULL)
 env=["WORKER_COUNT=1","WORKER_INDEX=0","CHROMS=22","VEPYR_PARTITIONS=1","VEPYR_VAR_PARTITIONS=1",f"VEPYR_ENTITY_CONCURRENCY={c}","CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38","OUTPUT_DIR=/output"]
 cmd=["docker","run","--rm","--cpus=6","--memory=24g","--memory-swap=24g"]
 for e in env: cmd += ["-e",e]
 cmd += ["-v",f"{vol}:/output","-v",f"{Path.cwd() / 'runs/point5_k8s/worker.py'}:/app/worker.py:ro",IMAGE]
 pre=ROOT/f"c{c}"; pre.with_suffix('.command').write_text(' '.join(cmd)+'\n'); samples=[]; t=time.time()
 proc=subprocess.Popen(cmd,stdout=pre.with_suffix('.log').open('w'),stderr=subprocess.STDOUT)
 while proc.poll() is None:
  ps=subprocess.run(["docker","ps","-q","--filter",f"ancestor={IMAGE}"],capture_output=True,text=True).stdout.strip().splitlines()
  if ps:
   out=subprocess.run(["docker","stats","--no-stream","--format","{{.MemUsage}}",ps[0]],capture_output=True,text=True).stdout.strip()
   m=re.match(r"([0-9.]+)([MG])iB",out)
   if m: samples.append(float(m.group(1))*(1024 if m.group(2)=='G' else 1))
  time.sleep(1)
 rc=proc.wait(); row={"entity_concurrency":c,"partitions":1,"seconds_wall":time.time()-t,"returncode":rc,"peak_mib":max(samples) if samples else None}
 rows.append(row); (ROOT/'summary.json').write_text(json.dumps(rows,indent=2)); subprocess.run(["docker","volume","rm",vol],check=True,stdout=subprocess.DEVNULL); print(json.dumps(row),flush=True)
 if rc: raise SystemExit(rc)
def main():
 ROOT.mkdir(parents=True,exist_ok=True)
 rows=[]
 for c in (3,4): one(c, rows)
if __name__=='__main__': main()
