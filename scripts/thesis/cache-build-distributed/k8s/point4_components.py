#!/usr/bin/env python3
import json
import subprocess
import time
from pathlib import Path

ROOT = Path.cwd() / "runs/point5_k8s/point4_components"
IMAGE = "vepyr-cache-worker:chr1-3"

def run_case(name, env_extra, reps=3):
    rows=[]
    for rep in range(1,reps+1):
        volume=f"vepyr-point4-{name}-{rep}"
        subprocess.run(["docker","volume","create",volume],check=True,stdout=subprocess.DEVNULL)
        env=["WORKER_COUNT=1","WORKER_INDEX=0","CHROMS=22","VEPYR_PARTITIONS=4","VEPYR_VAR_PARTITIONS=4","CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38","OUTPUT_DIR=/output"]+env_extra
        cmd=["docker","run","--rm","--cpus=6","--memory=24g","--memory-swap=24g"]
        for item in env: cmd += ["-e",item]
        cmd += ["-v",f"{volume}:/output","-v",f"{Path.cwd() / 'runs/point5_k8s/worker.py'}:/app/worker.py:ro",IMAGE]
        prefix=ROOT/f"{name}_r{rep}"
        prefix.with_suffix('.command').write_text(' '.join(cmd)+'\n')
        t0=time.time()
        with prefix.with_suffix('.log').open('w') as fh: proc=subprocess.run(cmd,stdout=fh,stderr=subprocess.STDOUT)
        row={"case":name,"repeat":rep,"seconds_wall":time.time()-t0,"returncode":proc.returncode}
        rows.append(row); (ROOT/'summary.json').write_text(json.dumps(rows,indent=2)); print(json.dumps(row),flush=True)
        subprocess.run(["docker","volume","rm",volume],check=True,stdout=subprocess.DEVNULL)
        if proc.returncode: raise SystemExit(proc.returncode)

def main():
    ROOT.mkdir(parents=True,exist_ok=True)
    run_case('variation_native',["VEPYR_ENTITIES=variation"])
    run_case('variation_datafusion',["VEPYR_ENTITIES=variation","VEPYR_USE_DATAFUSION_VARIATION=1"])
    run_case('other_entities',["VEPYR_ENTITIES=transcript,exon,regulatory,motif"])

if __name__=='__main__': main()
