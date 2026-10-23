#!/usr/bin/env python3
"""Full chr1-3 multi-file cache build with configurable worker pool."""
from __future__ import annotations
import concurrent.futures, json, os, shutil, subprocess, time
from pathlib import Path
from benchmark_range_shards import ROOT, prepare

IMAGE = "vepyr-cache-worker:chr1-3"
CHROMS = ["1", "2", "3"]
OTHER = ["transcript", "exon", "regulatory", "motif"]
WORKERS = int(os.environ.get("ARCH_WORKERS", "6"))

def main() -> None:
    run_dir = ROOT / f"full_queue_arch_n{WORKERS}"
    shutil.rmtree(run_dir, ignore_errors=True); run_dir.mkdir(parents=True)
    prepared = {}; prep_s = 0.0
    prep_started = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        for chrom, result in zip(CHROMS, pool.map(lambda c: prepare(c, 6), CHROMS)):
            p, _dt, ranges = result; prepared[chrom] = (p, ranges)
    prep_s = time.time() - prep_started
    tasks=[]
    for chrom in CHROMS:
        p, ranges = prepared[chrom]
        for e in ranges: tasks.append({"kind":"variation","chrom":chrom,"range":e,"prepared":str(p)})
    for chrom in CHROMS:
        for entity in OTHER: tasks.append({"kind":"entity","chrom":chrom,"entity":entity})
    pending=list(tasks); active=[]; completed=[]; started=time.time()
    while pending or active:
        while pending and len(active)<WORKERS:
            task=pending.pop(0); idx=len(completed)+len(active); out=run_dir/f"task_{idx:02d}_{task['kind']}_{task['chrom']}"; out.mkdir(parents=True)
            cmd=["docker","run","--rm","--cpus=2","--memory=6g","--memory-swap=6g","-e","CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38","-e","OUTPUT_DIR=/output","-e",f"CHROM={task['chrom']}","-e",f"ENTITY={task.get('entity','variation')}","-v",f"{out}:/output","-v",f"{Path.cwd()/'runs/point5_k8s/range_worker.py'}:/app/range_worker.py:ro"]
            if task["kind"]=="variation":
                source=Path(task["range"]["file"]).resolve(); cache=out/"cache"/"homo_sapiens_merged"/"116_GRCh38"; (cache/task["chrom"]).mkdir(parents=True); shutil.copy2(Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38/info.txt"),cache/"info.txt"); os.link(source,cache/task["chrom"]/"all_vars.gz"); cmd += ["-v",f"{out/'cache'}:/raw_cache"]
            cmd += ["--entrypoint","python",IMAGE,"/app/range_worker.py"]; (out/"command.txt").write_text(" ".join(cmd)+"\n"); log=(out/"worker.log").open("w"); proc=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT); active.append((proc,task,out,log,time.time()))
        time.sleep(1); still=[]
        for proc,task,out,log,t0 in active:
            if proc.poll() is None: still.append((proc,task,out,log,t0)); continue
            log.close()
            if proc.returncode: raise RuntimeError(f"worker failed: {task}")
            completed.append({"task":task,"seconds":time.time()-t0,"output":str(out)})
        active=still
    result={"workers":WORKERS,"ranges_per_chrom":6,"task_count":len(tasks),"preparation_seconds":prep_s,"build_wall_seconds":time.time()-started,"end_to_end_seconds":time.time()-started+prep_s,"tasks":completed}
    (run_dir/"summary.json").write_text(json.dumps(result,indent=2)); print(json.dumps({k:result[k] for k in result if k!="tasks"},indent=2))
if __name__=="__main__": main()
