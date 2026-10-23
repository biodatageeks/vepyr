#!/usr/bin/env python3
import json, re, shutil, subprocess, time
from pathlib import Path
ROOT=Path.cwd()/"runs/point5_k8s/point4_fragmented_full_chr22"; DATA=Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38"); IMAGE="vepyr-cache-worker:chr1-3"; LENGTH=50_818_468
def main():
 ROOT.mkdir(parents=True,exist_ok=True); prep=ROOT/'prepared'; shutil.rmtree(prep,ignore_errors=True)
 for i in range(3):
  start=i*LENGTH//3; end=(i+1)*LENGTH//3; base=prep/f'worker_{i}'/'homo_sapiens_merged'/'116_GRCh38'; (base/'22').mkdir(parents=True); shutil.copy2(DATA/'info.txt',base/'info.txt'); subprocess.run(['bash','-lc',f"tabix {DATA/'22'/'all_vars.gz'} 22:{start+1}-{end} | bgzip -c > {base/'22'/'all_vars.gz'}"],check=True)
 out=ROOT/'out'; shutil.rmtree(out,ignore_errors=True); out.mkdir(); orch=ROOT/'orchestrator.py'; orch.write_text('''import os,subprocess,time,json\nps=[];t=time.time()\nfor i in range(3):\n e=os.environ.copy();e.update({"CACHE_ROOT":f"/raw_cache/worker_{i}/homo_sapiens_merged/116_GRCh38","OUTPUT_DIR":f"/output/worker_{i}","CHROM":"22","ENTITY":"variation"});ps.append(subprocess.Popen(["python","/app/range_worker.py"],env=e,stdout=open(f"/output/worker_{i}.log","w"),stderr=subprocess.STDOUT))\ne=os.environ.copy();e.update({"CACHE_ROOT":"/full_cache_parent/116_GRCh38","OUTPUT_DIR":"/output/other","CHROMS":"22","WORKER_COUNT":"1","WORKER_INDEX":"0","VEPYR_ENTITIES":"transcript,exon,regulatory,motif","VEPYR_ENTITY_CONCURRENCY":"3","VEPYR_PARTITIONS":"1","VEPYR_VAR_PARTITIONS":"1"});ps.append(subprocess.Popen(["python","/app/worker.py"],env=e,stdout=open("/output/other.log","w"),stderr=subprocess.STDOUT))\nr=[p.wait() for p in ps];print(json.dumps({"seconds":time.time()-t,"returncodes":r}));raise SystemExit(max(r))\n''')
 cmd=['docker','run','--rm','--cpus=6','--memory=8g','--memory-swap=8g','-v',f'{prep}:/raw_cache','-v',f'{DATA.parent}:/full_cache_parent:ro','-v',f'{out}:/output','-v',f'{Path.cwd()/"runs/point5_k8s/range_worker.py"}:/app/range_worker.py:ro','-v',f'{Path.cwd()/"runs/point5_k8s/worker.py"}:/app/worker.py:ro','-v',f'{orch}:/app/orchestrator.py:ro','--entrypoint','python',IMAGE,'/app/orchestrator.py']; (ROOT/'command.txt').write_text(' '.join(cmd)+'\n'); t=time.time(); proc=subprocess.run(cmd,capture_output=True,text=True); (ROOT/'container.log').write_text(proc.stdout+proc.stderr)
 rows=[]
 for i in range(3):
  txt=(out/f'worker_{i}.log').read_text(); m=re.search(r'"seconds":\s*([0-9.]+)',txt); q=re.search(r'\[\["variation", \[\[.*?, (\d+)\]',txt); rows.append({'worker':i,'seconds':float(m.group(1)) if m else None,'rows':int(q.group(1)) if q else None})
 result={'container_wall_seconds':time.time()-t,'returncode':proc.returncode,'variation_workers':rows,'other_log':str(out/'other.log')}; (ROOT/'summary.json').write_text(json.dumps(result,indent=2)); print(json.dumps(result,indent=2)); shutil.rmtree(prep)
 if proc.returncode: raise SystemExit(proc.returncode)
if __name__=='__main__': main()
