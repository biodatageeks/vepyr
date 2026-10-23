#!/usr/bin/env python3
import concurrent.futures, json, re, shutil, subprocess, time
from pathlib import Path
ROOT=Path.cwd()/"runs/point5_k8s/point4_fragmented_chr1"; DATA=Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38"); IMAGE="vepyr-cache-worker:chr1-3"; LENGTH=248_956_422; N=6
def prepare():
 prep=ROOT/'prepared'; shutil.rmtree(prep,ignore_errors=True); t=time.time()
 def one(i):
  start=i*LENGTH//N; end=(i+1)*LENGTH//N; base=prep/f'worker_{i}'/'homo_sapiens_merged'/'116_GRCh38'; (base/'1').mkdir(parents=True); shutil.copy2(DATA/'info.txt',base/'info.txt'); subprocess.run(['bash','-lc',f"tabix {DATA/'1'/'all_vars.gz'} 1:{start+1}-{end} | bgzip -c > {base/'1'/'all_vars.gz'}"],check=True); return i
 with concurrent.futures.ThreadPoolExecutor(max_workers=N) as ex: list(ex.map(one,range(N)))
 return prep,time.time()-t
def main():
 ROOT.mkdir(parents=True,exist_ok=True); prep,prep_s=prepare(); out=ROOT/'out'; shutil.rmtree(out,ignore_errors=True); out.mkdir(); orch=ROOT/'orchestrator.py'; orch.write_text('''import os,subprocess,time,json\nps=[];t=time.time()\nfor i in range(6):\n e=os.environ.copy();e.update({"CACHE_ROOT":f"/raw_cache/worker_{i}/homo_sapiens_merged/116_GRCh38","OUTPUT_DIR":f"/output/worker_{i}","CHROM":"1","ENTITY":"variation"});ps.append(subprocess.Popen(["python","/app/range_worker.py"],env=e,stdout=open(f"/output/worker_{i}.log","w"),stderr=subprocess.STDOUT))\nr=[p.wait() for p in ps];print(json.dumps({"seconds":time.time()-t,"returncodes":r}));raise SystemExit(max(r))\n''')
 cmd=['docker','run','--rm','--cpus=6','--memory=8g','--memory-swap=8g','-v',f'{prep}:/raw_cache','-v',f'{out}:/output','-v',f'{Path.cwd()/"runs/point5_k8s/range_worker.py"}:/app/range_worker.py:ro','-v',f'{orch}:/app/orchestrator.py:ro','--entrypoint','python',IMAGE,'/app/orchestrator.py']; (ROOT/'command.txt').write_text(' '.join(cmd)+'\n'); t=time.time(); proc=subprocess.run(cmd,capture_output=True,text=True); (ROOT/'container.log').write_text(proc.stdout+proc.stderr)
 workers=[]
 for i in range(N):
  txt=(out/f'worker_{i}.log').read_text(); m=re.search(r'"seconds":\s*([0-9.]+)',txt); q=re.search(r'\[\["variation", \[\[.*?, (\d+)\]',txt); workers.append({'worker':i,'seconds':float(m.group(1)) if m else None,'rows':int(q.group(1)) if q else None})
 result={'prep_seconds':prep_s,'container_wall_seconds':time.time()-t,'returncode':proc.returncode,'workers':workers}; (ROOT/'summary.json').write_text(json.dumps(result,indent=2)); print(json.dumps(result,indent=2)); shutil.rmtree(prep)
 if proc.returncode: raise SystemExit(proc.returncode)
if __name__=='__main__': main()
