#!/usr/bin/env python3
import json, re, shutil, subprocess, time
from pathlib import Path
ROOT=Path.cwd()/"runs/point5_k8s/point4_fragmented_repeats"; DATA=Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38"); IMAGE="vepyr-cache-worker:chr1-3"; LENGTH=50_818_468
def prepare():
 prep=ROOT/'prepared'; shutil.rmtree(prep,ignore_errors=True)
 for i in range(3):
  start=i*LENGTH//3; end=(i+1)*LENGTH//3; base=prep/f'worker_{i}'/'homo_sapiens_merged'/'116_GRCh38'; (base/'22').mkdir(parents=True); shutil.copy2(DATA/'info.txt',base/'info.txt')
  subprocess.run(['bash','-lc',f"tabix {DATA/'22'/'all_vars.gz'} 22:{start+1}-{end} | bgzip -c > {base/'22'/'all_vars.gz'}"],check=True)
 return prep
def main():
 ROOT.mkdir(parents=True,exist_ok=True); prep=prepare(); rows=[]
 for rep in range(1,4):
  out=ROOT/f'out_{rep}'; out.mkdir(); orch=ROOT/f'orchestrator_{rep}.py'; orch.write_text('''import os,subprocess,time,json\nps=[];t=time.time()\nfor i in range(3):\n e=os.environ.copy();e.update({"CACHE_ROOT":f"/raw_cache/worker_{i}/homo_sapiens_merged/116_GRCh38","OUTPUT_DIR":f"/output/worker_{i}","CHROM":"22","ENTITY":"variation"});ps.append(subprocess.Popen(["python","/app/range_worker.py"],env=e,stdout=open(f"/output/worker_{i}.log","w"),stderr=subprocess.STDOUT))\nr=[p.wait() for p in ps];print(json.dumps({"seconds":time.time()-t,"returncodes":r}));raise SystemExit(max(r))\n''')
  cmd=['docker','run','--rm','--cpus=6','--memory=8g','--memory-swap=8g','-v',f'{prep}:/raw_cache','-v',f'{out}:/output','-v',f'{Path.cwd()/"runs/point5_k8s/range_worker.py"}:/app/range_worker.py:ro','-v',f'{orch}:/app/orchestrator.py:ro','--entrypoint','python',IMAGE,'/app/orchestrator.py']; (ROOT/f'repeat_{rep}.command').write_text(' '.join(cmd)+'\n'); t=time.time(); proc=subprocess.run(cmd,capture_output=True,text=True); (ROOT/f'repeat_{rep}.container.log').write_text(proc.stdout+proc.stderr)
  logs=[]
  for i in range(3):
   txt=(out/f'worker_{i}.log').read_text(); m=re.search(r'"seconds":\s*([0-9.]+)',txt); row=re.search(r'\[\["variation", \[\[.*?, (\d+)\]',txt); logs.append({'seconds':float(m.group(1)) if m else None,'rows':int(row.group(1)) if row else None})
  result={'repeat':rep,'wall_seconds':time.time()-t,'returncode':proc.returncode,'workers':logs}; rows.append(result); (ROOT/'summary.json').write_text(json.dumps(rows,indent=2)); print(json.dumps(result),flush=True); shutil.rmtree(out); orch.unlink()
  if proc.returncode: raise SystemExit(proc.returncode)
 shutil.rmtree(prep)
if __name__=='__main__': main()
