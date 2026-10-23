import concurrent.futures,json,os,shutil,subprocess,time
from pathlib import Path
ROOT=Path('runs/translation_source_shard_repeats'); STAGE=Path('runs/translation_source_shards_chr1'); IMAGE='vepyr-cache-worker:rangefilter'
def one(rep,i):
 out=(ROOT/f'repeat{rep}'/f'shard{i}').resolve(); out.mkdir(parents=True,exist_ok=True); source=(STAGE/f'shard{i}'/'homo_sapiens_merged'/'116_GRCh38').resolve()
 cmd=['docker','run','--rm','--cpus','4.5','--memory','12g','--memory-swap','12g','-e','WORKER_COUNT=1','-e','WORKER_INDEX=0','-e','CHROMS=1','-e','VEPYR_ENTITIES=translation','-e','CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38','-e','OUTPUT_DIR=/output','-e','MALLOC_ARENA_MAX=1','-e','RAYON_NUM_THREADS=1','-e','VEPYR_ENTITY_CONCURRENCY=1','-e','VEPYR_PARTITIONS=1','-e','VEPYR_VAR_PARTITIONS=1','-e','VEPYR_DISABLE_WARM=1','-v',f'{source}:/raw_cache/homo_sapiens_merged/116_GRCh38:ro','-v',f'{out}:/output',IMAGE]
 (out/'command.txt').write_text(' '.join(cmd)+'\n'); t=time.time()
 with (out/'run.log').open('w') as fh: rc=subprocess.run(cmd,stdout=fh,stderr=subprocess.STDOUT).returncode
 rows={}
 try:
  import sys; sys.path.insert(0,'/Users/lukaszjezapkowicz/workspace/data_vepyr/.venv-lpt/lib/python3.14/site-packages')
  import pyarrow.parquet as pq
  for e in ('translation_core','translation_sift'):
   rows[e]=sum(pq.ParquetFile(f).metadata.num_rows for f in (out/e).rglob('*.parquet')) if (out/e).exists() else 0
 except Exception as e: rows['error']=str(e)
 r={'repeat':rep,'shard':i,'returncode':rc,'seconds':time.time()-t,'rows':rows}; (out/'summary.json').write_text(json.dumps(r,indent=2)+'\n')
 for e in ('translation_core','translation_sift'): shutil.rmtree(out/e,ignore_errors=True)
 return r
allr=[]
for rep in range(1,6):
 with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex: rs=list(ex.map(lambda i:one(rep,i),range(6)))
 allr+=rs; (ROOT/f'repeat{rep}.json').write_text(json.dumps(rs,indent=2)+'\n')
 print(json.dumps({'repeat':rep,'wall':max(r['seconds'] for r in rs),'codes':[r['returncode'] for r in rs],'rows':{e:sum(r['rows'].get(e,0) for r in rs) for e in ('translation_core','translation_sift')}}),flush=True)
 if any(r['returncode']!=0 for r in rs): raise SystemExit(1)
summary={'repeats':5,'shards':6,'max_active':2,'walls':[max(r['seconds'] for r in allr if r['repeat']==rep) for rep in range(1,6)],'all_success':all(r['returncode']==0 for r in allr)}; summary['mean_wall']=sum(summary['walls'])/5; (ROOT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n'); print(json.dumps(summary),flush=True)
