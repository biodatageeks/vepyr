#!/usr/bin/env python3
import json,sys,time
from pathlib import Path
import pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq
def main():
 root=Path(sys.argv[1]); chrom=sys.argv[2]; sources=sorted((root/'out').glob('worker_*/variation/chr*.parquet')); out=root/'merged'; out.mkdir(exist_ok=True); target=out/f'chr{chrom}.parquet'; target.unlink(missing_ok=True); t=time.time(); writer=None; counts=[0,0]; metadata=pq.ParquetFile(sources[0]).schema_arrow.metadata
 for tier in (0,1):
  for source in sources:
   for batch in pq.ParquetFile(source).iter_batches(batch_size=100_000):
    col=batch.column(batch.schema.get_field_index('tier')); part=batch.filter(pc.equal(col,pa.scalar(tier,type=col.type)))
    if not part.num_rows: continue
    table=pa.Table.from_batches([part]).replace_schema_metadata(metadata)
    if writer is None: writer=pq.ParquetWriter(target,table.schema,compression='zstd',compression_level=1,use_dictionary=False,data_page_size=4096,write_statistics=True,write_page_index=True)
    writer.write_table(table,row_group_size=1_000_000); counts[tier]+=part.num_rows
 if writer: writer.close()
 result={'sources':len(sources),'rows':sum(counts),'warm_rows':counts[0],'cold_rows':counts[1],'seconds':time.time()-t,'output_bytes':target.stat().st_size}; (root/'merge_summary.json').write_text(json.dumps(result,indent=2)); print(json.dumps(result,indent=2))
if __name__=='__main__': main()
