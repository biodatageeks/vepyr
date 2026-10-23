#!/usr/bin/env python3
import json, time
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT=Path.cwd()/"runs/point5_k8s/point4_fragmented_variation"; OUT=ROOT/'merged'; OUT.mkdir(exist_ok=True)
def main():
    sources=sorted(ROOT.glob('out/worker_*/variation/chr22.parquet'))
    target=OUT/'chr22.parquet'; target.unlink(missing_ok=True); t=time.time(); writer=None; warm_rows=cold_rows=0
    source_metadata = pq.ParquetFile(sources[0]).schema_arrow.metadata
    for tier_value in (0,1):
        for source in sources:
            pf=pq.ParquetFile(source)
            for batch in pf.iter_batches(batch_size=100_000):
                idx=batch.schema.get_field_index('tier'); mask=pc.equal(batch.column(idx),pa.scalar(tier_value, type=batch.column(idx).type)); part=batch.filter(mask)
                if not part.num_rows: continue
                table=pa.Table.from_batches([part]).replace_schema_metadata(source_metadata)
                if writer is None: writer=pq.ParquetWriter(target,table.schema,compression='zstd',compression_level=1,use_dictionary=False,data_page_size=4096,write_statistics=True,write_page_index=True)
                writer.write_table(table,row_group_size=1_000_000)
                if tier_value==0: warm_rows+=part.num_rows
                else: cold_rows+=part.num_rows
    if writer: writer.close()
    result={'sources':len(sources),'warm_rows':warm_rows,'cold_rows':cold_rows,'rows':warm_rows+cold_rows,'seconds':time.time()-t,'output_bytes':target.stat().st_size}
    (ROOT/'merged_summary.json').write_text(json.dumps(result,indent=2)); print(json.dumps(result,indent=2))
if __name__=='__main__': main()
