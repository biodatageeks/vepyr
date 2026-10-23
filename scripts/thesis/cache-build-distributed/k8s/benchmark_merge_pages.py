#!/usr/bin/env python3
import json, shutil, sys, time
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

def merge(root: Path, page_size: int) -> dict:
    sources = sorted((root / "out").glob("worker_*/variation/chr*.parquet"))
    out = root / f"merged_page_{page_size}"
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir()
    target = out / "chr1.parquet"
    metadata = pq.ParquetFile(sources[0]).schema_arrow.metadata
    counts = [0, 0]
    writer = None
    started = time.time()
    for tier in (0, 1):
        for source in sources:
            for batch in pq.ParquetFile(source).iter_batches(batch_size=100_000):
                idx = batch.schema.get_field_index("tier")
                part = batch.filter(pc.equal(batch.column(idx), pa.scalar(tier, type=batch.column(idx).type)))
                if not part.num_rows:
                    continue
                table = pa.Table.from_batches([part]).replace_schema_metadata(metadata)
                if writer is None:
                    writer = pq.ParquetWriter(
                        target, table.schema, compression="zstd", compression_level=1,
                        use_dictionary=False, data_page_size=page_size,
                        write_statistics=True, write_page_index=True,
                    )
                writer.write_table(table, row_group_size=1_000_000)
                counts[tier] += part.num_rows
    if writer is not None:
        writer.close()
    result = {
        "page_size": page_size,
        "seconds": time.time() - started,
        "rows": sum(counts),
        "warm_rows": counts[0],
        "cold_rows": counts[1],
        "output_bytes": target.stat().st_size,
    }
    (root / f"merge_page_{page_size}.json").write_text(json.dumps(result, indent=2))
    return result

def main() -> None:
    root = Path(sys.argv[1])
    results = [merge(root, int(value)) for value in sys.argv[2:]]
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
