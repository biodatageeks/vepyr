"""Slice the VEP 116 chr21 cache/FASTA used by porting issue 221.

Run with the repository Python environment and DATA_VEPYR_DIR pointing at the
full local caches. The checked-in VCF oracles come from the pinned Docker image
recorded in provenance.json, not from vepyr.
"""

import json
import os
from pathlib import Path
import subprocess

from datafusion import SessionContext
import pyarrow.parquet as pq


HERE = Path(__file__).resolve().parent
DATA = Path(os.environ["DATA_VEPYR_DIR"])
SOURCE = DATA / "cache/116_GRCh38_ensembl"
OPTIONS = (
    "('compression' 'zstd(3)', 'dictionary_enabled' 'false', "
    "'statistics_enabled' 'page', 'data_pagesize_limit' '4096', "
    "'data_page_row_count_limit' '512', 'skip_arrow_metadata' 'false')"
)


def main():
    ctx = SessionContext()
    transcript = pq.read_table(
        SOURCE / "transcript/chr21.parquet",
        filters=[("start", "<=", 25592761), ("end", ">=", 25582759)],
    )
    assert transcript.num_rows == 34
    ids = transcript["stable_id"].to_pylist()
    tables = {"transcript": transcript}
    for entity in ("exon", "translation_core"):
        tables[entity] = pq.read_table(
            SOURCE / entity / "chr21.parquet",
            filters=[("transcript_id", "in", ids)],
        )
    tables["variation"] = pq.read_table(
        SOURCE / "variation/chr21.parquet",
        filters=[("start", ">=", 25582759), ("start", "<=", 25592761)],
    ).sort_by([("tier", "ascending"), ("start", "ascending")])
    uid_filters = [
        [("key", ">=", uid << 32), ("key", "<", (uid + 1) << 32)]
        for uid in transcript["transcript_uid"].to_pylist()
    ]
    tables["translation_sift"] = pq.read_table(
        SOURCE / "translation_sift/chr21.parquet", filters=uid_filters
    ).sort_by("key")
    for entity in ("regulatory", "motif"):
        tables[entity] = pq.read_table(
            SOURCE / entity / "chr21.parquet",
            filters=[("start", "<=", 25587761), ("end", ">=", 25587759)],
        )
        assert tables[entity].num_rows == 0
    assert tables["exon"].num_rows == 319
    assert tables["translation_core"].num_rows == 33
    assert set(tables["exon"]["transcript_id"].to_pylist()) <= set(ids)

    for entity, table in tables.items():
        destination = HERE / "cache" / entity
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / "chr21.parquet"
        metadata = dict(table.schema.metadata or {})
        metadata[b"bio.vep.cache_source_type"] = b"ensembl"
        metadata[b"bio.vep.cache_version"] = b"116"
        table = table.replace_schema_metadata(metadata)
        path.unlink(missing_ok=True)
        if table.num_rows:
            ctx.register_record_batches("shard", [table.combine_chunks().to_batches()])
            ctx.sql(
                f"COPY shard TO '{path}' STORED AS PARQUET OPTIONS {OPTIONS}"
            ).collect()
            ctx.deregister_table("shard")
        else:
            pq.write_table(table, path)
        (destination / "chrom_manifest.json").write_text(
            json.dumps(
                [
                    {
                        "chrom": "chr21",
                        "dataset": "chr21.parquet",
                        "rows": table.num_rows,
                    }
                ],
                indent=2,
            )
            + "\n"
        )
        print(entity, table.num_rows, path.stat().st_size, flush=True)

    start, end, length = 25567000, 25613000, 46709983
    fasta = DATA / "input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
    region = subprocess.check_output(
        ["samtools", "faidx", str(fasta), f"21:{start}-{end}"], text=True
    )
    sequence = "".join(region.splitlines()[1:])
    assert len(sequence) == end - start + 1
    sequence = "N" * (start - 1) + sequence + "N" * (length - end)
    compressed = HERE / "reference.fa.gz"
    with compressed.open("wb") as output:
        process = subprocess.Popen(
            ["bgzip", "-c"], stdin=subprocess.PIPE, stdout=output
        )
        process.communicate(
            (
                ">21\n"
                + "\n".join(sequence[i : i + 60] for i in range(0, len(sequence), 60))
                + "\n"
            ).encode()
        )
        assert process.returncode == 0
    subprocess.run(["samtools", "faidx", str(compressed)], check=True)
    for name in ("input", "control"):
        with (HERE / f"{name}.vcf.gz").open("wb") as output:
            subprocess.run(
                ["bgzip", "-c", str(HERE / f"{name}.vcf")], stdout=output, check=True
            )
        subprocess.run(
            ["tabix", "-f", "-p", "vcf", str(HERE / f"{name}.vcf.gz")], check=True
        )


if __name__ == "__main__":
    main()
