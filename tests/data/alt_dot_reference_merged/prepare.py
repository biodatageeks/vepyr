"""Slice genuine merged VEP 116 chr21 data for vepyr issue 151.

Run with the repository Python environment and DATA_VEPYR_DIR pointing at the
full local caches. The checked-in VCF oracles come from the pinned Docker image
recorded in provenance.json, not from vepyr. Requires datafusion and pyarrow
Python packages and samtools, bgzip and tabix on PATH.
"""

import json
import os
from pathlib import Path
import subprocess

from datafusion import SessionContext
import pyarrow.parquet as pq


HERE = Path(__file__).resolve().parent
LOCUS = 25587759
LAST_LOCUS = 25587762
WINDOW_START, WINDOW_END = LOCUS - 5000, LAST_LOCUS + 5000
FASTA_START, FASTA_END, CHROMOSOME_LENGTH = 25567000, 25613000, 46709983
OPTIONS = (
    "('compression' 'zstd(3)', 'dictionary_enabled' 'false', "
    "'statistics_enabled' 'page', 'data_pagesize_limit' '4096', "
    "'data_page_row_count_limit' '512', 'skip_arrow_metadata' 'false')"
)


def main():
    data_dir = os.environ.get("DATA_VEPYR_DIR")
    if not data_dir:
        raise SystemExit(
            "Set DATA_VEPYR_DIR to the full release-116 cache/FASTA directory"
        )
    data = Path(data_dir)
    source = data / "cache/116_GRCh38_merged"
    ctx = SessionContext()
    transcript = pq.read_table(
        source / "transcript/chr21.parquet",
        filters=[("start", "<=", WINDOW_END), ("end", ">=", WINDOW_START)],
    )
    ids = transcript["stable_id"].to_pylist()
    tables = {"transcript": transcript}
    for entity in ("exon", "translation_core"):
        tables[entity] = pq.read_table(
            source / entity / "chr21.parquet",
            filters=[("transcript_id", "in", ids)],
        )
    tables["variation"] = pq.read_table(
        source / "variation/chr21.parquet",
        filters=[("start", ">=", WINDOW_START), ("start", "<=", WINDOW_END)],
    ).sort_by([("tier", "ascending"), ("start", "ascending")])
    uid_filters = [
        [("key", ">=", uid << 32), ("key", "<", (uid + 1) << 32)]
        for uid in transcript["transcript_uid"].to_pylist()
    ]
    tables["translation_sift"] = pq.read_table(
        source / "translation_sift/chr21.parquet", filters=uid_filters
    ).sort_by("key")
    for entity in ("regulatory", "motif"):
        tables[entity] = pq.read_table(
            source / entity / "chr21.parquet",
            filters=[("start", "<=", LAST_LOCUS), ("end", ">=", LOCUS)],
        )
    assert set(tables["exon"]["transcript_id"].to_pylist()) <= set(ids)

    for entity, table in tables.items():
        destination = HERE / "cache" / entity
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / "chr21.parquet"
        metadata = dict(table.schema.metadata or {})
        assert metadata[b"bio.vep.cache_source_type"] == b"merged"
        assert metadata[b"bio.vep.cache_version"] == b"116"
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

    native_info = data / "homo_sapiens_merged/116_GRCh38/info.txt"
    bam = ""
    for line in native_info.read_bytes().decode().split("\n"):
        fields = line.split("\t")
        # VEP keeps the last declaration, except the literal skip marker "-".
        if fields[0] == "bam":
            value = fields[1] if len(fields) > 1 else ""
            if value != "-":
                bam = value
    assert bam not in ("", "0"), "fixture requires the native BAM-edited merged cache"
    (HERE / "cache/reference_policy.json").write_text(
        json.dumps(
            dict(
                schema_version=1,
                cache_source_type="merged",
                cache_version="116",
                bam_edited=True,
            ),
            indent=2,
        )
        + "\n"
    )

    start, end, length = FASTA_START, FASTA_END, CHROMOSOME_LENGTH
    fasta = data / "input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
    region = subprocess.check_output(
        ["samtools", "faidx", str(fasta), f"21:{start}-{end}"], text=True
    )
    sequence = "".join(region.splitlines()[1:])
    assert len(sequence) == end - start + 1
    sequence = "N" * (start - 1) + sequence + "N" * (length - end)
    compressed = HERE / "reference.fa.gz"
    with compressed.open("wb") as output:
        subprocess.run(
            ["bgzip", "-c"],
            stdout=output,
            check=True,
            input=(
                ">21\n"
                + "\n".join(sequence[i : i + 60] for i in range(0, len(sequence), 60))
                + "\n"
            ).encode(),
        )
    subprocess.run(["samtools", "faidx", str(compressed)], check=True)
    for name in ("trailing", "leading", "control"):
        with (HERE / f"{name}.vcf.gz").open("wb") as output:
            subprocess.run(
                ["bgzip", "-c", str(HERE / f"{name}.vcf")], stdout=output, check=True
            )
        subprocess.run(
            ["tabix", "-f", "-p", "vcf", str(HERE / f"{name}.vcf.gz")], check=True
        )


if __name__ == "__main__":
    main()
