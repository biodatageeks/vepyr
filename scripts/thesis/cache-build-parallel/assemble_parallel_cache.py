#!/usr/bin/env python3
"""Assemble worker-local cache shards into one VEPYR-compatible cache root."""
from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

RUN = Path("runs/parallel_full_cache/current")
OUT = Path("runs/parallel_full_cache/assembled/116_GRCh38_merged")
SOURCE_INFO = Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38/info.txt")
ENTITIES = ["variation", "transcript", "exon", "regulatory", "motif"]


def main() -> None:
    started = time.perf_counter()
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    manifests: dict[str, dict[str, dict[str, object]]] = {e: {} for e in ENTITIES}
    linked = 0
    bytes_total = 0
    sources = sorted(list(RUN.glob("worker_*")) + list(RUN.glob("task_*")))
    for worker in sources:
        for entity in ENTITIES:
            src_dir = worker / "cache" / entity
            if not src_dir.is_dir():
                continue
            dst_dir = OUT / entity
            dst_dir.mkdir(parents=True, exist_ok=True)
            for src in sorted(src_dir.glob("*.parquet")):
                dst = dst_dir / src.name
                if dst.exists():
                    raise RuntimeError(f"duplicate output: {dst}")
                os.link(src, dst)
                linked += 1
                bytes_total += src.stat().st_size
            manifest = src_dir / "chrom_manifest.json"
            if manifest.exists():
                for entry in json.loads(manifest.read_text()):
                    chrom = entry["chrom"]
                    if chrom in manifests[entity]:
                        raise RuntimeError(f"duplicate manifest entry: {entity}/{chrom}")
                    manifests[entity][chrom] = entry
    for entity, entries in manifests.items():
        if not entries:
            continue
        (OUT / entity / "chrom_manifest.json").write_text(
            json.dumps([entries[k] for k in sorted(entries)], indent=2) + "\n"
        )
    if SOURCE_INFO.exists():
        shutil.copy2(SOURCE_INFO, OUT / "info.txt")
    elapsed = time.perf_counter() - started
    summary = {
        "seconds_wall": elapsed,
        "output": str(OUT),
        "hardlinked_parquet_files": linked,
        "parquet_bytes": bytes_total,
        "entities": {e: len(v) for e, v in manifests.items()},
    }
    (OUT.parent / "assembly_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (OUT.parent / "assembly_command.json").write_text(
        json.dumps({"source": str(RUN), "output": str(OUT), "method": "hardlink+manifest-merge"}, indent=2) + "\n"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
