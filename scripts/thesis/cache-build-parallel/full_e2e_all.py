#!/usr/bin/env python3
"""Full merged core cache build for all 1904 contigs, with bounded translation."""
from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

import vepyr

DATA = Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38")
ROOT = Path(__file__).resolve().parent / "full_e2e_all"
OUT = ROOT / "cache"
NON_TRANSLATION = ("variation", "transcript", "exon", "regulatory", "motif")


def contigs() -> list[str]:
    return sorted(d.name for d in DATA.iterdir() if d.is_dir() and (d / "all_vars.gz").is_file())


def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    cs = contigs()
    command = {"release": 116, "cache_type": "merged", "partitions": 1,
               "local_cache": str(DATA), "output": str(OUT), "contigs": len(cs),
               "entities": [*NON_TRANSLATION, "translation"],
               "translation_mode": "one chromosome per call"}
    (ROOT / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    started = time.perf_counter(); rows = []
    def record(entity: str, scope: str, t0: float, stats: object) -> None:
        row = {"entity": entity, "scope": scope, "seconds": time.perf_counter() - t0, "stats": stats}
        rows.append(row)
        (ROOT / "progress.json").write_text(json.dumps({"completed": len(rows), "total": len(NON_TRANSLATION)+len(cs), "rows": rows}, default=str, indent=2) + "\n")
        print(json.dumps({"completed": len(rows), "total": len(NON_TRANSLATION)+len(cs), **row}, default=str), flush=True)
    for entity in NON_TRANSLATION:
        t0 = time.perf_counter()
        stats = vepyr.build_cache_entity(116, str(OUT), entity, cache_type="merged",
            local_cache=str(DATA), partitions=1, overwrite=True, chroms=None)
        record(entity, "all", t0, stats)
    for i, chrom in enumerate(cs, 1):
        t0 = time.perf_counter()
        stats = vepyr.build_cache_entity(116, str(OUT), "translation", cache_type="merged",
            local_cache=str(DATA), partitions=1, overwrite=True, chroms=[chrom])
        record("translation", chrom, t0, stats)
        if shutil.disk_usage(ROOT).free < 15 * 1024**3:
            raise RuntimeError("disk guard: less than 15 GiB free")
    result = {"wall_seconds": time.perf_counter()-started, "contigs": len(cs),
              "entities": [*NON_TRANSLATION, "translation"],
              "output_bytes": sum(p.stat().st_size for p in OUT.rglob("*") if p.is_file()),
              "rows": rows, "all_completed": True}
    (ROOT / "summary.json").write_text(json.dumps(result, default=str, indent=2) + "\n")
    print(json.dumps({k:v for k,v in result.items() if k != "rows"}, indent=2), flush=True)


if __name__ == "__main__":
    main()
