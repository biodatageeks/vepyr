#!/usr/bin/env python3
"""Five complete all-contig core-cache builds, excluding translation."""
from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

import vepyr

DATA = Path("/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38")
ROOT = Path(__file__).resolve().parent / "full_core_5x"
ENTITIES = ("variation", "transcript", "exon", "regulatory", "motif")
REPEATS = 5


def contigs() -> list[str]:
    return sorted(d.name for d in DATA.iterdir() if d.is_dir() and (d / "all_vars.gz").is_file())


def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    cs = contigs()
    manifest = {"contigs": len(cs), "entities": list(ENTITIES), "repeats": REPEATS,
                "partitions": 1, "cache_type": "merged", "release": 116}
    (ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    all_results = []
    for repeat in range(1, REPEATS + 1):
        run = ROOT / f"repeat_{repeat}"
        out = run / "cache"
        run.mkdir(parents=True, exist_ok=True)
        if out.exists():
            shutil.rmtree(out)
        out.mkdir()
        started = time.perf_counter(); rows = []
        (run / "command.json").write_text(json.dumps({**manifest, "repeat": repeat,
            "output": str(out), "local_cache": str(DATA)}, indent=2) + "\n")
        try:
            for entity in ENTITIES:
                t0 = time.perf_counter()
                stats = vepyr.build_cache_entity(116, str(out), entity,
                    cache_type="merged", local_cache=str(DATA), partitions=1,
                    overwrite=True, chroms=None)
                row = {"entity": entity, "seconds": time.perf_counter()-t0,
                       "files": len(stats), "rows": sum(n for _, n in stats)}
                rows.append(row)
                (run / "progress.json").write_text(json.dumps({"repeat": repeat,
                    "completed": len(rows), "total": len(ENTITIES), "rows": rows}, indent=2) + "\n")
                print(json.dumps({"repeat": repeat, **row}), flush=True)
            result = {"repeat": repeat, "wall_seconds": time.perf_counter()-started,
                      "rows": rows, "output_bytes": sum(p.stat().st_size for p in out.rglob("*") if p.is_file()),
                      "success": True}
        except Exception as exc:
            result = {"repeat": repeat, "wall_seconds": time.perf_counter()-started,
                      "rows": rows, "success": False, "error": repr(exc)}
            raise
        finally:
            (run / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
            all_results.append(result)
            (ROOT / "progress.json").write_text(json.dumps(all_results, indent=2) + "\n")
            shutil.rmtree(out, ignore_errors=True)
        print(json.dumps(result, indent=2), flush=True)
    (ROOT / "summary.json").write_text(json.dumps(all_results, indent=2) + "\n")


if __name__ == "__main__":
    main()
