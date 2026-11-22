"""Assemble a vepyr cache directory in which only some variation shards are rewritten.

dest_root/<layout>/<source.name>/ holds a symlink to every top-level entry of the source cache
except variation/, which is a real directory of symlinks, apart from <contig>.parquet for each
requested contig: that one is written by the relayout tool in the requested layout.
dest_root/<layout>/layout.json records the layout, the contigs and the relayout summaries.

    python make_layout_cache.py --source CACHE --dest-root DIR --layout tiered|sorted \
        --contigs chr22 --relayout-bin relayout/target/release/relayout
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

LAYOUTS = ("tiered", "sorted")
HEADROOM = 40 * 2**30


def free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


def make_layout_cache(
    source: Path, dest_root: Path, layout: str, contigs: list[str], relayout_bin: Path
) -> Path:
    if layout not in LAYOUTS:
        raise ValueError(f"layout must be one of {LAYOUTS}, got {layout!r}")
    source = Path(source).resolve()
    layout_dir = Path(dest_root) / layout
    cache = layout_dir / source.name
    if layout_dir.exists():
        raise FileExistsError(f"{layout_dir} exists")
    shards = [source / "variation" / f"{c}.parquet" for c in contigs]
    for shard in shards:
        if not shard.is_file():
            raise FileNotFoundError(shard)
    probe = Path(dest_root)
    while not probe.exists():
        probe = probe.parent
    need = 2 * sum(s.stat().st_size for s in shards) + HEADROOM
    if free_bytes(probe) < need:
        raise RuntimeError(
            f"not enough free disk on {probe}: {free_bytes(probe) / 2**30:.1f} GiB, "
            f"need {need / 2**30:.1f} GiB"
        )

    cache.mkdir(parents=True)
    for entry in sorted(source.iterdir()):
        if entry.name != "variation":
            os.symlink(entry, cache / entry.name)
    variation = cache / "variation"
    variation.mkdir()
    rewrite = {f"{c}.parquet" for c in contigs}
    for entry in sorted((source / "variation").iterdir()):
        if entry.name not in rewrite:
            os.symlink(entry, variation / entry.name)

    summaries = {}
    for contig, shard in zip(contigs, shards):
        out = subprocess.run(
            [
                str(relayout_bin),
                "--input",
                str(shard),
                "--output",
                str(variation / shard.name),
                "--layout",
                layout,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        summaries[contig] = json.loads(out.stdout.strip().splitlines()[-1])

    info = {
        "layout": layout,
        "contigs": list(contigs),
        "source": str(source),
        "relayout": summaries,
    }
    (layout_dir / "layout.json").write_text(json.dumps(info, indent=2) + "\n")
    return cache


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--dest-root", required=True, type=Path)
    parser.add_argument("--layout", required=True, choices=LAYOUTS)
    parser.add_argument("--contigs", required=True, nargs="+")
    parser.add_argument("--relayout-bin", required=True, type=Path)
    args = parser.parse_args()
    cache = make_layout_cache(
        args.source, args.dest_root, args.layout, args.contigs, args.relayout_bin
    )
    print(cache)


if __name__ == "__main__":
    main()
