#!/usr/bin/env python3
"""Rebuild only the `motif` entity of a converted Ensembl cache.

The MotifFeature schema gains columns more often than the rest of the cache
(binding matrix id, length, elements, unit, motif sequence), and a full
rebuild costs about 90 minutes and 36 GB to refresh 83 MB of motif shards.

The new shards are built into a staging directory and only swapped in once
they verify as populated, so an interrupted run leaves the existing cache
intact.

Usage:
    uv run python e2e-testing/scripts/rebuild_motif_entity.py            # dry run
    uv run python e2e-testing/scripts/rebuild_motif_entity.py --run
    uv run python e2e-testing/scripts/rebuild_motif_entity.py --run --release 116
"""

import argparse
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from comparison import profiles  # noqa: E402

# Columns the rebuild exists to populate. Verified before the swap.
REQUIRED_COLUMNS = (
    "motif_id",
    "binding_matrix",
    "binding_matrix_length",
    "binding_matrix_elements",
    "binding_matrix_unit",
    "motif_seq",
)


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--run", action="store_true", help="Actually rebuild")
    p.add_argument("--release", default="116", choices=profiles.RELEASES)
    p.add_argument(
        "--cache-type", default="merged", choices=["merged", "refseq", "ensembl"]
    )
    p.add_argument("--partitions", type=int, default=8)
    p.add_argument("--target", default=None, help="Converted cache directory")
    p.add_argument(
        "--local-cache",
        default=None,
        help="Extracted Ensembl cache version directory containing info.txt",
    )
    args = p.parse_args(argv)
    if args.partitions <= 0:
        p.error("--partitions must be a positive integer")
    return args


def verify(motif_dir):
    """Return (ok, message): every required column present and populated."""
    if not os.path.isdir(motif_dir):
        return False, f"no motif directory at {motif_dir}"
    shards = sorted(f for f in os.listdir(motif_dir) if f.endswith(".parquet"))
    if not shards:
        return False, f"no parquet shards in {motif_dir}"

    import pyarrow.parquet as pq

    sample = os.path.join(motif_dir, shards[0])
    present = set(pq.ParquetFile(sample).schema_arrow.names)
    missing = [c for c in REQUIRED_COLUMNS if c not in present]
    if missing:
        return False, f"{shards[0]} is missing columns: {', '.join(missing)}"

    data = pq.read_table(sample, columns=list(REQUIRED_COLUMNS)).to_pydict()
    rows = len(data["motif_id"])
    if rows == 0:
        return False, f"{shards[0]} has 0 rows"
    counts = {
        c: sum(1 for v in data[c] if v not in (None, "")) for c in REQUIRED_COLUMNS
    }
    detail = ", ".join(f"{c}={counts[c]:,}/{rows:,}" for c in REQUIRED_COLUMNS)
    empty = [c for c, n in counts.items() if n == 0]
    if empty:
        return (
            False,
            f"columns still empty in {shards[0]}: {', '.join(empty)} ({detail})",
        )
    return True, f"{shards[0]}: {detail}"


def main(argv=None):
    args = parse_args(argv)
    data = profiles.data_dir()
    target = args.target or profiles.cache_dir_for(
        args.cache_type, args.release, warn=False
    )
    source = args.local_cache or os.path.join(
        data,
        "homo_sapiens"
        if args.cache_type == "ensembl"
        else f"homo_sapiens_{args.cache_type}",
        f"{args.release}_GRCh38",
    )
    staging = target + ".motif-rebuild"
    live_motif = os.path.join(target, "motif")
    staged_motif = os.path.join(staging, "motif")

    print("=" * 68)
    print(f"  release      {args.release}")
    print(f"  cache type   {args.cache_type}")
    print(f"  source       {source}")
    print(f"  staging      {staged_motif}")
    print(f"  target       {live_motif}")
    print("=" * 68)

    problems = []
    if not os.path.isdir(source):
        problems.append(f"source cache not found: {source}")
    elif "info.txt" not in os.listdir(source):
        problems.append(
            f"no info.txt in {source} — the builder needs the version directory"
        )
    if not os.path.isdir(target):
        problems.append(f"converted cache not found: {target}")
    if os.path.exists(staging):
        problems.append(f"staging dir already exists, remove it first: {staging}")

    if os.path.isdir(live_motif):
        ok, detail = verify(live_motif)
        print(f"  current motif columns: {'OK' if ok else 'NEEDS REBUILD'} — {detail}")

    if problems:
        print("\nBLOCKED:")
        for problem in problems:
            print(f"  - {problem}")
        return 2

    if not args.run:
        print("\nDry run. Re-run with --run to rebuild.")
        return 0

    from vepyr import _core

    os.makedirs(staging, exist_ok=True)
    print(f"\nBuilding motif shards into {staged_motif} ...")
    started = time.time()
    stats = _core.build_cache_entity(
        source, staging, "motif", args.partitions, args.cache_type, True
    )
    elapsed = time.time() - started
    rows = sum(n for _entity, files, _ in stats for _path, n in files)
    print(f"Built {rows:,} motif rows in {elapsed / 60:.1f} min")

    ok, detail = verify(staged_motif)
    print(f"Verification: {'OK' if ok else 'FAILED'} — {detail}")
    if not ok:
        print(f"\nStaging left in place for inspection: {staging}")
        return 1

    previous = live_motif + ".old"
    if os.path.exists(previous):
        shutil.rmtree(previous)
    if os.path.isdir(live_motif):
        os.rename(live_motif, previous)
    os.rename(staged_motif, live_motif)
    shutil.rmtree(staging, ignore_errors=True)
    shutil.rmtree(previous, ignore_errors=True)
    print(f"\nSwapped into {live_motif}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
