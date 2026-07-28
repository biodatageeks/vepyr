#!/usr/bin/env python3
"""Regenerate the release-116 merged Parquet cache with motif fields populated.

Required after biodatageeks/datafusion-bio-formats#224: `binding_matrix` and
`transcription_factors` were written as NULL for every MotifFeature row, so the
nulls are already on disk and only a rebuild clears them.

Usage:
    uv run python e2e-testing/scripts/rebuild_cache_116_merged.py            # dry run
    uv run python e2e-testing/scripts/rebuild_cache_116_merged.py --run
    uv run python e2e-testing/scripts/rebuild_cache_116_merged.py --run --cache-type refseq
    uv run python e2e-testing/scripts/rebuild_cache_116_merged.py --run --keep-old

The rebuild writes to a fresh `<target>.rebuild` directory and only swaps it
into place once the motif columns verify as populated, so an interrupted or
faulty run cannot leave you with a half-written cache where a good one was.
"""

import argparse
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from comparison import profiles  # noqa: E402

RELEASE = "116"
MOTIF_COLUMNS = ("motif_id", "binding_matrix", "transcription_factors")


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--run",
        action="store_true",
        help="Actually rebuild. Without this the script only reports what it would do.",
    )
    p.add_argument(
        "--cache-type",
        default="merged",
        choices=["merged", "refseq", "ensembl"],
        help="Ensembl cache flavour (default: %(default)s)",
    )
    p.add_argument(
        "--partitions",
        type=int,
        default=8,
        help="DataFusion partitions during conversion (default: %(default)s)",
    )
    p.add_argument(
        "--keep-old",
        action="store_true",
        help="Rename the previous cache to <target>.old instead of deleting it",
    )
    p.add_argument(
        "--target",
        default=None,
        help="Output cache directory (default: $DATA/cache/116_GRCh38_<type>)",
    )
    p.add_argument(
        "--local-cache",
        default=None,
        help="Extracted Ensembl cache version directory containing info.txt "
        "(default: $DATA/homo_sapiens_<type>/116_GRCh38)",
    )
    args = p.parse_args(argv)
    if args.partitions <= 0:
        p.error("--partitions must be a positive integer")
    return args


def human(n_bytes):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n_bytes < 1024:
            return f"{n_bytes:.0f}{unit}"
        n_bytes /= 1024
    return f"{n_bytes:.0f}PB"


def dir_size(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def verify_motif_columns(cache_dir):
    """Return (ok, message). Checks that motif rows carry a binding matrix.

    This is the whole point of the rebuild, so it is checked before the new
    cache replaces the old one.
    """
    motif_dir = os.path.join(cache_dir, "motif")
    if not os.path.isdir(motif_dir):
        return False, f"no motif/ directory in {cache_dir}"

    shards = sorted(f for f in os.listdir(motif_dir) if f.endswith(".parquet"))
    if not shards:
        return False, f"no parquet shards in {motif_dir}"

    try:
        import pyarrow.parquet as pq
    except ImportError:
        return (
            False,
            "pyarrow not available to verify (install it, or skip with --run only)",
        )

    sample = os.path.join(motif_dir, shards[0])
    table = pq.read_table(sample, columns=list(MOTIF_COLUMNS))
    data = table.to_pydict()
    rows = len(data["motif_id"])
    if rows == 0:
        return False, f"{shards[0]} has 0 rows"

    counts = {
        col: sum(1 for v in data[col] if v not in (None, "")) for col in MOTIF_COLUMNS
    }
    detail = ", ".join(f"{c}={counts[c]:,}/{rows:,}" for c in MOTIF_COLUMNS)
    if counts["binding_matrix"] == 0:
        return False, (
            f"binding_matrix still NULL in {shards[0]} ({detail}) — "
            "is the build pinned to datafusion-bio-formats#224?"
        )
    return True, f"{shards[0]}: {detail}"


def main(argv=None):
    args = parse_args(argv)

    data = profiles.data_dir()
    # Resolve through the same helper the runner uses, so an existing cache is
    # rebuilt where it actually lives (legacy $DATA root or the new cache/
    # subdirectory) rather than silently written to a second location.
    target = args.target or profiles.cache_dir_for(args.cache_type, RELEASE, warn=False)
    staging = target + ".rebuild"
    # build_cache(local_cache=...) wants the directory that *contains*
    # info.txt -- the version directory, not the data root or species dir.
    source = args.local_cache or os.path.join(
        data,
        "homo_sapiens"
        if args.cache_type == "ensembl"
        else f"homo_sapiens_{args.cache_type}",
        f"{RELEASE}_GRCh38",
    )

    print("=" * 68)
    print(f"  release      {RELEASE}")
    print(f"  cache type   {args.cache_type}")
    print(f"  source       {source}")
    print(f"  staging      {staging}")
    print(f"  target       {target}")
    print(f"  partitions   {args.partitions}")
    print("=" * 68)

    # ---- preflight ---------------------------------------------------
    problems = []
    if not os.path.isdir(source):
        problems.append(f"source cache not found: {source}")
    else:
        entries = os.listdir(source)
        if "info.txt" not in entries:
            problems.append(
                f"no info.txt in {source} — build_cache(local_cache=...) requires the "
                f"directory containing info.txt"
            )
        if len(entries) < 100:
            problems.append(
                f"source has only {len(entries)} entries — likely the nested-extraction "
                f"trap; expected ~1,900 including chr_synonyms.txt"
            )
        elif "chr_synonyms.txt" not in entries:
            print(f"  note: no chr_synonyms.txt in {source} (only VEP itself needs it)")

    if os.path.exists(staging):
        problems.append(f"staging dir already exists, remove it first: {staging}")

    existing_size = 0
    if os.path.isdir(target):
        existing_size = dir_size(target)
        print(f"  existing target: {human(existing_size)}")
        ok, detail = verify_motif_columns(target)
        print(f"  existing motif columns: {'OK' if ok else 'NEEDS REBUILD'} — {detail}")
        if ok:
            print("\n  The current cache already has binding_matrix populated.")
            print("  Rebuilding is unnecessary unless the source cache changed.")

    # Staging is built alongside the old cache, so both exist at once.
    free = shutil.disk_usage(os.path.dirname(target) or ".").free
    needed = int(existing_size * 1.15) if existing_size else 0
    print(f"  free space:  {human(free)} (staging needs about {human(needed)} more)")
    if needed and free < needed:
        problems.append(
            f"insufficient free space: {human(free)} available, "
            f"~{human(needed)} needed to stage alongside the current cache"
        )

    if problems:
        print("\nBLOCKED:")
        for p in problems:
            print(f"  - {p}")
        return 2

    if not args.run:
        print("\nDry run. Re-run with --run to rebuild.")
        return 0

    # ---- rebuild into staging ----------------------------------------
    import vepyr

    os.makedirs(os.path.dirname(staging), exist_ok=True)
    print(f"\nBuilding into {staging} ...")
    t0 = time.time()
    written = vepyr.build_cache(
        int(RELEASE),
        staging,
        cache_type=args.cache_type,
        partitions=args.partitions,
        local_cache=source,
        overwrite=True,
    )
    elapsed = time.time() - t0
    total_rows = sum(n for _entity, n in written)
    print(f"\nConversion finished in {elapsed / 60:.1f} min, {total_rows:,} rows")
    for entity, n in written:
        print(f"  {entity:<20} {n:>12,}")

    # ---- verify before swapping --------------------------------------
    ok, detail = verify_motif_columns(staging)
    print(f"\nmotif column check: {'PASS' if ok else 'FAIL'} — {detail}")
    if not ok:
        print(f"\nLeaving {staging} in place and NOT replacing {target}.")
        return 1

    # ---- swap --------------------------------------------------------
    if os.path.isdir(target):
        backup = target + ".old"
        if os.path.exists(backup):
            shutil.rmtree(backup)
        os.rename(target, backup)
        print(f"previous cache moved to {backup}")
        if not args.keep_old:
            shutil.rmtree(backup)
            print("previous cache removed (pass --keep-old to retain it)")
    os.rename(staging, target)
    print(f"\nDONE: {target} ({human(dir_size(target))})")

    print("\nNext:")
    print("  uv run python e2e-testing/scripts/run_comparison.py \\")
    print(f"      --release {RELEASE} --profile {args.cache_type} --chroms 22 --force")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
