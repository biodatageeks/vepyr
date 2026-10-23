import glob
import hashlib
import os
import sys


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def collected_hashes(collected_dir):
    # Each pod's output landed in collected_dir/pod<N>/<entity>/chr*.parquet.
    # Normalize to <entity>/chr*.parquet so pod0's vs pod1's output for the
    # *same* entity/chrom never collide as distinct keys, and so this exact
    # key space matches across a conc=1 run's own directory too.
    hashes = {}
    for path in sorted(glob.glob(os.path.join(collected_dir, "pod*", "*", "*.parquet"))):
        entity_and_file = os.path.join(*path.split(os.sep)[-2:])
        hashes[entity_and_file] = sha256_file(path)
    return hashes


def main():
    # Compares two *collected* directories from this same sweep run (e.g. the
    # N=2 output against the N=1 baseline) -- both built inside the same
    # container image/mount path, so a plain byte-for-byte hash comparison is
    # valid here (unlike comparing against a build done on a different host,
    # where embedded source-path provenance columns always differ and make
    # raw file hashes meaningless).
    collected_dir = sys.argv[1] if len(sys.argv) > 1 else "/tmp/distributed_collected"
    ref_dir = sys.argv[2] if len(sys.argv) > 2 else None
    chrom_filter = sys.argv[3].split(",") if len(sys.argv) > 3 and sys.argv[3] else None

    if not ref_dir:
        print("VERIFY,REF", flush=True)
        return 0

    ref = collected_hashes(ref_dir)
    got = collected_hashes(collected_dir)
    if chrom_filter:
        ref = {k: v for k, v in ref.items() if any(c in k for c in chrom_filter)}
        got = {k: v for k, v in got.items() if any(c in k for c in chrom_filter)}

    missing = set(ref) - set(got)
    extra = set(got) - set(ref)
    mismatched = {k for k in (set(ref) & set(got)) if ref[k] != got[k]}

    if not missing and not extra and not mismatched:
        print(f"VERIFY,IDENTICAL,files={len(ref)}")
        return 0

    print(f"VERIFY,MISMATCH,missing={len(missing)},extra={len(extra)},mismatched={len(mismatched)}")
    for k in sorted(missing):
        print(f"  missing: {k}")
    for k in sorted(extra):
        print(f"  extra: {k}")
    for k in sorted(mismatched):
        print(f"  mismatched: {k}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
