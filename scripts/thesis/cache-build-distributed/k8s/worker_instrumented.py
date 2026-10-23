import json
import os
import resource
import time

import vepyr

ENTITIES = ["variation", "transcript", "exon", "regulatory", "motif"]


def rss_bytes():
    # Linux ru_maxrss is KiB; macOS reports bytes.
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value * 1024 if value < 10**9 else value)


def main():
    idx = int(os.environ.get("WORKER_INDEX", "0"))
    total = int(os.environ.get("WORKER_COUNT", "1"))
    cache_root = os.environ["CACHE_ROOT"]
    output_dir = os.environ["OUTPUT_DIR"]
    cache_type = os.environ.get("CACHE_TYPE", "merged")
    expected_cache_version = os.environ.get("EXPECTED_CACHE_VERSION", "116")
    chroms_env = os.environ.get("CHROMS", "").strip()
    chroms = [c.strip() for c in chroms_env.split(",") if c.strip()] or None
    shard = [e for i, e in enumerate(ENTITIES) if i % total == idx]
    os.makedirs(output_dir, exist_ok=True)
    t0 = time.time()
    rows = []
    for entity in shard:
        e0 = time.time()
        r0 = rss_bytes()
        vepyr._core.build_cache_entity(
            cache_root, output_dir, entity,
            partitions=1, cache_source_type=cache_type,
            overwrite=True, expected_cache_version=expected_cache_version,
            chroms=chroms,
        )
        rows.append({
            "entity": entity,
            "seconds": time.time() - e0,
            "rss_before_bytes": r0,
            "rss_peak_bytes": rss_bytes(),
        })
    result = {
        "worker_index": idx,
        "worker_count": total,
        "chroms": chroms,
        "entities": shard,
        "seconds_total": time.time() - t0,
        "entities_timing": rows,
    }
    print(json.dumps(result), flush=True)
    with open(os.path.join(output_dir, "TIMING.json"), "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
