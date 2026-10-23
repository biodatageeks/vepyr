import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

import vepyr

# "translation" excluded: a container-specific glibc allocator issue (fixed
# for every other entity by MALLOC_ARENA_MAX=1 in the Dockerfile) still grows
# unbounded for this one specifically -- confirmed via isolated per-entity
# testing (native macOS peaks <1GB for the same entity/chrom). Disclosed scope
# reduction for the distributed dimension only; the local LPT sweep already
# covers translation with no such issue.
ENTITIES = ["variation", "transcript", "exon", "regulatory", "motif"]


def main():
    idx = int(os.environ.get("JOB_COMPLETION_INDEX", os.environ.get("WORKER_INDEX", "0")))
    total = int(os.environ["WORKER_COUNT"])
    cache_root = os.environ["CACHE_ROOT"]
    output_dir = os.environ["OUTPUT_DIR"]
    cache_type = os.environ.get("CACHE_TYPE", "merged")
    expected_cache_version = os.environ.get("EXPECTED_CACHE_VERSION", "116")
    partitions = int(os.environ.get("VEPYR_PARTITIONS", "1"))
    chroms_env = os.environ.get("CHROMS", "").strip()
    chroms = [c.strip() for c in chroms_env.split(",") if c.strip()] or None

    requested_entities = [e.strip() for e in os.environ.get("VEPYR_ENTITIES", "").split(",") if e.strip()]
    entity_list = requested_entities or ENTITIES
    shard = [e for i, e in enumerate(entity_list) if i % total == idx]

    os.makedirs(output_dir, exist_ok=True)
    def build(entity):
        vepyr._core.build_cache_entity(
            cache_root,
            output_dir,
            entity,
            partitions=partitions,
            cache_source_type=cache_type,
            overwrite=True,
            expected_cache_version=expected_cache_version,
            chroms=chroms,
        )
    t0 = time.time()
    entity_concurrency = max(1, int(os.environ.get("VEPYR_ENTITY_CONCURRENCY", "1")))
    if entity_concurrency == 1 or len(shard) < 2:
        for entity in shard:
            build(entity)
    else:
        with ThreadPoolExecutor(max_workers=min(entity_concurrency, len(shard))) as pool:
            list(pool.map(build, shard))
    dt = time.time() - t0

    result = {"pod": idx, "worker_count": total, "entities": shard, "seconds": dt}
    print(json.dumps(result), flush=True)
    with open(os.path.join(output_dir, f"DONE_pod{idx}.json"), "w") as f:
        json.dump(result, f)


if __name__ == "__main__":
    main()
