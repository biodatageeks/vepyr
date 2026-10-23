import json
import os
import time

import vepyr


def main():
    cache_root = os.environ["CACHE_ROOT"]
    output_dir = os.environ["OUTPUT_DIR"]
    chrom = os.environ["CHROM"]
    entity = os.environ.get("ENTITY", "variation")
    t0 = time.time()
    stats = vepyr._core.build_cache_entity(
        cache_root,
        output_dir,
        entity,
        partitions=1,
        cache_source_type="merged",
        overwrite=True,
        expected_cache_version="116",
        chroms=[chrom],
    )
    result = {
        "chrom": chrom,
        "seconds": time.time() - t0,
        "stats": stats,
    }
    print(json.dumps(result, default=str), flush=True)


if __name__ == "__main__":
    main()
