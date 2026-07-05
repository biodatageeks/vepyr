#!/usr/bin/env python3
"""Prepare merged golden test data for vepyr integration tests.

This reuses the sampled input VCF and trimmed reference from
``tests/data/golden`` and creates:

- ``golden.vcf`` from the real merged Ensembl VEP output in ``data_vepyr``
- ``cache/`` from the real merged parquet cache in ``data_vepyr``

Usage:
    python tests/data/golden_merged/prepare.py

Env vars:
    DATA_VEPYR_DIR  Directory containing full HG002 inputs, VEP outputs, caches, FASTA
    CACHE_SRC   Full merged parquet cache directory
    GOLDEN_SRC  Full merged Ensembl VEP output VCF
    INPUT_VCF   Sampled VCF used by the existing golden tests
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent
BASE_GOLDEN_DIR = DATA_DIR / "golden"

# The shared trimmed-cache builder lives alongside the base golden prepare script.
sys.path.insert(0, str(BASE_GOLDEN_DIR))
from _cache_prep import write_trimmed_cache  # noqa: E402


def expand_path(path: str) -> Path:
    return Path(os.path.expandvars(path)).expanduser()


DATA_VEPYR_DIR = expand_path(
    os.environ.get("DATA_VEPYR_DIR", "$HOME/workspace/data_vepyr")
)

CACHE_SRC = expand_path(
    os.environ.get(
        "CACHE_SRC",
        str(DATA_VEPYR_DIR / "115_GRCh38_merged"),
    )
)
GOLDEN_SRC = expand_path(
    os.environ.get(
        "GOLDEN_SRC",
        str(DATA_VEPYR_DIR / "HG002_annotated_wgs_everything_hgvs_merged.vcf"),
    )
)
INPUT_VCF = Path(os.environ.get("INPUT_VCF", str(BASE_GOLDEN_DIR / "input.vcf")))
REGION_BUFFER = 10_000


def load_sample_keys(path: Path) -> tuple[set[tuple[str, str, str, str]], int, int]:
    keys: set[tuple[str, str, str, str]] = set()
    positions = []

    with open(path) as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            fields = line.rstrip("\n").split("\t")
            keys.add((fields[0], fields[1], fields[3], fields[4]))
            positions.append(int(fields[1]))

    return keys, min(positions), max(positions)


def write_golden_subset(
    sample_keys: set[tuple[str, str, str, str]], source: Path, output: Path
) -> int:
    matched = 0

    with open(source) as src, open(output, "w") as dst:
        for line in src:
            if line.startswith("#"):
                dst.write(line)
                continue

            fields = line.rstrip("\n").split("\t")
            key = (fields[0], fields[1], fields[3], fields[4])
            if key in sample_keys:
                dst.write(line)
                matched += 1

    return matched


def main() -> None:
    sample_keys, start, end = load_sample_keys(INPUT_VCF)
    buffered_end = end + REGION_BUFFER
    print(f"Preparing merged golden test data in {SCRIPT_DIR}")
    print(f"Input sample: {len(sample_keys)} variants, chr1:{start}-{end}")
    print(f"Trimmed cache window: chr1:1-{buffered_end}")

    golden_path = SCRIPT_DIR / "golden.vcf"
    matched = write_golden_subset(sample_keys, GOLDEN_SRC, golden_path)
    print(f"Wrote {matched} merged golden variants to {golden_path}")

    cache_dir = SCRIPT_DIR / "cache"
    write_trimmed_cache(CACHE_SRC, cache_dir, buffered_end)
    print(f"Wrote trimmed merged cache to {cache_dir}")


if __name__ == "__main__":
    main()
