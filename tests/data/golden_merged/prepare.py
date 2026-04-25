#!/usr/bin/env python3
"""Prepare merged golden test data for vepyr integration tests.

This reuses the sampled input VCF and trimmed reference from
``tests/data/golden`` and creates:

- ``golden.vcf`` from the real merged Ensembl VEP output in ``sandbox/``
- ``cache/`` from the real merged parquet cache in ``data_vepyr``

Usage:
    python tests/data/golden_merged/prepare.py

Env vars:
    CACHE_SRC   Full merged parquet cache directory
    GOLDEN_SRC  Full merged Ensembl VEP output VCF
    INPUT_VCF   Sampled VCF used by the existing golden tests
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent
BASE_GOLDEN_DIR = DATA_DIR / "golden"
REPO_ROOT = SCRIPT_DIR.parents[2]

CACHE_SRC = Path(
    os.environ.get(
        "CACHE_SRC",
        "/Users/mwiewior/workspace/data_vepyr/115_GRCh38_merged",
    )
)
GOLDEN_SRC = Path(
    os.environ.get(
        "GOLDEN_SRC",
        str(REPO_ROOT / "sandbox" / "HG002_annotated_wgs_everything_hgvs_merged.vcf"),
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


def write_trimmed_cache(cache_src: Path, cache_dir: Path, end: int) -> None:
    entities = {
        "variation": ("start", 0, end),
        "transcript": ("start", 0, end),
        "exon": ("start", 0, end),
        "translation_sift": ("start", 0, end),
        "regulatory": ("start", 0, end),
        "motif": ("start", 0, end),
    }

    for entity, (column, lo, hi) in entities.items():
        src = cache_src / entity / "chr1.parquet"
        dst_dir = cache_dir / entity
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "chr1.parquet"
        table = pq.read_table(str(src))
        mask = pc.and_(
            pc.greater_equal(table[column], lo), pc.less_equal(table[column], hi)
        )
        trimmed = table.filter(mask)
        pq.write_table(trimmed, str(dst))
        print(f"  {entity}: {trimmed.num_rows} rows")

    transcript_table = pq.read_table(str(cache_dir / "transcript" / "chr1.parquet"))
    transcript_ids = pa.array(transcript_table.column("stable_id").to_pylist())

    src = cache_src / "translation_core" / "chr1.parquet"
    dst_dir = cache_dir / "translation_core"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "chr1.parquet"

    table = pq.read_table(str(src))
    mask = pc.is_in(table["transcript_id"], value_set=transcript_ids)
    trimmed = table.filter(mask)
    pq.write_table(trimmed, str(dst))
    print(f"  translation_core: {trimmed.num_rows} rows")


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
