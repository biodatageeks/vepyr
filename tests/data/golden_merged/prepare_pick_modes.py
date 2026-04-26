#!/usr/bin/env python3
"""Prepare merged pick-mode golden VCF subsets for integration tests.

The tests reuse ``tests/data/golden/input.vcf`` and the trimmed merged cache
from ``tests/data/golden_merged/cache``. This script extracts matching variants
from full Ensembl VEP outputs in ``sandbox/`` into small committed fixtures.

Usage:
    python tests/data/golden_merged/prepare_pick_modes.py

Env vars:
    SANDBOX_DIR  Directory containing full Ensembl VEP pick-mode outputs.
    INPUT_VCF    Sampled VCF used by the existing golden tests.
"""

from __future__ import annotations

import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent
BASE_GOLDEN_DIR = DATA_DIR / "golden"
REPO_ROOT = SCRIPT_DIR.parents[2]

SANDBOX_DIR = Path(os.environ.get("SANDBOX_DIR", str(REPO_ROOT / "sandbox")))
INPUT_VCF = Path(os.environ.get("INPUT_VCF", str(BASE_GOLDEN_DIR / "input.vcf")))

PICK_MODE_FIXTURES = {
    "golden_merged_per_gene": (
        "HG002_annotated_wgs_everything_hgvs_merged_per_gene.vcf"
    ),
    "golden_merged_pick_allele": (
        "HG002_annotated_wgs_everything_hgvs_merged_pick_allele.vcf"
    ),
}


def load_sample_keys(path: Path) -> set[tuple[str, str, str, str]]:
    keys: set[tuple[str, str, str, str]] = set()

    with open(path) as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            fields = line.rstrip("\n").split("\t")
            keys.add((fields[0], fields[1], fields[3], fields[4]))

    return keys


def write_golden_subset(
    sample_keys: set[tuple[str, str, str, str]], source: Path, output: Path
) -> int:
    matched = 0
    output.parent.mkdir(parents=True, exist_ok=True)

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
    sample_keys = load_sample_keys(INPUT_VCF)
    print(f"Preparing merged pick-mode golden fixtures from {SANDBOX_DIR}")
    print(f"Input sample: {len(sample_keys)} variants")

    for fixture_dir, source_name in PICK_MODE_FIXTURES.items():
        source = SANDBOX_DIR / source_name
        output = DATA_DIR / fixture_dir / "golden.vcf"
        matched = write_golden_subset(sample_keys, source, output)
        print(f"  {fixture_dir}: wrote {matched} variants to {output}")


if __name__ == "__main__":
    main()
