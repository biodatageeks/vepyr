#!/usr/bin/env python3
"""Prepare golden test data for vepyr integration tests.

Prerequisites:
- Full parquet cache at CACHE_SRC (from vepyr.build_cache)
- HG002 chr1 VCF at VCF_SRC
- Ensembl VEP 115 golden output at GOLDEN_SRC
- Reference FASTA at FASTA_SRC
- bcftools, samtools, bgzip, tabix in PATH

Usage:
    python tests/data/golden/prepare.py

Env vars (all optional, with defaults):
    DATA_VEPYR_DIR  Directory containing full HG002 inputs, VEP outputs, caches, FASTA
    CACHE_SRC   Full parquet cache dir
    VCF_SRC     HG002 chr1 VCF (gzipped)
    GOLDEN_SRC  VEP 115 golden output VCF
    FASTA_SRC   GRCh38 reference FASTA
"""

import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).parent


def expand_path(path: str) -> Path:
    return Path(os.path.expandvars(path)).expanduser()


# Defaults
DATA_VEPYR_DIR = expand_path(
    os.environ.get("DATA_VEPYR_DIR", "$HOME/workspace/data_vepyr")
)
CACHE_SRC = expand_path(
    os.environ.get("CACHE_SRC", str(DATA_VEPYR_DIR / "115_GRCh38_vep"))
)
VCF_SRC = expand_path(
    os.environ.get(
        "VCF_SRC",
        str(DATA_VEPYR_DIR / "HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"),
    )
)
GOLDEN_SRC = expand_path(
    os.environ.get(
        "GOLDEN_SRC",
        str(DATA_VEPYR_DIR / "HG002_annotated_wgs_everything_hgvs_vep.vcf"),
    )
)
FASTA_SRC = expand_path(
    os.environ.get(
        "FASTA_SRC",
        str(DATA_VEPYR_DIR / "Homo_sapiens.GRCh38.dna.primary_assembly.fa"),
    )
)

SAMPLE_SIZE = 100
REGION_BUFFER = 10_000  # extra bp beyond last variant


def main():
    print(f"Preparing golden test data in {SCRIPT_DIR}")

    # 1. Sample first N variants from input VCF
    sampled = SCRIPT_DIR / "input.vcf"
    print(f"1. Sampling {SAMPLE_SIZE} variants from {VCF_SRC}...")
    with subprocess.Popen(
        ["gzcat" if sys.platform == "darwin" else "zcat", VCF_SRC],
        stdout=subprocess.PIPE,
        text=True,
    ) as proc:
        with open(sampled, "w") as out:
            n = 0
            for line in proc.stdout:
                if line.startswith("#"):
                    out.write(line)
                elif n < SAMPLE_SIZE:
                    out.write(line)
                    n += 1
                else:
                    break
    print(f"   Sampled {n} variants")

    # 2. Normalize with bcftools
    normalized = SCRIPT_DIR / "input_norm.vcf"
    print("2. Normalizing with bcftools norm -m -both...")
    subprocess.run(
        ["bcftools", "norm", "-m", "-both", "-o", str(normalized), str(sampled)],
        check=True,
    )
    os.remove(sampled)
    os.rename(normalized, sampled)

    # Bgzip + tabix
    gz = SCRIPT_DIR / "input.vcf.gz"
    subprocess.run(["bgzip", "-c", str(sampled)], stdout=open(gz, "wb"), check=True)
    subprocess.run(["tabix", "-p", "vcf", str(gz)], check=True)
    print(f"   -> {gz}")

    # 3. Get position range and exact sample keys
    positions = []
    chroms = set()
    sample_keys = set()
    with open(sampled) as f:
        for line in f:
            if not line.startswith("#"):
                fields = line.rstrip("\n").split("\t")
                chroms.add(fields[0])
                positions.append(int(fields[1]))
                sample_keys.add((fields[0], fields[1], fields[3], fields[4]))

    if len(chroms) != 1:
        sys.exit(f"Expected one sampled chromosome, found: {sorted(chroms)}")

    chrom = next(iter(chroms))
    fasta_chrom = chrom.removeprefix("chr")
    _start, end = min(positions), max(positions) + REGION_BUFFER
    print(f"   Position range: {chrom}:{min(positions)}-{max(positions)}")

    # 4. Extract golden subset
    golden = SCRIPT_DIR / "golden.vcf"
    print("4. Extracting golden subset for sampled variants...")
    with open(GOLDEN_SRC) as src, open(golden, "w") as dst:
        for line in src:
            if line.startswith("#"):
                dst.write(line)
            else:
                fields = line.rstrip("\n").split("\t")
                key = (fields[0], fields[1], fields[3], fields[4])
                if key in sample_keys:
                    dst.write(line)
    n_golden = sum(1 for line in open(golden) if not line.startswith("#"))
    print(f"   -> {n_golden} golden variants")

    # 5. Trim reference FASTA
    ref = SCRIPT_DIR / "reference.fa"
    print(f"5. Trimming reference FASTA to {fasta_chrom}:1-{end}...")
    subprocess.run(
        ["samtools", "faidx", FASTA_SRC, f"{fasta_chrom}:1-{end}"],
        stdout=open(ref, "w"),
        check=True,
    )
    # Fix header from >chrom:1-N to >chrom.
    content = ref.read_text()
    content = content.replace(f">{fasta_chrom}:1-{end}", f">{fasta_chrom}", 1)
    ref.write_text(content)
    subprocess.run(["samtools", "faidx", str(ref)], check=True)
    print(f"   -> {ref} ({ref.stat().st_size // 1024} KB)")

    # 6. Create trimmed parquet cache
    cache_dir = SCRIPT_DIR / "cache"
    print("6. Creating trimmed parquet cache...")

    entities = {
        "variation": ("start", 0, end),
        "transcript": ("start", 0, end),
        "exon": ("start", 0, end),
        "translation_sift": ("start", 0, end),
        "regulatory": ("start", 0, end),
        "motif": ("start", 0, end),
    }

    for entity, (col, lo, hi) in entities.items():
        src = Path(CACHE_SRC) / entity / "chr1.parquet"
        dst_dir = cache_dir / entity
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "chr1.parquet"
        if not src.exists():
            print(f"   {entity}: source not found, skipping")
            continue
        table = pq.read_table(str(src))
        mask = pc.and_(pc.greater_equal(table[col], lo), pc.less_equal(table[col], hi))
        table = table.filter(mask)
        pq.write_table(table, str(dst))
        print(f"   {entity}: {table.num_rows} rows ({dst.stat().st_size // 1024} KB)")

    # translation_core: filter by transcript IDs from trimmed transcript table
    tx_table = pq.read_table(str(cache_dir / "transcript" / "chr1.parquet"))
    tx_ids = pa.array(tx_table.column("stable_id").to_pylist())
    src = Path(CACHE_SRC) / "translation_core" / "chr1.parquet"
    dst_dir = cache_dir / "translation_core"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "chr1.parquet"
    table = pq.read_table(str(src))
    mask = pc.is_in(table["transcript_id"], value_set=tx_ids)
    table = table.filter(mask)
    pq.write_table(table, str(dst))
    print(
        f"   translation_core: {table.num_rows} rows ({dst.stat().st_size // 1024} KB)"
    )

    print(
        f"\nDone. Total test data: {sum(f.stat().st_size for f in SCRIPT_DIR.rglob('*') if f.is_file()) // 1024} KB"
    )


if __name__ == "__main__":
    main()
