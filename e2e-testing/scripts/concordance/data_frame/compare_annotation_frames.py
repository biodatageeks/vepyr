#!/usr/bin/env python3
"""Compare VEP and vepyr VCF annotations as unordered Polars data frames."""

from __future__ import annotations

import argparse
import contextlib
import gzip
import os
import re
from pathlib import Path

import polars as pl
import polars_bio as pb
from polars.testing import assert_frame_equal


KEY_COLUMNS = ["chrom", "pos", "ref", "alt", "csq_entry"]
ASSERT_COLUMNS = ["annotation_hash_0", "annotation_hash_1", "count"]


def open_text(path: Path):
    if path.suffix in {".gz", ".bgz", ".bgzf"}:
        return gzip.open(path, "rt")
    return path.open()


def csq_fields(path: Path) -> list[str]:
    with open_text(path) as handle:
        for line in handle:
            if line.startswith("##INFO=<ID=CSQ"):
                match = re.search(r'Format: ([^"]+)', line)
                if match:
                    return match.group(1).split("|")
    raise ValueError(f"No INFO/CSQ Format header found in {path}")


def annotation_rows(path: Path) -> pl.LazyFrame:
    return (
        pb.scan_vcf(str(path), info_fields=["CSQ"], format_fields=[])
        .select(["chrom", "start", "ref", "alt", "CSQ"])
        .rename({"start": "pos"})
        .explode("CSQ")
        .rename({"CSQ": "csq_entry"})
        .filter(pl.col("csq_entry").is_not_null() & (pl.col("csq_entry") != ""))
        .select(
            pl.col("chrom").cast(pl.Utf8),
            pl.col("pos").cast(pl.UInt64),
            pl.col("ref").cast(pl.Utf8),
            pl.col("alt").cast(pl.Utf8),
            pl.col("csq_entry").cast(pl.Utf8),
        )
    )


def annotation_multiset(path: Path) -> pl.LazyFrame:
    return (
        annotation_rows(path)
        .with_columns(
            pl.struct(KEY_COLUMNS).hash(seed=0).alias("annotation_hash_0"),
            pl.struct(KEY_COLUMNS).hash(seed=1).alias("annotation_hash_1"),
        )
        .group_by(["annotation_hash_0", "annotation_hash_1"])
        .len()
        .rename({"len": "count"})
        .select(ASSERT_COLUMNS)
        .sort(["annotation_hash_0", "annotation_hash_1"])
    )


def collect_quiet(lf: pl.LazyFrame) -> pl.DataFrame:
    with open(os.devnull, "w") as devnull, contextlib.redirect_stderr(devnull):
        return lf.collect(engine="streaming")


def mismatch_examples(
    vep: pl.DataFrame, vepyr: pl.DataFrame, max_examples: int
) -> pl.DataFrame:
    return (
        vep.join(
            vepyr,
            on=["annotation_hash_0", "annotation_hash_1"],
            how="full",
            coalesce=True,
        )
        .with_columns(
            pl.col("count").fill_null(0).cast(pl.Int64).alias("vep_count"),
            pl.col("count_right").fill_null(0).cast(pl.Int64).alias("vepyr_count"),
        )
        .drop(["count", "count_right"])
        .with_columns((pl.col("vep_count") - pl.col("vepyr_count")).alias("delta"))
        .filter(pl.col("delta") != 0)
        .sort(["annotation_hash_0", "annotation_hash_1"])
        .head(max_examples)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("vep_vcf", type=Path)
    parser.add_argument("vepyr_vcf", type=Path)
    parser.add_argument("--allow-field-differences", action="store_true")
    parser.add_argument("--max-examples", type=int, default=20)
    args = parser.parse_args()

    vep_fields = csq_fields(args.vep_vcf)
    vepyr_fields = csq_fields(args.vepyr_vcf)
    vepyr_field_set = set(vepyr_fields)
    shared_fields = [field for field in vep_fields if field in vepyr_field_set]
    vep_only = sorted(set(vep_fields) - set(vepyr_fields))
    vepyr_only = sorted(set(vepyr_fields) - set(vep_fields))

    if (vep_only or vepyr_only) and not args.allow_field_differences:
        print("CSQ field sets differ")
        print(f"vep_only\t{','.join(vep_only) if vep_only else '-'}")
        print(f"vepyr_only\t{','.join(vepyr_only) if vepyr_only else '-'}")
        return 1

    vep = collect_quiet(annotation_multiset(args.vep_vcf))
    vepyr = collect_quiet(annotation_multiset(args.vepyr_vcf))

    print(f"shared_csq_fields\t{len(shared_fields)}")
    print("reader\tpolars-bio")
    print("semantic_input_columns\tchrom,pos,ref,alt,csq_entry")
    print("asserted_dataframe_columns\tannotation_hash_0,annotation_hash_1,count")
    print(f"vep_only_fields\t{','.join(vep_only) if vep_only else '-'}")
    print(f"vepyr_only_fields\t{','.join(vepyr_only) if vepyr_only else '-'}")
    print(f"vep_annotation_rows\t{vep['count'].sum()}")
    print(f"vepyr_annotation_rows\t{vepyr['count'].sum()}")
    print(f"vep_unique_annotation_rows\t{vep.height}")
    print(f"vepyr_unique_annotation_rows\t{vepyr.height}")
    print("comparator\tpolars.testing.assert_frame_equal")

    try:
        assert_frame_equal(vep, vepyr, check_exact=True)
    except AssertionError as exc:
        examples = mismatch_examples(vep, vepyr, args.max_examples)
        print(f"mismatched_unique_annotation_rows\t{examples.height}+")
        print("\nMismatch examples:")
        print(examples)
        print(f"\nAssertionError: {exc}")
        return 1

    print("MATCH")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
