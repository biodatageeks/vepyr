#!/usr/bin/env python3
"""Compare VEP and vepyr annotation rows in VCF/CSQ order."""

from __future__ import annotations

import argparse
import gzip
import re
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import polars as pl
import polars_bio as pb
from polars.testing import assert_frame_equal


KEY_COLUMNS = ["chrom", "pos", "ref", "alt", "canonical_csq_entry"]


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


def normalized_csq_field(struct_name: str, field: str) -> pl.Expr:
    return (
        pl.col(struct_name)
        .struct.field(field)
        .fill_null("")
        .cast(pl.Utf8)
        .str.split("&")
        .list.sort()
        .list.join("&")
    )


def annotation_rows(
    path: Path, source_fields: list[str], compare_fields: list[str]
) -> pl.LazyFrame:
    csq_struct = (
        pl.col("csq_entry")
        .str.split_exact("|", len(source_fields) - 1)
        .struct.rename_fields(source_fields)
        .alias("__csq")
    )
    canonical_csq = pl.concat_str(
        [normalized_csq_field("__csq", field) for field in compare_fields],
        separator="|",
    ).alias("canonical_csq_entry")

    return (
        pb.scan_vcf(str(path), info_fields=["CSQ"], format_fields=[])
        .select(["chrom", "start", "ref", "alt", "CSQ"])
        .rename({"start": "pos"})
        .explode("CSQ")
        .rename({"CSQ": "csq_entry"})
        .filter(pl.col("csq_entry").is_not_null() & (pl.col("csq_entry") != ""))
        .with_columns(csq_struct)
        .select(
            pl.col("chrom").cast(pl.Utf8),
            pl.col("pos").cast(pl.UInt64),
            pl.col("ref").cast(pl.Utf8),
            pl.col("alt").cast(pl.Utf8),
            canonical_csq,
        )
    )


def iter_batches(lf: pl.LazyFrame, chunk_size: int) -> Iterator[pl.DataFrame]:
    yield from lf.collect_batches(
        chunk_size=chunk_size,
        maintain_order=True,
        engine="streaming",
    )


def next_nonempty(batches: Iterator[pl.DataFrame]) -> pl.DataFrame | None:
    for batch in batches:
        if batch.height:
            return batch
    return None


def mismatch_examples(
    vep: pl.DataFrame,
    vepyr: pl.DataFrame,
    row_offset: int,
    max_examples: int,
) -> pl.DataFrame:
    paired = vep.with_row_index("__row").join(
        vepyr.with_row_index("__row"),
        on="__row",
        suffix="_vepyr",
    )
    differs = pl.any_horizontal(
        [pl.col(col) != pl.col(f"{col}_vepyr") for col in KEY_COLUMNS]
    )
    return (
        paired.filter(differs)
        .with_columns((pl.col("__row") + row_offset).alias("annotation_row"))
        .select(
            ["annotation_row"]
            + KEY_COLUMNS
            + [f"{col}_vepyr" for col in KEY_COLUMNS]
        )
        .head(max_examples)
    )


def compare_ordered(
    vep_rows: pl.LazyFrame,
    vepyr_rows: pl.LazyFrame,
    chunk_size: int,
    progress_every: int,
    max_examples: int,
) -> tuple[bool, int]:
    vep_batches = iter_batches(vep_rows, chunk_size)
    vepyr_batches = iter_batches(vepyr_rows, chunk_size)
    vep = next_nonempty(vep_batches)
    vepyr = next_nonempty(vepyr_batches)
    compared_rows = 0
    next_progress = progress_every
    started_at = time.monotonic()

    while vep is not None or vepyr is not None:
        if vep is None or vepyr is None:
            print("annotation_row_count_mismatch")
            print(f"compared_annotation_rows\t{compared_rows}")
            return False, compared_rows

        rows = min(vep.height, vepyr.height)
        vep_part = vep.head(rows)
        vepyr_part = vepyr.head(rows)

        try:
            assert_frame_equal(vep_part, vepyr_part, check_exact=True)
        except AssertionError as exc:
            print(f"first_checked_annotation_row\t{compared_rows}")
            print("\nMismatch examples:")
            print(mismatch_examples(vep_part, vepyr_part, compared_rows, max_examples))
            print(f"\nAssertionError: {exc}")
            return False, compared_rows

        compared_rows += rows
        if progress_every and compared_rows >= next_progress:
            elapsed = time.monotonic() - started_at
            rate = compared_rows / elapsed if elapsed else 0
            print(
                "progress\t"
                f"compared_annotation_rows={compared_rows}\t"
                f"elapsed_seconds={elapsed:.1f}\t"
                f"annotation_rows_per_second={rate:.1f}",
                file=sys.stderr,
                flush=True,
            )
            while compared_rows >= next_progress:
                next_progress += progress_every

        vep = vep.slice(rows)
        vepyr = vepyr.slice(rows)

        if not vep.height:
            vep = next_nonempty(vep_batches)
        if not vepyr.height:
            vepyr = next_nonempty(vepyr_batches)

    return True, compared_rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("vep_vcf", type=Path)
    parser.add_argument("vepyr_vcf", type=Path)
    parser.add_argument("--allow-field-differences", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=250_000)
    parser.add_argument("--progress-every", type=int, default=1_000_000)
    parser.add_argument("--max-examples", type=int, default=5)
    args = parser.parse_args()

    vep_fields = csq_fields(args.vep_vcf)
    vepyr_fields = csq_fields(args.vepyr_vcf)
    vepyr_field_set = set(vepyr_fields)
    compare_fields = [field for field in vep_fields if field in vepyr_field_set]
    vep_only = sorted(set(vep_fields) - set(vepyr_fields))
    vepyr_only = sorted(set(vepyr_fields) - set(vep_fields))

    if (vep_only or vepyr_only) and not args.allow_field_differences:
        print("CSQ field sets differ")
        print(f"vep_only\t{','.join(vep_only) if vep_only else '-'}")
        print(f"vepyr_only\t{','.join(vepyr_only) if vepyr_only else '-'}")
        return 1

    print(f"shared_csq_fields\t{len(compare_fields)}")
    print("reader\tpolars-bio")
    print("comparison\tordered streaming")
    print("semantic_input_columns\tchrom,pos,ref,alt,CSQ")
    print("canonicalization\tsplit CSQ rows, sort ampersand-delimited values per CSQ field")
    print("asserted_dataframe_columns\tchrom,pos,ref,alt,canonical_csq_entry")
    print("row_multiplicity\tpreserved as repeated rows")
    print(f"chunk_size\t{args.chunk_size}")
    print(f"progress_every\t{args.progress_every}")
    print(f"vep_only_fields\t{','.join(vep_only) if vep_only else '-'}")
    print(f"vepyr_only_fields\t{','.join(vepyr_only) if vepyr_only else '-'}")
    print("comparator\tpolars.testing.assert_frame_equal")

    ok, compared_rows = compare_ordered(
        annotation_rows(args.vep_vcf, vep_fields, compare_fields),
        annotation_rows(args.vepyr_vcf, vepyr_fields, compare_fields),
        args.chunk_size,
        args.progress_every,
        args.max_examples,
    )
    print(f"compared_annotation_rows\t{compared_rows}")

    if not ok:
        return 1

    print("MATCH")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
