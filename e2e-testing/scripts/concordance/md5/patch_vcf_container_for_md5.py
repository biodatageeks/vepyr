#!/usr/bin/env python3
"""Restore input VCF container fields before MD5 comparison.

This is intentionally separate from canonical_md5_vcf.py. It fixes container
formatting that vepyr should eventually preserve directly:

1. use ID/QUAL/FILTER from the normalized input VCF,
2. use FORMAT/sample columns from the normalized input VCF,
3. keep INFO/CSQ annotations from the vepyr output.

The script does not touch INFO/CSQ annotations.
"""

from __future__ import annotations

import argparse
import gzip
import sys
from pathlib import Path


def open_text(path: Path, mode: str = "rt"):
    if str(path).endswith(".gz"):
        return gzip.open(path, mode)
    return path.open(mode)


def patch_record(
    container: list[str], source: list[str], line_no: int
) -> tuple[list[str], dict[str, int]]:
    if source[:2] + source[3:5] != container[:2] + container[3:5]:
        raise ValueError(
            f"record key mismatch at data line {line_no}: "
            f"container={container[:5]} source={source[:5]}"
        )

    stats = {
        "id_changed": 0,
        "qual_changed": 0,
        "filter_changed": 0,
        "format_or_sample_changed": 0,
    }
    patched = source[:]

    for idx, key in ((2, "id_changed"), (5, "qual_changed"), (6, "filter_changed")):
        if patched[idx] != container[idx]:
            stats[key] = 1
            patched[idx] = container[idx]

    if len(container) > 8 or len(source) > 8:
        if len(container) != len(source):
            raise ValueError(
                f"sample column count mismatch at data line {line_no}: "
                f"container has {len(container)} columns, source has {len(source)}"
            )
        if patched[8:] != container[8:]:
            stats["format_or_sample_changed"] = 1
            patched[8:] = container[8:]

    return patched, stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "container_vcf",
        type=Path,
        help="normalized input VCF whose non-INFO container fields VEP preserves",
    )
    parser.add_argument("source_vcf", type=Path, help="vepyr VCF to patch")
    parser.add_argument("output_vcf", type=Path, help="patched vepyr VCF")
    args = parser.parse_args()

    totals = {
        "records": 0,
        "id_changed": 0,
        "qual_changed": 0,
        "filter_changed": 0,
        "format_or_sample_changed": 0,
    }

    args.output_vcf.parent.mkdir(parents=True, exist_ok=True)
    with open_text(args.container_vcf) as container_handle, open_text(args.source_vcf) as source_handle, args.output_vcf.open("w") as out:
        container_records = (
            line.rstrip("\n").split("\t")
            for line in container_handle
            if not line.startswith("#")
        )
        for line in source_handle:
            if line.startswith("#"):
                out.write(line)
                continue

            totals["records"] += 1
            source = line.rstrip("\n").split("\t")
            try:
                container = next(container_records)
            except StopIteration as exc:
                raise ValueError("source VCF has more records than container VCF") from exc

            patched, stats = patch_record(
                container, source, totals["records"]
            )
            for key, value in stats.items():
                totals[key] += value
            out.write("\t".join(patched) + "\n")

        try:
            next(container_records)
        except StopIteration:
            pass
        else:
            raise ValueError("container VCF has more records than source VCF")

    print(
        "patched_vcf_container\t"
        + "\t".join(f"{key}={value}" for key, value in totals.items()),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
