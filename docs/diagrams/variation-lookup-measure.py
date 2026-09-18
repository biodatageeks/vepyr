#!/usr/bin/env python3
"""Measure every quantity that appears in ``variation-lookup.py``'s MEASURED table.

The figure claims that clustering the variation shard by tier, rather than by
position alone, makes the lookup's locate read touch far less data. This script
is where that claim comes from. It replays one real annotation buffer against a
real shard twice: once in the order the shard actually ships, and once against a
position-only counterfactual built from the same rows and the same page sizes.

Nothing here is estimated. Run it, then paste the printed values into MEASURED.

Inputs
------
--shard      one per-chromosome variation Parquet file from a built cache
--pages      CSV of the position column's page boundaries, see below
--vcf        a bgzipped, tabix-indexed VCF to draw the probe buffer from
--chrom      contig name as it appears in the VCF
--buffer     records in the buffer (5000 matches the engine's lookup slice)

Producing --pages
-----------------
pyarrow does not expose the Parquet offset index, so the page boundaries come
from a short Rust program built against the same ``parquet`` crate the engine
uses. Drop this into ``datafusion/bio-function-vep/examples/`` in a checkout of
biodatageeks/datafusion-bio-functions and run
``cargo run --example dump_page_index --features parquet-cache -- <shard> <out.csv>``::

    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::serialized_reader::ReadOptionsBuilder;
    use std::fs::File;
    use std::io::{BufWriter, Write};

    fn main() {
        let path = std::env::args().nth(1).unwrap();
        let out = std::env::args().nth(2).unwrap();
        let reader = SerializedFileReader::new_with_options(
            File::open(&path).unwrap(),
            ReadOptionsBuilder::new().with_page_index().build(),
        ).unwrap();
        let md = reader.metadata();
        let leaf = md.file_metadata().schema_descr().columns().iter()
            .position(|c| c.name() == "start").unwrap();
        let oi = md.offset_index().unwrap();
        let mut w = BufWriter::new(File::create(out).unwrap());
        writeln!(w, "first_row,end_row").unwrap();
        let mut global = 0i64;
        for rg in 0..md.num_row_groups() {
            let locs = oi[rg][leaf].page_locations();
            let rg_rows = md.row_group(rg).num_rows();
            for p in 0..locs.len() {
                let end = if p + 1 < locs.len() { locs[p + 1].first_row_index } else { rg_rows };
                writeln!(w, "{},{}", global + locs[p].first_row_index, global + end).unwrap();
            }
            global += rg_rows;
        }
    }

Method
------
A probe hits a page when the page holds a cached row whose ``start`` equals the
probe position. The locate read decodes every row of every page it hits, so the
cost reported here is the summed row span of those pages, not the match count.
The counterfactual keeps the same rows and the same page row-counts and only
changes the order, so the two columns differ by clustering alone.
"""

from __future__ import annotations

import argparse
import subprocess
import sys

import numpy as np
import pyarrow.parquet as pq


def probe_positions(vcf: str, chrom: str, buffer_size: int) -> np.ndarray:
    """The first `buffer_size` records of `chrom`, as sorted unique positions."""
    out = subprocess.run(
        ["bcftools", "query", "-r", chrom, "-f", "%POS\n", vcf],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        sys.exit(f"bcftools failed: {out.stderr.strip()[:200]}")
    pos = np.fromiter((int(x) for x in out.stdout.split()), dtype=np.int64)
    if pos.size == 0:
        sys.exit(f"no records for {chrom} in {vcf}")
    return np.unique(pos[:buffer_size])


def matching_rows(order: np.ndarray, probes: np.ndarray) -> np.ndarray:
    """Row indices, in `order`'s layout, whose start is one of `probes`."""
    idx = np.argsort(order, kind="stable")
    vals = order[idx]
    lo = np.searchsorted(vals, probes, "left")
    hi = np.searchsorted(vals, probes, "right")
    hits = [idx[a:b] for a, b in zip(lo, hi) if b > a]
    return np.concatenate(hits) if hits else np.empty(0, dtype=np.int64)


def layout_cost(rows, first_row, end_row):
    """Pages the locate read opens, and the rows it decodes doing so."""
    pages = np.unique(np.searchsorted(end_row, rows, "right"))
    decoded = int((end_row[pages] - first_row[pages]).sum())
    return len(pages), decoded


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--shard", required=True)
    ap.add_argument("--pages", required=True)
    ap.add_argument("--vcf", required=True)
    ap.add_argument("--chrom", default="chr22")
    ap.add_argument("--buffer", type=int, default=5000)
    args = ap.parse_args()

    table = pq.read_table(args.shard, columns=["start", "tier"])
    start = table.column("start").to_numpy(zero_copy_only=False).astype(np.int64)
    tier = table.column("tier").to_numpy(zero_copy_only=False).astype(np.int8)

    pages = np.loadtxt(args.pages, delimiter=",", skiprows=1, dtype=np.int64)
    first_row, end_row = pages[:, 0], pages[:, 1]

    probes = probe_positions(args.vcf, args.chrom, args.buffer)
    rows = matching_rows(start, probes)
    tier0_rows = int((tier == 0).sum())

    shipped_pages, shipped_decoded = layout_cost(rows, first_row, end_row)
    counter_pages, counter_decoded = layout_cost(
        matching_rows(np.sort(start), probes), first_row, end_row
    )

    page_of = np.searchsorted(end_row, rows, "right")
    t0_pages = np.unique(page_of[rows < tier0_rows])
    t1_pages = np.unique(page_of[rows >= tier0_rows])
    t0_rows = int((rows < tier0_rows).sum())
    t1_rows = int((rows >= tier0_rows).sum())

    print(f"shard             {args.shard}")
    print(f"  rows            {len(start):,}")
    print(f"  position pages  {len(pages):,}")
    print(f"  tier 0 rows     {tier0_rows:,}  ({100 * tier0_rows / len(start):.1f}%)")
    print(f"buffer            first {args.buffer:,} {args.chrom} records of {args.vcf}")
    print(f"  probes          {len(probes):,}")
    print(f"  matching rows   {len(rows):,}")
    print()
    print(
        f"{'row layout':26} {'pages read':>11} {'rows scanned':>13} {'matches per page':>17}"
    )
    for label, p, d in (
        ("sorted by position only", counter_pages, counter_decoded),
        ("as shipped, tier 0 first", shipped_pages, shipped_decoded),
    ):
        print(f"{label:26} {p:11,} {d:13,} {len(rows) / p:17.1f}")
    print(
        f"\nratios: {counter_pages / shipped_pages:.1f}x fewer pages, "
        f"{counter_decoded / shipped_decoded:.1f}x fewer rows scanned"
    )
    print()
    print(
        f"  tier 0: {len(t0_pages):4} pages, {t0_rows:,} rows -> {t0_rows / len(t0_pages):.1f} per page"
    )
    print(
        f"  tier 1: {len(t1_pages):4} pages, {t1_rows:,} rows -> {t1_rows / max(len(t1_pages), 1):.1f} per page"
    )
    print(f"  common share of matches: {100 * t0_rows / len(rows):.0f}%")


if __name__ == "__main__":
    main()
