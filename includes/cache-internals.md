The Ensembl `variation` and `translation_sift` entities and custom plugin caches
share Parquet writer settings for point lookups. The position-based layout below
describes variation and plugin shards; `translation_sift` is sorted by its
encoded `key`. Page indexes let a lookup read the pages that could contain the
queried positions.

### Parquet storage

The writer properties are tuned for random point lookups, not scans:

| Property | Value | Why |
|---|---|---|
| Compression | ZSTD, level 3 | Good ratio; fast enough to decode per page. |
| Dictionary encoding | **disabled** | Avoids a per-take dictionary load; ZSTD recovers the ratio (the no-dict file is actually smaller). |
| Data page size target | 4 KiB, best effort | Evaluated between write batches; actual pages can be larger. |
| Write batch size | 1,024 values (parquet-rs default) | Sets the granularity at which page limits are checked. |
| Observed variation page row count | Typically ~1,024 rows | Measured in shipped variation shards; not a fixed page size or a guaranteed upper bound. |
| Statistics | **Page-level** | Emits `ColumnIndex` + `OffsetIndex` in the footer — the read-side position→page directory. |
| Row group size | 1,000,000 rows | Large groups keep footer/metadata overhead low; the page index gives intra-group resolution. |
| Sorting columns | `(tier, start)` | Physical clustering — see [Sorting within a shard](#sorting-within-a-shard). |

The shipped merged-116 `chr22.parquet` has a median of **1,024 rows per page**
for `start`, `allele_string`, and `dbsnp_ids`, measured from the `OffsetIndex`.
For `start`, individual pages range from 5 to 1,535 rows. Page boundaries vary
with column values, nested lists, incoming batches, and row-group tails;
lookups use the indexes rather than assuming a fixed page length.
See [the page-layout measurements](https://github.com/biodatageeks/datafusion-bio-functions/issues/249).

!!! note "Write-batch defaults and page-row limits are different"
    The current writer requests a 512-row page limit, but parquet-rs checks it
    between write batches of 1,024 values. That explains the typical 1,024-row
    pages; the configured 512 is not a physical page-size guarantee.

    In [parquet-rs 58.0.0](https://github.com/apache/arrow-rs/blob/58.0.0/parquet/src/file/properties.rs),
    the default **write batch** is 1,024, while the default **page-row limit** is
    20,000. Removing `set_data_page_row_count_limit(512)` therefore changes the
    writer's behaviour. A 40,960-row probe with the same 4 KiB target and
    dictionary encoding disabled produced 1,024-row integer pages in both cases,
    but Boolean pages grew from 1,024 to 20,480 rows when the row limit was
    omitted. A 1,024-row target must be set explicitly; it remains best effort.

### Sorting within a shard

Rows within each shard are physically sorted by **`(tier, start)`** and written in
that order, and the sort is recorded in the Parquet `SortingColumn` metadata:

1. **By `tier` first** — all **warm** rows (tier `0`) are written before all
   **cold** rows (tier `1`). This clusters common variants into a contiguous
   run of pages, so a buffer of common-variant lookups touches a small, dense
   region instead of pages scattered across the file.
2. **By `start` within each tier** — each tier's run is ascending by genomic
   `start`. Ascending, non-overlapping `start` ranges per page are what make the
   `ColumnIndex` (per-page min/max of `start`) an effective pruning directory:
   resolving a query position to its candidate page(s) is a binary-search-like
   metadata lookup, and coalescing adjacent pages into one read is cheap.

Because the file is split into a warm block then a cold block (each
independently `start`-sorted), a single `start` value can appear in **both**
blocks; the lookup resolves candidate pages across both. Writing warm-first
keeps the hot working set contiguous — the whole point of the tier.

### Page index → the `PageDir`

Since page-level statistics are enabled, each shard's footer carries a
`ColumnIndex` (per-page min/max of `start`) and an `OffsetIndex` (per-page byte
offset + row range). At open time the reader builds a **`PageDir`** over the
`start` leaf column from these indexes. Resolving a set of query positions to the
minimal set of candidate page row-ranges is then a metadata-only operation — no
column data is read until the ranges are known.

### Row groups

Point-lookup shards use row groups of up to 1,000,000 rows. Within each group,
the page indexes provide lookup resolution at the actual page boundaries, with
typical variation pages of ~1,024 rows. Large chromosomes span many groups:
the shipped merged-116 variation cache has 145 for chr1 and 16 for chr22.

### Runtime lookup — async reader + monotonic cursor, in batches

Annotation runs in position-ordered buffers. For each buffer the runtime does one
**page-scoped, three-phase take** per shard, reading only that buffer's candidate
pages:

1. **Resolve** — the buffer's sorted, de-duplicated `start` positions are mapped
   through the `PageDir` to candidate page **row-ranges**. Metadata only; no data
   read.
2. **Locate** — a `start`-only projected read over just those pages (a
   `RowSelection` built from the ranges) streams `start` values back in batches
   through a **`CoalescingAsyncReader`** — an async Parquet reader that merges
   nearby page byte-ranges (within a 64 KiB gap) into single I/O calls. A
   **monotonic row-offset cursor** advances exactly one step per streamed row,
   staying in lockstep with the selection, and records the exact file offset of
   every row whose `start` is in the buffer's probe set. The cursor only moves
   forward, so there is no back-seeking.
3. **Take** — a final projected read at those exact offsets pulls just the payload
   columns for the matched rows into one compact `RecordBatch`.
