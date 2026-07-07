Every cache — the Ensembl variation/transcript/… entities and the custom plugin
caches — uses the same point-lookup-optimized Parquet layout, so a lookup reads
only the handful of pages that could contain the queried positions rather than
scanning the whole file.

### Parquet storage

The writer properties are tuned for random point lookups, not scans:

| Property | Value | Why |
|---|---|---|
| Compression | ZSTD, level 3 | Good ratio; fast enough to decode per page. |
| Dictionary encoding | **disabled** | Avoids a per-take dictionary load; ZSTD recovers the ratio (the no-dict file is actually smaller). |
| Data page size | ≤ 4 KiB | Small pages → fine-grained page index → a lookup touches minimal bytes. |
| Data page row count | ≤ 512 rows | Bounds how many rows a single page decode yields. |
| Statistics | **Page-level** | Emits `ColumnIndex` + `OffsetIndex` in the footer — the read-side position→page directory. |
| Row group size | 1,000,000 rows | Large groups keep footer/metadata overhead low; the page index gives intra-group resolution. |
| Sorting columns | `(tier, start)` | Physical clustering — see [Sorting within a shard](#sorting-within-a-shard). |

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

Shards use 1,000,000-row row groups. Row groups bound the footer metadata size;
within a group the small (≤ 512-row) pages plus the page index provide the actual
point-lookup resolution. A whole chromosome is typically one or a few row groups.

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
   nearby page byte-ranges (within a 512 KiB gap) into single I/O calls. A
   **monotonic row-offset cursor** advances exactly one step per streamed row,
   staying in lockstep with the selection, and records the exact file offset of
   every row whose `start` is in the buffer's probe set. The cursor only moves
   forward, so there is no back-seeking.
3. **Take** — a final projected read at those exact offsets pulls just the payload
   columns for the matched rows into one compact `RecordBatch`.
