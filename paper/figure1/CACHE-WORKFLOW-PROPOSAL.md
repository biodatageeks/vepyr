# Figure 1 — cache build, anatomy and lookup

Current layout proposal following the decision to remove panels B (Quality
validation) and D (Contracts + filtering), retain C (Performance), and expose
the three lookup steps in the main figure. This supersedes the layout in
`A3-CACHE-SIMPLIFIED-PROPOSAL.md`; earlier drafts remain available for comparison.

## Layout

- **A1 — Annotation architecture.** Retains the main annotation path. Input
  normalization and assembly/FASTA/cache consistency, typed rows and aligned
  lists, preserved VCF INFO/FORMAT plus CSQ, and streaming/materialization
  semantics are integrated as short labels from the former D panel.
- **A2 — Cache build.** The example shard is `chr22.parquet`, including the
  plugin directory example, consistent with A3 and A4. The expanded Parquet
  output block shows four schematic pages and the two native Parquet page
  indexes: ColumnIndex (position min–max per data page) and OffsetIndex
  (row → page location). The inset is explicitly scoped to
  variation/plugin lookup.
- **A3 — Cache anatomy.** Keys, payload and page index sit above a monochrome
  comparison of position-sorted and frequency-grouped layouts. Warm/cold tiers,
  common/rare hits and the 920-to-220 position-page comparison are retained.
- **A4 — Cache lookup.** A wider lower panel shows page candidates → locate
  file-row offsets → take projected payload columns, followed by allele
  matching in A1. Three small tables follow the same two probe positions.
  Short labels link page candidates to page-index metadata and take to
  OffsetIndex plus the selected rows.
- **C — Performance.** The existing embedded vector chart and its pending-data
  placeholders are preserved. Its letter is retained for comparison with the
  preceding draft; final manuscript lettering can be assigned after layout
  approval.

The full canvas remains 2400 × 2200. A4 occupies the lower left; Performance is
on the lower right. No B or D panel appears in this proposal.

## Draft manuscript caption

**(A1) Annotation architecture.** Normalized input variants are annotated by the
native engine using a partitioned Parquet cache and reference sequence. Arrow
batches feed a Polars LazyFrame or annotated VCF output. Variant annotations
can be filtered through the DataFrame API or SQL; per-consequence predicates
require handling the aligned annotation lists. Solid arrows indicate data
flow and the dashed return arrow indicates query demand. **(A2) Cache build.**
Ensembl cache conversion and declarative plugin ingestion produce separate
chromosome-partitioned Parquet datasets and associated metadata; chromosome 22
is used throughout the cache example. Lookup shards use small data pages,
with records sorted by position within each frequency tier. Native Parquet
page indexes store position bounds (ColumnIndex) and page locations with
first-row indices (OffsetIndex), enabling selective page reads.
**(A3) Cache anatomy.** A variation shard
contains lookup keys, annotation payload and page-index metadata. Records are
grouped by frequency tier and sorted by position within each tier. Both warm
and cold tiers remain queryable. Filled and open markers denote common and
rare lookup hits; heavy page outlines denote pages read. The diagram shows
the same illustrative hits in both layouts and is not to scale. For one
5,000-record HG002 chromosome-22 buffer using the GRCh38 merged VEP 116 cache,
the source illustration reports 920 position pages for a position-sorted
counterfactual versus 220 for frequency grouping (76% fewer), yielding the same
6,178 position matches. **(A4) Cache lookup.** Page position ranges identify
candidate pages in both tier runs. Reading the position column within these
pages locates exact matches and records their file-row offsets. A subsequent
take uses the selected rows and OffsetIndex to retrieve required payload
columns from the relevant pages. Retrieved records
then undergo allele matching against the input variants in A1. Tables show
illustrative excerpts; file-row offsets, page identifiers, record identifiers
and allele frequencies in A4 are schematic. **(C) Performance.** The existing
performance comparison is retained with its original scope and pending-data
annotations.

## Technical interpretation

The page counts in A3 are transcribed from Marek's `image-panelC.png`, not a new
benchmark. They refer only to the position-column locate pass, not to payload
page reads, transferred bytes, I/O-call count or end-to-end runtime. The 76%
label is `100 * (1 - 220 / 920)`, rounded to the nearest integer. The source
calls the position-only layout counterfactual. The original Performance SVG
is reused byte-for-byte as an embedded image; no performance data are rerun.

Warm and cold are frequency tiers on disk within one shard, not separate
storage devices or memory/cache states. Warm membership is assigned by
position using a maximum global AF threshold of 1% and the one-base
neighbourhood. Other alleles at those positions can therefore be warm even
when rare. Tier boundaries need not coincide with page or row-group
boundaries; A3 deliberately places the seam inside a page. Its illustrative
tier widths and hit proportions do not encode the measured 5.4% of cache rows
or approximately 96% of hits in the source.

In A4 the query positions are 16,050,075 and 16,050,213 on chr22. P0 and P2
overlap those positions; P1 and P3 do not. The schematic order illustrates
two independently sorted tier runs. Exact position matches occur at file
rows 1001, 1002 and 8501. Those same offsets appear in the payload table.
The two alleles A/G and A/T at one position make clear why positional lookup
precedes allele matching. The identifiers v1/v2/v3 and AFs are invented
example values, not reported biological observations.

Only selected rows and requested columns are returned by take, but decoding
and I/O still operate at Parquet page granularity. The figure does not imply
random access to individual compressed values without reading their pages.
The pipeline shown applies to variation/plugin lookup; transcript context
retains the distinct scan/interval-tree path in A1.

## Page-index implementation behind A2 / A4

Checked against the branch's pinned bio-function-vep v0.20.1 commit
`8b1abd09478b97aed6ce42d2a5b60912b315855b` and Parquet 58.0.0.

- [`point_lookup_writer_properties`](https://github.com/biodatageeks/datafusion-bio-functions/blob/8b1abd09478b97aed6ce42d2a5b60912b315855b/datafusion/bio-function-vep/src/parquet_cache/write.rs#L46)
  enables page statistics and small page targets (4 KiB / 512 rows), disables
  dictionary encoding, and uses ZSTD level 3. It declares sort columns; the
  build pipeline physically supplies records in the required order.
- The [plugin writer](https://github.com/biodatageeks/datafusion-bio-functions/blob/8b1abd09478b97aed6ce42d2a5b60912b315855b/datafusion/bio-function-vep/src/plugin_cache/write.rs#L52)
  reuses this profile with `(tier, start)`. The profile also serves
  `translation_sift` with its own lookup key. Context entities use a separate
  scan-oriented profile, so the inset does not describe every core shard.
- [`PageDir::build`](https://github.com/biodatageeks/datafusion-bio-functions/blob/8b1abd09478b97aed6ce42d2a5b60912b315855b/datafusion/bio-function-vep/src/parquet_cache/page_dir.rs#L63)
  combines key-column min/max from ColumnIndex with first-row indices from
  OffsetIndex, deriving file-wide page row ranges. This directory is built
  when the lookup opens the shard. `resolve_ranges` searches the sorted tier
  runs, handles positions spanning pages and the tier seam, and merges adjacent
  candidate ranges.
- Here position min–max means the smallest and largest `start` value in one
  data page of the `start` column, within a chromosome shard. It is not the
  genomic interval of an individual variant. A probe inside the bounds makes
  the page a candidate; the page need not contain that exact probe value.
- The [variation reader](https://github.com/biodatageeks/datafusion-bio-functions/blob/8b1abd09478b97aed6ce42d2a5b60912b315855b/datafusion/bio-function-vep/src/parquet_cache/variation_lookup.rs#L121)
  reads the candidate pages' `start` column to discover exact row offsets.
  The payload pass supplies these offsets as a RowSelection and projects
  required columns. The Parquet reader uses OffsetIndex page locations to
  translate selected rows to page byte ranges; nearby reads may be coalesced.

As specified by [Parquet Page Index](https://parquet.apache.org/docs/file-format/pageindex/),
the indexes are per-column structures stored near the footer; column metadata
holds their locations. They are neither separate index files nor a list of
exact variant matches. A2's four page symbols are illustrative, and page
boundaries may differ across columns. Exact matches remain the result of
A4's middle step. Writer size limits are targets, not measured page sizes of
the cache used for A3's comparison. The 76% reduction in A3 describes frequency
grouping, not an isolated index-on versus index-off experiment.

## Details for Methods / supplementary material

The main panel omits cursor bookkeeping, page coalescing, exact writer
parameters and the full frequency-distribution table. These can accompany
Marek's expanded lookup example in a supplementary figure, including an
unmatched input record and assignment back to the original buffer.

The former D panel's commands and fuller contracts belong in Methods or the
supplementary caption:

```bash
bcftools norm -f ref.fa -m -both input.vcf.gz
```

BGZF plus TBI/CSI is required for indexed region access and the parallel input
path. Assembly, reference FASTA and cache release must agree. VCF output
preserves original INFO/FORMAT and adds CSQ; LazyFrame output uses typed
variant rows and aligned annotation lists. `sink_*` streams output whereas
`collect()` materializes the result. A variant-level rare-or-unknown predicate
can be written as:

```sql
SELECT * FROM variants
WHERE gnomADg_AF < 0.01 OR gnomADg_AF IS NULL
```

Per-CSQ filters require aligned list handling (for example aligned explode
or an appropriate any predicate); ontology expansion must be explicit.

## Deliverables and regeneration

- `figure1-cache-workflow-proposal.{svg,png,drawio}` — current full proposal.
- `a2-cache-build-proposal.{svg,png,drawio}` — standalone build and page indexes.
- `a3-cache-anatomy-proposal.{svg,png,drawio}` — standalone anatomy.
- `a4-cache-lookup-proposal.{svg,png,drawio}` — standalone lookup.
- `build_cache_workflow_proposal.py` — generator; reuses drawing primitives
  from `build_figure.py` and the earlier monochrome anatomy comparison from
  `build_cache_simplified_proposal.py`.

```bash
python3 -B paper/figure1/build_cache_workflow_proposal.py --render
```

Chrome/Chromium is required only for PNG rendering. SVG text and diagram
elements are editable; Drawio uses native cells except for the unchanged
embedded Performance SVG. The original `figure1.*` artifacts and earlier
proposal files are preserved alongside this design proposal.
