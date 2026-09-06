# Genomic-range predicate pushdown for the LazyFrame path

**Date:** 2026-09-06
**Status:** Approved (design)
**Implementation repos:** `datafusion-bio-functions` (`datafusion/bio-function-vep`), then `vepyr`
**Related:** `docs/superpowers/specs/2026-08-28-chrxy-core-annotation-parity-design.md`
(golden parity gates), PR #79 (projection-driven flag inference in `_batch_source`),
engine design 2026-06-25 §5/§7 (grid-aligned warm-up on the sharded VCF path)

## Problem

`vepyr.annotate()` returns a Polars `LazyFrame` backed by an IO plugin. Polars hands
the plugin the projected columns, the pushed predicate and the row limit. Today the
plugin uses the projection to prune annotation flags (PR #79) and the limit as a SQL
`LIMIT`, but the predicate is only applied *after* annotation, batch by batch. A query
such as

```python
vepyr.annotate(vcf, cache).filter(
    (pl.col("chrom") == "chr22") & pl.col("start").is_between(20_000_000, 25_000_000)
).collect()
```

annotates the whole file and throws most of the result away. The VCF reader underneath
(`datafusion-bio-format-vcf`) already turns `chrom`/`start`/`end` filters into tabix or
CSI index seeks, and the engine already scans each contig through per-contig filters, so
the pieces exist. Nothing connects the Polars predicate to them.

This design adds that connection: genomic-range predicates are extracted from the
Polars predicate, passed to the engine as `regions`, and honoured by the engine before
annotation, so that only the selected contigs are prepared and only the selected rows
are looked up and annotated.

## Scope

In scope:

- Predicate extraction for `chrom`, `start` and `end` on the LazyFrame path.
- An engine option `regions` honoured on the streaming (LazyFrame) path for every cache
  source: Ensembl, Merged and RefSeq.
- Exact results on Merged and RefSeq, where annotation depends on Ensembl VEP's
  per-buffer state, by aligning range cuts to the input-buffer grid and replaying
  warm-up rows.
- Index detection with a warning when the input has no tabix/CSI index.
- Unit tests, golden-fixture parity tests and a real-data parity gate.

Out of scope, each a separate follow-up:

- An explicit `annotate(regions=...)` argument. Its design is written down in the
  "Deferred" section below so the follow-up is a wiring change, not a new design; the
  engine option this delivery adds is what it needs.
- `regions` on the sharded VCF-output path (`output_vcf` with `workers>1`). The engine
  rejects that combination with a plan error in this delivery.
- `workers>1` on the LazyFrame path. On the current engine pin a LazyFrame collect with
  `workers=2` fails with "parallel annotation (threads>1) requires a VCF shard
  context". That is a pre-existing gap; the docstring and `docs/performance.md`
  over-promise and need a separate fix.
- Predicates on any column other than `chrom`, `start`, `end`. They keep being applied
  in Polars after annotation.

## Contract

For every predicate `p`:

```python
annotate(...).filter(p).collect()  ==  annotate(...).collect().filter(p)
```

including row order. Pushdown only narrows the *input* to a superset of the rows `p`
can accept; Polars still evaluates the full predicate on every batch as it does today.
Extraction fails open: a predicate shape it does not recognise contributes no
restriction. The only observable differences are speed and the index warning.

Coordinates are the LazyFrame's own `start` and `end` columns: 1-based closed, as the
VCF provider emits them (`coordinate_system_zero_based = false` in
`create_streaming_annotator`). No conversion happens on either side.

## Why ranges need no base-pair padding, and why Merged/RefSeq need grid alignment

Everything an annotation depends on besides the record itself comes from the cache,
looked up by contig and by 1 Mb cache region around the variants the engine sees:
transcripts within the up/downstream distance, regulatory and motif features,
co-located variants. Trimming the VCF changes none of those lookups, so a plain cut is
exact for the Ensembl source. The engine's byte-budget sharding on the VCF-output path
already relies on this and is byte-identical at every worker count.

Merged and RefSeq caches are different because of Ensembl VEP's HGNC donation.
`merge_features()` copies the gene symbol and HGNC id from an Ensembl transcript onto a
RefSeq transcript of the same gene that lacks them and mutates the cached transcript
object in place. Those objects live in a per-region cache that persists across
consecutive input buffers, so a RefSeq transcript's output in one buffer depends on
which variants earlier buffers loaded. The engine replicates this through
`persisted_buffer_transcripts` and, on the sharded path, reconstructs it at a seam by
replaying whole buffers before the seam ("warm-up") within a bounded reach of
`max transcript span + 1 Mb`. A range cut is a seam, so it needs the same treatment.

## Python design (`vepyr`)

### Extraction module `src/vepyr/_regions.py`

Pure functions, no engine access, unit-tested in isolation. Public surface inside the
package:

```python
Region = dict  # {"chrom": str, "start": int | None, "end": int | None}

def extract_regions(predicate: pl.Expr, contigs: list[str]) -> list[Region] | None
```

Return values:

- `None`: no pushdown. The predicate has no recognised genomic restriction.
- `[]`: provably empty. The IO source yields nothing and never opens the engine.
- a non-empty list: regions in 1-based closed coordinates; `start`/`end` may be `None`
  for an open side.

Algorithm over `predicate.meta.serialize(format="json")`:

1. Split the root on `Or` into disjunct groups, and each group on `And` into conjuncts.
   A region list written as `(chr1 & range) | (chr2 & range)` therefore becomes two
   regions.
2. For each conjunct, classify by `meta.root_names()`:
   - exactly `{"chrom"}`: **evaluate**, do not interpret. After a shape gate that
     allows only `Column`, `Literal`, `BinaryExpr` (any operator), and `Function` nodes
     whose function is `Boolean.IsIn`, `Boolean.Not` or any `StringExpr`, the conjunct
     is run as `pl.DataFrame({"chrom": contigs}).filter(conjunct)`. The surviving
     names are the conjunct's chrom set. This covers `==`, `!=`, `is_in`,
     `str.starts_with` and their boolean combinations without decoding Polars' opaque
     list literals, and it needs no knowledge of contig spellings: `chrom == "1"`
     against a `chr1` file evaluates to an empty set, which is the right answer.
   - a subset of `{"start", "end"}`: **interpret**. Recognised shapes are
     `Column op Literal(int)` in either orientation with `op` in
     `Eq, Gt, GtEq, Lt, LtEq`, and `Function Boolean.IsBetween` on a column with two
     integer literals, honouring `closed` (`both`, `left`, `right`, `none`).
     `start` bounds both sides. `end <= b` and `end < b` give an upper bound (because
     `start <= end`); `end >= a` gives nothing. `Eq` sets both bounds. Anything else,
     including float literals, marks the conjunct unrecognised.
   - any other root-name set (other columns, or genomic columns mixed with others in
     one conjunct): ignored, it stays a residual for Polars.
3. A group's chrom set is the intersection of its chrom conjuncts (all contigs when
   there are none). Its bounds are the intersection of its range conjuncts. A group
   with no recognised genomic conjunct, or with an unrecognised genomic conjunct,
   disables pushdown for the whole predicate (`None`), because an `Or` over it could
   accept any row.
4. A group whose chrom set is empty, or whose lower bound exceeds its upper bound,
   contributes no regions. If every group is empty the result is `[]`.
5. Regions are the union over groups of one region per chrom in the group's chrom set,
   carrying the group's bounds. Overlap merging is the engine's job.

The Polars serialization format is documented as unstable. The module pins the node
shapes it reads with unit tests against the installed Polars version, and any
`KeyError`/`TypeError` while walking the tree is caught and reported as `None`.

### Header contigs

One new `_core` function:

```python
def vcf_contigs(path: str) -> list[str]
```

It opens a `VcfTableProvider` and returns the contig ids from the schema metadata the
provider already emits (`bio.vcf.contigs`, header order). Using the provider keeps
plain, bgzip and BCF inputs uniform. `annotate()` calls it lazily on the first collect
whose predicate names a genomic column and memoises the result in its closure.

### Index detection and warning

Index presence is `<vcf>.tbi` or `<vcf>.csi` next to the input, the same rule the
engine's VCF sink uses. When regions were extracted and no index exists, `_batch_source`
emits once per `annotate()` call:

```
RuntimeWarning: region filter on '<vcf>' without a tabix/CSI index (<vcf>.tbi or
.csi): the whole file is parsed and filtered before annotation. Compress with bgzip
and index with tabix for seek-based reads.
```

No warning is emitted when the predicate has no genomic restriction, and none when the
extraction yields `[]`.

### Wiring in `_batch_source`

After the projection-driven flag inference:

```python
regions = None
if predicate is not None and genomic_columns & set(predicate.meta.root_names()):
    regions = extract_regions(predicate, contigs())
    if regions == []:
        return
    if regions is not None:
        warn_if_unindexed()
        engine_opts["regions"] = regions
```

Everything downstream is unchanged: the Polars-side `filter(predicate)` still runs on
every batch, `n_rows` still becomes `LIMIT`, and plugin column handling is untouched.
`_core.pyi` gains the `vcf_contigs` stub.

## Rust bridge (`vepyr/src`)

- `lib.rs`: register `vcf_contigs`. It builds the provider exactly as
  `create_streaming_annotator` does, reads `VCF_CONTIGS_KEY` from the schema metadata
  and returns the ids. Errors map to `PyRuntimeError` with the "Failed to open VCF"
  prefix already used.
- `annotate.rs`: no change. `regions` travels inside `options_json`.
- `Cargo.toml`/`Cargo.lock`: pin bump to the engine revision that carries the feature.

## Engine design (`datafusion-bio-function-vep`)

### Option

`regions` in `options_json`: a JSON array of objects `{chrom, start?, end?}` in 1-based
closed coordinates. Validation at plan time (`AnnotateFunction::call` /
`AnnotateProvider::new`), each failure a `DataFusionError::Plan`:

- `chrom` is a non-empty string; `start`/`end`, when present, are integers `>= 1`
  with `start <= end`.
- `regions` together with a VCF shard context (sharded VCF output) is rejected.

Chrom names are matched to the VCF's contigs through `contig_alias_set`, the same
mapping used to match VCF contigs to cache shards, so `1`/`chr1` and `M`/`MT`/`chrM`
spellings are accepted. A region whose chrom is not in the VCF selects nothing.

Per contig, the intervals are merged into disjoint sorted **runs**; open sides merge as
expected (an open `start` means the contig start).

### Contig selection

In `scan_with_transcript_engine_partitioned`, after `discover_vcf_contigs` and
`select_cache_backed_contigs`, contigs without a run are dropped, VCF order preserved.
An empty selection returns `EmptyExec` (an empty result, not an error), and cache
identity validation only touches selected contigs.

### Runs replace the single pass per contig

The streaming state machine (`StreamState::AnnotatingContig` and the prepare/activate
halves) currently prepares one context per contig and drains one lookup stream. It
gains a run queue per contig:

- `prepare_contig_data` is unchanged: transcripts, exons, translations, regulatory and
  motif context are loaded once per contig.
- `activate_contig_lookups` activates the *first* run; on run completion the state
  machine activates the next run of the same contig before moving to `CleaningUp`.
- Each run builds its lookup with the filter
  `chrom = c AND start >= scan_lo AND start < scan_hi` (bounds omitted when open), so
  the VCF provider seeks per run when indexed and applies its record-level filters when
  not. For Ensembl runs, `scan_lo`/`scan_hi` are the run's own bounds. For stateful
  runs they are the slice's scan window (below).
- Per-run worker state is reset between runs: `window_buffer`,
  `input_buffer_accumulator`, `next_input_buffer_id`, `persisted_buffer_transcripts`.
  The colocated map is per contig and is kept.
- Without `regions` a contig has exactly one run with open bounds and no slice, which
  is today's behaviour. The count pass is not run and no warm-up happens, so the
  default path pays nothing.

### Stateful runs on Merged and RefSeq

When the cache source is Merged or RefSeq and the contig has bounded runs:

1. Run the existing count pass (`count_contig_buffer_boundaries`, positions only) once
   per contig to obtain the buffer grid `boundaries` (length `B+1`).
2. Map each run `[lo, hi]` to whole buffers: `bk = max{k : boundaries[k].pos <= lo}`
   (0 when none) and `bk1 = min{k : boundaries[k].pos > hi}` (`B` when none). Buffer
   `bk` may contain rows below `lo`; including it is a superset and the output filter
   trims them. Runs whose buffer ranges overlap or touch are merged.
3. Build one `WorkerGridSlice` per merged run with the existing `build_grid_slices`
   over the cut pair `[bk, bk1]` and the contig's `overlap_width_bp`. That gives
   `scan_lo_pos`, `skip_leading_rows`, `warm_up_start_row`, `emit_start_row`,
   `emit_end_row` and `scan_hi_pos` exactly as the sharded workers get them.
4. In the lookup drain of the state machine, add the gate the sharded worker
   (`spawn_annotation_from_lookup_sharded`) already implements: drop
   `skip_leading_rows` position ties at the scan floor, divert rows with rank below
   `emit_start_row` into a warm-up list, replay that list through
   `warm_up_worker_state` before the first window of the run is dispatched, and
   rank-stop at `emit_end_row` by marking the run's lookup done. Warm-up rows also get
   `set_probe_floor_pos(emit_start_pos)` so their variation probe is skipped.
5. Windows are still cut at exactly `input_buffer_size` input units from
   `emit_start_row`, which is a buffer boundary, so the run's windows coincide with
   the contig-global grid.

The warm-up reconstructs the persisted state a serial run would hold at the seam because
that state only depends on buffers within `overlap_width_bp` of the seam, the same
argument that makes the sharded path byte-identical.

### Output filter

Rows emitted by a run are trimmed to `start in [lo, hi]` inside the engine for every
source: stateful runs annotate whole buffers, and indexed reads are overlap-based. When
several original intervals were merged into one run, the trim uses the union of those
intervals. `LIMIT` counts rows after the trim.

### Cost profile

| Item | Cost |
|---|---|
| Ensembl run | one index seek per run, no extra pass |
| Count pass (stateful contigs only) | one positional scan of the contig: seconds with an index, a full-file parse without |
| Warm-up (stateful runs only) | the buffers covering `max transcript span + 1 Mb` before the run, replayed state-only |
| Unindexed input | full parse with record-level filtering; annotation still restricted |

## Testing

### Engine unit tests (Rust)

- `regions` parsing: valid shapes, each validation error, alias matching, merging of
  overlapping/adjacent/open intervals.
- Buffer mapping: `bk`/`bk1` for runs starting mid-buffer, exactly on a boundary, on a
  position tie across a boundary, with open sides, and beyond the contig end.
- Run merging over the buffer grid.
- Parity on the synthetic HGNC-donation fixture behind
  `warmup_reconstructs_serial_persisted_state`: a run starting mid-buffer after the
  donation must produce the same rows as the full serial pass filtered to the run.
- The default path (no `regions`) still produces one open run and skips the count pass.

### vepyr unit tests (`tests/test_regions.py`, `tests/test_annotate.py`)

- Extraction table: `==`, `!=`, `is_in`, `str.starts_with`, boolean combinations of
  chrom conjuncts, each `start`/`end` comparison in both orientations, `is_between`
  with every `closed` mode, mixed non-genomic conjuncts, top-level `Or` groups,
  unsatisfiable groups (`[]`), chrom spelled unlike the file (`[]`), float literals and
  mixed-column conjuncts (`None`).
- With `_create_annotator` monkeypatched: `regions` appears in the options only when a
  genomic predicate is pushed; `[]` short-circuits without creating an annotator.
- Warning: emitted for the plain `input.vcf` fixture, absent for `input.vcf.gz` with
  its `.tbi`, emitted once across two collects of the same LazyFrame.
- `vcf_contigs` returns the fixture's header contigs for plain and bgzip inputs.

### Golden-fixture parity tests (`tests/test_annotate.py`, Ensembl and merged caches)

With `buffer_size` lowered (for example 7) so the 100-variant fixture spans many
buffers, for each predicate in a table covering single chrom, chrom plus range starting
mid-buffer, several ranges on one contig, an `Or` of two contigs, and an empty result:

```python
pushed = annotate(...).filter(p).collect()
reference = annotate(...).collect().filter(p)
assert pushed.equals(reference)
```

`everything=True` with the reference FASTA, so HGVS, co-located and plugin columns are
all exercised.

### Real-data parity gate (`e2e-testing/scripts/region_pushdown_parity.py`)

A runner following the `comparison` package pattern, using the HG002 GRCh38 benchmark
VCF and the converted caches under `~/workspace/data_vepyr/cache/`:

1. Cut a chr22 slice with `bcftools view -r chr22`, `bgzip` and `tabix`, and keep a
   plain-text copy without an index.
2. For each cache source (Ensembl, Merged, RefSeq) and for a list of regions that start
   mid-buffer (for example `chr22:20000000-25000000`, `chr22:30000000-30100000`, two
   disjoint ranges, and an open-ended range), compute the reference as
   `annotate(slice).collect()` followed by the Polars filter, and the candidate as
   `annotate(slice).filter(p).collect()`.
3. Assert frame equality including row order, and report wall time for both so the
   speedup is visible.
4. Repeat one region on the unindexed copy: results must still be equal and the
   warning must be raised.

This is the release gate for the feature; the fixture tests are the iteration loop.

## Documentation

- `docs/quickstart.md` or `docs/performance.md`: a "Region filters" section with the
  contract, the supported predicate shapes, the coordinate system, the index warning,
  and the note that Merged/RefSeq ranges cost a positional count pass per contig.
- `annotate()` docstring: a short pointer to that section.
- Correct the `workers` documentation to say the LazyFrame path is serial until the
  parallel LazyFrame path lands.

## Delivery

1. Engine PR on `datafusion-bio-functions`: option, runs, stateful slices, output
   filter, unit tests. Reviewed and merged first; its SHA is the pin.
2. vepyr PR: pin bump, `_regions.py`, `vcf_contigs`, wiring, warning, unit and fixture
   tests, docs, and the e2e runner. The real-data gate runs before merge and its
   numbers go into the PR description.

## Deferred: explicit `annotate(regions=...)`

Not part of this delivery. The design is recorded here because the follow-up is only
Python wiring on top of the engine option above.

New keyword argument, default `None`:

```python
regions: str | tuple | list[str | tuple] | None = None
```

Accepted elements, normalised by a `_regions.parse_regions()` helper:

- a string in tabix/samtools syntax, 1-based closed: `"chr22"`,
  `"chr22:20000000-25000000"`, `"chr22:20000000-"` (open end), `"chr22:-25000000"`
  (open start). Thousands separators are not accepted.
- a tuple `(chrom,)`, `(chrom, start, end)` with `None` for an open side.

A single string or tuple is treated as a one-element list. Validation raises
`ValueError` at call time: empty chrom, non-integer or `< 1` bounds, `start > end`,
an empty list. Intervals on the same chrom are merged in Python so the engine and the
tests see one canonical list. Chrom spellings are passed through unchanged; the engine
resolves `1`/`chr1` aliases, and a chrom absent from the VCF selects nothing.

Contract, for an explicit list `R`:

```python
annotate(..., regions=R).collect()  ==  annotate(...).collect().filter(start_in(R))
```

where `start_in(R)` keeps rows whose `chrom` is in `R` and whose `start` lies inside
one of `R`'s intervals for that chrom. The same holds record for record on the VCF
output path. Unlike a predicate, an explicit list restricts the *source*, so rows
outside it never reach the frame or the file.

On the LazyFrame path the list is the base restriction: it goes into `opts["regions"]`
at call time and reaches every collect. When a collect also carries a genomic
predicate, `_batch_source` intersects the predicate's regions with the explicit list
(an `_regions.intersect_regions()` helper, per chrom, interval by interval) and sends
the intersection; an empty intersection short-circuits like `[]` above.

On the VCF-output path the list goes into the same options and the serial sink honours
it through the shared streaming scan (`annotate_to_vcf` reads through
`AnnotateProvider::scan`). `regions` with `output_vcf` and `workers > 1` raises
`ValueError("regions with output_vcf requires workers=1")` before the engine is
opened, matching the engine's own rejection. The output header is the input header
unchanged; only the records are restricted. The index warning fires at `annotate()`
call time for an explicit list, so the VCF-output path warns too.

Tests for the follow-up: `parse_regions` and `intersect_regions` tables, forwarding on
both paths with the annotator and the VCF writer monkeypatched, the `workers>1`
rejection, the fixture parity table run through `regions=R` for the frame and for the
VCF output (records compared to the whole-file output filtered by `POS`), and a fifth
leg of the real-data gate doing the same on the chr22 slice.
