# LazyFrame carries the input's INFO/FORMAT columns, and `pb.sink_vcf` can write it

**Status:** design approved 2026-09-20, §8 settled as recommended (metadata carry-through
rides on the collision-fix PR; `skip_csq` stays `True`; ships as 0.8.0). Nothing implemented;
implementation plan not yet written. Depends on the
collision fix in `docs/superpowers/plans/2026-09-20-input-info-annotation-name-collision.md`.

## 1. Goal

One lazy plan from VCF to filtered, annotated VCF:

```python
import polars as pl, polars_bio as pb, vepyr

lf = vepyr.annotate("in.vcf.gz", cache, reference_fasta=fa, skip_csq=False)
q = lf.filter(
    (pl.col("chrom") == "chr22") & pl.col("start").is_between(20_000_000, 25_000_000)
    & pl.col("IMPACT").list.eval(pl.element().is_in(["HIGH", "MODERATE"])).list.any()
)
pb.sink_vcf(q, "filtered.vcf.bgz")
```

The output must be a VCF a downstream tool accepts in place of `filter_vep`'s: the
input's INFO keys and genotypes, plus `CSQ` with a correct `Format:` header. This is
what makes the paper's LazyFrame arm comparable with `vep | filter_vep`, and it brings
the LazyFrame in line with `output_vcf`, which already keeps the input's fields.

**Non-goals.** Byte parity between `pb.sink_vcf` output and `filter_vep` output (see
§6); `filter_vep --only_matched` (rewriting CSQ per entry); a VCF writer inside vepyr.

## 2. What the spike established (2026-09-20, HG002 chr22, polars-bio 0.35.1)

| Question | Result |
|---|---|
| Does the engine carry input columns? | Yes — `annotate_vep`'s output schema is input fields + `CSQ` + typed columns. vepyr alone drops them (`src/annotate.rs:500`, `Some(vec![])` twice). With `None` the frame's first 30 columns equal `pb.scan_vcf`'s in name, type and order. |
| Does `config_meta` survive `.filter().select()` on a `register_io_source` frame? | Yes. |
| Does the writer reject the typed list columns? | No — a column with no `info_fields` entry is ignored; output is byte-identical with or without a `select()`. |
| Correctness | 64/64 filtered records value-identical to the `output_vcf` path, key order ignored. |
| Cost of carried-but-unread columns | 1.31 s vs 1.24 s on chr22 (~5 %), single sample. |
| Does the probe schema carry the header? | **No.** Field metadata (`bio.vcf.field.*`) survives; schema-level metadata (contigs, filters, sample names, version) is empty, so `extract_all_schema_metadata()` detects no format. |

Defects found on the way: the writer emits FORMAT in header order, not GT-first
(reproduces on a pure `pb.scan_vcf → pb.sink_vcf` round trip); `##FILTER`, `##fileDate`
and provenance lines are not written; INFO is written in schema order.

## 3. Design

### 3.1 API (vepyr)

```python
annotate(..., info_fields: list[str] | None = None, format_fields: list[str] | None = None)
```

`None` means **all**, a list selects, `[]` means none — the meaning `pb.scan_vcf`
gives the same arguments. This is a **default change**: a frame today has 8 variant
columns before `most_severe_consequence`; afterwards it also has the input's INFO and
FORMAT columns, in the input header's order, in the position a `scan_vcf` frame has
them. Multi-sample inputs get the nested `genotypes` struct. `output_vcf=` ignores
both arguments (it always keeps everything, as VEP does).

Column names are the header ids. An id that matches an annotation column arrives
renamed by the engine — `INFO_<id>`, or `fmt_<id>` for single-sample FORMAT — per the
collision fix; annotation columns never change name.

### 3.2 Reader-side pruning (vepyr)

`_batch_source` builds a fresh annotator per collect and already knows the columns the
query reads (`with_columns` ∪ predicate roots). It passes the intersection of those
with the carried fields to `create_annotator`, so a query that touches no input
column parses no INFO and no samples — the fourth pushdown, beside limit, region and
flag inference. Without a projection (`collect()`, `sink_vcf`) every carried field is
read, which is what was asked for. Required rather than optional: the ~5 % measured on
one sample grows with sample count.

### 3.3 Header metadata (functions + vepyr)

- **functions:** `AnnotateProvider::new` builds its schema with
  `Schema::new_with_metadata(fields, vcf_schema.metadata().clone())`. Same function
  the collision fix edits; cannot move a VCF byte (the sink builds its header from the
  source file). Proposed to ride on the fix PR — see §8 Q1.
- **vepyr**, at frame creation, when `polars_bio` imports (extra `vepyr[polars-bio]`;
  otherwise skipped silently and the frame behaves as today):
  1. `extract_all_schema_metadata(probe.schema)["format_specific"]["vcf"]` → header
     dict (`info_fields`, `format_fields`, `sample_names`, `version`, `contigs`,
     `filters`, `alt_definitions`) — polars-bio stays the single owner of the keys;
  2. `info_fields["CSQ"] = {number ".", type "String", description "Consequence
     annotations from Ensembl VEP. Format: <fields>"}`, `<fields>` being the frame's
     CSQ sub-field list including plugin fields — the string the engine sink writes;
     an input's own `CSQ` entry is thereby replaced, as VEP does;
  3. key every `info_fields` / `format_fields` entry by its VCF id, not its column
     name, so a renamed column (`INFO_AF`) is declared as `AF` (§3.4);
  4. `pb.set_source_metadata(lf, format="vcf", path=vcf, header=header)` and
     `set_coordinate_system(lf, zero_based=False)`.

With `skip_csq=True` (the default) there is no `CSQ` column and step 2 is skipped:
`sink_vcf` then writes the input's fields with no annotation. Documented, with
`skip_csq=False` shown in every `sink_vcf` example; not an error, because filtering a
VCF by annotation without writing the annotation is a legitimate use.

### 3.4 Writer (polars-bio, plus one line in formats)

Corrected while planning (2026-09-20), from reading the write path:

1. **`INFO_<id>` → `<id>`, in polars-bio.** polars-bio classifies INFO columns with
   `info_meta.get(name)` (`src/write.rs:380`). On a frame holding both `INFO_AF` (the
   input's field) and `AF` (VEP's frequency) that test picks the *annotation* column and
   would write VEP's value under the input's key — wrong before the formats writer is
   reached. So polars-bio projects the DataFrame first: a column named `INFO_<id>` whose
   `<id>` the header declares is selected `AS <id>`, and the bare `<id>` column is
   dropped from the write. Scoped to declared ids, so a real field called `INFO_x` is
   left alone; name-based because Polars strips Arrow field metadata. `fmt_<id>` is
   already handled there (`src/write.rs:418-450`).
2. **GT first in FORMAT, in formats** (`serializer.rs`, `build_format_and_samples`): the
   caller's order is header order whenever per-record keys are not carried, which also
   affects vepyr's `output_vcf` with `preserve_record_layout=False`.
3. **`##FILTER`, `##ALT`, `##fileformat`, in polars-bio.** The formats header builder
   already emits them from schema metadata (`header_builder.rs:93-133`); polars-bio only
   forwards contigs today.

### 3.5 Plugin-name collisions (vepyr)

Plugin columns are created in Python from `CSQ`, so the engine never sees their names;
`__init__.py:1697-1700` raises on a clash with an existing column. Once INFO is
carried, an input INFO named `CADD_PHRED` or `ClinVar` would hit that. Rule: the
plugin column keeps the bare name and the input column is renamed `INFO_<id>` in
Python — the writer's name rule (§3.4) covers it, so one policy holds everywhere.

## 4. Components and ownership

| Unit | Repo | Responsibility |
|---|---|---|
| schema metadata carry-through | functions | input schema-level metadata on the output schema |
| `info_fields`/`format_fields` plumbing | vepyr `src/annotate.rs`, `lib.rs`, `_core.pyi` | pass field selection to `VcfTableProvider` |
| `_vcf_metadata.py` (new, small) | vepyr | build the header dict, CSQ entry, id re-keying; lazy polars-bio import |
| pruning | vepyr `_batch_source` | carried fields ∩ columns read |
| writer | polars-bio | `INFO_<id>` projection, FILTER/ALT/fileformat forwarding, formats pin bump |
| GT-first | formats | one arm of `build_format_and_samples` |

## 5. Errors

- `info_fields`/`format_fields` naming an id absent from the header → `ValueError`
  listing the available ids (checked against the probe schema, before any annotation).
- polars-bio absent → no metadata, no error; `pb.sink_vcf` cannot be called anyway.
- `workers > 1` keeps its indexed-input requirement; carried columns change nothing.

## 6. What `sink_vcf` output will not match

INFO key order is schema order, not per-record input order, and an all-missing FORMAT
key is dropped — `preserve_record_layout` exists only on `output_vcf`. Provenance
lines (`##fileDate`, tool command lines, `##datafusion-bio-function-vep`) are absent.
Correctness is therefore asserted on field values, not bytes; users who need byte
parity with VEP use `output_vcf`. The paper's supplement states this.

## 7. Testing

- vepyr: schema equals `pb.scan_vcf`'s prefix (names, types, order) on the chr22
  fixture; `info_fields=[]` reproduces today's schema exactly; pruning — a query reading
  no carried column opens the VCF with none (assert on the annotator arguments);
  metadata present and surviving `filter`/`select`; `sink_vcf` round trip value-equal
  to `output_vcf` on the same filter (order-insensitive), for single- and multi-sample
  inputs and for an `AF`-colliding input; plugin-name collision.
- polars-bio: `INFO_AF` → `AF=` with one header line and the annotation `AF` not written;
  a real `INFO_x` id left alone; FILTER/ALT/fileformat kept. formats: GT-first.
- Existing gates: LazyFrame↔VCF parity script (`e2e-testing/scripts/lazyframe_workers_parity.py`)
  and the strict md5 gate must not move — the engine's VCF output is untouched.
- Docs: `docs/dataframes.md` schema listing, the "INFO … not in the frame" sentence,
  a new "Writing a filtered VCF" section; `docs/api.md`.

## 8. Open questions

1. Fold the one-line schema-metadata carry-through into the collision-fix PR (same
   function, no output change) rather than a second functions PR? *Recommended: yes.*
2. `skip_csq` stays `True` by default, with the `sink_vcf` consequence documented
   (§3.3)? The alternative — default `False` — roughly doubles `collect()` peak memory.
   *Recommended: keep `True`.*
3. The default change (§3.1) alters `lf.collect()`'s width for every user. Release as
   0.8.0 with a changelog note and `info_fields=[]`, `format_fields=[]` as the
   documented way back? *Recommended: yes.*
