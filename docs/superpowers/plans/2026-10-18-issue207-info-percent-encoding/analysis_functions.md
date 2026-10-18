# vepyr #207 — functions (engine) analysis

Sources: datafusion-bio-functions **v0.23.1** (detached worktree `$S/engine`, b960112). All
`file:line` below are v0.23.1 unless marked. Paths are relative to `datafusion/bio-function-vep/`.

Line drift (checked with `git show`, no checkout touched):
- **origin/master** (4704ff9; the remote has no `main`, default branch is `master`): `src/vcf_sink.rs`,
  `src/plugin_cache/csq.rs`, both `Cargo.toml`s are **byte-identical** to v0.23.1 (`git diff --stat`
  empty), so every vcf_sink/csq.rs/Cargo line below holds there. `src/annotate_provider.rs` moved
  (+520/-122 from #272/#273): csq_escape 2742→2813, HGVSp `%3D` 4192→4295, CSQ field 3950→4040,
  reserved set 3943→4033, input passthrough 8110→8265, CSQ block `,` 6911→7081,
  VEP_SOURCE_FIELD_NAME_KEY 872→873, input_fields_for_output 901→902.
- **46d8c20** (human checkout, branch fix/mnv-allele-trim-parity) is `v0.20.1-4`, i.e. it **predates
  v0.23.1** (merge-base 8b1abd0 = v0.20.1; 29 tag commits not in it). Line numbers do NOT hold:
  batch_to_vcf_lines calls at vcf_sink.rs:208/1825, no `restore_input_columns`, no `INFO_<id>` rename
  (vepyr #121 not present), CSQ field at annotate_provider.rs:3719, HGVSp `%3D` at :3944. Do not
  use that checkout as a base.

## 1. Where the behaviour lives (engine side)

Flow of an input INFO value, `output_vcf` path:

1. **Read.** `annotate_to_vcf` opens the input with formats' `VcfTableProvider::new(path, ...)`
   (`src/vcf_sink.rs:1777-1784`; `.with_record_layout()` when `preserve_record_layout`). The
   percent-decode happens here, inside formats/noodles (issue §Root cause step 1-2). The engine
   classifies INFO columns only by the reader's `bio.vcf.field.field_type` metadata
   (`vcf_sink.rs:1803-1828`) — names only, no values.
2. **Rename on collision (vepyr #121).** `input_fields_for_output` (`src/annotate_provider.rs:901-955`)
   renames an input field whose name is reserved (`CSQ`, `most_severe_consequence`, typed annotation
   columns; reserved set `:3943-3946`) to `INFO_<id>` / `fmt_<id>`, keeps all `bio.vcf.field.*`
   metadata and records the source name under `bio.vep.source_field_name` (`:872`, `:950`).
   **Schema-only: values are not touched.**
3. **Passthrough of values.** The output batch reuses the input Arrow arrays verbatim:
   `out_cols.push(batch.column(idx).clone())` for every input column (`annotate_provider.rs:8103-8110`),
   then appends `CSQ` (`:8112`). Comment at `:13255-13256`: "annotation/output still consume the
   original batches". No string transform of input INFO anywhere in the engine.
4. **Projection.** `annotation_output_columns` (`vcf_sink.rs:1550-1569`) = core cols + input INFO
   (engine names) + `CSQ` + FORMAT + optional layout cols. The input's own `CSQ` is dropped, not
   renamed-and-written: `info_fields.retain(|name| name != "CSQ")` (`vcf_sink.rs:1919`), and
   `restore_input_columns` strips `CSQ` from the carried key order (`vcf_sink.rs:232-295`, `:298-300`).
   `vcf_info_fields = info_fields + ["CSQ"]` (`vcf_sink.rs:1943-1944`).
5. **Alias-back.** `restore_input_columns` (`vcf_sink.rs:232-295`) renames `INFO_AF`→`AF` via
   `writer_field_names` (`:200-217`), dropping only the `bio.vep.source_field_name` key; metadata and
   **array data are cloned unchanged** (`:265`, `:268-286`). Early-return when nothing renamed (`:248-250`).
6. **Serialize.** Every body byte goes through one formats call, `batch_to_vcf_lines(&batch,
   vcf_info_fields, unique_format_tags, sample_names, zero_based)`:
   - serial path `vcf_sink.rs:2188-2195` (after `restore_input_columns` at `:2188`);
   - sharded / helper path `format_vcf_body_chunk` `vcf_sink.rs:302-347` (restore `:320`, call `:321`),
     used by `VcfBodyShardWriter::write_batch` `vcf_sink.rs:454-465`, created per worker in
     `annotate_provider.rs:12523-12530` (`spawn_annotation_from_lookup_sharded`).
   Output is `VcfRecordLine.line` bytes joined with `\n` (`vcf_lines_to_body_chunk`, `:143-152`).
   The engine never writes INFO text itself.

So: the engine **does not touch INFO values**. It renames columns (#121) and aliases them back, and
it drops input `CSQ`; the bytes of every other INFO value are whatever formats' serializer makes of the
decoded Arrow string.

### CSQ escaping (engine-owned) and how CSQ reaches the serializer

- `csq_escape` (`annotate_provider.rs:2723-2777`, fn at `:2742`): per value, `-`→empty,
  `,`/`|`→`&`, `;`→`%3B`, whitespace run→`_` (VEP `OutputFactory/VCF.pm:396-405`). **No `=` rule**
  (test `csq_escape_does_not_touch_equals`, `:21724`).
- HGVSp: `format_hgvsp_output` `annotate_provider.rs:4165-4196`, `out.replace('=', "%3D")` at `:4192`
  unless `no_escape` → `p.Ter104%3D` (VEP `OutputFactory.pm:1757`).
- Plugin values: `plugin_cache/csq.rs:29-38` `escape_csq_value` = `csq_escape` + **an extra `=`→`%3D`**
  (`:34`). Its doc (`:20-28`) says this is a deliberate deviation compensating for "an upstream INFO
  percent-decode" of ClinVar `CLNVI` in the *plugin source VCF* (read via `VcfTableProvider`,
  `plugin_cache/provider.rs:247`), "when that is fixed this deviation should be removed".
- CSQ blocks joined with `,` (`csq_buf.push(',')`, `annotate_provider.rs:6911`).
- CSQ is handed to formats as a **plain scalar `Utf8` column** named `CSQ`
  (`Field::new("CSQ", DataType::Utf8, true)` with only `bio.vep.annotation_cache_layout` metadata,
  `annotate_provider.rs:3949-3958`) listed last in `info_fields`. It carries **no**
  `bio.vcf.field.field_type` metadata in the batch (the INFO/Number/Type metadata is only put on the
  header-only `write_schema`, `vcf_sink.rs:1967-1975`, which is not the batch schema).
- In formats v1.14.0 a scalar Utf8 goes `extract_info_value_string` → `StringArray` arm
  (`serializer.rs:1029-1037`) → `format!("{field_name}={value_str}")` (`serializer.rs:988-993`).

**Would a formats-side re-encode of all INFO strings double-encode CSQ? Yes.** CSQ is already
VCF-safe text containing `%` (`%3B`, `%3D`) and structural `,`. Encoding the reserved set
(`; = % , \r \n \t` + controls) would emit `%253D`/`%253B` and turn every block separator into `%2C`
(one block per record). The HG002 md5 gate would fail on every annotated record (every record with
>1 consequence block, and every synonymous HGVSp `%3D`). The fix must exempt CSQ.

## 2. Owner / carrier / untouched

- **Owner: formats** (`bio-format-vcf/src/serializer.rs`, `build_info_string` :959-1001,
  `extract_info_value_string` :1007-1057, `extract_list_values` :1062-1112). The engine has no INFO
  text code to change; evidence §1 steps 2-6.
- **Carrier: functions (this repo).** It calls the serializer for all output (`vcf_sink.rs:321`,
  `:2189`) and owns the one column that must NOT be re-encoded (`CSQ`). Needs at minimum a formats pin
  bump; possibly a one-line marker on CSQ (see §5).
- **Untouched here:** `csq_escape`, HGVSp `%3D`, plugin `escape_csq_value` (plugin values live inside
  CSQ; the fix is writer-side, the plugin decode is reader-side, so the `=`→`%3D` deviation is **still
  needed** after #207 — the comment at `plugin_cache/csq.rs:27-28` should not be read as triggered),
  #121 rename/alias-back, assembler (`run_assembler_thread*` `vcf_sink.rs:1431-1550`, byte concat),
  header builder.

## 3. Existing coverage

- `src/vcf_sink.rs` tests (65 `#[test]`): `a_renamed_input_column_reaches_the_writer_under_its_own_name`
  (:2333), `an_input_csq_key_leaves_the_carried_order` (:2347),
  `the_body_formatter_writes_gt_first_when_the_layout_is_not_carried` (:2406, calls
  `format_vcf_body_chunk`), `a_batch_with_nothing_to_restore_is_returned_as_is` (:2468), layout
  columns (:3711, :3724). None has an INFO string with a reserved/percent character, none asserts CSQ
  bytes survive the serializer.
- `tests/vcf_output_roundtrip.rs`: despite the name it uses a **hand-rolled writer**
  (`write_test_vcf`, :12-100) — it never calls `batch_to_vcf_lines`/vcf_sink. Zero coverage of this bug.
- `tests/plugin_csq_emission.rs`, `plugin_cache/csq.rs` tests (:74-147): CSQ value escaping only.
- No engine test runs `annotate_to_vcf` end to end (only callers: `examples/annotate_vcf.rs`, the
  comment at `annotate_provider.rs:12174`).
- Golden: `vep-benchmark/data/golden/golden_1000_vep115.vcf` — the only escapes are 367 `%3D` *inside
  CSQ*; 0 `%` in non-CSQ INFO. HG002 GIAB INFO keys: DPSum, platforms, platformnames, platformbias,
  datasets, datasetnames, datasetsmissingcall, callsets, callsetnames, varType, filt, callable,
  difficultregion, arbitrated, callsetwith{this,other}uniqgenopassing.
- HG002 chr1-22 gate input `~/workspace/data_vepyr/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz`
  (4,048,342 records), scanned here: **0** INFO with `%`, **0** `;`-segments with a second `=`,
  **0** with space/CR. So a reserved-set encoder that exempts CSQ leaves the HG002 bytes unchanged;
  the gate **does** guard the CSQ exemption (it would explode without it), but proves nothing about
  the encoding itself. Coverage must come from the ClinVar fixtures + probes in the issue.

## 4. Blast radius (engine writers)

- **Single choke point:** both output_vcf paths (serial `vcf_sink.rs:2189`, sharded workers>1
  `:321` via `VcfBodyShardWriter` `:455`) call `batch_to_vcf_lines` after `restore_input_columns`; a
  formats change applies identically to workers=1 and workers>1, plain/gzip/bgzf. The assembler only
  concatenates shard bytes — no re-encode. Shards are byte-identical to serial output by design, so
  no new divergence.
- **`preserve_record_layout`:** the carried `vcf_info_keys`/`vcf_format_keys` columns are excluded as
  INFO (formats `is_record_layout_field`, serializer.rs:788-795); they hold keys, not values, so the
  encoding does not touch them. Key order is unaffected.
- **#121 renamed columns** (`INFO_AF`→`AF`): written under the input name with reader metadata intact
  → would be encoded like any input INFO (correct). The input `CSQ` is never written (dropped), so a
  name-based `CSQ` exemption cannot leak an un-encoded input CSQ.
- **LazyFrame / streaming path:** engine returns Arrow batches; no engine text writer. Unchanged.
  But vepyr's `pb.sink_vcf` (polars-bio) also uses the formats serializer on engine output, and Polars
  drops Arrow field metadata — so a **metadata-marker** CSQ exemption would not survive that path,
  while a **name-based** exemption would. (Cross-repo; flag to the vepyr/formats agents.)
- **Plugin CSQ:** values inside CSQ; unaffected if CSQ exempt. Remaining reader-side decode artefacts
  in plugin values (`%2C`→`,`→`&`, `%25`→`%`) are a separate, pre-existing difference.
- **FORMAT encoding** (issue suggests the same with `:` added): engine passes FORMAT through the same
  serializer; HG002 FORMAT values (GT/PS/DP/ADALL/AD/GQ) contain no reserved characters, so bytes
  unchanged there.
- Anything that would change bytes: only inputs with reserved characters in decoded INFO/FORMAT
  strings (i.e. ones that were `%XX` in input, or malformed `KEY=a=b` values, which would now gain
  `%3D`). CSQ bytes must not change.

## 5. If the fix lands in formats — what moves here

Pin lines (all on formats tag `v1.14.0`; must move together or Cargo resolves two
`datafusion-bio-format-core` copies, as the comment at `bio-function-vep/Cargo.toml:40-44` warns):
- `datafusion/bio-function-vep/Cargo.toml:45` `datafusion-bio-format-vcf`
- `datafusion/bio-function-vep/Cargo.toml:46` `datafusion-bio-format-bed`
- `datafusion/bio-function-vep/Cargo.toml:47` `datafusion-bio-format-gff`
- `datafusion/bio-function-vep/Cargo.toml:48` `datafusion-bio-format-core`
- `datafusion/bio-function-vep/Cargo.toml:49` `datafusion-bio-format-ensembl-cache` (optional)
- `datafusion/bio-function-pileup/Cargo.toml:23` `datafusion-bio-format-bam` (optional dep)
- `datafusion/bio-function-pileup/Cargo.toml:30` `datafusion-bio-format-bam` (second table)
- plus `Cargo.lock`. noodles revs (`bio-function-vep/Cargo.toml:35-38` at 42a3c016; dev-dep
  `noodles-vcf` `:61` at 9b7b2c5) only move if formats moves noodles.
- Downstream: vepyr `Cargo.toml:217` (engine tag v0.23.1), `:218-219` (formats ensembl-cache + vcf,
  v1.14.0) — all three must move with the engine.

Behaviour change here depends on the exemption design chosen in formats:
- **Name-based (`CSQ` exempt in formats):** pin bump only. Engine CSQ name is fixed (no
  `vcf_info_field` option exists in the engine).
- **Metadata marker (e.g. `bio.vcf.field.pre_encoded`):** engine must set it on the CSQ field —
  either at the schema definition `annotate_provider.rs:3950-3957` (flows to both SQL and sharded
  paths) or in `restore_input_columns` (but that has an early return at `:248-250` that would have to
  go). Polars caveat above.
- **"Encode only reader-typed INFO" (has `bio.vcf.field.field_type`)**: would exempt engine CSQ for
  free (its batch field lacks that key) but silently stops encoding for metadata-less (Polars) input.
  Not recommended.
- Recommended engine test regardless: a `vcf_sink.rs` unit test through `format_vcf_body_chunk`
  (template :2406) with a decoded `List<Utf8>` INFO column carrying reader metadata (`["a;b|c"]`,
  `["a,b","c"]`, `["50%_x"]`) + a renamed `INFO_<id>` column + `CSQ = "A|...|p.Ter104%3D,A|..."`,
  asserting the input values re-encode and CSQ is byte-identical; and the same through the sharded
  writer to pin workers>1.

## VEP 116 rule

VEP keeps the input record's raw tab-split fields and only appends CSQ; it never decodes or re-encodes
input INFO.

- `Parser/VCF.pm:124-128` — the parser is `Bio::EnsEMBL::IO::Parser::VCF4->open($self->file)`;
  `create_VariationFeatures` takes `$record = $parser->{record}` (`:223-224`) and stores it as
  `_line => $record` (`:349`; also SV paths `:541`, `:573`).
  https://github.com/Ensembl/ensembl-vep/blob/release/116.0/modules/Bio/EnsEMBL/VEP/Parser/VCF.pm#L220-L350
- `OutputFactory/VCF.pm:314-355`:
  ```perl
  # if input was VCF then we get _line with the original contents
  if($self->{input_format} eq 'vcf' && $vf->{_line}) {
    $line = $vf->{_line};
    # array copy to keep original intact
    my @tmp = @$line;
    $line = \@tmp;
    if(!defined($line->[7]) || $line->[7] eq '.') { $line->[7] = ''; }
    # nuke existing CSQ field?
    if($line->[7] =~ /(^|\;)$fieldname\=/ && !$self->{keep_csq}) {
      $line->[7] =~ s/(^|\;)$fieldname\=\S+?(\;\S|$)/$2/;
      $line->[7] =~ s/^\;//;
    }
  }
  ...
  if(@chunks) {
    $line->[7] .= ';' if $line->[7];
    $line->[7] .= $fieldname.'='.join(",", @chunks);
  }
  ...
  push @return, join("\t", map {defined($_) ? $_ : '.'} @$line);
  ```
  https://github.com/Ensembl/ensembl-vep/blob/release/116.0/modules/Bio/EnsEMBL/VEP/OutputFactory/VCF.pm#L314-L355
- CSQ value escaping `OutputFactory/VCF.pm:387-419`: per value `convert_arrayref($data,'&')`, `-`→''
  (non-Allele), then `s/\,/\&/g; s/\;/\%3B/g; s/\s+/\_/g; s/\|/\&/g;`, `join('|', @chunk)`; blocks
  joined with `,` at `:349`. No `=` and no `%` rule.
  https://github.com/Ensembl/ensembl-vep/blob/release/116.0/modules/Bio/EnsEMBL/VEP/OutputFactory/VCF.pm#L387-L419
- Field-specific URI escapes in `OutputFactory.pm`: HGVSp `=`→`%3D` unless `no_escape` (`:1756-1757`);
  SYMBOL whitespace→`%20` for VCF output unless `no_escape` (`:1525-1526`).
  https://github.com/Ensembl/ensembl-vep/blob/release/116.0/modules/Bio/EnsEMBL/VEP/OutputFactory.pm#L1751-L1760
  https://github.com/Ensembl/ensembl-vep/blob/release/116.0/modules/Bio/EnsEMBL/VEP/OutputFactory.pm#L1524-L1526

Rule being ported: output INFO = input INFO text byte-for-byte (minus an existing `CSQ=` key), then
`;CSQ=` + already-escaped blocks. vepyr re-serializes from decoded Arrow values, so the closest port is
"re-encode input string values with the reserved set; leave engine-built CSQ alone"; exact parity for
unreserved/lower-case escapes needs raw INFO passthrough (issue's optional follow-up).
(ensembl-io's VCF4 parser source is not checked out locally; that `{record}` is the raw tab split is
inferred from VEP's use of it and from the issue's byte-for-byte reproduction.)

## Sources read

- engine v0.23.1: `datafusion/bio-function-vep/src/vcf_sink.rs` (130-470, 1545-1600, 1721-2230,
  tests 2333-2475, 3711-3724), `src/annotate_provider.rs` (865-985, 2710-2777, 3930-3965,
  4155-4196, 6911, 8095-8115, 12510-12530, 13255-13290), `src/plugin_cache/csq.rs` (1-60),
  `src/plugin_cache/provider.rs` (247), `src/cache_source.rs:11`, `Cargo.toml`,
  `datafusion/bio-function-pileup/Cargo.toml`, `Cargo.lock` (noodles-vcf entries),
  `tests/vcf_output_roundtrip.rs`, `tests/plugin_csq_emission.rs` (grep),
  `vep-benchmark/data/golden/golden_1000_vep115.vcf` (INFO scan).
- human checkout via `git show`: v0.23.1, 46d8c20, origin/master (4704ff9).
- formats v1.14.0: `bio-format-vcf/src/serializer.rs` (580-700, 788-905, 959-1112),
  `bio-format-core/src/metadata.rs` (keys), `header_builder.rs` (grep CSQ).
- vepyr `Cargo.toml:217-219`.
- ensembl-vep release/116.0: `modules/Bio/EnsEMBL/VEP/OutputFactory/VCF.pm` (300-430),
  `Parser/VCF.pm` (79, 124-128, 177-350, 541, 573), `OutputFactory.pm` (151, 1524-1526, 1750-1760).
- HG002 `~/workspace/data_vepyr/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz` (header + full INFO scan).
