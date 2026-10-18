# vepyr #207 — formats analysis (datafusion-bio-formats v1.14.0 @ 72efe67, noodles 42a3c01)

Paths: `F` = `$S/formats/datafusion/bio-format-vcf/src`, `N` = `~/.cargo/git/checkouts/noodles-b4f93bd9cc0a0e76/42a3c01/noodles-vcf/src`,
`E` = `$S/engine/datafusion/bio-function-vep/src` (bio-function-vep v0.23.1, b960112).

Drift note: on the human checkout's `dbc62fb` (feat/cooler, forked from v1.11.0, NOT a descendant of v1.14.0)
`physical_exec.rs`, `storage.rs`, `write_exec.rs`, `bcf.rs` are byte-identical to v1.14.0, so every line cited there holds.
`serializer.rs` differs (17+/220-): shifts are `batch_to_vcf_lines` 598→581, `build_info_string` 959→942,
`extract_info_value_string` 1007→990, `extract_list_values` 1062→1045, `extract_sample_value_string` 1356→1330,
`write_typed_value` 318→318. **Cite v1.14.0 numbers, do not branch from dbc62fb.**

## 1. Where the behaviour lives

### Decode on read: noodles, not formats, one code path for all four text loops
All four VCF text record loops hand a lazy `noodles_vcf::Record` to the same `load_infos_single_pass`:
- async local, plain + bgzf (`VcfLocalReader::read_records`, storage.rs:469-536): `F/physical_exec.rs:926-993`
- sync local (`VcfSyncReader::read_record`, storage.rs:575-607): `F/physical_exec.rs:1161-1166`, info at `:1269-1277`
- remote/async object store (`VcfRemoteReader::read_records`, storage.rs:797): `F/physical_exec.rs:1465`, info at `:1532`
- indexed/tabix/CSI (`IndexedVcfReader::query` → `q.records()`, storage.rs:854-861, yields `Record`): `F/physical_exec.rs:3033-3054`, info at `:3171`

`load_infos_single_pass` (`F/physical_exec.rs:713-809`) calls `record.info().iter(header)` (`:729-730`) and stores
values as given: `Value::String(v)` → `builder.append_string(&v)` (`:767-769`); `Array::String` → each element
`.to_string()` → `append_array_string_nullable` (`:770-777`). formats does no decoding of its own (no `percent` anywhere in `F`).

The decoding is inside noodles (lazy record path):
- `N/record/info.rs:39-69` `Info::iter` → `field::next` splits the raw INFO text on `;` and the first `=` (`N/record/info/field.rs:7-8,61-98`) **before** decoding, so a `%3B`/`%3D` never splits a field.
- `N/record/info/field.rs:31-33` → `value::parse_value` (`N/record/info/field/value.rs:9-25`).
- Number=1 String: `parse_string_value` → `percent_decode(src)` (`N/record/info/field/value.rs:63-69`).
- Number=1 Character: `parse_character_value` decodes too (`:47-61`).
- Number≠1 String: kept as raw `&str` (`:83-85`); each element decoded lazily after the `,` split: `N/variant/record/info/field/value/array/values.rs:98-117` (`percent_decode(s)` at `:110`), so `%2C` survives as an in-element `,`. Character arrays: same at `:62-96`.
- `percent_decode` = `percent_decode_str(s).decode_utf8()` (`N/io/reader/record_buf/value.rs:3-7`). percent-encoding 2.3.2 decodes **every** `%XX` with two hex digits, either case, and leaves malformed `%` sequences literal (`percent-encoding-2.3.2/src/lib.rs:225,252-258`).
- (The RecordBuf path decodes the same way: `N/io/reader/record_buf/info/field/value.rs:138-156`; formats does not use it.)

Value types: String (scalar and list, Number=1/A/R/G/./n) affected. Character: decoded by noodles, but formats has
no INFO arm for it → `Unsupported INFO value type` (`F/physical_exec.rs:788-791`); confirms the issue's side finding.
Integer/Float/Flag: no decoding, unaffected.

FORMAT/sample strings are decoded identically: `N/record/samples/series/value.rs:87-88` (String), `:71-72` (Character),
`N/variant/record/samples/series/value/array/values.rs:76,110` (array elements). formats stores them as decoded:
`F/physical_exec.rs:1883-1884` (scalar String/Character), `:1926-1956` (arrays) in the columnar builder; same in `load_formats_single_pass` (`:2446`, Character at `:2586`).

BCF: `F/bcf.rs` also feeds `load_infos_single_pass` (import at `:45`), but BCF strings are binary and noodles-bcf has
no percent handling (grep: none). A BCF value with a literal `;` reaches the serializer exactly as a decoded VCF value
would: same bug, different source. The record layout carry is refused for BCF (`tests/record_layout_test.rs:311`).

### Write back: `serializer.rs`, no encoding anywhere
Single entry point `batch_to_vcf_lines` (`F/serializer.rs:598-775`). INFO: `build_info_string` (`:959-1003`) emits
`format!("{field_name}={value_str}")` (`:993`), joined with `;` (`:1001`). Values come from `extract_info_value_string`
(`:1007-1058`): scalar Utf8/LargeUtf8/Utf8View verbatim (`:1029-1037`); List/LargeList via `extract_list_values`
(`:1062-1119`, string arms `:1090-1115`) joined with `","` (`:1046,1054`).
FORMAT: fast multisample path `write_typed_value` Utf8/LargeUtf8/Utf8View (`:342-377`, `buf.push_str(s)`); flat
single-sample path `extract_sample_value_string` (`:1374-1394`) and its lists (`:1397-1411`, again `extract_list_values`
joined with `,`); `extract_list_element_as_string` (`:1325-1352`). Note `extract_list_values` is shared by INFO and FORMAT,
so an encoder put there must take the reserved set as a parameter (INFO vs FORMAT sets differ, see below).

No other writer path exists in formats: `VcfWriteExec` (`F/write_exec.rs:339,366`) and the tests in `F/writer.rs` all call
`batch_to_vcf_lines`; nothing uses `noodles_vcf::io::Writer`. The "record layout" carry (`append_record_layout`,
`F/physical_exec.rs:513-538`) stores only INFO **keys** (`raw.split(';')` → `entry.split('=').next()`, `:518-525`) and
FORMAT keys; values are never carried raw. The serializer uses it only for ordering (`F/serializer.rs:704-733`).

noodles' own writer does encode, for reference: INFO `N/io/writer/record/info/field/value/string.rs:27-38`
(`CONTROLS + ; = % , \r \n \t`, and a literal `.` is written `%2E`, `:13-15`); FORMAT
`N/io/writer/record/samples/sample/value/string.rs:27-36` (`CONTROLS + : % , \r \n \t` — **no `;` or `=`**).

## 2. Owner / carrier / untouched

- **Owner: formats** (`bio-format-vcf/src/serializer.rs`). The defect is the asymmetry: the reader returns decoded logical values (correct), the serializer writes them as wire text (wrong).
- **Carrier: functions** (`E/vcf_sink.rs:321` and `:2189` call `batch_to_vcf_lines`; it also produces CSQ, which must not be re-encoded — see CSQ path) and **polars-bio** (`write_vcf`/`sink_vcf` → `VcfTableProvider::insert_into` → `VcfWriteExec`).
- **Untouched: noodles** (reader decode is spec-correct; no change wanted) and the formats reader.

### Is decoding on read correct? Yes — keep it.
VCF 4.3 §1.2 (cited verbatim by noodles at `N/io/writer/record/info/field/value/string.rs:25-26`) defines `%XX` as the
encoding of reserved characters; the logical value is the decoded one. Changing the reader would change Arrow/Polars
values for every polars-bio and vepyr LazyFrame user (`['a;b|c']` would become `['a%3Bb|c']`, list arity unchanged since
the split happens first), and would change **plugin caches built from VCF sources**: `E/plugin_cache/provider.rs:23,247`
reads ClinVar-style VCFs through `VcfTableProvider`, so CLNDN etc. would land in the Parquet cache (and then in CSQ via
`csq_escape`) escaped instead of decoded, invalidating published caches. Reject a reader change.

### Option (a): re-encode in the serializer — feasible, recommended
Encode each String/Character value (scalar, and each list element before the `,` join) with the reserved set.
- INFO set: `; = % ,` + ASCII controls (incl. TAB/CR/LF). `:` literal.
- FORMAT set: use noodles' `: % ,` + controls (not `;`/`=`) to minimise byte change.
- **Do NOT reuse `percent_encoding::utf8_percent_encode`** (what noodles uses): it always encodes every non-ASCII byte
  (`percent-encoding-2.3.2/src/ascii_set.rs:52-54`, `!byte.is_ascii() || self.contains(byte)`). ClinVar 20261011 has
  **6,576 records with UTF-8 in INFO** (6,565 `CLNDN`, 10 `CLNDNINCL`, 2 `SCIDN`; e.g. en-dash `E2 80 93` in
  `MCADD_-_..._–_full_ACADM...`, soft hyphen `C2 AD`). vepyr writes them raw today (= VEP). A noodles-style encoder
  would regress all 6,576 to `%E2%80%93`. Use a hand-written byte loop over ASCII reserved bytes only, upper-case hex.
- Keep the `.`→`%2E` rule only if wanted (noodles does it); otherwise a decoded `"."` re-reads as missing. Edge case, not in ClinVar.

What (a) cannot restore (issue claims partly wrong — correct the regression-test list):
- p5/p9 unreserved escapes (`%20`, `%2B`, `%3A`) → written decoded (`X Y+Z`, `a:b`). Release: 4×`%20`, 2×`%2B` occurrences (2 records). `%20` writes a literal space into INFO (legal in 4.3, but not byte-identical to VEP).
- **p6 lower-case `%3b`** → `%3B` (case is lost on decode). Issue lists p6 among byte-identical probes — it will not be.
- **p10 malformed `a%ZZb%`** → `a%25ZZb%25` (the literal `%` is re-encoded). Issue lists p10 among byte-identical — it will not be. (Escaping `%` only when followed by two hex digits would keep p10 but write a spec-violating bare `%` and break p4's `%25` → `%` bytes; not recommended.)
- ClinVar inventory, whole release (`gzip -dc clinvar_20261011.vcf.gz | grep -o '%[0-9A-Fa-f]{2}'`): `%3D` 3032, `%3B` 365, `%20` 4, `%2B` 2; no `%2C`, no `%25`, no lower case, no bare `%`. So (a) restores all ClinVar records except the 2 with `%20`/`%2B`.

What (a) breaks: any producer that stores already-serialized text in a String column. Concretely the engine's CSQ
(next section), and any polars-bio user who builds a Number=`.` field as a pre-joined scalar `"a,b,c"` (today written
as 3 values, after (a) as `a%2Cb%2Cc` = 1 value). Needs an exemption rule (see CSQ path).

### Option (b): carry raw INFO text — feasible only for text VCF, larger blast radius
The raw INFO is available in every text loop (`record.info().as_ref()`, as `append_record_layout` already uses at
`F/physical_exec.rs:516-517`). Shape: a third record-layout column (new role next to `VCF_RECORD_LAYOUT_INFO_KEYS`,
`bio-format-core/src/metadata.rs:98-104`) holding per-key raw values; the serializer writes a key's raw text when
`percent_decode(raw)` equals the current typed value, else falls back to (a). That gives byte identity including `%20`,
`%2B`, lower case and malformed escapes, and stays correct when a user edits a column.
Costs/limits: (1) still needs (a) as the fallback, so it is additive, not an alternative; (2) BCF/zarr/DataFrame-built
inputs have no raw text; (3) memory: a copy of the whole INFO per row (ClinVar INFO is hundreds of bytes); (4) every
carrier must project the new column: engine `annotation_output_columns` (`E/vcf_sink.rs:1550-1569`, layout list
`:1556-1558`) and the vepyr LF / polars-bio `record_layout` path; core crate gets a new constant. Recommend as a
follow-up only (2 ClinVar records benefit).

## 3. Existing coverage

- noodles has unit tests for decode (`N/record/info/field/value.rs:103-116`, `"a%3Bb"` → `"a;b"`; `N/io/reader/record_buf/value.rs:13-18`) and its writer.
- formats: **no** test touches a percent escape or a reserved character in a value (grep for `%3B|%3D|%2C|%25|percent` over `bio-format-vcf` and `bio-format-core`: only an unrelated hit in `core/src/object_storage.rs`).
- Round-trip harnesses that exist and can host the regression test: `tests/record_layout_test.rs:55-105` (`round_trip`: read with `VcfTableProvider`, `INSERT OVERWRITE` via `new_for_write`, compare bodies verbatim; fixtures `:21-45` are Integer/Float/Flag + GT only), `tests/write_test.rs` (round trips at `:409`, `:749`; DP/AF/DB only). `tests/special_char_info_test.rs` is about special characters in INFO **keys** (`HGMD-PUBLIC_20204`), not values.
- Serializer unit tests (`F/serializer.rs:1417-3009`) use numeric INFO and GT; the `layout_batch` CSQ stand-in (`:2428-2470`) is a scalar Utf8 `"T|missense"` — no reserved chars, so nothing pins "CSQ is not re-encoded".
- Not reached: String/Character INFO with reserved chars (scalar or list), FORMAT strings with `:`/`,`/`%`, non-ASCII UTF-8 values, Number≠1 scalar pre-joined columns, BCF strings with reserved chars, all four reader loops with escaped input (indexed fixture `tests/data/record_layout.vcf.gz` has none).

## 4. Blast radius

Callers of `batch_to_vcf_lines`:
- formats: `F/write_exec.rs:339,366` (`VcfWriteExec`, all `INSERT INTO`/`insert_into` writes); tests.
- functions: `E/vcf_sink.rs:321` (`format_vcf_body_chunk`, used by the sharded/threads>1 path at `:455,2441`) and `:2189` (single-stream path). This is vepyr `output_vcf`/CLI (`vepyr src/annotate.rs:8,193`).
- polars-bio master (ea9e99a) pins formats rev `4b58ea6` (= v1.14.0 + 2 non-VCF commits) in `Cargo.toml:62`; `src/write.rs:547-599` → `execute_write` → `provider.insert_into` (`:153-155`) → `VcfWriteExec`. This is `pb.write_vcf`/`pb.sink_vcf`, including vepyr LazyFrame → `pb.sink_vcf`.

Reader decode path consumers (unchanged under (a)): every `VcfTableProvider` scan — vepyr `src/annotate.rs:461,550,652,674`, engine plugin cache builds `E/plugin_cache/provider.rs:247`, polars-bio `src/scan.rs:41`.

Byte changes under (a), by input:
- INFO String/Character values whose decoded text contains `; = , %` or ASCII controls: now escaped (fixes #207; for ClinVar: 1,966 non-`ALT=.` records, plus the `ALT=.` `%3D` records).
- `%25` input: decodes to `%`, written `%25` again — round trips. A bare `%` (malformed) gains `%25`.
- Lower-case escapes: re-cased to upper. Unreserved escapes: stay decoded (as today).
- FORMAT strings with `: , %` (FORMAT set): now escaped. GT (`0|1`, `./.`) unaffected.
- Non-ASCII: unchanged **only if** the encoder skips non-ASCII bytes (see 2).
- HG002 md5 gate input: INFO/FORMAT are ASCII only (0 non-ASCII lines), String fields are names/lists like `platformnames=PacBio,10X,Illumina`; percent count — see addendum. Expect md5 unchanged.
- Latent, worth fixing while there: `build_info_string` treats any value that *stringifies* to `"true"`/`"false"` as a Flag (`F/serializer.rs:989-994`), so a String INFO `X=true` is written as bare `X` and `X=false` is dropped. Decide flag-ness from `DataType::Boolean`, not the text.

## 5. Downstream moves if the fix lands in formats

- Public API: `batch_to_vcf_lines(&RecordBatch, &[String], &[String], &[String], bool)` (`F/serializer.rs:598-604`) need not change if the CSQ exemption is driven by field metadata or structure; it changes only if an explicit "verbatim fields" parameter is chosen.
- functions: bump all formats pins together (`bio-function-vep/Cargo.toml:45-49` vcf, bed, gff, core, ensembl-cache at `tag = "v1.14.0"`; `bio-function-pileup/Cargo.toml:23,30` bam). Plus a **code change** unless the exemption is name-based: tag the CSQ field (e.g. `E/annotate_provider.rs:3950`, which today carries only `ANNOTATION_CACHE_LAYOUT_KEY`).
- vepyr: bump `Cargo.toml:218-219` (ensembl-cache, vcf) to the same formats tag as the engine, and the engine pin; if the LF → `pb.sink_vcf` path must keep CSQ verbatim, `src/vepyr/_vcf_metadata.py:37-42` must carry the marker/number, and polars-bio must pass it through (it maps only `number`/`type`/`description` into field metadata, `polars-bio src/write.rs:54-70`).
- polars-bio: re-pin formats for its own `write_vcf` users to get the fix.

## CSQ path

Yes, a naive re-encode would double-encode CSQ. The engine's CSQ is a scalar `Utf8` column
(`E/annotate_provider.rs:3950`, `Field::new("CSQ", DataType::Utf8, true)`) holding the already-joined VCF value:
`,` between consequence blocks, `|` between sub-fields, `;` → `%3B` and `,`/`|` → `&` inside values (`csq_escape`,
`E/annotate_provider.rs:2742-2775`), `=` → `%3D` only in HGVS (`:4191-4193`) — `csq_escape` explicitly does not escape
`=` (`:2741`), so CSQ can contain literal `=` (e.g. `--no_escape`, plugin values) and literal `%` (from `%3B`/`%3D`).
Route: the sink appends `"CSQ"` to the INFO list (`E/vcf_sink.rs:1943-1944`; select list `:1550-1586`),
`restore_input_columns` (`:232-292`) only renames/strips the input's CSQ key, then `batch_to_vcf_lines` (`:321`, `:2189`)
→ `extract_info_value_string` scalar `StringArray` arm (`F/serializer.rs:1029-1031`) → `CSQ=<text>`. A re-encode would
write `%253D`, `%253B`, `%2C` between blocks, `%3D` for literal `=`: every annotated line changes.
The vepyr LF → `pb.sink_vcf` path writes the same scalar CSQ string through `VcfWriteExec`.

Exemption options (decision for the plan):
1. **Structural rule (recommended)**: a scalar Utf8 column whose declared `bio.vcf.field.number` is not `1` (`.`, A, R, G, n>1) already holds the joined wire text → write verbatim. The reader never produces that shape (Number≠1 → List), and without metadata the header infers Number=1 for a scalar (`F/header_builder.rs:657-662`), so encoding stays on. It also protects polars-bio users who pre-join lists. Needs the engine to put `number="."` on CSQ in the batch schema; vepyr's LF metadata already declares CSQ `number: "."` (`_vcf_metadata.py:38-42`) and polars-bio maps `number` into field metadata, so the pb path needs no polars-bio change (verify with a test).
2. Explicit marker metadata key (e.g. `bio.vcf.field.preencoded`): cleanest contract, but needs engine + vepyr + polars-bio changes (pb drops unknown keys).
3. Name `CSQ`: zero downstream code, but wrong for a generic round trip: a CSQ read back from a VCF is a `List<Utf8>` of decoded blocks and must be encoded. If used, restrict to scalar Utf8 `CSQ`.

## Sources read

- formats v1.14.0 (72efe67, worktree `$S/formats`): `bio-format-vcf/src/physical_exec.rs` (513-545, 713-813, 839-1000, 1085-1280, 1378-1535, 1855-1965, 2939-3180), `storage.rs` (455-610, 797-861), `serializer.rs` (200-475, 598-1120, 1325-1415, test list), `write_exec.rs` (320-380), `writer.rs` (330-350), `header_builder.rs` (640-662), `bcf.rs` (fn index), `lib.rs`; `bio-format-core/src/metadata.rs` (98-146); tests `record_layout_test.rs`, `write_test.rs`, `special_char_info_test.rs` (heads/indexes).
- human checkout `datafusion-bio-formats` via `git show/diff v1.14.0 dbc62fb` (read-only).
- noodles 42a3c01: `noodles-vcf/src/record/info.rs`, `record/info/field.rs`, `record/info/field/value.rs`, `variant/record/info/field/value/array/values.rs`, `io/reader/record_buf/value.rs`, `io/reader/record_buf/info/field/value.rs`, `record/samples/series/value.rs`, `variant/record/samples/series/value/array/values.rs`, `io/writer/record/info/field/value/string.rs`, `io/writer/record/samples/sample/value/string.rs`; `percent-encoding-2.3.2` (`ascii_set.rs:52-54`, `lib.rs:217-258`).
- engine v0.23.1 (`$S/engine`, b960112): `vcf_sink.rs` (180-335, 1540-1600, 1795-1960, 2170-2200), `annotate_provider.rs` (2680-2775, 4180-4196, 3945-3960), `plugin_cache/provider.rs` (grep), `Cargo.toml` pins.
- polars-bio master ea9e99a (`git grep`/`git show`, read-only): `Cargo.toml:62`, `src/write.rs` (54-70, 125-160, 547-700), `src/option.rs` (grep).
- vepyr master: `Cargo.toml:218-219`, `src/annotate.rs` (grep), `src/vepyr/_vcf_metadata.py` (25-61).
- Data: `~/workspace/data_vepyr/input/clinvar/clinvar_20261011.vcf.gz` (escape inventory, non-ASCII scan); `HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz` (non-ASCII scan, String INFO declarations).

## Addendum: HG002 gate input scan

`HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz`, columns 8+ of every body line: **0 lines contain `%`**, 0 contain non-ASCII.
The only literal true/false-like INFO value is `arbitrated=TRUE` (12,383 records, upper case), which the
`"true"`/`"false"` flag check at `F/serializer.rs:989-994` does not match. The INFO String lists hold names with no
reserved characters inside an element. So the reserved-set re-encode (a) should leave the HG002 chr1-22 md5 gate
unchanged, **provided CSQ is exempted** (CSQ is on every annotated line).
