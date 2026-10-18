# vepyr #207: vepyr-side analysis (runbook step 1a, read-only)

Code is cited at **origin/master c089bef**. Lines that differ at the v0.10.0 release
commit **3f21520** are noted as `(v0.10.0: N)`; no note means the line is the same there.
Nothing was checked out or edited. Probes ran with the installed vepyr 0.10.0
(`$S/venv010`) and VEP 116.0 docker (`$S/bin/run_vep116.sh`). Probe files are in `$S/fix207/probe/`.

## 1. Where the behaviour reaches the user, and whether vepyr touches INFO values

vepyr has three ways to write a VCF. Two of them go through the engine's VCF sink, which
calls the formats serializer. The third goes through polars-bio's own compiled copy of
that serializer.

| Entry point | vepyr code path | Serializer that writes INFO |
|---|---|---|
| CLI `vepyr annotate -i X -o out.vcf` | `src/vepyr/cli.py:162` (`-o` help), `cli.py:573` `"output_vcf": args.output_file`, then `annotate()` | engine `vcf_sink::annotate_to_vcf`, then formats `serializer.rs` |
| `vepyr.annotate(..., output_vcf=...)` | `src/vepyr/__init__.py:1841` `if output_vcf is not None:` (v0.10.0: 1959), `:1886` `_result[0] = _annotate_vcf(...)` (v0.10.0: 2004), then `src/lib.rs:834` `fn annotate_vcf`, then `src/annotate.rs:128` `annotate_to_vcf_file`, then `annotate.rs:193` `annotate_to_vcf(vcf_path, cache_dir, backend, output_path, &config)` (import at `annotate.rs:8`) | same |
| LazyFrame, then `polars_bio.sink_vcf` | `__init__.py:2425` `register_io_source` (v0.10.0: 2501), `:2459` `_attach_vcf_metadata(` (v0.10.0: 2535), `src/vepyr/_vcf_metadata.py` (header metadata only: `raw_lines`, `record_layout`, `info_fields` definitions) | polars-bio 0.36.1's own build of the formats serializer. This is **outside vepyr's Cargo pins**. |

On the LazyFrame read side, `src/annotate.rs:655` `VcfTableProvider::new(...)` (v0.10.0: 652)
and `:677` (v0.10.0: 674) register the formats VCF reader. The reader is where noodles
percent-decodes the values. `create_streaming_annotator` is at `annotate.rs:597`.

**vepyr does not touch INFO values.**
- `annotate.rs:360-410` copies options into the engine `AnnotateVcfConfig`. That covers
  `preserve_record_layout` at `:376`, `allow_non_variant`, `colocated` and provenance.
- `annotate.rs:538-593` reads header INFO/FORMAT **ids** only.
- `src/vepyr/_vcf_columns.py` and `_vcf_metadata.py` handle column names and header
  metadata. The record-layout columns `_vcf_info_keys`/`_vcf_format_keys` carry keys,
  not values.
- The Python batch loop (`__init__.py` ~2380-2420) only re-parses **CSQ** into plugin
  columns, then drops, selects and filters.
- No `%`, unquote or decode logic exists anywhere in `src/`. A grep of `src/vepyr/*.py`
  and `src/*.rs` for percent-related code finds nothing.

One documentation claim is now false. `docs/quickstart.md:287` says "The input's own
`INFO` keys and sample columns are written back unchanged". That does not hold for escaped
values. It will still not fully hold after a reserved-set-only fix (see §3 and §4).

## 2. Owner / carrier / untouched

- **Owner: datafusion-bio-formats.** `bio-format-vcf/src/serializer.rs` writes the
  decoded Arrow strings without re-encoding them, for both INFO and FORMAT. The decode
  itself is in the noodles fork (`42a3c016`, Cargo.lock origin/master:3348-3350). That
  decode is correct for the logical value and should stay.
- **Carrier: datafusion-bio-functions.** The VCF sink calls the serializer. The engine
  must also guarantee that its own `CSQ` column is exempt from re-encoding. CSQ is already
  VCF-safe: the HGVSp `=` is escaped as `%3D` by the engine.
- **Carrier: vepyr (pins only).** `Cargo.toml` origin/master:220-222. vepyr passes the
  file paths and options through and never sees a value.
- **Second carrier outside this fix: polars-bio.** The `sink_vcf` path has the same bug
  (probe below). It is fixed only by a polars-bio release re-pinned to the new formats.
  vepyr's `pyproject.toml:27` `polars-bio>=0.36.1` floor would then need raising.
- **Untouched: vepyr Python and Rust logic.** The LazyFrame values are already correct
  decoded logical values. No API change is needed.

## 3. Existing coverage

**Tests.** No test in `tests/` covers percent-encoded INFO or FORMAT.
- `git grep` of `origin/master -- tests e2e-testing/scripts src` for
  `%3[BbDd]|%2[CcBb05]|%25|percent` returns nothing.
- Every plain `.vcf` and `.vcf.gz` fixture under `tests/data/` has 0 records with
  `%XX` in INFO.
- The golden `golden.vcf` files (golden, golden_merged, golden_mnv) have **0 `%3D` even
  inside CSQ**. So no in-repo test would catch CSQ being double-encoded either.

The nearest tests that cover INFO passthrough check verbatim keys and order, but use no
reserved characters:
- `tests/test_annotate.py:2978` `class TestInputFieldNamedLikeAnAnnotationColumn`
  (v0.10.0: 2869). It uses `COLLIDING_INFO_VCF` at `:2937` (v0.10.0: 2828) and
  `test_input_info_keys_are_kept_under_their_own_names` at `:3009`.
- `TestFormatKeyOrderInTheOutputVcf` at `:3117` (v0.10.0: 3008).
- `tests/test_vcf_columns.py:593` `test_sink_vcf_matches_output_vcf_field_for_field` and
  `:621` `..._can_reproduce_record_lines`. These compare sink_vcf with output_vcf on the
  golden input, which has no `%`.

**e2e harnesses.**
- `e2e-testing/scripts/comparison/compare.py:1` is a CSQ-only comparison
  (`_CSQ_RE` at `:35`). **run_comparison cannot see this bug at all.**
- `md5_concordance.py` hashes the full record. In both canonical mode (`:125-153`, which
  sorts INFO split on `;`) and strict mode it sees the INFO bytes, so it does detect the
  bug wherever the input has escapes.

**HG002 inputs carry no `%`.**
- `~/workspace/data_vepyr/input/HG002_normalized.vcf.gz` is the md5/WGS gate input per
  `e2e-testing/README.md:322`. `HG002_norm.vcf.gz` and `HG002_GRCh38_chrXY_smallvar_v1.0.vcf.gz`
  were checked too.
- All three have **0 records** with `%` in INFO or the sample column.
- Their headers declare only String/Integer INFO fields: `platformnames`,
  `datasetnames`, `callsetnames`, `filt`, and so on.
- `tests/data/hg002_chr22/input_chr22.vcf.gz` and `tests/data/golden/input.vcf` also
  have 0.

**ClinVar fixtures** (branch `origin/e2e/clinvar-parity` @ 1acf208, copy at
`$S/fixtures/.../mismatches/*/info_percent_encoding/`). There are **11 contigs, not 13**:
chr1, chr3, chr4, chr6, chr8, chr10, chr11, chr13, chr15, chr16, chr19.
- Each fixture has 1 record, except chr13, which has 3.
- In every fixture `vep.vcf` and `vepyr.vcf` bodies differ and CSQ is identical.
- The chr13 records are 32316406 (`%3D`), 32333365 / ClinVar 51220 (`%3D` + `%20`) and
  48476738 (`%3B`).

## Probe results

All records are 15:29268829 C>T with one sample (`GT:FS`). Input: `$S/fix207/probe/probe.vcf`,
`probe_char.vcf`, `probe_fmtchar.vcf`, `issue_extra.vcf`.

**Notes on how these were produced**
- "VEP 116.0" is VEP's output with CSQ stripped. VEP's INFO+FORMAT+sample columns equal
  the input byte for byte for every probe (checked with `diff`).
- vepyr CSQ equals VEP CSQ on every probe.
- The "LazyFrame value" column comes from `vepyr.annotate(...).collect()` (script
  `$S/fix207/lf_probe.py`).
- **sink_vcf** (polars-bio 0.36.1 from the repo `.venv` on venv010's path,
  `$S/fix207/sink_probe.py`) wrote the **same decoded text as output_vcf** for every
  row. The second carrier is confirmed.
- **After reserved-set fix** is simulated with `$S/fix207/simulate_fix.py`: noodles
  decode, then re-encode with `CONTROLS + ; = % , \r \n \t`.

| Probe | Header | Input = VEP 116.0 output | vepyr 0.10.0 `output_vcf`/CLI | LazyFrame value | After reserved-set fix |
|---|---|---|---|---|---|
| p1 | CLNDN Number=. String | `CLNDN=a%3Bb` | `CLNDN=a;b` (INFO split, `b` becomes a flag key) | `['a;b']` | identical |
| p2 | X Number=1 String | `X=k%3Dv` | `X=k=v` | `'k=v'` | identical |
| p3 | L Number=. String | `L=a%2Cb,c` | `L=a,b,c` (**arity 2 becomes 3, silent**) | `['a,b', 'c']` | identical |
| p4 | P Number=1 String | `P=50%25` | `P=50%` | `'50%'` | identical |
| p5 | S Number=1 String | `S=a%20b` | `S=a b` (space in INFO) | `'a b'` | **still `a b`** |
| p6 | PLUS Number=1 String | `PLUS=a%2Bb` | `PLUS=a+b` | `'a+b'` | **still `a+b`** |
| p7 | SC Number=1 String | `SC=G%3B1:23` | `SC=G;1:23` | `'G;1:23'` | identical |
| p8 | COL Number=1 String | `COL=a%3Ab` | `COL=a:b` | `'a:b'` | **still `a:b`** |
| p9 | FORMAT FS String | `0/1:a%3Bb` | `0/1:a;b` | `FS='a;b'` | identical if FORMAT is encoded too |
| p10 | FORMAT FS String | `0/1:a%3Ab` | `0/1:a:b` (**extra sample sub-field; FORMAT keys misalign**) | `FS='a:b'` | identical only if FORMAT set adds `:` |
| p11 | all-in-one + FORMAT `x%2Cy` | `CLNDN=a%3Bb;X=k%3Dv;L=a%2Cb,c;P=50%25` / `0/1:x%2Cy` | `CLNDN=a;b;X=k=v;L=a,b,c;P=50%` / `0/1:x,y` | decoded | identical |
| c1 | XCHR Number=1 **Character** INFO | `XCHR=%3B;Y=q` | **run fails**: `Unsupported INFO value type for field 'XCHR'` (CLI exit 2; LazyFrame `ComputeError`) | fails | n/a (formats physical_exec side finding) |
| c2 | FORMAT FC **Character** | `0/1:%3B` | `0/1:;` | `FC=';'` | identical if FORMAT is encoded |
| e1 (issue p6) | lower-case `%3b` | `CLNDN=a%3bb` | `CLNDN=a;b` | `['a;b']` | **`a%3Bb`, case changes, not byte-identical** |
| e2 (issue p10) | malformed | `CLNDN=a%ZZb%` | `CLNDN=a%ZZb%` (unchanged today) | `['a%ZZb%']` | **`a%25ZZb%25`, regresses a value that matches today** |
| e3 | UTF-8 escape | `CLNDN=caf%C3%A9` | `CLNDN=café` | `['café']` | **still `café`** |
| csq_guard | chr22:16592176 A>G + `CLNDN=a%3Bb` | VEP keeps `a%3Bb`; CSQ has `p.Ala125%3D` | `CLNDN=a;b;CSQ=...p.Ala125%3D...` (with the committed `tests/data/hg002_chr22` cache too) | n/a | CLNDN identical; CSQ must stay `%3D`, never `%253D` |

**Two contradictions in the issue's "Regression test" section.** That section says p6
(lower case) and p10 (malformed) become byte-identical with the reserved-set encoding.
The simulation says otherwise:
- p6 comes out upper-cased.
- p10 has its literal `%` escaped to `%25`. vepyr 0.10.0 writes p10 correctly today.

So p6 and p10 must move to "known difference without raw pass-through", unless the
serializer leaves a `%` that is not followed by two hex digits unescaped. That is not
round-trip safe, so the trade-off has to be decided in formats. The issue's own caveat
already flags lower case. ClinVar has no malformed `%`, so this does not affect the
ClinVar fixtures.

## 4. Blast radius

**HG002 md5 gate (chr1-22, WGS profiles) and the committed chr22 golden.**
- The inputs have no `%` in INFO or FORMAT, so the gate **cannot prove the fix**. Its
  body md5 must stay unchanged.
- The gate **is** the guard for the CSQ exemption. In the VEP outputs, CSQ carries
  `%3D`:
  - `e2e-testing/golden/116/chr22/merged.vcf.gz` (LFS) has 300 of 50,861 records with
    `%3D`, all inside CSQ, and 0 with `%` outside CSQ.
  - The first 2M records of `~/workspace/data_vepyr/output/116/HG002_annotated_wgs_everything_hgvs_merged.vcf.gz`
    have 5,302 lines with `%3D`.
- If the serializer encoded CSQ, these would become `%253D` and the CSQ separators would
  become `%2C`, and the md5 gate would fail. The same applies to plugin values, which
  live in CSQ.

**In-repo pytest.**
- No existing test should change. No fixture carries reserved escapes, and
  `TestInputFieldNamedLikeAnAnnotationColumn` uses plain values.
- The golden CSQ fixtures have no `%3D`, so pytest would **not** catch a CSQ
  double-encode. The new test below adds that guard.
- `test_vcf_columns.py:593/:621` (sink_vcf == output_vcf) stay green on golden. But
  sink and output_vcf **diverge for escaped inputs** until polars-bio re-pins. A new
  sink assertion therefore has to be gated on the polars-bio version (skip/xfail).

**Fixture that can prove the fix.** The ClinVar `info_percent_encoding` fixtures,
11 contigs.
- After a reserved-set fix, **10 of 11 become byte-identical** to `vep.vcf`: chr1, chr3,
  chr4, chr6, chr8, chr10, chr11, chr15, chr16, chr19.
- **chr13 stays different**: 1 of its 3 records, ClinVar 51220 at 13:32333365,
  `base_change%3Dins%20A`, comes out as `base_change%3Dins A`. This matches the issue's
  "2 release records" caveat and needs the raw pass-through follow-up.
- The issue's "Done when" lists chr6/chr8/chr10/chr15 only. The plan should name chr13
  explicitly as an expected residual.

**Other outputs.**
- The nf-core test-datasets snapshot (HG002 chr22, 1,000 records) and the plugin e2e
  inputs are HG002, so they are not affected.
- `docs/quickstart.md:287` needs a caveat for unreserved, lower-case and malformed
  escapes.

## 5. Pin cascade and vepyr regression test

**Every Cargo.toml dependency line pinning the formats and functions crates**
(origin/master; v0.10.0 lines 217-219):
```
Cargo.toml:220  datafusion-bio-function-vep         = { git = ".../datafusion-bio-functions.git", rev = "483a49982a490fe9a256b68e724f66bcb210485f", features = ["cache-builder"] }
Cargo.toml:221  datafusion-bio-format-ensembl-cache = { git = ".../datafusion-bio-formats.git", tag = "v1.14.0" }
Cargo.toml:222  datafusion-bio-format-vcf           = { git = ".../datafusion-bio-formats.git", tag = "v1.14.0" }
```
- At v0.10.0, line 217 pinned the engine by `tag = "v0.23.1"`. origin/master moved it
  to `rev 483a499` (functions#272, projection pushdown).
- Cargo.lock origin/master resolves every formats crate (bed, core, ensembl-cache, gff,
  vcf) to `tag=v1.14.0#72efe677`. noodles-vcf is `rev=42a3c016` and does not change.
- The engine's `bio-function-vep/Cargo.toml:45-49` pins vcf, bed, gff, core and
  ensembl-cache to `tag = "v1.14.0"`.
- The two vepyr formats lines must use **exactly the same source** (same tag, or same
  rev, spelled the same way) as the engine's. Otherwise two copies of
  `datafusion-bio-format-vcf/-core` fail to compile (`Cargo.toml` comment ~:76-88,
  :169).

**vepyr-side change.**
1. **Pin bump only.** Move the engine line 220 to the functions merge or tag that
   carries the new formats. Move lines 221-222 to the new formats tag (for example
   `v1.14.1`) in lockstep. Add a history comment block following the file's convention.
   Regenerate Cargo.lock. Mind the stale-lock clippy hook trap.
2. **Docs.** Add a caveat to `docs/quickstart.md:287`.
3. **polars-bio follow-up, not in this PR.** Raise the `pyproject.toml:27` floor once a
   polars-bio release carries the new formats.
4. **Regression test**, below.

No Python or Rust logic change is needed.

**Proposed regression test.** Add it to `tests/test_annotate.py`, next to
`TestInputFieldNamedLikeAnAnnotationColumn`, using its `_annotate` helper pattern.
Alternatively, put it in `test_projection_pushdown.py`-style chr22 fixture code so the
CSQ guard is real.

```python
# hg002_chr22 fixture: chr22:16592176 A>G is synonymous, so its CSQ carries the
# engine's own `p.Ala125%3D` (verified with vepyr 0.10.0 on tests/data/hg002_chr22).
PERCENT_ESCAPED_VCF = """##fileformat=VCFv4.2
##contig=<ID=chr22>
##INFO=<ID=CLNDN,Number=.,Type=String,Description="list">
##INFO=<ID=CLNVI,Number=.,Type=String,Description="list">
##INFO=<ID=GENEINFO,Number=1,Type=String,Description="scalar">
##INFO=<ID=P,Number=1,Type=String,Description="scalar">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=FS,Number=1,Type=String,Description="format string">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1
chr22\t16592176\tp1\tA\tG\t.\t.\tCLNDN=a%3Bb|c;CLNVI=BIC:5817&base_change%3DC_to_G\tGT:FS\t0/1:x%3Ay
chr22\t16592176\tp2\tA\tG\t.\t.\tCLNDN=a%2Cb,c;GENEINFO=G%3B1:23;P=50%25\tGT:FS\t0/1:x%3By
"""

class TestPercentEscapesInTheOutputVcf:  # vepyr#207
    def test_reserved_escapes_are_written_back_byte_for_byte(self, tmp_path):
        # output_vcf: INFO minus the trailing CSQ, and the sample column, equal the input
        #   (Ensembl VEP OutputFactory/VCF.pm:314-349 keeps the input line).
        # assert record[7].rsplit(";CSQ=", 1)[0] == input_info
        # assert record[9] == input_sample
    def test_csq_is_not_re_encoded(self, tmp_path):
        # `p.Ala125%3D` present, `%253D` absent, CSQ still has more than one block
        #   split on ',', none contains `%2C`.
    def test_lazyframe_keeps_the_decoded_values(self):
        # CLNDN == [['a;b|c'], ['a,b', 'c']], GENEINFO 'G;1:23', P '50%', FS 'x:y' / 'x;y'
    def test_sink_vcf_writes_the_same_escapes(self, tmp_path):
        # pytest.importorskip("polars_bio"); skip/xfail when polars-bio's formats
        # predates the fix. Same assertions as the first test.
```

Unreserved, lower-case and malformed escapes (`%20`, `%2B`, `%3A` in INFO, `%3b`,
`a%ZZb%`) go in a separate `xfail(strict=True)` that documents the known difference
until the raw pass-through follow-up lands. As written, strict xfail also flips when that
follow-up lands. The e2e proof stays the ClinVar fixtures: 10 contigs byte-identical, and
chr13/51220 as the expected residual.

## Sources read

- `$S/fix207/issue207.md` (issue and comment).
- vepyr origin/master (c089bef) and 3f21520:
  - `Cargo.toml` (1-51, 196-222) and `Cargo.lock` (formats, functions and noodles entries);
  - `src/annotate.rs` (115-215, 360-410, 538-700);
  - `src/lib.rs` (828-860);
  - `src/vepyr/__init__.py` (1838-1905, 2380-2478, plus a grep);
  - `src/vepyr/cli.py` (560-600);
  - `src/vepyr/_vcf_metadata.py`;
  - `pyproject.toml`;
  - `tests/test_annotate.py` (30-50, 316-405, 2937-3200);
  - `tests/test_vcf_columns.py` (567-680);
  - `e2e-testing/scripts/md5_concordance.py` (1-170), `comparison/compare.py` and
    `comparison/vcfio.py` (greps);
  - `e2e-testing/README.md:322`, `.gitattributes`, `docs/quickstart.md:281-291`,
    `docs/dataframes.md:640-650`.
- Engine checkout `$S/engine/datafusion/bio-function-vep/Cargo.toml:33-49`.
- Data:
  - `~/workspace/data_vepyr/input/HG002_{normalized,norm,GRCh38_chrXY_smallvar_v1.0}.vcf.gz`
    (headers and `%` grep);
  - working-tree LFS `e2e-testing/golden/116/chr22/merged.vcf.gz`;
  - `~/workspace/data_vepyr/output/116/HG002_annotated_wgs_everything_hgvs_merged.vcf.gz`
    (first 2M records);
  - the ClinVar fixtures under `$S/fixtures/e2e-testing/golden/116/clinvar/mismatches/*/info_percent_encoding/`.
- Runs:
  - vepyr 0.10.0 CLI on all probes, plus on `csq_guard.vcf` with the merged cache and with
    `tests/data/hg002_chr22`;
  - VEP 116.0 docker on all probes;
  - LazyFrame collect (`lf_probe.py`);
  - polars-bio 0.36.1 `sink_vcf` (`sink_probe.py`);
  - reserved-set simulation (`simulate_fix.py`).
