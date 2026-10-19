# Issue #207: re-encode reserved characters when writing INFO/FORMAT strings

Status: **implemented on the combined stack** (formats#264 → formats#266 →
functions#275 → vepyr#251, none merged). Approved 2026-10-18 (§9). The gates
for the current pins, and their verdicts, are recorded in the vepyr#251
description. §§10–11 record execution and the current stack.
The sections up to §9 are the plan as approved, kept unchanged.

Issue: [biodatageeks/vepyr#207](https://github.com/biodatageeks/vepyr/issues/207)
(tracked in #240). The three per-repo analyses (formats, functions, vepyr) this plan
comes from are summarised in §3–§6; the full reports stay with the vepyr-fix evidence,
not in the repo.

Sources read: formats at the pinned tag `v1.14.0` = `72efe67` (the human checkout is
on `feat/cooler` `dbc62fb`, branched from v1.11.0, where `serializer.rs` line numbers
are 17–26 lower), the noodles fork at `42a3c016` (cargo checkout), functions at
`v0.23.1` = `b960112` (human checkout on `fix/mnv-allele-trim-parity` `46d8c20`, older
than v0.23.1, do not base on it), vepyr `origin/master` `c089bef` (v0.10.0 release
commit `3f21520`), Ensembl `ensembl-vep` tag `release/116.0`.

## 1. Defect

ClinVar (and the VCF 4.3 spec, §1.2) percent-encodes reserved characters inside INFO
values: `;` as `%3B`, `=` as `%3D`, `,` as `%2C`, `%` as `%25`. The reader decodes them
(correct for DataFrame users: the LazyFrame holds `'k=v'`, `['a,b', 'c']`). The VCF
writer then emits the decoded text without encoding it again, so:

- `;` ends the INFO field early and the rest of the value is read as a key of its own
  (`CLNDN=a%3Bb` → `CLNDN=a;b`);
- `=` creates a key/value inside a value (`X=k%3Dv` → `X=k=v`);
- `,` silently changes list arity (`L=a%2Cb,c`, 2 values → `L=a,b,c`, 3 values);
- FORMAT `:` misaligns the sample sub-fields (`x%3Ay` → `x:y`).

The line is no longer valid VCF and differs from Ensembl VEP's output, which copies
the input INFO verbatim. Every output path is affected: CLI `-o`, `annotate(output_vcf=)`
(serial and sharded workers), and LazyFrame → `polars_bio.sink_vcf`.

Scale: ClinVar 20261011 has 3,032 records with `%3D`, 365 with `%3B`, 4 with `%20`,
2 with `%2B` (no `%2C`, `%25`, lower-case or bare `%`). In the parity run this is the
`info_percent_encoding` class on 11 contigs (chr1, 3, 4, 6, 8, 10, 11, 13, 15, 16, 19),
e.g. 770 records on chr13 (BRCA2 `CLNVI`), 131 on chr1, 114 on chr16.

## 2. The rule, quoted from Ensembl VEP 116.0

VEP keeps the raw tab-split input record and never decodes or re-encodes INFO.
[`Parser/VCF.pm:349`](https://github.com/Ensembl/ensembl-vep/blob/release/116.0/modules/Bio/EnsEMBL/VEP/Parser/VCF.pm#L349)
stores it as `_line => $record`;
[`OutputFactory/VCF.pm:314-355`](https://github.com/Ensembl/ensembl-vep/blob/release/116.0/modules/Bio/EnsEMBL/VEP/OutputFactory/VCF.pm#L314-L355)
copies it and only appends CSQ:

```perl
    # if input was VCF then we get _line with the original contents
    if($self->{input_format} eq 'vcf' && $vf->{_line}) {

      $line = $vf->{_line};

      # array copy to keep original intact
      my @tmp = @$line;
      $line = \@tmp;
...
      # nuke existing CSQ field?
      if($line->[7] =~ /(^|\;)$fieldname\=/ && !$self->{keep_csq}) {
        $line->[7] =~ s/(^|\;)$fieldname\=\S+?(\;\S|$)/$2/;
        $line->[7] =~ s/^\;//;
      }
...
    if(@chunks) {
      $line->[7] .= ';' if $line->[7];
      $line->[7] .= $fieldname.'='.join(",", @chunks);
    }
```

So exact parity is "input INFO bytes out unchanged, then `;CSQ=`". The CSQ values
themselves are escaped by VEP (`s/,/&/; s/;/%3B/; s/\s+/_/; s/\|/&/`,
[`OutputFactory/VCF.pm:387-419`](https://github.com/Ensembl/ensembl-vep/blob/release/116.0/modules/Bio/EnsEMBL/VEP/OutputFactory/VCF.pm#L387-L419))
and the engine already reproduces that.

Confirmed on records: VEP 116.0 writes the input INFO and FORMAT text back byte for
byte on every probe in §6 and on every `info_percent_encoding` fixture record.

## 3. Ownership

- **Owner: datafusion-bio-formats**, `bio-format-vcf/src/serializer.rs`. One writer
  path, `batch_to_vcf_lines` (`serializer.rs:598`), is used by every caller:
  `VcfWriteExec` (`write_exec.rs:339,366`), the engine's VCF sink (`vcf_sink.rs:321`,
  `:2189`) and polars-bio `write_vcf`/`sink_vcf`. It writes INFO (`:993`), scalar
  strings (`:1029-1037`), lists (`:1062-1119`, joined at `:1046/1054`) and FORMAT
  (`:342-377`, `:1374-1411`) with no encoding.
- **Not the reader.** Decoding happens in noodles (`record/info/field/value.rs:63-69`,
  list elements after the `,` split at `.../array/values.rs:110`, FORMAT at
  `record/samples/series/value.rs:87-88`), and it is spec-correct. Changing it would
  change LazyFrame values for every user and the plugin caches built from VCF sources
  (`plugin_cache/provider.rs:247`). Keep it.
- **functions: carrier.** It never modifies an INFO value (rename/alias-back for #121 is
  schema-only, `annotate_provider.rs:901-955`, `vcf_sink.rs:232-295`). It must (a) bump
  every formats pin together and (b) mark CSQ so the writer does not encode it again
  (§4.2).
- **vepyr: carrier.** No percent logic anywhere in `src/`. Pin bump, a regression test
  and a docs correction (`docs/quickstart.md:287` says INFO is "written back unchanged").
- **polars-bio: a fourth carrier**, outside the three-repo stack. Its own build of the
  serializer (formats rev `4b58ea6` = v1.14.0 + 2 VCF-neutral commits) gives
  `sink_vcf` the same bug. It needs its own re-pin and release; see §9 Q4.

## 4. The change, in dependency order

### 4.1 formats (owner)

In `serializer.rs`, encode values on write, with a byte loop over an **ASCII-only**
reserved set and upper-case hex:

- INFO values (scalar strings and every list element, before the `,` join):
  `;` `=` `%` `,` and ASCII controls (incl. `\t \r \n`).
- FORMAT values: `:` `%` `,` and ASCII controls (noodles' own writer set).
- `extract_list_values` is shared by INFO and FORMAT, so it takes the reserved set as
  a parameter.

Do **not** use `percent_encoding::utf8_percent_encode` (what noodles' writer uses): it
encodes every non-ASCII byte. ClinVar 20261011 has 6,576 records with UTF-8 in INFO
(mostly en-dashes in `CLNDN`) that vepyr writes byte-identical to VEP today; that
encoder would break all of them.

**Exemption for already-joined text (CSQ).** The engine's CSQ is a scalar `Utf8`
column that already holds VCF text (`annotate_provider.rs:3950`) and can contain `%3D`,
`%3B` and the `,` block separator. Encoding it would give `%253D` and `%2C`. Rule
(recommended, see Q2): a scalar `Utf8` column whose field metadata declares
`bio.vcf.field.number` other than `1` is written verbatim. The reader never produces
that shape (a Number≠1 field is read as a `List`), so input fields are unaffected.
As delivered, only `Number=.` qualifies (tightened after review, §10).

### 4.2 functions (carrier)

- Put `bio.vcf.field.number = "."` (and type String) on the CSQ output field at
  `annotate_provider.rs:3950`, so the writer's exemption applies. Plugin values already
  map `=` → `%3D` (`plugin_cache/csq.rs:29-38`); that workaround is for the *reader*
  decode of plugin source VCFs and stays.
- Bump all formats pins to the formats PR head, together:
  `bio-function-vep/Cargo.toml:45-49` (vcf, bed, gff, core, ensembl-cache),
  `bio-function-pileup/Cargo.toml:23,30`, and `Cargo.lock`.

### 4.3 vepyr (carrier)

- Bump `Cargo.toml:220` (engine rev) and `:221-222` (the two formats crates, which must
  match the engine's formats source or the build fails), regenerate `Cargo.lock`.
- Correct `docs/quickstart.md:287`.

## 5. Failing tests first

- **formats** (owner), `tests/record_layout_test.rs` `round_trip` harness
  (`:55-105`) plus a serializer unit test: INFO `a%3Bb`, `k%3Dv`, `50%25`, a
  Number=. list `a%2Cb,c`, a FORMAT `x%3Ay`, and a UTF-8 value `a–b`. Read, write, and
  assert the output bytes equal the input. A scalar `Utf8` column with
  `number="."` holding `p.Ala125%3D,x` is written verbatim. Fails today on every
  reserved case.
- **functions**: through `format_vcf_body_chunk`, a batch with a decoded `List<Utf8>`
  input field, a renamed `INFO_AF` column and a CSQ string containing `%3D` and `,`;
  assert the input field is encoded and CSQ is untouched.
- **vepyr**: `tests/test_annotate.py`, two records at chr22:16592176 A>G on the
  committed `tests/data/hg002_chr22` fixture (synonymous, so its CSQ carries
  `p.Ala125%3D`), with the probe values in INFO and one FORMAT field. Assert: the INFO
  text before `;CSQ=` and the sample column are byte-identical to the input for
  reserved escapes; CSQ has `%3D` and no `%253D`/`%2C`; the LazyFrame still returns
  decoded values. `sink_vcf` assertion skipped until polars-bio carries the fix. A
  strict `xfail` records the cases option (a) cannot restore (§6).
- **Fixtures that can see it:** the ClinVar `*/info_percent_encoding/` fixtures on
  branch `e2e/clinvar-parity`. The HG002 golden **cannot**: its input has no `%`, no
  second `=` and no space in INFO (scanned, 4,048,342 records).

## 6. What option (a) can and cannot restore

Probe results (VEP 116.0 output = input, every row):

| Input | vepyr 0.10.0 today | After the fix |
|---|---|---|
| `CLNDN=a%3Bb` (Number=.) | `CLNDN=a;b` | identical to input |
| `X=k%3Dv` | `X=k=v` | identical |
| `L=a%2Cb,c` (Number=.) | `L=a,b,c` | identical |
| `P=50%25` | `P=50%` | identical |
| FORMAT `x%3Ay` | `x:y` | identical |
| UTF-8 `a–b` | identical | identical (ASCII-only set) |
| `S=a%20b` | `S=a b` | still `a b` |
| `PLUS=a%2Bb` | `PLUS=a+b` | still `a+b` |
| `a%3bb` (lower case) | `a;b` | `a%3Bb` (case lost) |
| `a%ZZb%` (malformed) | identical | `a%25ZZb%25` (**new difference**) |

On ClinVar 20261011 this restores every escaped record except the 2 with `%20`/`%2B`
(e.g. chr13 ClinVar 51220 `base_change%3Dins%20A` → `base_change%3Dins A`). The last
four rows need the raw input text carried through (option b, §9 Q1).

## 7. Pin cascade

| PR | pins |
|---|---|
| formats `fix/issue-207-info-percent-encoding` | nothing |
| functions, same branch name | formats PR head, all 7 formats lines + lock |
| vepyr, same branch name | functions PR head (rev) + the two formats crates at the formats PR head |

## 8. Expected gate movement

- **md5 strict, HG002 chr1-22:** unchanged. The input has no escapes, and CSQ is
  exempt. The gate *does* guard the exemption: VEP's CSQ carries `%3D` (300 of 50,861
  records in the chr22 golden), so a missing exemption would fail it.
- **ClinVar `info_percent_encoding` fixtures:** 10 of 11 contigs become byte-identical
  to `vep.vcf` for that class. chr13 keeps one record (ClinVar 51220, `%20`). Checked
  as an extra gate, alongside the strict md5.
- **Performance:** neutral within noise. One byte scan per INFO/FORMAT string value,
  no allocation when nothing needs escaping. Phase durations ±5%, wall/RSS ±10% at 1
  and 8 workers.
- **Step 2 precondition:** the data volume has 60 GiB free. The perf sweep wants
  ~65 GiB, so about 5 GiB has to be cleared first (e.g. the regenerable
  `vep_chr*_merged.vcf` md5 slices).

## 9. Decisions (2026-10-18, answered by the maintainer)

1. Option (a), re-encode on write, now; raw passthrough (b) is filed as a follow-up
   (biodatageeks/vepyr#252).
2. CSQ is exempted by the metadata rule (scalar `Utf8` + `number="."` written verbatim;
   the engine sets `number="."` on CSQ), with a test on the `sink_vcf` path.
3. The `"true"`/`"false"` flag bug and the `Type=Character` abort are filed as separate
   issues, out of this fix (biodatageeks/datafusion-bio-formats#265 and #263).
4. polars-bio is a follow-up re-pin and release after the formats release
   (biodatageeks/polars-bio#493).

Go-ahead: 2026-10-18 ("proceed with #207").

## 10. Changes during execution

- **Exemption tightened to `Number=.`** (formats `2a1e622`, after a codex P2): a
  scalar declared `A`, `R`, `G` or a count is an ordinary value and is encoded.
- **Performance, not neutral as §8 expected.** The interleaved sweep against vepyr
  master `0b24319` measured `vcf_format` +9.0% (8 workers, median of 3 pairs) for one
  scan per value. A 256-entry lookup table (formats `c419b7b`) left it at +8.8%: the
  HG002 INFO lists hold ~29 values of ~16 bytes per record, so the cost is per call,
  not per byte. Formats `56d678d` scans each INFO column's value bytes once per batch,
  with a branch-free fold over 32-byte chunks the compiler vectorizes (~0.05 ns/byte),
  and writes a column with no reserved character exactly as before. FORMAT values keep
  the per-value scan: GT is their only string on HG002.
- **A space in INFO under a header before VCF 4.3** (formats `e52af5c`, functions
  `7dcef7a`; asked for by the maintainer on 2026-10-19 after the hand-off). VCF 4.1 and
  4.2 allow no whitespace in an INFO value, and 4.3 allows spaces. Writing a space
  decoded from `%20` back as a plain space made ClinVar 51220 differ from VEP and broke
  the 4.1 its header declares. The serializer now takes the version the header writes
  (`header_file_format`, resolved exactly as the header writer resolves it) and, before
  4.3, escapes the space too. FORMAT is unchanged: 4.1 and 4.2 give sample values no
  whitespace rule. ClinVar 20261011 has no literal space in INFO and HG002 none at all,
  so neither gate input can lose a byte this way. Left for #252: ClinVar 125781 and
  125779 (chr17), whose CLNVI holds both `%2B` and a literal `+`.

## 9a. Execution layout

| Repo | Worktree | Branch `fix/issue-207-info-percent-encoding` based on |
|---|---|---|
| formats | `~/research/git/datafusion-bio-formats-fix-207` | tag `v1.14.0` (`72efe67`), exactly what the engine and vepyr pin |
| functions | `~/research/git/datafusion-bio-functions-fix-207` | `483a499`, the engine rev vepyr master pins (engine master has since added #273, which is #199's fix; basing on it would mix #199 into this gate) |
| vepyr | `~/research/git/vepyr-fix-207` | `origin/master` `c089bef` |
| vepyr baseline binary | `~/research/git/vepyr-base-207` (detached `c089bef`, never edited) | |

Evidence: `~/research/git/vepyr-fix-207/.superpowers/sdd/issue207/evidence/`, kept
outside the shared `e2e-testing/results` directory. Performance is measured as an
interleaved base/fix sweep in one session (as for #199/#200), because the host is
shared; the step-2 baseline sweep is kept as well.

## 10. Open questions (as asked)

1. **Re-encode (a) now, raw passthrough (b) as a follow-up?** (a) restores every
   ClinVar escape except `%20`/`%2B`, but lower-case and malformed escapes change form,
   and `a%ZZb%` becomes a *new* difference. (b) is byte parity with VEP (carry the raw
   INFO/FORMAT text for text-VCF input). It needs a new layout column across core, the
   engine and the LazyFrame path, and (a) as the fallback for non-VCF sources.
   Recommendation: (a) here, (b) filed as a follow-up issue.
2. **How to exempt CSQ:** the structural rule (scalar `Utf8` + `number≠1` written
   verbatim, engine sets `number="."`) or by the name `CSQ`. The name rule is wrong for
   a CSQ read back from a VCF, which arrives as a decoded `List` and must be encoded.
   The metadata rule must survive polars-bio's `sink_vcf` field-metadata mapping;
   vepyr's LazyFrame already declares CSQ `number: "."`, which polars-bio maps, but
   that is untested. Recommendation: structural, with a test on the `sink_vcf` path.
3. **Scope:** FORMAT encoding is in (same function, same bug). Two neighbouring bugs
   found by the analysis are out of scope unless you say otherwise:
   - `build_info_string` (`serializer.rs:989-994`) writes a String INFO value `"true"`
     as a bare flag and drops `"false"`;
   - a `Type=Character` INFO field aborts the whole run (`physical_exec.rs:788-791`).
   Recommendation: file both as separate issues.
4. **polars-bio:** re-pin and release it as a separate follow-up after the formats
   release (it is outside this three-repo stack)? Until then `sink_vcf` keeps the bug
   while `output_vcf` is fixed. Recommendation: yes, follow-up.

## 11. Current stack and re-verification (2026-10-19)

- The stack is formats#264 (`b61279e`) → formats#266 (`64c4001`, the Character INFO
  reader fix for datafusion-bio-formats#263, based on the #264 branch) →
  functions#275 (`57106aa`, pins `64c4001`, merges functions master and so carries
  #274's MT codon table) → vepyr#251 (pins `64c4001` and `57106aa`). The separate
  #263 pin PRs functions#276 and vepyr#253 were closed as superseded; the maintainer
  chose to keep the combined stack. Nothing is merged.
- An earlier pin, formats `cfc7094`, also carried two unrelated formats master
  commits (#260, #261); `64c4001` drops them.
- On a one-record fixture, Ensembl VEP 116.2 and vepyr both emit `XCHR=%3B` and
  `XCHARS=A,B`.
- Whole-genome strict md5, the ClinVar gate and the interleaved performance gate are
  re-run on this exact stack and recorded in the vepyr#251 description, which
  supersedes the gate numbers recorded for earlier pins.
