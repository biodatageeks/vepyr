# VEP 115/116 Dual-Release Parity Implementation Plan

> **For implementers:** execute this plan in task order. Use one commit boundary per
> repository per task, keep the existing dirty vepyr files out of those commits, and
> do not accept any of the six full chr1–22 release/profile gates until the final
> pinned build.

**Goal:** Make one vepyr build reproduce Ensembl VEP 115 or 116 according to the
cache release, reduce the measured release-116 mismatch count from 734 to zero,
and prove exact chr1–22 parity for the merged, Ensembl-only, and RefSeq-only
profiles under both releases.

**Architecture:** The raw-cache reader (`bio-formats`) establishes a cache-release
contract. The converted-cache builder and annotation engine (`bio-functions`)
preserve it. At runtime, `bio-functions` validates the relevant Parquet metadata
once per requested contig, memoizes the result, and passes that contig's
`VepSemantics` value into the consequence/HGVS engine. Only the two source-proven
115→116 behavior changes consume that value: partial-transcript HGVS and the
stop-predicate family.
ClinVar reference-allele handling is activated by an optional cache column, and
the chr11/chr16/chr20 fixes are unconditional. Each vepyr package release embeds
an explicit support matrix. The Python wrapper (`vepyr`) may assert an expected
cache version; the native engine checks each requested contig's Parquet metadata
against the matrix immediately before that contig runs. The comparison harness
writes an uncapped mismatch ledger and reproducible build provenance.

**Primary analysis:**
[2026-07-29-remaining-734-root-cause-and-plan.md](../2026-07-29-remaining-734-root-cause-and-plan.md)

**Source delta:**
[2026-07-28-vep-115-to-116-changelog_8815.md](../../../e2e-testing/115-116/2026-07-28-vep-115-to-116-changelog_8815.md)

**Current 116 benchmark:**
[fast_chr1_chr22_merged_116_summary_20260728_2105.md](../../../e2e-testing/reports/fast_chr1_chr22_merged_116_summary_20260728_2105.md)

## Repository and data topology

| Responsibility | Working tree |
|---|---|
| Python API, native wrapper, comparator, release reports | `/Users/mwiewior/research/git/vepyr` |
| Consequence engine, converted-cache builder and lookup | `/Users/mwiewior/research/git/_wt/biofunctions-pwm` |
| Raw Ensembl cache reader and Arrow schema contract | `/Users/mwiewior/research/git/_wt/bioformats-pwm` |
| Generated release-115 caches | `~/workspace/data_vepyr/cache/115_GRCh38_{merged,ensembl,refseq}` |
| Generated release-116 caches | `~/workspace/data_vepyr/cache/116_GRCh38_{merged,ensembl,refseq}` |

The local-development absolute worktree patches have now been removed. vepyr
commit `edd2995` pins the pushed PR heads listed below. Qualification revision
`01350d6` was rebuilt natively from that durable graph and produced all six final
chr1–22 parity gates.

## Implementation snapshot — 2026-07-29

The checklist below remains the reviewable execution plan. Its current state is:

| Work | State |
|---|---|
| Comparator, uncapped ledger, reference identity, and per-report build provenance | implemented |
| OpenSpec release/cache contract | implemented and strictly validated |
| bio-formats raw-release metadata | pushed to PR #224 at `eee2d6926331fe5106cbbefbc1ca673e94357327` |
| bio-functions metadata preservation, strict resolver, and contig-lazy validation | implemented |
| Python/native support-matrix and expected-version APIs | implemented |
| Optional ClinVar reference allele and all three exact variants | implemented |
| chr11/chr16/chr20 residuals | implemented; chr11 predicate is now proven |
| Nine VEP 116 stop behaviors | implemented behind one policy |
| VEP 116 partial-overlap HGVS | implemented behind one policy |
| Safe generalized 115/116 cache rebuild | implemented in `e2e-testing/scripts/rebuild_release_cache.py`; dry-run verified |
| Machine release gate | implemented in `e2e-testing/scripts/verify_parity_gate.py` |
| Rebuilt 115/116 merged cache artifacts | complete; all manifest-referenced footers and row counts verified |
| Rebuilt 115/116 Ensembl and RefSeq cache artifacts | all four transactional builds complete and verified |
| Full current-build chr1–22 merged parity | pinned [115](../../../e2e-testing/reports/fast_chr1_chr22_merged_115_summary_20260729_2308.md) and [116](../../../e2e-testing/reports/fast_chr1_chr22_merged_116_summary_20260729_2317.md) gates exact |
| Full current-build chr1–22 Ensembl/RefSeq parity | pinned [115 Ensembl](../../../e2e-testing/reports/fast_chr1_chr22_ensembl_115_summary_20260729_2243.md), [115 RefSeq](../../../e2e-testing/reports/fast_chr1_chr22_refseq_115_summary_20260729_2233.md), [116 Ensembl](../../../e2e-testing/reports/fast_chr1_chr22_ensembl_116_summary_20260729_2308.md), and [116 RefSeq](../../../e2e-testing/reports/fast_chr1_chr22_refseq_116_summary_20260729_2243.md) gates exact |
| Durable upstream revisions and removal of absolute path patches | bio-formats `eee2d6926331fe5106cbbefbc1ca673e94357327`, bio-functions `0d02d711b352baf4087e2e9421e12716e10bb290`, vepyr qualification revision `01350d6` containing pin commit `edd2995` |

Verified suites at this snapshot: bio-formats 461 passed/1 ignored,
bio-functions with all features 904 passed/1 ignored, pinned vepyr Python
994 passed/2 skipped, and pinned vepyr Rust 4 passed. All six final profile
gates compare 4,096,123 variants with every enforced mismatch counter at zero.

## Non-negotiable design decisions

1. Cache version and VEP codebase version are distinct identifiers. This vepyr
   release supports exactly:

   | Cache | VEP | API | Ensembl core | Variation | Semantics |
   |---|---|---|---|---|---|
   | `"115"` | `"115.2"` | `"115"` | `266b84d` | `b7c2637` | `V115` |
   | `"116"` | `"116.0"` | `"116"` | `c0cf13d` | `2fb834b` | `V116` |

   The support matrix is versioned with vepyr. Supporting VEP/cache 117 later
   requires an explicit matrix and policy change in a new vepyr release.
2. Add one semantic enum:

   ```rust
   pub enum VepSemantics {
       V115,
       V116,
   }
   ```

3. Resolve it once per requested contig and memoize that validation for the
   invocation. Do not parse paths in transcript or per-variant hot loops.
4. Annotation cache identity comes only from `bio.vep.cache_version` Arrow
   metadata. Generated-cache directory names are never an annotation fallback.
   Strict raw-cache basename parsing is permitted only during conversion because
   the shipped raw 115/116 `info.txt` files omit the version.
5. Do not add an annotation-cache version sidecar or marker file. Existing
   `chrom_manifest.json` files may locate a contig's shards, but only Parquet
   footer/schema metadata may establish their version. The existing provenance
   column is informational and is not an identity source.
6. Cache identity is mandatory. Missing, malformed, conflicting and unsupported
   cache versions are errors before annotation; there is no V115 fallback.
7. An optional `expected_cache_version` is an assertion only. It must agree with
   the independently detected Parquet metadata and cannot make an unlabeled
   cache acceptable.
8. A recognized-looking but unsupported future release, such as `117`, is an
   error even when supplied as the expected version. It must not silently inherit
   V116 behavior.
9. Only these decisions are gated by `VepSemantics`:
   `partial_overlap_hgvs` and `stop_codon_predicates`.
10. `clin_sig_ref_allele` is an optional nullable cache field. Its presence and
   non-empty row value activate the VEP 116 allele-orientation behavior; it is not
   a required release-115 column.
11. The chr11, chr16 and chr20 corrections are release-independent.
12. IMPACT is derived from consequence terms. HGVSp consumes the same coding
    classification. Neither gets a separate release conditional.
13. Do not port VEP 116's dead `$consider_ins_len` plumbing.
14. Preserve existing non-boundary HGVS behavior and existing cache lookup
    layout/performance.
15. Runtime validation is contig-lazy. Immediately before loading/annotating a
    contig, validate only that contig's manifest-referenced shards across every
    present entity. Those shards must carry one supported cache version and one
    consistent source type. A chr1-only run must not open chr2 data.
16. Never rebuild a live cache in place. Build and verify a complete staging
    cache, eagerly verify every emitted shard once, swap it recoverably, and
    retain the old cache until final parity passes.

## Dependency graph and burn-down

```text
comparison evidence ───────────────────────────────────────────────┐
                                                                  │
bio-formats cache-release contract                                │
          │                                                       │
          v                                                       │
bio-functions metadata preservation + VepSemantics resolver       │
          │                         │                              │
          │                         ├── stop policy (224)           │
          │                         └── partial HGVS policy (388)   │
          │                                                       │
          ├── optional ClinVar field + lookup (112) ── cache swap │
          └── release-independent residual fixes (10)              │
                                                                  v
vepyr API + supported-target assertion ────────────── pinned release gates
```

| Milestone | Expected 116 mismatches |
|---|---:|
| Frozen current build | 734 |
| Release metadata/semantics plumbing | 734 |
| ClinVar reference allele | 622 |
| chr11/chr16/chr20 residuals | 612 |
| VEP 116 stop predicates | 388 |
| VEP 116 partial-overlap HGVS | 0 |

The count is a diagnostic expectation, not permission to ignore a different
result. Any deviation pauses the burn-down and requires ledger reconciliation.

---

### Task 0: Freeze worktree state and implementation baselines

**Repositories:** all three, read-only.

**Produces:** a recorded pre-change state and no mixed user/implementation
commits.

- [ ] Record `git status --short`, branch and full SHA in each repository.
- [ ] Record the current vepyr `Cargo.toml` path overrides and `Cargo.lock` hash.
- [ ] Preserve all unrelated dirty vepyr files. In particular, do not stage
  `uv.lock`, existing untracked plans/assets, the changelog directory, or the
  plotting script unless a later task explicitly owns one of them.
- [ ] Confirm the exact local raw and generated cache roots used for both
  releases; do not infer them from `$HOME` inside implementation code.
- [ ] Confirm available disk space before any cache staging operation.

Commands:

```bash
git -C /Users/mwiewior/research/git/vepyr status --short
git -C /Users/mwiewior/research/git/_wt/biofunctions-pwm status --short
git -C /Users/mwiewior/research/git/_wt/bioformats-pwm status --short
shasum -a 256 /Users/mwiewior/research/git/vepyr/Cargo.lock
```

**Acceptance:** all subsequent commits can be reviewed repository-by-repository,
and no pre-existing user change is included.

---

### Task 1: Make parity evidence uncapped and reproducible

**Repository:** vepyr.

**Files:**

- Modify: `e2e-testing/scripts/comparison/compare.py`
- Modify: `e2e-testing/scripts/comparison/vcfio.py`
- Modify: `e2e-testing/scripts/comparison/report.py`
- Modify: `e2e-testing/scripts/comparison/cli.py`
- Modify: `tests/test_comparison_compare.py`
- Modify: `tests/test_comparison_vcfio.py`
- Modify: `tests/test_comparison_report.py`
- Modify: `tests/test_comparison_cli.py`

**Interfaces:**

- Add a release-qualified JSONL mismatch-ledger path per contig.
- Add a ledger row count and SHA-256 to each report JSON.
- Add per-field equality-shape counts.
- Replace source-text dependency guessing with resolved Cargo provenance.

- [ ] Add comparator tests before implementation for:
  - both values empty;
  - both values non-empty and equal;
  - vepyr empty only;
  - VEP empty only;
  - both non-empty and unequal;
  - `&`-order-only equality;
  - duplicate Feature/consequence entries from different output alleles;
  - one-sided CSQ entries when entry counts differ;
  - a mismatch set larger than ten, proving the ledger is uncapped while report
    examples remain capped.
- [ ] Parse the Ensembl reference's `##VEP` header into a structured identity:

  ```text
  VEP version
  API version
  cache version
  Ensembl core revision
  ensembl-variation revision
  assembly
  ```

- [ ] Add a pure validator that accepts a selected support record and rejects a
  non-matching header. Task 5 wires the native support record into this validator
  before annotation; unit tests cover the exact two target records above.
- [ ] Pair CSQ entries by output allele identity plus Feature, using
  `ALLELE_NUM` when available and a deterministic duplicate ordinal. Do not rely
  on `(Feature, Consequence)` alone for multi-allelic records.
- [ ] Emit one JSONL record per semantic field mismatch with:

  ```text
  variant_key
  allele / ALLELE_NUM
  Feature
  duplicate_ordinal
  field
  vepyr
  vep
  mismatch_shape
  ```

- [ ] Emit distinct ledger records for one-sided CSQ entries. Keep structural
  entry-count mismatch separate from field mismatch totals.
- [ ] Return per-field counters whose buckets sum exactly to `field_total`.
  Preserve `field_mismatch_counts` for compatibility.
- [ ] Write ledgers beside the per-contig JSON reports using release-qualified
  names. An exact run produces an empty file with a valid hash.
- [ ] Aggregate ledger counts and equality-shape counts into the Markdown
  summary without embedding the entire ledger.
- [ ] Rework `get_build_info()` to use `cargo metadata --locked` plus Git state:
  - full vepyr SHA and dirty flag;
  - resolved bio-functions source, SHA/path and dirty flag;
  - resolved bio-formats source, SHA/path and dirty flag;
  - `Cargo.lock` SHA-256.
- [ ] Make release report generation fail if either dependency
  cannot be resolved or a local dependency worktree is dirty.
- [ ] Run:

  ```bash
  uv run pytest \
    tests/test_comparison_compare.py \
    tests/test_comparison_report.py \
    tests/test_comparison_cli.py -v
  uv run ruff check e2e-testing/scripts/comparison tests
  uv run ruff format --check e2e-testing/scripts/comparison tests
  ```

- [ ] With the current pre-semantic-change extension, regenerate the 116
  comparison ledgers without `--force` so existing annotation VCFs are reused.
- [ ] Run a current-build release-115 chr1–22 comparison before changing
  semantics. This closes the historical chr2/chr4 evidence gap.
- [ ] Verify that the uncapped 116 ledger reconciles exactly to:

  ```text
  partial HGVS      388
  stop family       224
  CLIN_SIG          112
  chr11               2
  chr16               2
  chr20               6
  total              734
  ```

**Commit boundary:** one vepyr comparator/provenance commit.

**Acceptance:** the baseline is independently auditable; no conclusion depends
on ten-example caps or on a Cargo.toml line hidden by a path override.

---

### Task 2: Extend the existing bio-formats OpenSpec change

**Repository:** bio-formats.

The repository's `AGENTS.md` requires OpenSpec approval for this cache-contract
change. Extend `add-vep-annotation`; do not create a competing proposal for the
same VEP cache schema.

**Files:**

- Modify: `openspec/changes/add-vep-annotation/proposal.md`
- Modify: `openspec/changes/add-vep-annotation/design.md`
- Modify: `openspec/changes/add-vep-annotation/tasks.md`
- Modify: `openspec/changes/add-vep-annotation/specs/vep-annotation/spec.md`

- [ ] Add a “VEP cache release provenance” requirement with scenarios for:
  - explicit `cache_version`/`version` in `info.txt`;
  - strict fallback from raw basename `115_GRCh38` or `116_GRCh38`;
  - no release derivation from arbitrary parent directories;
  - disagreement between metadata and basename;
  - unknown/malformed release;
  - `bio.vep.cache_version` on all entity schemas and every emitted shard;
  - contig-lazy annotation rejection when a requested contig's shard lacks or
    conflicts on that metadata.
- [ ] Add a downstream support-contract requirement: a consumer must reject a
  cache version not declared by that vepyr package's support matrix.
- [ ] Add an “optional ClinVar reference allele” requirement:
  release-116 variation schemas expose nullable `clin_sig_ref_allele`; release-115
  schemas remain valid without it.
- [ ] State explicitly that release-basename fallback is provenance recovery at
  the raw cache root. It does not infer `cache_source_type`, preserving the
  existing no-source-mode-path-inference decision.
- [ ] State that generated-cache identity comes only from Parquet Arrow
  key-value metadata. No sidecar, manifest field, directory name or data column
  may replace it.
- [ ] Add implementation tasks as a new subsection after the existing cache
  source-mode work.
- [ ] Run:

  ```bash
  openspec validate add-vep-annotation --strict
  ```

- [ ] Obtain OpenSpec approval before Task 3 changes Rust source.

**Commit boundary:** OpenSpec artifacts only.

**Acceptance:** the approved schema contract covers release provenance and the
optional ClinVar field without weakening explicit source-mode semantics.

---

### Task 3: Establish release provenance in bio-formats

**Repository:** bio-formats.

**Files:**

- Modify: `datafusion/bio-format-ensembl-cache/src/info.rs`
- Modify: `datafusion/bio-format-ensembl-cache/src/schema.rs`
- Modify: `datafusion/bio-format-ensembl-cache/src/lib.rs` or the module that
  exports metadata keys
- Modify: `datafusion/bio-format-ensembl-cache/tests/integration_tests.rs`
- Modify: focused fixtures under
  `datafusion/bio-format-ensembl-cache/tests/fixtures/`

**Produces:**

```rust
pub const VEP_CACHE_VERSION_METADATA_KEY: &str = "bio.vep.cache_version";
```

- [ ] Add unit tests for `CacheInfo::from_root`:
  - explicit `cache_version 115`;
  - explicit `version 116`;
  - missing field under raw root `115_GRCh38`;
  - missing field under raw root `116_GRCh38`;
  - a nonconforming basename remains `"unknown"`;
  - explicit value and basename conflict is rejected, not silently chosen.
- [ ] Implement one strict raw-root basename parser. It may accept the raw VEP
  forms `{release}_{assembly}`, `{release}_{assembly}_merged`, and
  `{release}_{assembly}_refseq`; it must examine only `cache_root.file_name()`.
- [ ] Populate `CacheInfo.cache_version` from explicit info first and the strict
  raw-root basename second.
- [ ] Add `bio.vep.cache_version=<release>` to `new_schema()` alongside coordinate
  and source-type metadata. If release provenance cannot be established, expose
  the unknown state to the caller so the vepyr cache builder can reject it; do
  not manufacture a supported value.
- [ ] Keep the existing `cache_version` provenance column consistent with the
  schema metadata, but document that annotation identity never scans/trusts this
  column.
- [ ] Verify the generic `variation_cols` path exposes
  `clin_sig_ref_allele: Utf8?` for a 116 fixture and omits it for a 115 fixture.
  Do not hard-code it as required in the raw reader.
- [ ] Verify transcript, variation, exon, translation, regulatory and motif
  provider schemas all carry the same release metadata.
- [ ] Run:

  ```bash
  cargo fmt --all --check
  cargo test -p datafusion-bio-format-ensembl-cache
  cargo clippy -p datafusion-bio-format-ensembl-cache --all-targets -- -D warnings
  openspec validate add-vep-annotation --strict
  ```

**Commit boundary:** one bio-formats implementation commit after the approved
OpenSpec commit.

**Acceptance:** both actual raw roots used by this project resolve to their
release despite omitting the version line, while arbitrary paths do not.

---

### Task 4: Preserve cache metadata and resolve `VepSemantics` lazily by contig

**Repository:** bio-functions.

**Files:**

- Create: `datafusion/bio-function-vep/src/vep_semantics.rs`
- Modify: `datafusion/bio-function-vep/src/lib.rs`
- Modify: `datafusion/bio-function-vep/src/cache_source.rs`
- Modify: `datafusion/bio-function-vep/src/cache_builder.rs`
- Modify: `datafusion/bio-function-vep/src/cache/schema.rs`
- Modify: `datafusion/bio-function-vep/src/cache/build.rs`
- Modify: `datafusion/bio-function-vep/src/parquet_cache/write.rs`
- Modify: `datafusion/bio-function-vep/src/annotate_table_function.rs`
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs`
- Modify: `datafusion/bio-function-vep/src/transcript_consequence.rs`
- Modify: `datafusion/bio-function-vep/src/vcf_sink.rs`
- Modify: `datafusion/bio-function-vep/Cargo.toml`

**Interfaces:**

```rust
pub enum VepSemantics { V115, V116 }

pub struct SupportedVepTarget {
    pub cache_version: &'static str,
    pub vep_codebase_version: &'static str,
    pub api_version: &'static str,
    pub ensembl_core_revision: &'static str,
    pub ensembl_variation_revision: &'static str,
    pub semantics: VepSemantics,
}

pub struct CacheIdentity {
    pub source_type: CacheSourceType,
    pub cache_version: String,
    pub vep_codebase_version: &'static str,
    pub semantics: VepSemantics,
}
```

Options JSON:

```json
{
  "expected_cache_version": "115|116"
}
```

- [ ] Pin `datafusion-bio-format-ensembl-cache` to the pushed Task 3 revision
  before landing this task. Local vepyr path patches may be used while iterating,
  but not as the committed dependency.
- [ ] Define the canonical, compiled support matrix containing only
  the two complete support records in the table above.
- [ ] Add resolver/lazy-validator tests covering the full matrix:
  - chr1 shards with consistent 115 metadata;
  - chr1 shards with consistent 116 metadata;
  - generated directory named `115_GRCh38_merged` but missing metadata → error;
  - metadata conflicting with the generated directory name still follows
    metadata for identity; the expected-version assertion catches a wrong cache
    in comparison runs;
  - one chr1 entity shard missing metadata → error naming that shard before any
    chr1 annotation;
  - one chr1 entity shard carrying a different version → error naming both
    values;
  - one chr1 entity shard carrying a different source type → error;
  - broken chr2 metadata is not read by a chr1-only run;
  - after successful chr1 processing, broken chr2 metadata errors before the
    first chr2 annotation row;
  - expected cache version agrees with detected identity;
  - expected cache version conflicts with detected identity;
  - expected cache version without metadata still errors;
  - missing all identity sources → error;
  - malformed value → error;
  - unsupported `117` from metadata or assertion → error.
- [ ] Add a `LazyCacheIdentityValidator` shared by workers in one annotation
  invocation. It resolves each requested contig's shard paths from the existing
  entity manifests and memoizes successful validation by canonical contig.
- [ ] Read identity directly from each selected Parquet footer's Arrow schema
  key-value metadata. Do not introduce or consult a cache-version sidecar.
- [ ] Immediately before context/variation data for a contig is opened, require
  every present shard for that contig to carry `bio.vep.cache_source_type` and
  `bio.vep.cache_version`, with exactly one consistent value for each.
- [ ] Establish the invocation's `CacheIdentity` from the first requested
  contig, then require every later contig to match it. Required core entities
  remain governed by existing layout checks; motif or another optional entity
  may have no shard for the contig.
- [ ] Keep footer I/O contig-local: a chr1 invocation may read manifests but must
  not open the footer/data of any chr2 Parquet shard.
- [ ] Extend `CacheBuilder` with an expected cache version supplied by vepyr's
  `build_cache(release=...)`. Before writing the first shard, require the raw
  provider metadata to:
  - exist;
  - name a cache version in the compiled support matrix;
  - equal the requested build release.

  Apply the same check to full builds and single-entity builds so a raw 116 cache
  cannot be written under a `115_GRCh38_*` output directory.
- [ ] Pass the shared lazy validator through both annotation paths: the
  SQL/table-function path and the sharded direct-provider VCF path.
- [ ] Store the validated `VepSemantics` on the immutable per-contig annotation
  configuration and construct `TranscriptConsequenceEngine` only after that
  contig validates.
- [ ] Add exactly two policy helpers, initially preserving behavior:

  ```rust
  semantics.partial_overlap_hgvs()
  semantics.vep116_stop_predicates()
  ```

- [ ] Fail before the affected contig's context loading or variant annotation
  when its cache identity is missing, mixed or unsupported. For direct VCF
  output, preserve the current temporary-output behavior so a later-contig
  validation error never publishes a partial final file.
- [ ] Preserve release metadata through every converted-cache physical schema:
  - variation projection and AF encoding;
  - transcript UID attachment;
  - exon/regulatory/motif passthrough;
  - translation core projection;
  - compact translation SIFT schema.

  Several current transforms construct `Schema::new(fields)` and therefore drop
  upstream metadata. Copy source schema metadata before adding or replacing
  `bio.vep.cache_source_type`.
- [ ] Add cache-builder tests proving all emitted entity schemas retain both
  metadata keys.
- [ ] Add `expected_cache_version` to `AnnotateVcfConfig` and its JSON
  round-trip tests.
- [ ] Expose the compiled support matrix through a small read-only native API so
  Python/reporting does not maintain a second hand-copied list.
- [ ] Confirm this plumbing changes no consequence output under either semantics.
- [ ] Run:

  ```bash
  cargo fmt --all --check
  cargo test -p datafusion-bio-function-vep --features cache-builder --lib
  cargo test -p datafusion-bio-function-vep --features cache-builder --tests
  cargo clippy -p datafusion-bio-function-vep \
    --features cache-builder --all-targets -- -D warnings
  ```

**Commit boundary:** one bio-functions release-contract/plumbing commit.

**Acceptance:** only metadata-bearing shards for requested contigs open.
Unlabeled, mixed and unsupported contig shards fail before that contig
annotates; unrelated contigs are not opened; no field output changes yet.

---

### Task 5: Expose the release contract in vepyr and the comparison profile

**Repository:** vepyr.

**Files:**

- Modify: `src/vepyr/__init__.py`
- Modify: `src/annotate.rs`
- Modify: `src/lib.rs`
- Modify: `src/vepyr/_core.pyi` if native signatures change
- Modify: `e2e-testing/scripts/comparison/profiles.py`
- Modify: `e2e-testing/scripts/comparison/annotate.py`
- Modify: `e2e-testing/scripts/comparison/cli.py`
- Modify: `tests/test_annotate.py`
- Modify: `tests/test_build_cache.py`
- Modify: `tests/test_comparison_profiles.py`
- Modify: `tests/test_comparison_annotate.py`
- Modify: `tests/test_comparison_cli.py`

- [ ] Add a keyword-only public assertion:

  ```python
  expected_cache_version: str | None = None
  ```

- [ ] Add `vepyr.supported_vep_targets()` returning the native support records,
  including cache/VEP/API versions and exact core/variation revisions.
- [ ] Validate that an expected cache value is exactly one of the cache versions
  in that support matrix. Do not accept booleans, floats, Docker tag `115.2`, or
  arbitrary integers.
- [ ] Pass the assertion through streaming annotation and direct VCF output.
- [ ] Add wrapper tests proving output and streaming paths serialize the same
  assertion.
- [ ] Add a deferred `comparison.annotate.supported_vep_targets()` adapter. This
  remains the only comparison module that imports vepyr.
- [ ] Have `comparison.cli` fetch the native support matrix once, validate
  `--release`, and match the parsed reference `##VEP` header before invoking
  annotation.
- [ ] Add the validated selected cache release to `annotate_kwargs` as
  `expected_cache_version`. The comparator therefore catches a cache/reference
  mix-up before annotation.
- [ ] Do not add a second Python codebase-version map. `RELEASE_DIRS` may continue
  to describe where local reference artifacts live, but it is not the support
  contract. Unit tests without a native extension inject/mimic support records.
- [ ] Persist expected/detected cache version, target VEP codebase version,
  parsed reference-header identity, cache path and resolved dependency provenance
  in every report JSON.
- [ ] Pass `build_cache(release=...)` into the native builder as the expected
  cache version. Do the same for the single-entity native entry used by
  maintenance scripts; neither build path may write before raw metadata agrees.
- [ ] Ensure `--isolate` forwards all release-related CLI state.
- [ ] Run:

  ```bash
  uv run pytest \
    tests/test_annotate.py \
    tests/test_comparison_profiles.py \
    tests/test_comparison_annotate.py \
    tests/test_comparison_cli.py -v
  cargo test --locked --no-default-features
  uv run ruff check src tests e2e-testing/scripts/comparison
  uv run ruff format --check src tests e2e-testing/scripts/comparison
  ```

**Commit boundary:** one vepyr API/harness commit.

**Acceptance:** every annotation requires a supported, detectable cache;
comparison runs additionally assert the intended cache/reference pair.

---

### Task 6: Preserve and consume `clin_sig_ref_allele`

**Repository:** bio-functions first, then vepyr for the safe rebuild script.

#### Task 6A: Optional cache field and VEP-equivalent matching

**bio-functions files:**

- Modify: `datafusion/bio-function-vep/src/cache/schema.rs`
- Modify: `datafusion/bio-function-vep/src/cache/build.rs`
- Modify: `datafusion/bio-function-vep/src/cache/lookup_exec.rs`
- Modify: `datafusion/bio-function-vep/src/colocated.rs`
- Modify: `datafusion/bio-function-vep/src/allele.rs`
- Modify: `datafusion/bio-function-vep/src/annotate_provider.rs`

- [ ] Add schema tests with and without `clin_sig_ref_allele`.
- [ ] Introduce an optional variation-column list rather than adding the field to
  `VARIATION_REQUIRED_COLUMNS`.
- [ ] When the source schema contains the field, pass it through tiering and the
  physical Parquet schema as nullable UTF-8. When absent, retain the current 115
  schema and behavior.
- [ ] Add the field to colocated cold/warm projections conditionally; an old
  shard without it must still open.
- [ ] Carry it through `WarmColocIndices`, `ColocatedCacheEntry`, deduplication,
  and `ColocatedEntry`.
- [ ] Pass the live VCF/VariationFeature reference allele (`ref_al` at the
  annotation row, before VEP allele minimization) into clinical-field assembly.
- [ ] For each labelled `clin_sig_allele` entry:
  - if cached ClinVar reference is absent/empty or equals the live reference,
    keep the label allele;
  - if it differs, reverse-complement the label allele before lookup;
  - then run the existing match against the output allele.
- [ ] Reuse one canonical VEP-compatible allele reverse-complement helper.
  Include `-`, `N`, case normalization and supported IUPAC symbols in its tests.
- [ ] Do not change `matched_alleles.is_empty()` behavior as part of this task.
- [ ] Add exact tests for:
  - `chr3:42210085 C>CGGAGGA`, 41 entries become blank;
  - `chr15:89333596 T>TTGC`, 63 entries become blank;
  - `chr14:74506880 C>CGCGCGCAT`, 8 entries become `benign`;
  - absent field and null row preserve release-115 behavior.
- [ ] Run the full bio-functions test/lint commands from Task 4.

**Commit boundary:** one bio-functions ClinVar commit.

#### Task 6B: Rebuild complete 115 and 116 caches, recoverably

**vepyr files:**

- Create: `e2e-testing/scripts/rebuild_release_cache.py`
- Create or modify: focused script tests under `tests/`
- Deprecate or delegate from:
  `e2e-testing/scripts/rebuild_cache_116_merged.py`

- [ ] Model the safe staging/swap behavior on
  `rebuild_cache_116_merged.py`, with dry-run as the default.
- [ ] Require explicit release/cache type and resolve source/target through the
  comparison profile helpers.
- [ ] Build the complete release cache into a fresh sibling staging directory
  with `vepyr.build_cache`; never pass the live cache as the output.
- [ ] Rebuild release 115 and release 116 sequentially so only one staging copy
  consumes disk at a time.
- [ ] Before swap, verify:
  - every present entity has a manifest;
  - every manifest entry points to an existing Parquet shard;
  - every referenced shard has the expected release and source metadata;
  - no entity/shard carries a mixed value;
  - release 116 variation shards expose optional `clin_sig_ref_allele`;
  - release 115 variation shards omit it or contain only the explicitly
    documented nullable compatibility shape;
  - the three exact 116 ClinVar source rows have expected cached values;
  - per-entity/per-chromosome row counts are reconciled against the prior cache,
    with every intentional delta documented before swap.
- [ ] Do not write a cache-version sidecar during build or swap. Manifests remain
  shard inventories only.
- [ ] Rename the live cache to a timestamped backup, rename staging into place,
  and automatically roll back if the second rename fails.
- [ ] Retain the backup by default through final release gates. Cleanup is a
  separate explicit operation.
- [ ] Eagerly verify every shard in each rebuilt staging cache before swap.
- [ ] Against copied test caches, prove that removing chr1 metadata fails before
  chr1 loads, while broken chr2 metadata does not block a chr1-only run and does
  block a subsequent chr2 run before its first annotation row.
- [ ] Run targeted release-116 comparisons:

  ```bash
  uv run python e2e-testing/scripts/run_comparison.py \
    --release 116 --profile merged --chroms 3 14 15 --force
  ```

- [ ] Confirm the ledger loses exactly 112 CLIN_SIG rows and gains no mismatch.

**Expected 116 total:** `734 - 112 = 622`.

**Commit boundary:** one vepyr cache-maintenance script commit. The generated
cache data itself is not committed.

---

### Task 7: Fix the ten release-independent residual fields

**Repository:** bio-functions.

These fixes may be implemented as separate commits and reviewed in parallel, but
they must remain independent of `VepSemantics`.

#### Task 7A: chr20 terminal CDS insertion boundary

**Files:**

- Modify: `datafusion/bio-function-vep/src/transcript_consequence.rs`

- [ ] Add failing fixtures for plus/minus strand insertions immediately before
  and after both CDS boundaries.
- [ ] Correct the plus-strand terminal predicate in
  `insertion_left_flank_in_cds`: `left_flank == cds_end` must not enter the
  coding branch for the insertion after the padding base.
- [ ] Assert the exact `chr20:45840343 A>AC / ENST00000984773.1` result:
  `3_prime_UTR_variant&NMD_transcript_variant`, MODIFIER, and blank CDS/protein
  fields.
- [ ] Run the fixture under V115 and V116 semantics and require the same answer.

#### Task 7B: chr16 primary-only exon-boundary span

**Files:**

- Modify: `datafusion/bio-function-vep/src/transcript_consequence.rs`

- [ ] Add a fixture where the primary genomic flank maps to CDS and the alternate
  flank does not.
- [ ] Add the symmetric boundary branch beside the existing
  primary-missing/alternate-mapped branch.
- [ ] Assert `Protein_position=74-75` for both
  `ENST00001096215.1` and `ENST00001096224.1` at
  `chr16:5072071 G>GGTCT`, without changing `CDS_position=222-223`.
- [ ] Require identical V115/V116 semantic results.

#### Task 7C: Prove, then fix, the chr11 RefSeq shift predicate

**Files:**

- Modify: `datafusion/bio-function-vep/src/hgvs.rs`
- Modify if proven necessary:
  `datafusion/bio-function-vep/src/transcript_consequence.rs`

- [ ] First create a failing `NM_002457.5` fixture that records:
  `bam_edit_status`, original genomic shift alleles, formatter alleles,
  GIVEN_REF, USED_REF, shift offset and final HGVSc.
- [ ] Trace the equivalent VEP 115.2 and 116.0 values through
  `hgvsc_uses_genomic_shift`. Document the exact allele space used by VEP in the
  test or adjacent source comment.
- [ ] Do not change the predicate until the fixture proves why the current
  failed-edit comparison rejects the 47-base shift.
- [ ] Narrow only the proven failed-edit condition.
- [ ] Assert for both semantics:

  ```text
  HGVSc=NM_002457.5:c.4443_4463del
  HGVS_OFFSET=47
  GIVEN_REF=CCAACCACCACTCCCAGCCCT
  USED_REF=CACCACTCCCAGCCCTCCAAC
  ```

- [ ] Run all existing RefSeq/BAM-edit HGVS tests to prevent a broad relaxation.

#### Task 7D: Integration check

- [ ] Run the full bio-functions test/lint commands from Task 4.
- [ ] Rebuild the extension and compare only the affected contigs:

  ```bash
  uv run python e2e-testing/scripts/run_comparison.py \
    --release 116 --profile merged --chroms 11 16 20 --force
  ```

- [ ] Confirm the ledger loses exactly 10 fields and no new row.

**Expected 116 total:** `622 - 10 = 612`.

**Commit boundaries:** one commit per residual class, or one clearly scoped
release-independent-residual commit after Task 7C is proven.

---

### Task 8: Implement the nine VEP 116 stop-predicate behaviors

**Repository:** bio-functions.

**Files:**

- Modify: `datafusion/bio-function-vep/src/transcript_consequence.rs`
- Modify only if classification consumption requires it:
  `datafusion/bio-function-vep/src/hgvs.rs`

- [ ] Build a table-driven paired golden suite. The same input fixture must
  assert a V115 answer and a V116 answer for each net behavior:
  1. no `inframe_insertion` when both peptides are `*`;
  2. no `inframe_deletion` when reference peptide is `*`;
  3. `stop_gained` false when `stop_lost` is true;
  4. no `stop_lost` for partial terminal codon;
  5. no `stop_lost` with `X` in alternate peptide;
  6. no `stop_retained` with `X` in alternate peptide;
  7. no-alt stop-retained uses genomic `_cil` overlap;
  8. V116 `ref_eq_alt_sequence` final-stop rules;
  9. no frameshift when affected reference peptide starts with `*`.
- [ ] Include plus/minus strands, insertion/deletion, intron-crossing genomic
  overlap, and both surviving/deleted `ref_eq_alt_sequence` clauses.
- [ ] Keep the consolidated Rust classifier. Implement net semantic differences
  behind `semantics.vep116_stop_predicates()` rather than reproducing Perl cache
  plumbing.
- [ ] Compute one coding classification object and use it for:
  consequence terms, frameshift-vs-delins HGVSp choice, and downstream IMPACT.
- [ ] Assert that no `if release`/`if semantics` branch is added to IMPACT
  mapping or final HGVSp string formatting.
- [ ] Do not add `consider_ins_len`.
- [ ] Run all transcript consequence, SO-term, IMPACT and HGVSp tests.
- [ ] Rebuild vepyr and run representative integration contigs:

  ```bash
  uv run python e2e-testing/scripts/run_comparison.py \
    --release 116 --profile merged --chroms 3 6 16 22 --force
  uv run python e2e-testing/scripts/run_comparison.py \
    --release 115 --profile merged --chroms 22 --force
  ```

- [ ] Confirm all targeted 116 stop-family rows disappear and release-115 chr22
  remains exact.

**Expected 116 total:** `612 - 224 = 388`.

**Commit boundary:** one bio-functions V116 stop-semantics commit.

---

### Task 9: Implement VEP 116 partial-transcript HGVS in slice space

**Repository:** bio-functions.

**Files:**

- Modify: `datafusion/bio-function-vep/src/hgvs.rs`
- Modify: `datafusion/bio-function-vep/src/transcript_consequence.rs`

**Boundary-only model:**

```text
unclamped_slice_start
unclamped_slice_end
clamped_slice_start
clamped_slice_end
shift_offset
transcript_length
strand
```

- [ ] Add paired V115/V116 fixtures before implementation for:
  - left and right transcript overhang;
  - plus and minus strand;
  - wholly outside on either side;
  - partial overlap with zero shift;
  - partial overlap with a valid positive 3′ shift;
  - shifted clamped end beyond transcript length;
  - allele clipping at each boundary;
  - an ordinary non-boundary deletion proving byte-identical behavior.
- [ ] Convert the unshifted genomic interval to transcript-slice coordinates in
  transcript orientation.
- [ ] Under V115, retain the current “either end outside” early return.
- [ ] Under V116:
  - reject only when both ends are below 1 or both exceed transcript length;
  - clamp both ends to `[1, transcript_length]`;
  - keep shift as a non-negative slice-space offset;
  - reject when `clamped_end + shift_offset > transcript_length`;
  - clip alleles as VEP does, then format the shifted clamped interval.
- [ ] Do not replace the interval with materialized
  `HgvsGenomicShift::display_start/display_end` on this branch.
- [ ] Keep the existing non-boundary path unchanged.
- [ ] Do not materialize complete genomic transcript slices or substitute
  hydrated cDNA. If a fixture proves missing flank bases are required, perform a
  lazy interval read through the existing per-worker `hgvs_reader` only on the
  boundary branch.
- [ ] Run the full HGVS and RefSeq edit test suites, including Task 7C.
- [ ] Rebuild vepyr and run representative integration contigs:

  ```bash
  uv run python e2e-testing/scripts/run_comparison.py \
    --release 116 --profile merged --chroms 1 3 6 15 21 --force
  uv run python e2e-testing/scripts/run_comparison.py \
    --release 115 --profile merged --chroms 1 21 22 --force
  ```

- [ ] Reconcile every removed row against the partial-overlap class in the
  uncapped ledger.

**Expected 116 total:** `388 - 388 = 0`.

**Commit boundary:** one bio-functions V116 partial-HGVS commit.

---

### Task 10: Add a machine-enforced release gate

**Repository:** vepyr.

**Files:**

- Create: `e2e-testing/scripts/verify_parity_gate.py`
- Create: `tests/test_verify_parity_gate.py`
- Modify: `e2e-testing/scripts/comparison/report.py`

- [ ] Make the verifier consume per-contig JSON reports and ledger files for one
  exact `(release, profile, contig set)`.
- [ ] Reject missing, null-comparison, duplicate or wrong-release reports.
- [ ] Require:

  ```text
  variants_only_in_vepyr   = 0
  variants_only_in_vep     = 0
  csq_entry_count_mismatch = 0
  csq_order_mismatch       = 0
  field_mismatch_total     = 0
  mismatch_ledger_rows     = 0
  ```

- [ ] Require expected and detected cache versions to match and require the
  selected support record to belong to the running vepyr package version.
- [ ] Require the parsed reference `##VEP` header to match the same support
  record, including API, core revision and variation revision.
- [ ] Verify per-field equality buckets sum to totals.
- [ ] Fail on an unknown/unsupported cache identity or a local absolute
  dependency in release mode.
- [ ] Add unit tests for every failure mode.
- [ ] Keep performance separate from correctness: compare throughput and peak
  memory with the frozen baseline, and require investigation/resolution for a
  regression over 5%; do not hide correctness behind a speed result.

**Commit boundary:** one vepyr release-gate commit.

---

### Task 11: Pin upstream revisions and run the only full release gates

**Repositories:** bio-formats → bio-functions → vepyr.

- [x] Land/push bio-formats first and record the exact revision containing the
  approved release metadata contract.
- [x] Update bio-functions to that revision; run its complete CI; land/push and
  record the exact revision containing all parity work.
- [x] Replace vepyr's absolute path patches and temporary motif integration pins
  with the exact durable bio-functions/bio-formats revisions or releases.
- [x] Choose the vepyr package release number, update `pyproject.toml` and
  `Cargo.toml` together, and document its exact support matrix (cache 115/VEP
  115.2 and cache 116/VEP 116.0) in the public README/release notes.
- [x] Verify `vepyr.supported_vep_targets()` reports that package version and
  exactly those two complete targets.
- [x] Regenerate `Cargo.lock` deliberately and review that only intended
  dependencies moved.
- [x] Verify `cargo metadata --locked` contains no local source for either VEP
  dependency.
- [x] Build the exact release candidate with the same native release flags used
  for cache construction and parity qualification:

  ```bash
  env -u VIRTUAL_ENV -u CONDA_PREFIX \
    RUSTFLAGS="-C target-cpu=native" \
    uv sync --reinstall-package vepyr
  uv run pytest -q
  cargo test --locked --no-default-features
  ```

- [x] Remove/recreate release result directories or use a new immutable run ID
  so no annotation produced by an older extension is reused.
- [x] Run release 116 chr1–22 for all three cache/reference profiles:

  ```bash
  for profile in merged ensembl refseq; do
    uv run python e2e-testing/scripts/run_comparison.py \
      --release 116 --profile "$profile" --chroms all --force --isolate
    uv run python e2e-testing/scripts/verify_parity_gate.py \
      --release 116 --profile "$profile" --chroms 1-22
  done
  ```

- [x] Run release 115 chr1–22 for all three profiles from the same extension and lockfile:

  ```bash
  for profile in merged ensembl refseq; do
    uv run python e2e-testing/scripts/run_comparison.py \
      --release 115 --profile "$profile" --chroms all --force --isolate
    uv run python e2e-testing/scripts/verify_parity_gate.py \
      --release 115 --profile "$profile" --chroms 1-22
  done
  ```

- [x] Confirm all six release/profile gates report zero structural, ordering and field mismatches.
- [x] Confirm release-116 CLIN_SIG parity uses the rebuilt variation data and
  release-115 uses the rebuilt absent-field path.
- [x] Confirm no absolute path patch, unknown/unsupported cache, dirty
  dependency, or stale report appears in either final summary.
- [x] Commit the final vepyr release evidence, push the
  `release-testing` branch, and create or update its PR. Record the exact pushed
  bio-formats, bio-functions, and vepyr revisions in the PR body and in the
  root-cause document; do not describe an unpushed local commit as included in
  any PR.
- [x] Confirm that no retained pre-metadata cache backup exists at any of the
  six preferred targets; the first promotion created none because the targets
  did not yet exist.

**Commit boundary:** one vepyr dependency-pin/release-artifact commit. Generated
large cache and result files remain outside Git unless intentionally published
as release evidence.

---

### Task 12: Publish the six verified caches to Google Drive

**Source:** `~/workspace/data_vepyr/cache`

**Destination:** `gdrive-mw:/vepyr/cache`

- [x] Start only after all cache rebuilds, eager shard verification, and six
  pinned-binary parity gates are complete. Never upload a hidden
  `.rebuild-*` staging directory or retained backup.
- [x] Copy all six immutable release/profile directories without renaming them:

  ```bash
  for cache in \
    115_GRCh38_merged \
    115_GRCh38_ensembl \
    115_GRCh38_refseq \
    116_GRCh38_merged \
    116_GRCh38_ensembl \
    116_GRCh38_refseq
  do
    rclone copy \
      "$HOME/workspace/data_vepyr/cache/$cache" \
      "gdrive-mw:/vepyr/cache/$cache" \
      -P
  done
  ```

- [x] A successful copy exit is necessary but not sufficient. For every
  directory, compare local and remote file counts and byte totals and run a
  one-way `rclone check` (hash comparison where the remote exposes hashes;
  otherwise explicitly record the size-only fallback).
- [x] Re-run the local cache verifier after upload. Remote publication must not
  mutate or replace the qualified local caches.
- [x] Record the six remote destinations and verification results in the final
  vepyr PR evidence.

Publication completed on 2026-07-30. The remote root contains exactly the six
named directories above. For every cache, the local and remote file count and
byte total matched exactly, `rclone check --one-way` reported zero differences,
and `rebuild_release_cache.py --verify-only` passed again against the unchanged
local source. The authoritative counts, byte totals, row totals, and remote
destinations are recorded in §10.3 of the root-cause document.

The upload is an artifact-publication step, not a correctness gate: no Git
revision changes after the pinned binary has passed may be hidden by uploading
an older cache.

---

## Test matrix

| Layer | V115 | V116 | Purpose |
|---|---|---|---|
| Resolver unit tests | yes | yes | identity, support matrix, conflicts, unsupported future behavior |
| Cache schema tests | absent ClinVar field | present ClinVar field | data-driven compatibility |
| Stop golden fixtures | old answer | nine new answers | only stop policy is gated |
| Partial HGVS fixtures | early return | clamp then shift | only HGVS policy is gated |
| chr11/16/20 fixtures | fixed answer | same fixed answer | release-independent behavior |
| ClinVar fixtures | old/null path | ref-aware reverse complement | field-driven behavior |
| Targeted E2E | chr1/21/22 + residuals | affected representative contigs | fast development checks |
| Final E2E | chr1–22 × merged/Ensembl/RefSeq | chr1–22 × merged/Ensembl/RefSeq | six-gate release acceptance |

## Review checklist

- [ ] Searching for `VepSemantics`, `expected_cache_version`, or
  `partial_overlap_hgvs`
  finds no release conditional outside the resolver, stop policy and HGVS
  boundary policy.
- [ ] Searching IMPACT/HGVSp code finds no independent release branch.
- [ ] `clin_sig_ref_allele` is optional at raw schema, converted schema and
  runtime lookup boundaries.
- [ ] Metadata-less legacy V115/V116 shards are rejected when their contig is
  requested.
- [ ] The final V115 and V116 caches were fully rebuilt from the matching raw
  cache, and every manifest-referenced shard validates.
- [ ] New cache schemas retain both `bio.vep.cache_source_type` and
  `bio.vep.cache_version`.
- [ ] The chr11 fix is backed by a proven failed-edit fixture, not the original
  hypothesis alone.
- [ ] Comparator ledgers are uncapped, release-qualified and hashed.
- [ ] Final reports identify the exact clean source revisions and lockfile.
- [ ] All six full release/profile gates pass from the same pinned binary.

## Definition of done

Implementation is complete only when all of the following are true:

1. Cache identity is mandatory and maps through this vepyr version's explicit
   support matrix: cache 115→VEP 115.2 or cache 116→VEP 116.0.
2. Only partial-overlap HGVS and the stop-predicate family are release-gated.
3. Release-116 ClinVar reference-allele behavior is driven by optional cache
   data and release-115 caches remain valid without that column.
4. The chr11, chr16 and chr20 fixtures pass identically under both semantics.
5. Complete merged, Ensembl-only, and RefSeq-only caches for 115 and 116 are
   rebuilt into staging and every manifest-referenced Parquet shard carries
   consistent version/source metadata.
6. Current pinned-code chr1–22 comparisons are structurally and field-exact for
   all six release/profile combinations.
7. All 132 per-contig mismatch ledgers are empty and every equality bucket
   reconciles.
8. The bio-formats and bio-functions commits are pushed to their corresponding
   PR branches; the vepyr release build pins those exact Git revisions and
   contains no machine-local dependency path, unknown source revision, or
   fallback for an unlabeled/unsupported cache.
9. The vepyr branch and PR are pushed with the exact dependency revisions and
   the six-gate evidence recorded.
10. All six verified cache directories are copied to their matching
    `gdrive-mw:/vepyr/cache/<cache>` destinations and the remote copies pass the
    recorded count/size/hash verification.
11. Any performance regression over 5% has a documented cause and disposition.

## Explicitly out of scope

- Automatically treating VEP 117 or later as V116.
- Porting unrelated 115→116 changelog entries that do not affect this parity
  profile.
- Inferring annotation semantics from a generated-cache directory name.
- Adding or trusting a version sidecar/marker file for generated caches.
- Accepting or silently migrating a metadata-less generated cache at annotation
  time.
- Adding release switches for ClinVar, chr11, chr16, chr20, IMPACT or HGVSp.
- Merging the reverted `wip/hgvs-transcript-clip-116` branch as-is.
