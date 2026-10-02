# REF == ALT parity with Ensembl VEP 116

Status: implemented with RED/GREEN evidence in isolated branches. Both baseline
and final full-genome runs passed strict MD5 parity at both worker counts, and
the combined performance gate passed. Publication uses linked draft PRs; see
GitHub for current CI and review status.

Upstream PR: https://github.com/biodatageeks/datafusion-bio-functions/pull/263
Downstream branch: `biodatageeks/vepyr:fix/ref-equals-alt`.

Issue: https://github.com/biodatageeks/vepyr-porting-tests/issues/207

## Defect and independently measured evidence

A normalized VCF record whose ALT equals REF is retained by Ensembl VEP 116
without consequences. The current engine instead generates annotations for a
sequence change that did not occur. The reader preserves the alleles correctly.

On 2026-10-02, native-built vepyr master `07b6506a6de162e4178522ff22230c12bd7cf320`
(engine `8c613dd3ea8855bb2b9cbe30fcacc01f2280b5d2`, tag `v0.23.0`) was compared
directly through its CLI against the pinned Ensembl VEP 116.0 Docker image
`ensemblorg/ensembl-vep@sha256:f354dd8d09073e4d943acbbd02f5eb234a9d9e9d444371c1c349910f2123de11`.
Both used release 116 Ensembl caches and the GRCh38 primary-assembly FASTA.
Inputs were normalized with `bcftools norm -m -both`; the FASTA bases were checked.

| Input | VEP | Current vepyr |
|---|---|---|
| `21:25585733 C>C` | retained, no CSQ | 34 CSQ entries |
| `21:25607474 A>A` | retained, no CSQ | 36 CSQ entries |
| `21:25585733 C>T` | 34 CSQ entries | byte-identical body |
| normalized `C>C,T` | only C>T annotated | both rows annotated |
| committed chr21/chr22 fixture | C>C unannotated; G>T annotated | only C>C differs |

All body MD5 values reproduced the issue. `--allow_non_variant` does not change
the C>C result. The committed VEP oracle matches the fresh Docker run byte for
byte. Evidence, exact commands, logs, and source identity:
`/Users/mwiewior/workspace/data_vepyr/evidence/issue-207-20261002.Qncpez/`.

## The rule in pinned Ensembl source

Sources were read from immutable git revisions, rather than the issue text:

- `ensembl-vep` release/116.0 = `57ea5c52340acc1f156267f810ad162e26597082`.
- `ensembl-variation` local origin/release/116 =
  `2fb834b987ede3824e200197a838ce11e91aeb4b` (the revision named by the Docker image).

`modules/Bio/EnsEMBL/Variation/VariationFeatureOverlap.pm:479-489`:

```perl
for my $allele (@alleles) {
  next if $allele eq ($vf_ref || $ref_allele);
  $allele_number++;
  next if $allele eq '*' || $allele eq '<DEL:*>';
```

This excludes a reference-equal allele from alternate overlap-allele creation.
It is a per-allele skip, not removal of the original VCF record. For normalized
input, each record carries one ALT. Raw multi-ALT annotation remains outside
this fix; split input first, preserving the changed sibling allele.

`modules/Bio/EnsEMBL/VEP/Parser/VCF.pm:259-266` separately tests only the first
ALT for `.` and drops that record unless `allow_non_variant` is set. At `:342`
it builds `ref/alt` for REF==ALT as for other ordinary records. Equality must
therefore not be reclassified as the existing `AltKind::NonVariant`.

`modules/Bio/EnsEMBL/VEP/OutputFactory/VCF.pm:315-355` copies the original record,
removes an existing CSQ by default (`:328-330`), and appends a new CSQ only when
consequence chunks exist (`:347-352`). Preserve other INFO/FORMAT values; an
old input CSQ must not survive merely because the new result is null.

Verified the release refs exist before comparing 115 to 116. These three files
differ only in copyright years between the available 115 and 116 refs.

## Ownership and checkout provenance

Three read-only analyses were dispatched as required by the fix runbook.

| Repository | Responsibility | Planned change |
|---|---|---|
| datafusion-bio-formats | Carries REF/ALT unchanged | None; keep v1.13.0 |
| datafusion-bio-functions | Owns consequence generation | RED tests, then guards in provider and direct engine |
| vepyr | Carries engine behavior to Python/CLI | Integration regression and engine pin update |

The sibling working trees are drifted (`functions` HEAD `46d8c20a`, `formats`
HEAD `dbc62fb1`). Analysis uses immutable Cargo checkouts for functions
`8c613dd3` and formats `00487b9edf53cbf36918d7d22a03a79fd0acf863`; do not switch
or overwrite those working trees.

Formats' four VCF loops copy REF and ALT independently:
`bio-format-vcf/src/physical_exec.rs:989`, `:1239`, `:1528`, `:3140`.
BCF similarly preserves both at `src/bcf.rs:2791-2806`. Filtering here would
change ordinary VCF/BCF reader semantics and could invalidate genotype allele
indices. No formats fix or otherwise empty formats PR is warranted.

At the pinned functions source, `annotate_provider.rs:6336` dispatches on
ALT alone, reads REF at `:6378`, and can return cached consequences at `:6485`.
Its null-annotation row mechanism preserves input rows. This row loop also
serves workers through the call at `:13609`.

The public Rust transcript engine is another entry point:
`transcript_consequence.rs:1145-1209`. All normal/prepared/profiled evaluation
calls converge on `evaluate_variant_prepared_inner`, which currently skips
only star alleles. A provider-only fix leaves direct Rust callers incorrect;
an engine-only fix leaves the provider's cached-CSQ fast path and its fallback
consequence creation incorrect. Cover both boundaries.

## TDD sequence

1. Capture baseline performance and strict body-MD5 parity before source edits,
   with the validation scope resolved as described below. Preserve the existing
   issue reproductions as the expected failing baseline.
2. In functions, add focused tests that fail against the unchanged engine:
   - Direct transcript evaluation: equal SNV and multi-base alleles yield no
     transcript, intergenic, regulatory, or motif consequences; a real change
     in the same context remains annotated. Exercise prepared/profiled entry
     points through their shared implementation.
   - Annotation provider integration: retained REF==ALT rows have null CSQ,
     null most-severe consequence, and null generated typed annotation fields.
     Exercise both computed consequences and a nonempty cached-CSQ fast path,
     so a stale cache value cannot leak past the guard.
   - VCF output: retain row order, REF/ALT, INFO, FORMAT, and samples; suppress
     new CSQ and remove an existing input CSQ as VEP does. Keep changed-allele
     controls annotated. Check both allow_non_variant settings.
3. Record each RED command, exit code, and assertion. A compile error or missing
   fixture does not count as the required failing regression.
4. Add an exact REF/ALT equality guard before provider cache/annotation output;
   append a complete null-annotation row and continue without marking the row
   for deletion. Add the corresponding early return in the direct transcript
   evaluator. Keep ALT-dot/star behavior and general allele conversion intact.
5. Re-run the RED tests to GREEN. Run existing non-variant, star, MNV, cached
   consequence, worker, and VCF-output tests affected by these paths.
6. Add vepyr integration coverage using its checked-in golden cache and a new
   REF==ALT input (the ordinary golden input cannot prove this fix). Assert
   the behavior through LazyFrame and output_vcf, including typed-column
   projection that may prune CSQ. Include an indexed multi-worker run.
7. Rebuild with `RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr`
   before measurements; fast iteration builds must not be compared with native
   release builds. Lint through pre-commit.
8. Re-run all real issue reproductions against the same pinned Docker oracle.
   C>C, A>A, normalized mixed input, and the committed two-contig fixture must
   match VEP body MD5; the C>T control must remain byte-identical.

## Baseline and resource constraint

The runbook requires a discarded 8-worker warm-up, measured 8- and 1-worker
WGS sweeps, and strict body-MD5 comparison across all 22 autosomes. Its plain
VCF run requires at least 65 GiB free. This host currently has only 6 GiB.
Do not delete unrelated outputs or bypass the free-space guard silently.

The runner's existing `--compression bgzf` option resolves the storage constraint
without reducing genomic coverage. Use it identically for baseline and final,
with a 4.5 GiB minimum-free-space guard. The existing merged WGS reference is
1.6 GB, and the runner documents roughly 17-fold smaller output. Compression is
timed; these measurements are compared only to each other, never to published
plain-output numbers. A smaller-scope question was raised before discovering
this supported option; the full-genome compressed run avoids that reduction.

Run the existing `md5_concordance.py --pair VEP_WGS VEPYR_WGS --mode strict`
against each measured WGS output before removing that run's temporary VCF.
This reuses the measured annotation and hashes the entire 4,096,123-record body,
instead of repeating annotation and accumulating separate contig slices. Both
baseline and final must compare the same normalized input and merged reference.
Retain commands, traces, metrics, comparator reports, and per-contig counts.

Keep all new outputs under this repository's results directory or /tmp. The
current sandbox permits reading the large cache/FASTA but not writing there.
Do not let normalization/index helpers overwrite indexes beside shared inputs.

Expected gate movement: issue-specific parity turns from RED to GREEN; changed
allele controls and existing corpus digests remain unchanged. No performance
improvement is claimed. Apply the runbook's <=5% phase-duration and <=10%
wall/RSS regression bars to whichever matched baseline/final scope is used.
Any unrun full-genome gate remains explicitly pending in the draft PR.

## PRs and dependency order

1. Functions draft PR: regression tests and engine/provider fix on current
   master `8c613dd3`; keep formats pinned to `v1.13.0`.
2. Vepyr draft PR: integration tests, this plan, and a pin to the functions PR's
   exact tested commit. Re-pin after any upstream review fix. No local path
   patches belong in either PR.
3. Record RED/GREEN results, oracle provenance, baseline/final measurements,
   source references, and any pending gate in the PR descriptions. Complete
   the runbook's review loop where available; do not claim readiness while
   required verification remains pending.
4. Never merge. After human merges, downstream pins must move to the actual
   upstream merge commit in dependency order.

The initial publication attempt was blocked by the session's `never` approval
policy. After the user enabled approvals, `gh` authenticated successfully using
the existing keyring outside the sandbox, and the tested upstream commit was
published unchanged. The earlier authentication failure was sandbox-specific.
Isolated checkouts under /tmp avoid modifying the user's sibling working trees
or original workspace .git directory. Local bundles preserve the tested commits.


## Execution evidence

The baseline completed before either test or production edits. At both 8 and 1
workers, all 4,096,123 records passed the strict comparator (body MD5
`157fca5b065c20eb7cec3e7a18ef28b7`, header also passed). Baseline annotation times
were 77.188 s / 338.793 s and peak RSS 8,872,032 / 4,079,520 KiB respectively.

TDD results, with assertion failures rather than compilation failures:

- Functions regression commit `a68ce9c`: 3 new tests failed on the original
  engine. Fix commit `901e3034c4a98b7cd0a0f42e6b679578065ae47f`: all 3 passed.
- Vepyr regression commit `cff7171`: all 6 tests failed before the fix and
  passed after it. A repeat against the final committed dependency pin also
  passed all 6.
- Full affected Rust library suite: 1,158 passed, 3 ignored. One existing
  ATG>ATG test had encoded the defect as a start-retained consequence; its
  expectation now matches VEP's no-consequence rule. Existing tests of real
  start-preserving changes remain covered.
- Python regression/non-variant/golden/MNV/merged/VCF-column/CLI suites:
  502 passed.
- Rust fmt and targeted all-targets/all-features Clippy passed through
  pre-commit; Python Ruff and formatting passed through pre-commit.
- All six original Docker reproducers now have byte-identical VCF bodies:
  C>C, A>A, normalized mixed ALT, C>T control, committed two-contig input,
  and C>C with allow_non_variant. The fixed runs reuse the actual release-116
  Docker outputs preserved from the initial independent reproduction.

The production diff is limited to reference equality guards in the shared
provider row loop and direct transcript evaluator. No VCF-reader change is
needed. The vepyr Cargo pin resolves the exact local functions commit above,
with no path patch in the deliverable. Cargo.lock and uv.lock also synchronize the stale
vepyr package version from 0.7.0 to the manifests' existing 0.9.0; no Python
dependency versions change.

### Build and storage deviations

The baseline used the prescribed native `uv sync --reinstall-package vepyr`.
In the subsequently restricted sandbox, uv panics in macOS system-configuration
before building. The final build uses the same maturin PEP 517 release backend
and native RUSTFLAGS directly, with Rust 1.91.1. This is not a develop/debug
build, but the wrapper command differs; retain the build logs when interpreting
performance. Cargo resolves the committed functions revision from a private
local git cache, since the remote write is blocked.

The same BGZF output and 4.5-GiB free-space guard are used on both sides.
Only recorded artifacts generated by this task's completed Rust test build
were removed to regain space. No shared inputs or pre-existing user outputs
were removed. Full-WGS strict MD5 is checked before each temporary output is
removed; all 22 autosome counts and all trace/metric files are retained.

Evidence directory: `e2e-testing/results/fix-20261002-ref-equals-alt/`, including
initial Docker commands and outputs, baseline/final traces, RED/GREEN logs,
fixed reproduction outputs, and native extension hashes. These local evidence
files are ignored by git and will accompany the bundles rather than the PR diff.

## Final measured performance

Full-genome performance gate: PASS. Both worker counts used all 4,096,123
normalized records, the merged release-116 cache, BGZF output, and native release
builds. All 11 measured phase durations stayed within the 5% regression bar;
annotation time and RSS stayed within their 10% bars. No speedup is claimed.

| Workers | Baseline seconds | Final seconds | Delta | Baseline RSS KiB | Final RSS KiB | RSS delta |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 338.793 | 343.755 | +1.46% | 4,079,520 | 3,995,872 | -81.7 MiB (-2.05%) |
| 8 | 77.188 | 73.415 | -4.89% | 8,872,032 | 9,020,064 | +144.6 MiB (+1.67%) |

Baseline used native uv sync; sandboxed uv later panicked before building, so
final used the same maturin PEP 517 release backend directly with Rust 1.91.1
and -C target-cpu=native. The wrapper command differs. BGZF was used on both
sides to fit disk, so these timings are only compared against each other.

Full-genome strict parity: PASS at both 1 and 8 workers, before and after the
fix. All 4,096,123 records and headers match the merged VEP reference. Body MD5:
157fca5b065c20eb7cec3e7a18ef28b7. All six issue-specific cases also match their
actual VEP 116 Docker oracle.

Remote CI, bot review, and ready-for-review handoff are tracked on the linked
PRs. Passing local checks does not by itself make the change merge-ready.
