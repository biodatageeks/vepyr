# Handover: MotifFeature parity, release 116

**Date:** 2026-07-28
**Scope:** vepyr vs Ensembl VEP 116 CSQ parity for MotifFeature annotations.

> **Historical snapshot.** The residuals and open items below describe the
> 2026-07-28 handover, not the final release state. The completed six-profile,
> zero-mismatch result is recorded in
> [the final root-cause and release report](2026-07-29-remaining-734-root-cause-and-plan.md).

## Where things stand

chr22, release 116, `merged` cache, `--everything --hgvs`:

| | session start | now |
|---|---|---|
| field mismatches | 17,783 | **600** |
| fields affected | 46 | **6** |
| CSQ entry-count mismatch | 72 | **0** |

Residual 600 is fully attributed:

| field | count | blocked on |
|---|---|---|
| `HIGH_INF_POS` | 316 | PWM not in cache (fn#202) |
| `MOTIF_SCORE_CHANGE` | 270 | PWM not in cache (fn#202) |
| `Consequence` | 6 | fn#130, pre-existing |
| `HGVSp` | 4 | fn#130, pre-existing |
| `IMPACT` | 2 | pre-existing |
| `HGVSc` | 2 | fn#112, pre-existing |

Every motif field VEP can produce **without** the position-weight matrix now matches:
`Feature`, `MOTIF_NAME`, `TRANSCRIPTION_FACTORS`, `STRAND`, `MOTIF_POS`.

## Current pins (temporary — must be undone)

`vepyr/Cargo.toml` on branch `release-testing` (HEAD `dc07233`):

```toml
datafusion-bio-function-vep         rev = e551ccc4e256cfab87a2c0eeae942444feb1f00f
datafusion-bio-format-ensembl-cache rev = d760d5a50fd7455db8748d09c4da5a6ac63cd379
datafusion-bio-format-vcf           rev = d760d5a50fd7455db8748d09c4da5a6ac63cd379
```

`e551ccc` is the tip of `integration/motif-parity-116`, a **do-not-merge** branch = PR #203's
commits plus one pinning bio-formats to the #224 revision. That indirection exists because
`bio-function-vep` depends on bio-formats itself, cargo rejects two specs for one git repo, and
cargo also refuses a `[patch]` aimed back at the same source.

**After both PRs merge:** revert to released tags, delete `integration/motif-parity-116`.

## Open upstream

| | link |
|---|---|
| PR bio-formats #224 | https://github.com/biodatageeks/datafusion-bio-formats/pull/224 |
| PR bio-functions #203 | https://github.com/biodatageeks/datafusion-bio-functions/pull/203 |
| issue fn#202 (remaining work) | https://github.com/biodatageeks/datafusion-bio-functions/issues/202 |
| issue bio-formats #223 | https://github.com/biodatageeks/datafusion-bio-formats/issues/223 |
| issue fn#201 | https://github.com/biodatageeks/datafusion-bio-functions/issues/201 |

PR branch tips: bio-functions `1bf1009`, bio-formats `d760d5a`.

## The remaining work (fn#202)

`HIGH_INF_POS` and `MOTIF_SCORE_CHANGE`. From
`Bio/EnsEMBL/Variation/MotifFeatureVariationAllele.pm` in `ensemblorg/ensembl-vep:release_116.0`:

```perl
sub in_informative_position {
    # true SNPs only
    unless (($vf->start == $vf->end) && ($self->variation_feature_seq ne '-')) { return 0; }
    my $start = $self->motif_start;
    return 0 unless defined $start && $start >= 1 && $start <= $self->motif_feature->length;
    $self->{in_informative_position} = $mf->get_BindingMatrix->is_position_informative($start);
}
# motif_score_delta: scores ref and alt sequences against the full frequency matrix
```

Both need `binding_matrix.elements` (the frequency matrix), plus `threshold` and
`unit: "Frequencies"`. All are present in the cache's `raw_object_json` but **not extracted into
any typed column**.

Suggested order:

1. bio-formats: extract the PWM elements into a column.
2. Rebuild the 116 cache (~90 min, see below).
3. bio-functions: implement `is_position_informative` and `motif_score_delta`.
4. Re-measure chr22.

## Gotchas learned the hard way

Each of these cost a wasted build/rebuild cycle. Do not rediscover them.

1. **`build_cache(release, cache_dir)` treats `cache_dir` as a PARENT** and creates
   `<cache_dir>/<release>_GRCh38_<type>/` inside it.
2. **`build_cache(local_cache=...)` wants the directory containing `info.txt`** — the version
   directory (`$DATA/homo_sapiens_merged/116_GRCh38`), not the data root.
3. **CSQ multi-valued sub-fields use `&`, not `,`.** The cache joins lists with `,`, which is the
   CSQ *entry* separator. Emitting raw split one entry into three and produced 14,282 phantom
   mismatches. Use `csq_multi_value()`.
4. **Motif column lists are declared in THREE places** in `annotate_provider.rs`: two
   `MotifFeature` loaders plus the `scan_context_entity` call feeding `parse_motif_batches`. Only
   the third is reached by the parquet-cache path. Miss it and the columns silently stay `None`.
5. **`maturin develop` fails if both `VIRTUAL_ENV` and `CONDA_PREFIX` are set.** Use
   `env -u VIRTUAL_ENV -u CONDA_PREFIX RUSTFLAGS="-C target-cpu=native" uv run maturin develop --release`.
6. **Do not infer VEP behaviour from a handful of mismatching variants.** A "multi-base REF ⇒ empty
   MOTIF_POS" rule inferred from 2 cases regressed 2 → 38. Read the Perl in the Docker image
   instead; it is at `/opt/vep/src/ensembl-vep/Bio/EnsEMBL/Variation/`.

## Rebuilding the cache

```bash
# Complete release-qualified rebuild (dry run, then execute)
uv run python e2e-testing/scripts/rebuild_release_cache.py \
    --release 116 --cache-type merged
uv run python e2e-testing/scripts/rebuild_release_cache.py \
    --release 116 --cache-type merged --run

# Targeted motif rebuild through the generic entity workflow (dry run, then execute)
uv run python e2e-testing/scripts/rebuild_cache_entity.py \
    --release 116 --cache-type merged --entity motif
uv run python e2e-testing/scripts/rebuild_cache_entity.py \
    --release 116 --cache-type merged --entity motif --run
```

The complete path stages the whole cache and validates every entity. The
targeted path uses public `vepyr.build_cache_entity()`, validates every motif
manifest shard and its Parquet release/source metadata, and retains the prior
motif directory as a timestamped backup.

## Measuring

```bash
uv run python e2e-testing/scripts/run_comparison.py \
    --release 116 --profile merged --chroms 22 --force \
    --no-normalize --vcf ~/workspace/data_vepyr/HG002_normalized.vcf.gz
```

Report lands at `e2e-testing/reports/fast_chr22_merged_116_report.json`.

`--no-normalize` with the pre-normalized VCF skips a ~10 min `bcftools norm` over the full 2.9 GB
input; drop it for a from-scratch run.

## Other open items (not motif-related)

1. **Completed later:** the full chr1–22 release-116 run is zero-mismatch for
   merged, Ensembl, and RefSeq.
2. **Completed later:** release-116 Parquet caches now exist for merged,
   Ensembl, and RefSeq.
3. **`merged_flag_pick` and `merged_pick_filter` references were never generated** — unavailable at
   both releases.
4. **`$DATA` reorganisation is pending.** The runner prefers `$DATA/input/` and `$DATA/cache/` and
   falls back to the legacy root with a notice. Move the caches when no vepyr run is active; move
   the inputs only when no VEP container has `$DATA` bind-mounted.
5. **`compare.py` still pairs CSQ entries positionally** (sort by `(Feature, Consequence)`, then
   zip). This is what turns a single defect into a 46-field failure, and it never examines entries
   past `min(len(a), len(b))`. Joining on `(Feature_type, Feature)` was designed but not
   implemented; verified no duplicate keys exist across all 50,861 chr22 variants.
6. **GSD workflow is not initialized** (`.planning/` absent) though CLAUDE.md mandates routing edits
   through it. All edits this session were direct, flagged each time.
