#!/usr/bin/env bash
# Build transient per-chromosome plugin caches and compare vepyr with VEP 116.
#
# The complete five-plugin cache is larger than the available local disk.  This
# runner therefore preserves the cache shards that existed at startup, builds
# each missing chromosome just long enough to compare it, then removes only the
# transient shards and restores the original manifests.  Reports, strict-body
# logs, and compressed vepyr outputs are retained.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_ROOT="${VEPYR_DATA_ROOT:-${DATA_VEPYR_DIR:-/Users/mwiewior/workspace/data_vepyr}}"
# The comparison CLI resolves its input VCF, FASTA and core cache from
# DATA_VEPYR_DIR, not from this script's variable.  Export it so overriding the
# runner's root moves every path, not only the two passed as explicit flags.
export DATA_VEPYR_DIR="$DATA_ROOT"
CACHE_DIR="${VEPYR_CACHE_DIR:-$DATA_ROOT/cache/116_GRCh38_merged}"
PLUGIN_CACHE="${VEPYR_PLUGIN_CACHE:-$DATA_ROOT/plugin_cache}"
PLUGIN_REPO="${VEPYR_PLUGIN_REPO:-/Users/mwiewior/workspace/vepyr-plugins}"
REFERENCE_DIR="${VEP_REFERENCE_DIR:-$DATA_ROOT/output/116/plugins}"
SOURCE_BASE="${VEP_PLUGIN_SOURCE_URL:-http://localhost:18080}"
WORK_ROOT="${VEP_COMPARISON_WORK_ROOT:-$DATA_ROOT/output/116/plugins/.comparison_cache_work}"
LOG_DIR="${VEP_COMPARISON_LOG_DIR:-$DATA_ROOT/output/116/plugins/comparison_logs}"
CHROMS="${VEP_COMPARISON_CHROMS:-1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22}"
WORKERS="${VEP_COMPARISON_WORKERS:-4}"

PLUGINS="clinvar alphamissense dbnsfp spliceai cadd"
mkdir -p "$WORK_ROOT" "$LOG_DIR"

for tool in git uv; do
  command -v "$tool" >/dev/null || { echo "ERROR: missing required tool: $tool" >&2; exit 1; }
done
[[ -d "$CACHE_DIR/variation" ]] || { echo "ERROR: missing variation cache: $CACHE_DIR" >&2; exit 1; }
[[ -d "$PLUGIN_REPO/.git" ]] || { echo "ERROR: missing plugin repo: $PLUGIN_REPO" >&2; exit 1; }

# Validate the requested chromosomes before anything reads them, so a typo
# reports itself rather than surfacing as a missing-shard or source-server
# error further down.
for chrom in $CHROMS; do
  case "$chrom" in
    ''|*[!0-9]*) echo "ERROR: invalid chromosome: $chrom" >&2; exit 1 ;;
  esac
  if (( chrom < 1 || chrom > 22 )); then
    echo "ERROR: chromosome out of range: $chrom" >&2
    exit 1
  fi
done

manifest_snapshot="$(mktemp -d "$WORK_ROOT/manifests.XXXXXX")"
for plugin in $PLUGINS; do
  manifest="$PLUGIN_CACHE/plugin/$plugin/manifest.json"
  [[ -s "$manifest" ]] || { echo "ERROR: missing baseline manifest: $manifest" >&2; exit 1; }
  cp "$manifest" "$manifest_snapshot/$plugin.json"
done

restore_manifests() {
  local plugin
  for plugin in $PLUGINS; do
    if [[ -s "$manifest_snapshot/$plugin.json" ]]; then
      cp "$manifest_snapshot/$plugin.json" "$PLUGIN_CACHE/plugin/$plugin/manifest.json"
    fi
  done
}
# State for the chromosome currently in flight, so an early exit under `set -e`
# cleans up after it.  Restoring manifests alone would leave orphaned shards
# that a later cache_complete() -- which only tests file existence -- would
# happily reuse against a manifest that no longer describes them.
CURRENT_CHROM=""
CURRENT_WORK=""
CURRENT_TRANSIENT=0
CURRENT_KEEP=""
CURRENT_PIDS=""

# A signal arriving while build_transient_caches()' background builders are
# running must reap them BEFORE anything is deleted. Otherwise a builder
# recreates its shard or manifest after cleanup has run, which is precisely the
# orphaned state the cleanup exists to prevent.
# A builder PID is the outer `build_x ... &` subshell; the work happens in
# nested subshells running tabix, bgzip and `uv run python`. Killing only the
# outer shell orphans those, and an orphan can still write a shard after
# cleanup. Walk the tree depth-first so children die before their parent.
kill_tree() {
  local pid="$1" child
  for child in $(pgrep -P "$pid" 2>/dev/null || true); do
    kill_tree "$child"
  done
  kill "$pid" 2>/dev/null || true
}

# Builders are launched under `set -m`, so each is its own process-group
# leader and one signal reaches the whole tree. `wait` cannot help confirm
# that: it reaps only the recorded child, never its descendants, so a
# descendant that handles TERM slowly -- or forks during the kill_tree walk --
# could outlive the call and write a shard after cleanup. Poll the group until
# it is genuinely gone, escalating to KILL.
terminate_builders() {
  local pid deadline
  [[ -z "$CURRENT_PIDS" ]] && return 0
  for pid in $CURRENT_PIDS; do
    kill -TERM -- "-$pid" 2>/dev/null || kill_tree "$pid"
  done
  for pid in $CURRENT_PIDS; do
    # Reap the builder itself BEFORE asking whether its group is empty. `wait`
    # is the only thing that can clear this child, and an unreaped one is still
    # a member of its own process group -- so checking the group first would
    # make the leader's own corpse the reason the group never looks empty.
    # Both loops are bounded and break after escalating, so neither can spin.
    deadline=$((SECONDS + 10))
    while kill -0 "$pid" 2>/dev/null; do
      if (( SECONDS >= deadline )); then
        kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
        break
      fi
      sleep 0.2
    done
    wait "$pid" 2>/dev/null || true

    # With the leader reaped, anything left in the group is a real survivor.
    deadline=$((SECONDS + 10))
    while kill -0 -- "-$pid" 2>/dev/null; do
      if (( SECONDS >= deadline )); then
        kill -KILL -- "-$pid" 2>/dev/null || true
        break
      fi
      sleep 0.2
    done
  done
  CURRENT_PIDS=""
  return 0
}

# Bash resumes the interrupted command after an INT/TERM handler returns, so
# the handler must exit rather than fall through -- the EXIT trap then performs
# the actual cleanup, once, with no builders still alive.
on_signal() {
  local name="$1" num="$2"
  echo "SIGNAL $name received; terminating builders and cleaning up" >&2
  terminate_builders
  trap - INT TERM
  exit $((128 + num))
}

cleanup_on_exit() {
  terminate_builders
  if [[ "$CURRENT_TRANSIENT" -eq 1 && -n "$CURRENT_CHROM" ]]; then
    echo "CLEANUP chr$CURRENT_CHROM removing transient artifacts" >&2
    remove_transient_caches "$CURRENT_CHROM" "$CURRENT_KEEP" || true
    if [[ -n "$CURRENT_WORK" && -d "$CURRENT_WORK" ]]; then
      find "$CURRENT_WORK" -depth -delete || true
    fi
    CURRENT_TRANSIENT=0
  fi
  restore_manifests
}
trap cleanup_on_exit EXIT
trap 'on_signal INT 2' INT
trap 'on_signal TERM 15' TERM

plugin_version() {
  case "$1" in
    clinvar) echo 4c92563adb49389ce1569a77681274f8f37c9fd8 ;;
    dbnsfp) echo c9fe785 ;;
    alphamissense|cadd|spliceai) echo f9cfa30 ;;
    *) echo "ERROR: unknown plugin: $1" >&2; return 1 ;;
  esac
}

cache_complete() {
  local chrom="$1" plugin
  for plugin in $PLUGINS; do
    [[ -s "$PLUGIN_CACHE/plugin/$plugin/chr${chrom}.parquet" ]] || return 1
  done
}

# Slicing tools and the source server are needed only when a requested
# chromosome is missing from the cache.  With a complete cache every iteration
# takes the CACHE_REUSE path and never opens a source, so probing for them up
# front would fail a run that needs nothing they provide.
needs_sources=0
for chrom in $CHROMS; do
  cache_complete "$chrom" || { needs_sources=1; break; }
done
if (( needs_sources )); then
  for tool in bgzip curl tabix; do
    command -v "$tool" >/dev/null || { echo "ERROR: missing required tool: $tool" >&2; exit 1; }
  done
  curl --fail --silent --show-error --head "$SOURCE_BASE/clinvar/clinvar.vcf.gz" >/dev/null
else
  echo "PREFLIGHT all requested chromosomes cached; skipping source-server checks"
fi

slice_bgzf() {
  local work="$1" remote="$2" region="$3" output="$4" preset="$5"
  local partial="${output}.partial.$$.gz"
  echo "SLICE $(basename "$output") region=$region"
  (
    cd "$work"
    tabix -h "${SOURCE_BASE%/}/$remote" "$region" | bgzip -c > "$partial"
  )
  mv "$partial" "$output"
  case "$preset" in
    vcf) tabix -f -p vcf "$output" ;;
    tsv) tabix -f -s 1 -b 2 -e 2 "$output" ;;
    *) echo "ERROR: unknown tabix preset: $preset" >&2; return 1 ;;
  esac
}

slice_plain() {
  local work="$1" remote="$2" region="$3" output="$4"
  local partial="${output}.partial.$$"
  echo "SLICE $(basename "$output") region=$region"
  (
    cd "$work"
    tabix -h "${SOURCE_BASE%/}/$remote" "$region" > "$partial"
  )
  mv "$partial" "$output"
}

build_cache() {
  local chrom="$1" plugin="$2" source="$3"
  local version log
  version="$(plugin_version "$plugin")"
  log="$LOG_DIR/cache_chr${chrom}_${plugin}.log"
  echo "CACHE_START chr$chrom $plugin source=$(basename "$source")"
  (
    cd "$REPO_DIR"
    uv run python -c '
import sys
import vepyr

plugin, version, source, cache_dir, plugin_cache, chrom, plugin_repo = sys.argv[1:]
result = vepyr.build_plugin_cache(
    plugin,
    version,
    source_path=source,
    cache_dir=cache_dir,
    plugin_cache_root=plugin_cache,
    chroms=[chrom],
    plugins_repo=plugin_repo,
    overwrite=True,
)
print(result)
' "$plugin" "$version" "$source" "$CACHE_DIR" "$PLUGIN_CACHE" "$chrom" "$PLUGIN_REPO"
  ) >"$log" 2>&1
  echo "CACHE_DONE chr$chrom $plugin $(tail -1 "$log")"
}

build_clinvar() {
  local chrom="$1" work="$2" source="$work/clinvar_chr${chrom}.vcf.gz"
  slice_bgzf "$work" clinvar/clinvar.vcf.gz "$chrom" "$source" vcf
  build_cache "$chrom" clinvar "$source"
  unlink "$source"
  unlink "$source.tbi"
}

build_alphamissense() {
  local chrom="$1" work="$2" source="$work/alphamissense_chr${chrom}.tsv.gz"
  slice_bgzf "$work" alphamissense/AlphaMissense_hg38.bgz.tsv.gz "chr${chrom}" "$source" tsv
  build_cache "$chrom" alphamissense "$source"
  unlink "$source"
  unlink "$source.tbi"
}

build_dbnsfp() {
  # The Perl plugin requires BGZF+tabix, but the cache manifest intentionally
  # declares an uncompressed TSV provider.  Feeding its BGZF form to Arrow CSV
  # makes the gzip header look like a one-column first record.
  local chrom="$1" work="$2" source="$work/dbNSFP5.3.1a_grch38_chr${chrom}.tsv"
  slice_plain "$work" dbnsfp/dbNSFP5.3.1a_grch38.gz "$chrom" "$source"
  build_cache "$chrom" dbnsfp "$source"
  unlink "$source"
}

build_spliceai() {
  local chrom="$1" work="$2" source="$work/spliceai_chr${chrom}.vcf.gz"
  slice_bgzf "$work" spliceai/spliceai_scores.masked.snv.ensembl_mane.grch38.110.vcf.gz "$chrom" "$source" vcf
  build_cache "$chrom" spliceai "$source"
  unlink "$source"
  unlink "$source.tbi"
}

build_cadd() {
  local chrom="$1" work="$2" source="$work/cadd_all_chr${chrom}.tsv"
  local partial="${source}.partial.$$"
  echo "SLICE $(basename "$source") region=$chrom"
  (
    cd "$work"
    tabix "${SOURCE_BASE%/}/cadd/whole_genome_SNVs.tsv.gz" "$chrom" > "$partial"
    tabix "${SOURCE_BASE%/}/cadd/gnomad.genomes.r4.0.indel.tsv.gz" "$chrom" >> "$partial"
  )
  mv "$partial" "$source"
  build_cache "$chrom" cadd "$source"
  unlink "$source"
}

shard_present() {
  local chrom="$1" plugin="$2"
  [[ -s "$PLUGIN_CACHE/plugin/$plugin/chr${chrom}.parquet" ]]
}

# cache_complete() is all-or-nothing, so one missing plugin used to rebuild all
# five -- overwriting the four already on disk. Recording their paths does not
# save them: preservation keeps the path, so what survives cleanup is the
# rebuilt file, left paired with the metadata restore_manifests() puts back.
# Build only what is genuinely absent; then the kept shard really is the
# original, and it still matches its manifest.
build_transient_caches() {
  local chrom="$1" work="$2" pid failed plugin
  local pids=""

  CURRENT_PIDS=""
  # These three builders have low measured peak RSS and write independent
  # manifests, so running them together is safe on the 64 GiB host.
  # `set -m` puts each in its own process group; see terminate_builders().
  set -m
  for plugin in clinvar alphamissense dbnsfp; do
    if shard_present "$chrom" "$plugin"; then
      echo "SKIP_BUILD chr$chrom $plugin (already cached)"
      continue
    fi
    "build_$plugin" "$chrom" "$work" &
    # Publish before starting the next one: a signal arriving mid-loop would
    # otherwise find CURRENT_PIDS still holding the pre-loop value and reap
    # nothing, leaving a live builder to write after cleanup.
    CURRENT_PIDS="$CURRENT_PIDS $!"
    pids="$pids $!"
  done
  set +m
  failed=0
  for pid in $pids; do wait "$pid" || failed=1; done
  CURRENT_PIDS=""
  [[ "$failed" -eq 0 ]] || return 1

  # SpliceAI and CADD are each memory-heavy; keep them sequential. They run
  # backgrounded-then-waited rather than in the foreground so they are tracked
  # like the others, and so a signal during a long CADD build is handled at
  # once instead of after it finishes.
  for plugin in spliceai cadd; do
    if shard_present "$chrom" "$plugin"; then
      echo "SKIP_BUILD chr$chrom $plugin (already cached)"
      continue
    fi
    set -m
    "build_$plugin" "$chrom" "$work" &
    pid=$!
    CURRENT_PIDS="$pid"
    set +m
    if ! wait "$pid"; then
      CURRENT_PIDS=""
      return 1
    fi
    CURRENT_PIDS=""
  done
}

run_comparison() {
  local chrom="$1"
  local reference="$REFERENCE_DIR/HG002_chr${chrom}_5plugins_vep116_caddfix.vcf.gz"
  local result_dir="$REPO_DIR/e2e-testing/results/116/fast_chr${chrom}"
  local run_log="$LOG_DIR/compare_chr${chrom}.log"
  local strict_log="$LOG_DIR/strict_chr${chrom}.log"
  local vep_slice="$result_dir/vep_chr${chrom}_merged_plugins.vcf"
  local vepyr_bgzf="$result_dir/vepyr_parquet_chr${chrom}_merged_plugins.vcf.gz"
  local vepyr_plain="$result_dir/vepyr_parquet_chr${chrom}_merged_plugins.vcf"
  local strict_rc

  [[ -s "$reference" && -s "$reference.tbi" ]] || {
    echo "ERROR: missing reference or index for chr$chrom: $reference" >&2
    return 1
  }

  echo "COMPARE_START chr$chrom"
  (
    cd "$REPO_DIR"
    uv run python e2e-testing/scripts/run_comparison.py \
      --release 116 --profile merged_plugins --chroms "$chrom" \
      --plugin-cache "$PLUGIN_CACHE" --vep "$reference" \
      --workers "$WORKERS" --bgzf --force
  ) >"$run_log" 2>&1

  [[ -s "$vep_slice" && -s "$vepyr_bgzf" ]] || {
    echo "ERROR: comparison outputs missing for chr$chrom" >&2
    return 1
  }

  set +e
  (
    cd "$REPO_DIR"
    uv run python e2e-testing/scripts/md5_concordance.py \
      --pair "$vep_slice" "$vepyr_bgzf" --mode strict --explain --explain-limit 0
  ) >"$strict_log" 2>&1
  strict_rc=$?
  set -e
  echo "$strict_rc" > "$LOG_DIR/strict_chr${chrom}.exit"
  echo "COMPARE_DONE chr$chrom strict_rc=$strict_rc report=$REPO_DIR/e2e-testing/reports/fast_chr${chrom}_merged_plugins_116_report.json"

  # The original reference remains compressed+indexed in REFERENCE_DIR, so its
  # multi-gigabyte plain slice is redundant after strict hashing.  Keep the
  # compressed vepyr output and all JSON/log evidence.
  [[ -f "$vep_slice" ]] && unlink "$vep_slice"
  [[ -f "$vepyr_plain" ]] && unlink "$vepyr_plain"
  return 0
}

# cache_complete() demands all five shards, so a chromosome missing even one
# rebuilds all five with overwrite=True.  Deleting all five afterwards would
# destroy the four that were already there -- expensive, and the opposite of
# what this script's header promises.  Record what existed at startup and keep
# exactly those paths.
preexisting_shards() {
  local chrom="$1" plugin shard
  for plugin in $PLUGINS; do
    shard="$PLUGIN_CACHE/plugin/$plugin/chr${chrom}.parquet"
    [[ -f "$shard" ]] && printf '%s\n' "$shard"
  done
  return 0
}

remove_transient_caches() {
  local chrom="$1" keep="${2:-}" plugin shard
  for plugin in $PLUGINS; do
    shard="$PLUGIN_CACHE/plugin/$plugin/chr${chrom}.parquet"
    [[ -f "$shard" ]] || continue
    if [[ -n "$keep" && -f "$keep" ]] && grep -qxF "$shard" "$keep"; then
      echo "KEEP_PREEXISTING $shard"
      continue
    fi
    unlink "$shard"
  done
  restore_manifests
}

for chrom in $CHROMS; do
  echo "CHROM_START chr$chrom $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  transient=0
  work=""
  keep=""
  CURRENT_CHROM="$chrom"
  CURRENT_WORK=""
  CURRENT_TRANSIENT=0
  CURRENT_KEEP=""
  if ! cache_complete "$chrom"; then
    transient=1
    work="$(mktemp -d "$WORK_ROOT/chr${chrom}.XXXXXX")"
    keep="$work/preexisting_shards.txt"
    preexisting_shards "$chrom" > "$keep"
    CURRENT_WORK="$work"
    CURRENT_KEEP="$keep"
    CURRENT_TRANSIENT=1
    echo "WORK chr$chrom $work (preserving $(wc -l < "$keep" | tr -d ' ') pre-existing shard(s))"
    build_transient_caches "$chrom" "$work"
  else
    echo "CACHE_REUSE chr$chrom"
  fi

  run_comparison "$chrom"

  if [[ "$transient" -eq 1 ]]; then
    remove_transient_caches "$chrom" "$keep"
    find "$work" -depth -delete
    CURRENT_TRANSIENT=0
  fi
  CURRENT_CHROM=""
  echo "CHROM_DONE chr$chrom $(date -u '+%Y-%m-%dT%H:%M:%SZ') free=$(df -h "$DATA_ROOT" | awk 'NR==2 {print $4}')"
done

restore_manifests
find "$manifest_snapshot" -depth -delete
trap - EXIT INT TERM
echo "ALL_DONE $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
