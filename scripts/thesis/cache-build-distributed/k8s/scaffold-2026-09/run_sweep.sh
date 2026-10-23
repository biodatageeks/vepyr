#!/usr/bin/env bash
# Distributed cache-build sweep over a multi-node minikube cluster.
#
# NOT run yet — scaffold only. Do not `minikube start` while the local LPT
# concurrency sweep (scratchpad/lpt_build_concurrency_sweep.py) is still
# running; both are CPU/RAM heavy on the same M1/16GB machine.
#
# Usage:
#   ./run_sweep.sh setup           # build image (inside minikube's own docker-env),
#                                   # start minikube --nodes 3, load image
#   ./run_sweep.sh sweep chr22     # quick smoke test: N=1,2 workers, chr22 only
#   ./run_sweep.sh sweep full      # N=1,2,3 workers, chr20+chr21+chr22 (the thesis's real scope)
#   ./run_sweep.sh teardown        # minikube delete
set -euo pipefail
cd "$(dirname "$0")"

VEPYR_MASTER="../vepyr"          # plain master checkout, NOT cache-build-lpt-v2
RAW_CACHE_SRC="/tmp/raw_chr20_21_extract/homo_sapiens_merged"
IMAGE="cache-build-worker:local"
NODES=3                         # matches the 3-worker ceiling of the "full" sweep below
NODE_MEMORY="5000mb"            # 2200/3000mb both OOMKilled a single-worker chr22 build in
                                 # the container despite the same workload succeeding locally
                                 # in well under 1GB -- container/DataFusion memory behavior
                                 # differs from bare-metal here, not fully root-caused, this is
                                 # the empirically-needed value. Needs colima's own VM (docker
                                 # driver) started with enough memory for 3 nodes at this size,
                                 # e.g. `colima start --cpu 4 --memory 14`.
COLLECTED_ROOT="/tmp/distributed_collected"
RESULTS_LOG="/tmp/distributed_sweep.log"

kubectl() { minikube kubectl -- "$@"; }

cmd_setup() {
  echo "[1/3] staging vepyr master source (isolating this experiment from the LPT patch)"
  # The main vepyr checkout may be on cache-build-lpt-v2 (has a local Cargo.toml
  # [patch] override) -- use a throwaway worktree at master instead of touching
  # that checkout's branch/state at all. The Dockerfile compiles this INSIDE
  # the Linux image (a wheel built on macOS is a different platform and won't
  # run in the container).
  local worktree=/tmp/vepyr-master-worktree
  rm -rf "$worktree" vepyr-src
  (cd "$VEPYR_MASTER" && git worktree add --force "$worktree" master)
  cp -R "$worktree" vepyr-src
  rm -rf vepyr-src/.git
  (cd "$VEPYR_MASTER" && git worktree remove --force "$worktree" 2>/dev/null || true)

  rm -rf raw_cache
  mkdir -p raw_cache
  cp -RL "$RAW_CACHE_SRC" raw_cache/homo_sapiens_merged   # -L: dereference any symlinked chrom dirs

  echo "[2/3] starting minikube ($NODES nodes, driver=docker via colima)"
  minikube start --nodes "$NODES" --driver=docker --cpus=2 --memory="$NODE_MEMORY"

  echo "[3/3] building image (minikube's docker driver shares colima's docker daemon -- no load step needed)"
  docker build -t "$IMAGE" .
  echo "setup done."
}

## Returns "complete" or "failed" on stdout; never throws. Polls each pod's
## own log for the "COLLECT_READY" marker the job template's command prints
## right after worker.py exits (see build-job.yaml.tmpl) -- the job itself
## never reaches K8s condition=complete quickly, since its container keeps
## sleeping afterward specifically so collection can still reach it.
wait_job() {
  local job="$1" n="$2" timeout_s="${3:-1800}"
  local waited=0
  while (( waited < timeout_s )); do
    local ready_count=0
    for i in $(seq 0 $((n - 1))); do
      local pod
      pod=$(minikube kubectl -- get pods -l "job-name=$job,batch.kubernetes.io/job-completion-index=$i" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
      if [[ -n "$pod" ]] && minikube kubectl -- logs "$pod" 2>/dev/null | grep -q COLLECT_READY; then
        ready_count=$((ready_count + 1))
      fi
    done
    if [[ "$ready_count" -eq "$n" ]]; then
      echo complete
      return
    fi
    sleep 5
    waited=$((waited + 5))
  done
  echo failed
}

## Max, across all n pods, of the "seconds" field worker.py prints in its own
## JSON result line -- a better wall-clock proxy here than K8s job timestamps,
## since the job's container intentionally keeps running (sleeping) after the
## real work finishes.
job_duration_seconds() {
  local job="$1" n="$2"
  local max=0
  for i in $(seq 0 $((n - 1))); do
    local pod secs
    pod=$(minikube kubectl -- get pods -l "job-name=$job,batch.kubernetes.io/job-completion-index=$i" \
      -o jsonpath='{.items[0].metadata.name}')
    secs=$(minikube kubectl -- logs "$pod" 2>/dev/null | python3 -c "
import sys, json
for line in sys.stdin:
    line = line.strip()
    if line.startswith('{'):
        print(json.loads(line)['seconds'])
        break
" || echo 0)
    max=$(python3 -c "print(max($max, $secs))")
  done
  echo "$max"
}

collect_pod_outputs() {
  local job="$1" n="$2" outdir="$3"
  mkdir -p "$outdir"
  for i in $(seq 0 $((n - 1))); do
    local pod
    pod=$(minikube kubectl -- get pods -l "job-name=$job,batch.kubernetes.io/job-completion-index=$i" \
      -o jsonpath='{.items[0].metadata.name}')
    mkdir -p "$outdir/pod$i"
    minikube kubectl -- cp "$pod:/output" "$outdir/pod$i" -c worker
  done
}

cmd_sweep() {
  local mode="${1:?usage: sweep MODE, where MODE is chr22 or full}"
  local chroms worker_counts
  local ref_json
  case "$mode" in
    chr22) chroms="22"; worker_counts="1 2" ;;
    full)  chroms="20,21,22"; worker_counts="1 2 3" ;;
    *) echo "unknown mode: $mode (want chr22|full)" >&2; exit 1 ;;
  esac
  local chrom_filter_arg
  chrom_filter_arg=$(python3 -c "print(','.join('chr'+c for c in '$chroms'.split(',')))")

  # N=1's own collected output is the reference for N>1 -- both run inside
  # the identical container image/mount path, so plain byte hashes are valid
  # here (they would NOT be valid against a build done on a different host:
  # the parquet output embeds the raw cache's source file path as a
  # provenance column, which always differs across mount points/hosts).
  local ref_dir=""

  : > "$RESULTS_LOG"
  for n in $worker_counts; do
    echo "=== mode=$mode worker_count=$n ==="
    WORKER_COUNT="$n" IMAGE="$IMAGE" CHROMS="$chroms" envsubst < k8s/build-job.yaml.tmpl | minikube kubectl -- apply -f -
    job="cache-build-$n"
    job_status=$(wait_job "$job" "$n")

    if [[ "$job_status" != "complete" ]]; then
      status=$(minikube kubectl -- get pods -l "job-name=$job" -o jsonpath='{.items[*].status.phase}' 2>/dev/null || echo unknown)
      echo "RESULT,mode=$mode,worker_count=$n,FAILED,pod_phases=${status:-unknown}" | tee -a "$RESULTS_LOG"
      minikube kubectl -- delete job "$job" --ignore-not-found
      continue
    fi

    dt=$(job_duration_seconds "$job" "$n")
    outdir="$COLLECTED_ROOT/${mode}_n$n"
    rm -rf "$outdir"
    collect_pod_outputs "$job" "$n" "$outdir"
    verify=$(python3 verify_distributed.py "$outdir" "$ref_dir" "$chrom_filter_arg" | head -1)
    if [[ -z "$ref_dir" ]]; then
      ref_dir="$outdir"
    fi

    echo "RESULT,mode=$mode,worker_count=$n,${dt}s,${verify}" | tee -a "$RESULTS_LOG"
    minikube kubectl -- delete job "$job"
  done
  echo "DISTRIBUTED_SWEEP_DONE,mode=$mode" | tee -a "$RESULTS_LOG"
}

cmd_teardown() {
  minikube delete
}

case "${1:-}" in
  setup) cmd_setup ;;
  sweep) cmd_sweep "${2:-}" ;;
  teardown) cmd_teardown ;;
  *) echo "usage: $0 {setup|sweep {chr22|full}|teardown}" >&2; exit 1 ;;
esac
