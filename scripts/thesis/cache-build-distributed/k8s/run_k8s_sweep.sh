#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
IMAGE="cache-build-worker:local"
CHROMS="${CHROMS:-22}"
OUT="$ROOT/results_${CHROMS}_$(date +%Y%m%dT%H%M%S)"
mkdir -p "$OUT"

run_job() {
  local name="$1" count="$2"
  cat > "$ROOT/${name}.yaml" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${name}
spec:
  completions: ${count}
  parallelism: ${count}
  completionMode: Indexed
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: worker
        image: ${IMAGE}
        imagePullPolicy: Never
        env:
        - name: WORKER_COUNT
          value: "${count}"
        - name: CHROMS
          value: "${CHROMS}"
YAML
  kubectl delete job "$name" --ignore-not-found >/dev/null 2>&1 || true
  local started=$(date +%s)
  kubectl apply -f "$ROOT/${name}.yaml"
  kubectl wait --for=condition=complete "job/${name}" --timeout=12h
  local elapsed=$(( $(date +%s) - started ))
  echo "${name},${count},${elapsed}" | tee -a "$OUT/timings.csv"
  mkdir -p "$OUT/$name"
  for pod in $(kubectl get pods -l "job-name=${name}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
    idx=$(kubectl get pod "$pod" -o jsonpath='{.metadata.labels.batch\.kubernetes\.io/job-completion-index}')
    mkdir -p "$OUT/$name/pod${idx}"
    kubectl logs "$pod" > "$OUT/$name/pod${idx}/pod.log"
    kubectl cp "$pod:/output/." "$OUT/$name/pod${idx}/" 2>"$OUT/$name/pod${idx}/cp.error" || true
  done
  kubectl delete job "$name" >/dev/null
}

echo 'job,count,seconds' > "$OUT/timings.csv"
run_job cache-n1 1
run_job cache-n3 3
echo "RESULTS=$OUT"
