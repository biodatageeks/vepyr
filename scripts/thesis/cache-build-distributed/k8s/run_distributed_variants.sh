#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
IMAGE="${POINT5_IMAGE:-cache-build-worker:local}"
CHROM_SET="${POINT5_CHROM_SET:-20,21,22}"
OUT="$ROOT/distributed_variants_$(date +%Y%m%dT%H%M%S)"
mkdir -p "$OUT"

run_job() {
  local name="$1" mode="$2"
  cat > "$ROOT/${name}.yaml" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${name}
  labels:
    point5-variant: ${name}
spec:
  completions: 3
  parallelism: 3
  completionMode: Indexed
  backoffLimit: 0
  template:
    metadata:
      labels:
        point5-variant: ${name}
    spec:
      restartPolicy: Never
      tolerations:
      - key: node-role.kubernetes.io/control-plane
        operator: Exists
        effect: NoSchedule
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchLabels:
                point5-variant: ${name}
            topologyKey: kubernetes.io/hostname
      containers:
      - name: worker
        image: ${IMAGE}
        imagePullPolicy: Never
        env:
        - name: WORKER_COUNT
          value: "3"
        - name: CHROMS
          value: "${CHROM_SET}"
        - name: CHROM_SET
          value: "${CHROM_SET}"
        command: ["sh", "-c"]
        args:
        - |
          if [ "${mode}" = "chromosome" ]; then
            case "\$CHROM_SET:\$JOB_COMPLETION_INDEX" in
              1,2,3:0|20,21,22:0) export CHROMS=${CHROM_SET%%,*};;
              1,2,3:1|20,21,22:1) export CHROMS=$(echo ${CHROM_SET} | cut -d, -f2);;
              1,2,3:2|20,21,22:2) export CHROMS=$(echo ${CHROM_SET} | cut -d, -f3);;
            esac
          fi
          exec python /app/worker.py
YAML
  kubectl delete job "$name" --ignore-not-found >/dev/null 2>&1 || true
  local started=$(date +%s)
  kubectl apply -f "$ROOT/${name}.yaml"
  kubectl wait --for=condition=complete "job/${name}" --timeout=12h
  local elapsed=$(( $(date +%s) - started ))
  mkdir -p "$OUT/$name"
  echo "${name},${mode},${elapsed}" | tee -a "$OUT/timings.csv"
  kubectl get pods -l "job-name=${name}" -o wide > "$OUT/$name/pods.txt"
  for pod in $(kubectl get pods -l "job-name=${name}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
    idx=$(kubectl get pod "$pod" -o jsonpath='{.metadata.labels.batch\.kubernetes\.io/job-completion-index}')
    kubectl logs "$pod" > "$OUT/$name/pod${idx}.log"
  done
  kubectl delete job "$name" >/dev/null
}

echo 'job,mode,seconds' > "$OUT/timings.csv"
run_job point5-chrom-shard chromosome
run_job point5-entity-shard entity
echo "RESULTS=$OUT"
