# Build context must be this directory, staged first by run_sweep.sh so it
# contains:
#   vepyr-src/                         (a checkout of vepyr's plain `master`,
#                                       NOT the cache-build-lpt-v2 patched
#                                       branch -- built for Linux/x86_64 or
#                                       arm64 INSIDE this Dockerfile; a wheel
#                                       built on macOS is a different platform
#                                       and cannot run in this Linux image)
#   raw_cache/homo_sapiens_merged/     (copy of the chr20+chr21+chr22 raw
#                                       Ensembl cache used throughout this
#                                       thesis's other measurements)

FROM ghcr.io/pyo3/maturin AS builder
WORKDIR /src
COPY vepyr-src /src
RUN maturin build --release --out /wheels

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /wheels/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -f /tmp/*.whl

COPY raw_cache /raw_cache
COPY worker.py /app/worker.py

RUN mkdir -p /output
# glibc's default per-thread arena allocator fragments badly under this
# workload's allocation pattern inside a Linux container (confirmed via direct
# measurement: identical code peaks at <1GB natively on macOS but grows past
# 10GB+ unbounded here without this). MALLOC_ARENA_MAX=1 forces a single
# shared arena, which fixes it for every entity except "translation" (see
# worker.py -- excluded from the distributed sweep's scope for this reason).
ENV MALLOC_ARENA_MAX=1
ENV CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38
ENV OUTPUT_DIR=/output
ENV CACHE_TYPE=merged
ENV EXPECTED_CACHE_VERSION=116

ENTRYPOINT ["python", "/app/worker.py"]
