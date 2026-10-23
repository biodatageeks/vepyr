FROM ghcr.io/pyo3/maturin AS builder
WORKDIR /src/vepyr-lpt
COPY vepyr-lpt /src/vepyr-lpt
COPY datafusion-bio-functions /src/datafusion-bio-functions
RUN maturin build --release --out /wheels
FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /wheels/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -f /tmp/*.whl
COPY worker.py /app/worker.py
RUN mkdir -p /output
ENV MALLOC_ARENA_MAX=1
ENV CACHE_ROOT=/raw_cache/homo_sapiens_merged/116_GRCh38
ENV OUTPUT_DIR=/output
ENV CACHE_TYPE=merged
ENV EXPECTED_CACHE_VERSION=116
ENTRYPOINT ["python", "/app/worker.py"]
