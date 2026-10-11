# Job image for /parity-wgs on the self-hosted runner. Built locally on the
# runner by build-image.sh and never pulled at job time. vepyr itself is not
# baked in: each job installs the PR's wheel into a fresh venv under /work.
# Digest is the multi-arch index of ubuntu:24.04 (resolved 2026-10-10).
FROM ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3.12 python3.12-venv ca-certificates curl bcftools tabix git \
 && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/local/bin/uv
ENV UV_NO_CACHE=1 PYTHONDONTWRITEBYTECODE=1
