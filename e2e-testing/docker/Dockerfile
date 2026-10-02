FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bcftools tabix git git-lfs ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG VEPYR_VERSION=0.9.0
RUN pip install --no-cache-dir "vepyr==${VEPYR_VERSION}" "huggingface_hub==1.28.0"

ENV PYTHONUNBUFFERED=1 \
    HF_HUB_DISABLE_TELEMETRY=1 \
    HF_HUB_DISABLE_IMPLICIT_TOKEN=1 \
    HF_HOME=/work/huggingface
WORKDIR /repo
CMD ["bash"]
