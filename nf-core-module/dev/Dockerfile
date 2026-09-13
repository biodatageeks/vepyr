# Local linux/arm64 stand-in for the Seqera Wave image of environment.yml.
#
# bioconda has no linux-aarch64 vepyr yet (bioconda-recipes#69191), so Wave
# cannot build an arm64 image from environment.yml. This mirrors that
# environment with vepyr taken from its PyPI manylinux aarch64 wheel instead,
# which runs natively on Apple Silicon. Delete it once the Wave URIs land.
#
# Built by dev/nf-test-arm64.sh; by hand, from nf-core-module/:
#
#   mkdir -p dev/wheels   # COPY source; ignored by the root .gitignore
#   docker build --platform linux/arm64 -t vepyr-dev:0.7.0-arm64 dev
#
# To test an unreleased engine, drop a linux aarch64 wheel into dev/wheels/ and
# pass --build-arg VEPYR_SPEC=/wheels/<file>.whl.
FROM mambaorg/micromamba:1.5.10-bookworm-slim

ARG VEPYR_SPEC=vepyr==0.7.0

# procps-ng: Nextflow needs `ps` in the task container to collect metrics.
RUN micromamba install -y -n base -c conda-forge -c bioconda \
        python=3.12 \
        bioconda::htslib=1.24 \
        'pyarrow>=18.0' \
        'polars>=1.37.1' \
        'tqdm>=4.60' \
        pip \
        procps-ng \
    && micromamba clean -afy

COPY wheels/ /wheels/
RUN micromamba run -n base pip install --no-cache-dir --no-deps "${VEPYR_SPEC}" \
    && micromamba run -n base pip check \
    && micromamba run -n base vepyr --version

# Nextflow runs the task script with bash directly, bypassing the micromamba
# entrypoint, so the environment has to be on PATH.
ENV PATH=/opt/conda/bin:$PATH
