#!/usr/bin/env bash
# Run the vepyr/annotate nf-tests natively on linux/arm64 (Apple Silicon).
#
# Usage: ./dev/nf-test-arm64.sh [extra nf-test args, e.g. --update-snapshot]
#
# Stopgap until bioconda ships linux-aarch64 vepyr (bioconda-recipes#69191) and
# the module's Seqera Wave URIs exist. It
#   1. fetches the UNTAR module the test's setup block needs (pinned),
#   2. stages the fixture locally, since it is not on nf-core/test-datasets yet,
#   3. builds dev/Dockerfile as linux/arm64,
#   4. runs nf-test with dev/nf-test.config.
#
# Env: NF_TEST (nf-test binary, default `nf-test`), VEPYR_DEV_IMAGE (image tag),
# VEPYR_SPEC (pip spec baked into the image, e.g. /wheels/<file>.whl).
#
# Any main.nf.test.snap this writes comes from the dev image, not the final
# containers -- do not commit it as the submission snapshot.
set -euo pipefail

module_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${module_root}"

nf_test="${NF_TEST:-nf-test}"
if ! command -v "${nf_test}" >/dev/null 2>&1; then
    echo "nf-test not found (${nf_test}); see 'nf-test setup' in nf-core-module/README.md" >&2
    exit 1
fi
image="${VEPYR_DEV_IMAGE:-vepyr-dev:0.7.0-arm64}"
untar_ref="6d46786420b4d7bc88eba026eb389c0c5535d120"

untar_dir="modules/nf-core/untar"
if [[ ! -f "${untar_dir}/main.nf" ]]; then
    mkdir -p "${untar_dir}"
    for f in main.nf meta.yml environment.yml; do
        curl -fsSL -o "${untar_dir}/${f}" \
            "https://raw.githubusercontent.com/nf-core/modules/${untar_ref}/modules/nf-core/untar/${f}"
    done
fi

testdata="${module_root}/.testdata"
if [[ ! -f "${testdata}/data/genomics/homo_sapiens/vepyr/cache.tar.gz" ]]; then
    ./stage-testdata.sh "${testdata}"
fi

# The root .gitignore ignores wheels/, so the COPY source is created here.
mkdir -p dev/wheels
docker build --platform linux/arm64 \
    ${VEPYR_SPEC:+--build-arg "VEPYR_SPEC=${VEPYR_SPEC}"} \
    -t "${image}" dev

# dev/tests/hg002_chr22.nf.test reads the offline VEP parity fixture in place.
VEPYR_NF_TESTDATA="${testdata}/data/" VEPYR_DEV_IMAGE="${image}" \
VEPYR_HG002_CHR22="${module_root}/../tests/data/hg002_chr22" \
    "${nf_test}" test \
    modules/nf-core/vepyr/annotate/tests/main.nf.test \
    dev/tests/hg002_chr22.nf.test \
    --config dev/nf-test.config "$@"
