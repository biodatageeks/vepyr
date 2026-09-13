#!/usr/bin/env bash
# Run the vepyr/annotate nf-tests natively with the module's own Wave containers,
# on Linux (x86_64 or aarch64) or Apple Silicon.
#
# Usage: ./dev/nf-test-local.sh [extra nf-test args, e.g. --update-snapshot]
#
# main.nf names only the linux/amd64 image; meta.yml lists both. The runner
# picks the one matching the host from meta.yml, so an arm64 host never runs the
# amd64 image under emulation (it dies with SIGILL: the emulated guest has no AVX).
# It also
#   1. fetches the UNTAR module the golden test's setup block needs (pinned),
#   2. stages the golden fixture, since it is not on nf-core/test-datasets yet,
#   3. runs both test files with dev/nf-test.config.
#
# Env: NF_TEST (nf-test binary, default `nf-test` on PATH),
# VEPYR_DOCKER_PLATFORM (default: linux/arm64 or linux/amd64 from `uname -m`),
# VEPYR_CONTAINER (image to test instead of the meta.yml one).
set -euo pipefail

module_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${module_root}"

nf_test="${NF_TEST:-nf-test}"
if ! command -v "${nf_test}" >/dev/null 2>&1; then
    echo "nf-test not found (${nf_test}); see 'Shared setup' in nf-core-module/README.md" >&2
    exit 1
fi

if [[ -z "${VEPYR_DOCKER_PLATFORM:-}" ]]; then
    case "$(uname -m)" in
        arm64 | aarch64) VEPYR_DOCKER_PLATFORM=linux/arm64 ;;
        x86_64 | amd64) VEPYR_DOCKER_PLATFORM=linux/amd64 ;;
        *)
            echo "unsupported host $(uname -m); set VEPYR_DOCKER_PLATFORM" >&2
            exit 1
            ;;
    esac
fi
export VEPYR_DOCKER_PLATFORM

# containers.docker.<platform>.name from meta.yml, as `nf-core modules
# containers create` writes it.
meta_docker_image() {
    awk -v platform="    $1:" '
        /^containers:/ { in_containers = 1; next }
        in_containers && /^[^ ]/ { exit }
        in_containers && /^  [a-z]/ { engine = $1; in_platform = 0; next }
        in_containers && engine == "docker:" && $0 == platform { in_platform = 1; next }
        in_platform && $1 == "name:" { print $2; exit }
    ' modules/nf-core/vepyr/annotate/meta.yml
}

if [[ -z "${VEPYR_CONTAINER:-}" ]]; then
    VEPYR_CONTAINER="$(meta_docker_image "${VEPYR_DOCKER_PLATFORM}")"
    if [[ -z "${VEPYR_CONTAINER}" ]]; then
        echo "no docker ${VEPYR_DOCKER_PLATFORM} image in meta.yml; run" \
            "'nf-core modules containers create vepyr/annotate'" >&2
        exit 1
    fi
fi
export VEPYR_CONTAINER
echo "platform ${VEPYR_DOCKER_PLATFORM}, container ${VEPYR_CONTAINER}"

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
# reference.fa.gz marks the current (release-116, chr22) test data; an older
# .testdata without it is restaged.
if [[ ! -f "${testdata}/data/genomics/homo_sapiens/vepyr/reference.fa.gz" ]]; then
    ./stage-testdata.sh "${testdata}"
fi

# dev/tests/hg002_chr22.nf.test reads the offline VEP parity fixture in place.
VEPYR_NF_TESTDATA="${testdata}/data/" \
VEPYR_HG002_CHR22="${module_root}/../tests/data/hg002_chr22" \
    "${nf_test}" test \
    modules/nf-core/vepyr/annotate/tests/main.nf.test \
    dev/tests/hg002_chr22.nf.test \
    --config dev/nf-test.config "$@"
