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
untar_files=(main.nf meta.yml environment.yml)
untar_complete() {
    local f
    for f in "${untar_files[@]}"; do
        [[ -s "${untar_dir}/${f}" ]] || return 1
    done
}
# Fetch into a temporary directory and move it into place only when every file
# arrived: an interrupted curl can leave a partial file behind, and a module
# directory missing a file would otherwise be reused on every later run.
if ! untar_complete; then
    untar_tmp="$(mktemp -d "${module_root}/.untar.XXXXXX")"
    trap 'rm -rf "${untar_tmp}"' EXIT
    for f in "${untar_files[@]}"; do
        curl -fsSL --retry 3 -o "${untar_tmp}/${f}" \
            "https://raw.githubusercontent.com/nf-core/modules/${untar_ref}/modules/nf-core/untar/${f}"
    done
    chmod 755 "${untar_tmp}" # mktemp -d creates it 0700
    rm -rf "${untar_dir}"
    mkdir -p "$(dirname "${untar_dir}")"
    mv "${untar_tmp}" "${untar_dir}"
    trap - EXIT
fi

testdata="${module_root}/.testdata"
# Restage whenever anything the staged data is built from changes: the staging
# scripts and the chr22 fixture they cut it from. The hash of those inputs is
# stored beside .testdata/data; a missing or different hash restages, so a
# local run never tests data built by an older stage_testdata.py or fixture.
fixture="${module_root}/../tests/data/hg002_chr22"
# nullglob: an empty cache entity directory contributes nothing instead of a
# literal pattern that cat cannot open.
shopt -s nullglob
stage_inputs=(
    stage-testdata.sh
    stage_testdata.py
    "${fixture}/prepare.py"
    "${fixture}"/input_chr22.vcf.gz*
    "${fixture}"/chr22.fa.gz*
    "${fixture}"/cache/*/*
)
shopt -u nullglob
if command -v sha256sum >/dev/null 2>&1; then
    stage_hash="$(cat "${stage_inputs[@]}" | sha256sum | cut -d' ' -f1)"
else
    stage_hash="$(cat "${stage_inputs[@]}" | shasum -a 256 | cut -d' ' -f1)"
fi
stage_stamp="${testdata}/stage-inputs.sha256"
if [[ "$(cat "${stage_stamp}" 2>/dev/null)" != "${stage_hash}" ]]; then
    ./stage-testdata.sh "${testdata}"
    echo "${stage_hash}" > "${stage_stamp}"
fi

# dev/tests/hg002_chr22.nf.test reads the offline VEP parity fixture in place.
VEPYR_NF_TESTDATA="${testdata}/data/" \
VEPYR_HG002_CHR22="${module_root}/../tests/data/hg002_chr22" \
    "${nf_test}" test \
    modules/nf-core/vepyr/annotate/tests/main.nf.test \
    dev/tests/hg002_chr22.nf.test \
    --config dev/nf-test.config "$@"
