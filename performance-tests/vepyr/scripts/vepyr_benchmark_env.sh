#!/usr/bin/env bash
# Shared configuration for the Vepyr benchmark scripts. Source it, do not run it.
#
# DATA_VEPYR_DIR is the only variable that must be set: inputs, caches and the
# measured output directory all live under it, so a laptop run needs nothing
# else. Setting VEPYR_ARCHIVE_ROOT in addition moves the archived artifacts to a
# second volume and turns on the storage-layout checks, which is how the WGS
# numbers are measured on the benchmark server (annotate on SSD, archive on HDD).

data_vepyr_dir=${DATA_VEPYR_DIR:?DATA_VEPYR_DIR must point at the benchmark data directory}
release=${RELEASE:-116}
benchmark_scripts_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$benchmark_scripts_dir/../../.." && pwd)

# The sweep's worker counts have a single declaration shared with the Python
# runner, collector and plotter.
benchmark_workers=(
  $(grep -v '^[[:space:]]*#' "$benchmark_scripts_dir/benchmark_workers.txt")
)

if [[ ! -d "$data_vepyr_dir" ]]; then
  printf 'DATA_VEPYR_DIR %s does not exist\n' "$data_vepyr_dir" >&2
  exit 1
fi

# Prefer the project environment over whatever venv happens to be active, so a
# benchmark always measures the build in this checkout unless told otherwise.
if [[ -n "${VEPYR_PYTHON:-}" ]]; then
  vepyr_python=$VEPYR_PYTHON
elif [[ -x "$repo_root/.venv/bin/python3" ]]; then
  vepyr_python="$repo_root/.venv/bin/python3"
else
  vepyr_python=python3
fi

if ! vepyr_version=$(
  "$vepyr_python" -c 'import importlib.metadata; print(importlib.metadata.version("vepyr"))' \
    2>/dev/null
); then
  printf 'vepyr is not installed for %s; run "uv sync" in %s or set VEPYR_PYTHON\n' \
    "$vepyr_python" "$repo_root" >&2
  exit 1
fi

export VEPYR_PYTHON="$vepyr_python"
printf 'using vepyr %s from %s\n' "$vepyr_version" "$vepyr_python" >&2

if [[ -n "${VEPYR_ARCHIVE_ROOT:-}" ]]; then
  archive_root=$VEPYR_ARCHIVE_ROOT
  require_separate_fs=--require-separate-filesystems
  if [[ ! -d "$archive_root" ]]; then
    printf 'VEPYR_ARCHIVE_ROOT %s does not exist; mount the archive volume first\n' \
      "$archive_root" >&2
    exit 1
  fi
else
  archive_root="$data_vepyr_dir/archive"
  require_separate_fs=
fi

# Artifacts of a previous attempt block a re-run, so offer the overwrite here.
if [[ -n "${VEPYR_FORCE:-}" ]]; then
  force_flag=--force
else
  force_flag=
fi

# Plain output keeps runs comparable with the published numbers; bgzf trades
# that away for roughly a 17x smaller output.
vepyr_compression=${VEPYR_COMPRESSION:-plain}

# The remaining arguments of the sourcing wrapper are worker counts. Consume the
# leading flags first so that "--force" is not read as a worker count, and
# reject anything else rather than passing it on as a number.
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      force_flag=--force
      shift
      ;;
    --require-separate-filesystems)
      require_separate_fs=--require-separate-filesystems
      shift
      ;;
    --compression)
      if [[ $# -lt 2 ]]; then
        printf -- '--compression needs a value (plain, bgzf or gzip)\n' >&2
        exit 2
      fi
      vepyr_compression=$2
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -*)
      printf 'unknown option: %s\n' "$1" >&2
      printf 'usage: %s [--force] [--require-separate-filesystems] %s\n' \
        "$(basename "$0")" '[--compression plain|bgzf|gzip] [workers ...]' >&2
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

mkdir -p "$data_vepyr_dir/output/$release" "$archive_root/$release"
