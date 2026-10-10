# /parity-wgs runner kit

Everything the self-hosted runner (org runner id 4682) needs to run the
whole-genome VEP 116 parity check (`/parity-wgs`). The job runs in a locally
built image with the staged data mounted read-only; `run-profile.sh` is the
in-container entrypoint (`run-profile.sh <profile> <wheel-path>`, repo at
`/src`, data at `/data`, scratch at `/work`).

Exit codes of `run-profile.sh`: 0 parity match, 1 md5 mismatch, 3 staged data
differs from `e2e-testing/golden/wgs-manifest.json`, anything else is an error.
It writes `/work/result.json` and `/work/summary.txt`.

## One-time setup

1. Install Docker, add the runner user to the `docker` group and restart the
   runner service.
2. Add the runner label `vepyr-wgs` (org settings, Actions, Runners, 4682).
3. Restrict the runner group to repository `biodatageeks/vepyr` and workflow
   `.github/workflows/parity-wgs.yml@refs/heads/master`.
4. Create `/data/vepyr` and `/scratch/vepyr-wgs`, owned by the runner user.
5. In a checkout of `master`, run `ci/wgs-runner/build-image.sh`.
6. Run `ci/wgs-runner/stage-data.sh --goldens-from <user@host:~/workspace/data_vepyr/output/116>`.
   Needs `uvx`, `curl`, `rsync`, `python3` (3.11+) on the host.
7. Run `ci/wgs-runner/check.sh`.

## Re-staging when the manifest changes

`wgs-manifest.json` pins the goldens, inputs and cache revisions. When it
changes (a new cache revision or regenerated goldens), re-run step 6. The
script is idempotent: existing verified downloads are reused, and the final
`record-staged` rewrites `STAGED.json`. Until then every job exits 3 (stale
data) rather than comparing against the wrong files. `check.sh` shows the
same drift without running a job.

## Rebuilding the image

When `Dockerfile` changes, bump `IMAGE` (for example `vepyr-wgs:2`) and the
workflow's `WGS_IMAGE` in the same commit, merge, then run `build-image.sh`
from a checkout of `master` on the runner. Jobs never pull the image; a tag
that was not built locally fails fast.

## Accepted residual risk

The runner executes a PR's wheel on a persistent host. The label, the runner
group restriction to this repository and the workflow ref pin, the read-only
data mount, the throw-away container and the absence of secrets in it bound
the exposure, but they do not eliminate it: a hostile wheel can still burn the
runner's CPU and disk for the length of the job. This is accepted for a
maintainer-triggered command (see the design spec, section 5).
