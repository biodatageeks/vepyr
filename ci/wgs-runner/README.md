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
   `biodatageeks/vepyr/.github/workflows/parity-wgs.yml@refs/heads/master`.
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

The runner agent and its `_work` directory persist between jobs. A hostile
fork PR, after an admin has commented `/parity-wgs` on it, could run code that
leaves files behind for a later job to read (design spec, section 5).
Mitigations:

- the staged data is mounted read-only and the job container is throw-away;
- no secrets are available to the job, and its GitHub token is read-only;
- privileged jobs (anything with secrets or write access) never run on this
  machine;
- `_work` and scratch are cleaned before and after each job;
- the runner group is restricted to this repository and to the
  `parity-wgs.yml` workflow on `master`.

The main defence is the admin's review of the PR before commenting
`/parity-wgs`. The upgrade path, if that is not enough, is ephemeral runners.

Two further rules for the host:

- Docker-group membership is root-equivalent for anything running as the
  runner user, so treat the runner user as root on this machine.
- Never leave a private SSH key on the runner after the goldens rsync. Use
  agent forwarding, or push the goldens from the maintainer's machine.
