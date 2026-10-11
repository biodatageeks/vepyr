# /parity-wgs runner kit

Everything the self-hosted runner (`gh-runner-bdg-1`, org runner id 4683; 16 vCPU, 64 GB) needs to run the
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
2. Add the runner label `vepyr-wgs` (org settings, Actions, Runners, `gh-runner-bdg-1`).
3. Restrict the runner group to repository `biodatageeks/vepyr` and workflow
   `biodatageeks/vepyr/.github/workflows/parity-wgs.yml@refs/heads/master`
   where the plan allows it. **Currently not done** (see "Accepted residual
   risk"): the runner is in the shared default group `bdg-runners`.
4. Create `/data/vepyr` and `/scratch/vepyr-wgs`, owned by the runner user.
5. **Mandatory: egress firewall.** Job containers run PR code with
   `--network bridge` (dependencies come from PyPI). They must reach only the
   public internet, never the host, the LAN or cloud metadata. From a checkout
   of `master`:

   ```bash
   # Containers must not use a LAN resolver once RFC1918 is dropped. Merge the
   # keys into any existing daemon.json (needs jq); do not overwrite it.
   sudo test -f /etc/docker/daemon.json || echo '{}' | sudo tee /etc/docker/daemon.json >/dev/null
   sudo jq '. + {"dns": ["1.1.1.1", "8.8.8.8"], "ipv6": false}' /etc/docker/daemon.json \
     | sudo tee /etc/docker/daemon.json.new >/dev/null
   sudo mv /etc/docker/daemon.json.new /etc/docker/daemon.json
   sudo systemctl restart docker
   sudo install -m 0755 ci/wgs-runner/egress-firewall.sh /usr/local/sbin/vepyr-wgs-egress
   sudo tee /etc/systemd/system/vepyr-wgs-egress.service >/dev/null <<'UNIT'
   [Unit]
   Description=Egress firewall for /parity-wgs job containers
   After=docker.service
   Requires=docker.service
   PartOf=docker.service

   [Service]
   Type=oneshot
   ExecStart=/usr/local/sbin/vepyr-wgs-egress
   RemainAfterExit=yes

   [Install]
   WantedBy=docker.service
   UNIT
   sudo systemctl daemon-reload
   sudo systemctl enable --now vepyr-wgs-egress.service
   sudo iptables -S DOCKER-USER; sudo iptables -S INPUT; sudo ip6tables -S INPUT   # expect the docker0 DROPs
   ```

   The script inserts, for traffic entering from `docker0`, a `DROP` in
   `DOCKER-USER` for 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16,
   169.254.0.0/16 and 100.64.0.0/10, and an `INPUT` `DROP` that blocks the
   docker0 host address (and every other host address), with the same `INPUT`
   rule in `ip6tables`. That `INPUT` drop also cuts every other container on
   this host's default bridge off from host services (a host DNS stub, a
   local proxy or registry); run such containers on a user-defined network
   or not on this host. The script is idempotent and
   the unit re-applies it whenever Docker starts, so the rules survive reboots
   and Docker restarts (a systemd unit rather than `netfilter-persistent`,
   whose boot-time restore would also replay stale Docker rules).
   `check.sh` fails if any rule is missing.
6. In a checkout of `master`, run `ci/wgs-runner/build-image.sh`.
7. Run `ci/wgs-runner/stage-data.sh --goldens-from <user@host:~/workspace/data_vepyr/output/116>`.
   Needs `uvx`, `curl`, `rsync`, `python3` (3.11+) on the host.
8. Run `ci/wgs-runner/check.sh` (as root or with passwordless sudo, so it can
   read the firewall rules; otherwise it warns and you check them by hand).

## Re-staging when the manifest changes

`wgs-manifest.json` pins the goldens, inputs and cache revisions. When it
changes (a new cache revision or regenerated goldens), re-run step 7. The
script is idempotent: existing verified downloads are reused, and the final
`record-staged` rewrites `STAGED.json`. Until then every job exits 3 (stale
data) rather than comparing against the wrong files. `check.sh` shows the
same drift without running a job.

## Serialisation

The workflow has no concurrency group: jobs are serialised only because a
single runner carries the `vepyr-wgs` label (GitHub queues jobs for it FIFO,
for up to 24 hours) and the profile matrix uses `max-parallel: 1`. Adding a
second `vepyr-wgs` runner removes that serialisation: two runs would then
share `/data/vepyr`, the disk and the memory budget at once.

## Runtime

Measured 2026-10-11 on `gh-runner-bdg-1` (16 vCPU, 64 GB): `merged` 29 min,
`ensembl` 23 min, `refseq` 21 min per job, after a ~15 min wheel build on
`ubuntu-latest`. Annotation is ~15 s per contig; the rest is the
single-threaded reference slicing and md5 verification. Each profile job has
a 60-minute timeout (2x the slowest).

## Statuses

`parity/wgs` is the verdict to rely on: it is confirmed against the profile
jobs' result and is `error` (not `failure`) when any profile hit stale data, a
crash or a failed wheel build. `parity/wgs/<profile>` are informational and
taken from the profile's own result file; do not make them required checks.
A cancelled run posts nothing, so `parity/wgs` stays `pending` (never green).

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

- the staged data is mounted read-only and the job container is throw-away
  (no capabilities, read-only root, non-root user, pid and memory limits);
- the host never follows a path the PR controls: after checkout it does not
  touch the workspace, and it reads the container's scratch only after
  force-removing the (named, labelled) job container and deleting every symlink and special file in it, copying size-capped regular
  files to a host-made directory from which all artifacts are uploaded;
- the egress firewall (setup step 5) keeps job containers off the host and
  the LAN;
- no secrets are available to the job, and its GitHub token is read-only;
- privileged jobs (anything with secrets or write access) never run on this
  machine;
- `_work` and scratch are cleaned before and after each job;
- a fork PR must pin the reviewed head (`/parity-wgs <sha>`), so a push after
  the admin's comment cannot run; any pinned SHA must equal the head.

The main defence is the admin's review of the PR before commenting
`/parity-wgs`. The upgrade path, if that is not enough, is ephemeral runners.

**Shared runner group (accepted 2026-10-11).** `gh-runner-bdg-1` is in the
org's default runner group `bdg-runners`, which is not restricted to a
workflow and serves eight repositories (vepyr, polars-bio, the sequila repos,
openstack-bdg-runners, biodatageeks.github.io). The organization is on the
Free plan. Any workflow in those repositories that targets this runner's
labels runs **on the host**, outside the container, as the runner user
(docker group, so root-equivalent): an outside contributor's PR that is
auto-approved (vepyr's fork policy only gates first-time contributors), or
anyone with write access to one of those repositories, could alter
`/data/vepyr`, the image or the firewall, or leave files for later jobs. The
md5 verdicts are only as trustworthy as that host. To close it: a dedicated
VM registered as a vepyr repository runner, with fork-PR approval required for
all outside collaborators, or a workflow-restricted runner group (GitHub
Team).

Two further rules for the host:

- Docker-group membership is root-equivalent for anything running as the
  runner user, so treat the runner user as root on this machine.
- Never leave a private SSH key on the runner after the goldens rsync. Use
  agent forwarding, or push the goldens from the maintainer's machine.
